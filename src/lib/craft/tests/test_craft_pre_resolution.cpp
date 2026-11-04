/*********************************************************************************
 * Modifications Copyright 2026 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/

// Unit tests for CraftReplDev::pre_resolve_slots (S5 leader pre-resolution / SDSTOR-22907), and for
// append() (the SyncRSCommitLSN proposer built on it, SDSTOR-22908) under CraftAppendTest at the end.
//
// Verifies:
//   - candidates are the union of missing_lsns_ <= upto AND the range past last_append_lsn up to upto (a
//     client Resolve or a login rs_commit_lsn can name an upto this leader never itself appended)
//   - no candidates -> {} without ever calling the quorum fetcher
//   - an unwired peer_fetcher_ fails closed (error), never a silent partial result
//   - a quorum member reporting is_empty=true wins that slot even if another member still has data for it,
//     regardless of which member's response is aggregated first
//   - a slot no responding member reports data or Empty for is quorum-lacks-evidence, minted fresh as Empty
//   - a slot some member has data for is written into the local journal and erased from missing_lsns_
//   - an outright fetch_from_quorum failure propagates the error with zero local mutation
//   - a member's response naming an unrequested/duplicate LSN discards only that member's reply
//   - a local write_slot failure leaves the slot unresolved (out of both the return value and missing_lsns_)
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles craft_repl_dev.cpp
// directly (same pattern as test_craft_raft_entries.cpp).

#include <gtest/gtest.h>
#include <array>
#include <functional>
#include <map>
#include <optional>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "craft/craft_repl_dev.hpp"
#include "home_blks_config.hpp"
#include "coro_helpers.hpp"
#include "mock_journal_backend.hpp"

SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)
SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)

namespace homeblocks {
namespace {

static constexpr uint32_t k_page_size = 4096;

// ── journal mock ──────────────────────────────────────────────────────────────
//
// Backed by a std::map so tests can inspect exactly which LSNs got persisted. fail_on_write optionally
// injects an I/O error for a specific LSN. alloc_write_data is a dumb stub (default multi_blk_id) --
// pre_resolve_slots's tests never inspect the allocated blkid's value, same posture as
// test_craft_raft_entries.cpp's own mock.

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots;
    std::optional< int64_t > fail_on_write;
    bool fail_alloc{false};
    int free_data_calls{0};
    std::function< void(int64_t) > on_write; // runs inside write_slot, to change the term mid-await
    std::function< void() > on_alloc;        // runs inside alloc_write_data, same purpose

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const&, lba_count_t) override {
        if (on_alloc) on_alloc();
        if (fail_alloc) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return homestore::multi_blk_id{};
    }

    async_status write_slot(int64_t lsn, uint64_t /* term */, lba_t lba, lba_count_t len, homestore::multi_blk_id blkid,
                            bool all_zeros, std::vector< homestore::csum_t > const& csums) override {
        if (on_write) on_write(lsn);
        if (fail_on_write && *fail_on_write == lsn)
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        slots[lsn] = JournalSlot{
            .lsn = lsn, .all_zeros = all_zeros, .lba_off_bytes = lba, .len_bytes = len, .blkid = blkid, .csums = csums};
        co_return ok();
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override { return mock_read_slot(*this, lsn); }
    async_status truncate_to(int64_t) override { co_return ok(); }
    async_status free_data(homestore::multi_blk_id) override {
        ++free_data_calls;
        co_return ok();
    }
    async_status free_slot(int64_t lsn) override { return mock_free_slot(*this, lsn); }
    async_status read_data(homestore::multi_blk_id, sisl::sg_list&) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    bool has_slot(int64_t lsn) const { return slots.count(lsn) > 0; }
};

// ── peer fetcher mock ─────────────────────────────────────────────────────────
//
// Records the LSN list fetch_from_quorum was last called with; returns a programmable response (one
// QuorumSlotResponse per simulated responding member) or an injected error. fetch_data isn't exercised
// by this file (see test_craft_raft_entries.cpp) -- CraftPeerFetcher bundles both because
// pre_resolve_slots's quorum broadcast and apply_sync_rs_commit_lsn's single-peer catch-up are wired
// through the same production channel (CraftConnector, S9).

class MockCraftQuorumFetcher : public CraftPeerFetcher {
public:
    std::vector< int64_t > last_requested;
    uint32_t last_timeout_ms{0};
    int call_count{0};
    std::vector< QuorumSlotResponse > response;
    bool should_fail{false};
    std::function< void() > on_fetch; // runs inside the fetch, to change leader/term mid-await

    async_result< craft::lsn_pair > get_rs_commit_lsn(uint64_t /* term */, bool /* is_login */) override {
        co_return craft::lsn_pair{};
    }

    async_result< std::vector< JournalSlot > > fetch_data(const std::vector< int64_t >&, uint32_t) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    async_result< std::vector< QuorumSlotResponse > > fetch_from_quorum(std::vector< int64_t > lsns,
                                                                        uint32_t timeout_ms) override {
        ++call_count;
        last_requested = lsns;
        last_timeout_ms = timeout_ms;
        if (on_fetch) on_fetch();
        if (should_fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return response;
    }

    async_result< std::vector< craft::lsn_pair > > poll_quorum_lsns(uint64_t, bool, uint32_t) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }
};

} // namespace

// craft_repl_dev.hpp friends this exact type (homeblocks::CraftPreResolutionTest) so it can call the
// private pre_resolve_slots directly -- it must NOT sit in the anonymous namespace above, or it would be
// a distinct, unrelated type from the friend's perspective.

// ── test fixture ─────────────────────────────────────────────────────────────

class CraftPreResolutionTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto mock = std::make_unique< MockCraftJournalBackend >();
        journal_ = mock.get();
        dev_ = CraftReplDev::create(volume_id_t{}, std::move(mock), k_page_size, nullptr);
        dev_->set_raft_proposer(&proposer_);
    }

    // pre_resolve_slots is private and only this fixture is a friend (TEST_F bodies are subclasses, which
    // don't inherit friendship), so tests reach it through these helpers.
    auto do_pre_resolve(int64_t upto, uint64_t term) {
        return homeblocks::detail::sync_get(dev_->pre_resolve_slots(upto, term));
    }
    // Runs within the device's current session, as a well-behaved caller does.
    auto do_pre_resolve(int64_t upto) { return do_pre_resolve(upto, dev_->term()); }

    MockCraftJournalBackend* journal_{nullptr};
    MockCraftQuorumFetcher fetcher_;
    MockCraftRaftProposer proposer_;
    std::shared_ptr< CraftReplDev > dev_;
};

namespace {

// ── no candidates ─────────────────────────────────────────────────────────────

TEST_F(CraftPreResolutionTest, NoUnresolvedSlotsReturnsEmptyWithoutCallingFetcher) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {});

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_EQ(fetcher_.call_count, 0);
}

// ── unwired fetcher fails closed ──────────────────────────────────────────────

TEST_F(CraftPreResolutionTest, NullQuorumFetcherReturnsError) {
    dev_->seed_lsns(0, {}); // peer_fetcher_ never set

    auto r = do_pre_resolve(/*upto=*/3);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::not_supported));
    EXPECT_EQ(dev_->missing_count(), 0u);
}

// ── candidate computation ─────────────────────────────────────────────────────

// A missing_lsns_ entry above upto must never be requested or touched.
TEST_F(CraftPreResolutionTest, KnownGapCandidatesStrictlyFilteredToUpto) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(10, {3, 7});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .is_empty = true}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(fetcher_.last_requested, (std::vector< int64_t >{3}));
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
    EXPECT_TRUE(dev_->is_missing(7)); // above upto -- untouched
}

// upto can exceed this leader's own last_append_lsn (a client Resolve naming a dLSN this leader never
// received, or a login rs_commit_lsn that is the quorum's max) -- those slots were never opened as a gap
// in missing_lsns_, but must still be requested.
TEST_F(CraftPreResolutionTest, CandidatesExtendPastOwnAppendFrontierUpToUpto) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(3, {});
    fetcher_.response = {QuorumSlotResponse{.slots = {
                                                JournalSlot{.lsn = 4, .is_empty = true},
                                                JournalSlot{.lsn = 5, .is_empty = true},
                                                JournalSlot{.lsn = 6, .is_empty = true},
                                            }}};

    auto r = do_pre_resolve(/*upto=*/6);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(fetcher_.last_requested, (std::vector< int64_t >{4, 5, 6}));
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{4, 5, 6}));
    EXPECT_EQ(dev_->last_append_lsn(), 3); // unchanged -- apply_sync_rs_commit_lsn's job, not this one's
}

// set_peer_fetch_timeout_ms() threads the configured deadline through to fetch_from_quorum verbatim.
TEST_F(CraftPreResolutionTest, PassesConfiguredTimeoutToQuorumFetcher) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->set_peer_fetch_timeout_ms(1234);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .is_empty = true}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(fetcher_.last_timeout_ms, 1234u);
}

// Guard shape mirrors write()'s own k_max_ooo_gap check (test_craft_write.cpp's GapCapFenceposts):
// upto too far ahead of last_append_lsn is rejected (value_too_large) before ever calling the
// fetcher, and upto near INT64_MAX trips the overflow-safe guard (invalid_argument) first.
TEST_F(CraftPreResolutionTest, UptoGapCapFenceposts) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(0, {});

    auto r1 = do_pre_resolve(/*upto=*/2'000'000);
    ASSERT_FALSE(r1.has_value());
    EXPECT_EQ(r1.error(), std::make_error_condition(std::errc::value_too_large));
    EXPECT_EQ(fetcher_.call_count, 0);

    auto r2 = do_pre_resolve(/*upto=*/INT64_MAX);
    ASSERT_FALSE(r2.has_value());
    EXPECT_EQ(r2.error(), std::make_error_condition(std::errc::invalid_argument));
    EXPECT_EQ(fetcher_.call_count, 0);
}

// ── outright fetch failure ────────────────────────────────────────────────────

TEST_F(CraftPreResolutionTest, FetchFromQuorumFailsPropagatesErrorNoLocalMutation) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(0, {});
    fetcher_.should_fail = true;

    auto r = do_pre_resolve(/*upto=*/2);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::io_error));
    EXPECT_EQ(dev_->missing_count(), 0u);
    EXPECT_FALSE(journal_->has_slot(1));
}

// fetch_from_quorum can succeed with a genuinely empty response (CraftPeerFetcher's own contract: "a
// non-responding member is simply absent from the result") -- zero responding members must fail closed,
// not be treated the same as "some members responded, none had evidence" (which mints Empty below).
TEST_F(CraftPreResolutionTest, ZeroRespondingMembersReturnsErrorNoLocalMutation) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {}; // succeeds, but literally zero members responded

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::not_supported));
    EXPECT_TRUE(dev_->is_missing(3)); // untouched
    EXPECT_FALSE(journal_->has_slot(3));
}

// A non-empty response where every single member's reply is malformed is just as untrustworthy as
// zero responding members -- must also fail closed, not fall through to mint Empty verdicts for
// candidates nobody actually vouched for.
TEST_F(CraftPreResolutionTest, AllMembersMalformedReturnsErrorNoLocalMutation) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 99, .is_empty = true}}}, // names an unrequested lsn
    };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::not_supported));
    EXPECT_TRUE(dev_->is_missing(3)); // untouched
    EXPECT_FALSE(journal_->has_slot(3));
}

// ── data resolution ────────────────────────────────────────────────────────────

TEST_F(CraftPreResolutionTest, SinglePresentDataSlotWrittenAndErasedFromMissing) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 10, .len_bytes = 4}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(dev_->is_missing(3));
}

TEST_F(CraftPreResolutionTest, SinglePresentZeroWriteSlotWrittenAndErasedFromMissing) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .all_zeros = true}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(dev_->is_missing(3));
}

// ── Empty verdicts ─────────────────────────────────────────────────────────────

TEST_F(CraftPreResolutionTest, SlotInheritedEmptyFromOneRespondingPeer) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .is_empty = true}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
    // Empty verdict stays leader-preliminary until this leader's own proposal commits -- local state is
    // apply_sync_rs_commit_lsn's job, not this one's.
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_FALSE(dev_->is_empty_slot(3));
    EXPECT_FALSE(journal_->has_slot(3));
}

TEST_F(CraftPreResolutionTest, QuorumLacksEvidenceMintsNewEmptyVerdict) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {}}}; // one responding member, nothing for lsn 3

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_FALSE(journal_->has_slot(3));
}

TEST_F(CraftPreResolutionTest, DataFromOnlyOnePeerAmongMultipleStillUsed) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {
        QuorumSlotResponse{.slots = {}}, // doesn't have it
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}},
    };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(dev_->is_missing(3));
}

// The ticket's core motivating scenario: a lagging member still holds stale data for a slot the quorum
// already declared Empty -- Empty must win, not the data. Order matters here: data is aggregated FIRST,
// Empty SECOND, so this exercises the purge-on-Empty path (see EmptyBeatsDataAcrossPeersReverseOrder
// below for the opposite order, exercising the insert-guard path instead).
TEST_F(CraftPreResolutionTest, EmptyBeatsDataAcrossPeers) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}}, // lagging, stale data
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .is_empty = true}}},
    };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
}

// Same scenario, opposite member order: Empty arrives FIRST, stale data SECOND. Empty must still win --
// this is what the insert-guard (not just the purge-on-Empty) protects, since aggregation makes no
// ordering guarantee across quorum members.
TEST_F(CraftPreResolutionTest, EmptyBeatsDataAcrossPeersReverseOrder) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .is_empty = true}}},
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}}, // lagging, stale data
    };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
}

TEST_F(CraftPreResolutionTest, DuplicateEmptyVerdictsAcrossPeersDeduped) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .is_empty = true}}},
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .is_empty = true}}},
    };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
}

// ── mixed batch ────────────────────────────────────────────────────────────────

TEST_F(CraftPreResolutionTest, MixedBatchAggregatesEachCandidateIndependentlyInAscendingOrder) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {}); // candidates {3,4,5} all come from the beyond-frontier loop
    fetcher_.response = {QuorumSlotResponse{.slots = {
                                                JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}, // data
                                                JournalSlot{.lsn = 4, .is_empty = true}, // inherited empty
                                                // lsn 5: omitted entirely -- quorum-lacks-evidence
                                            }}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{4, 5}));
    EXPECT_TRUE(journal_->has_slot(3));
}

// ── per-peer response validation ──────────────────────────────────────────────

TEST_F(CraftPreResolutionTest, PerPeerUnrequestedOrDuplicateLsnDiscardsOnlyThatPeersResponse) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3, 4});
    fetcher_.response = {
        // Member A's reply names lsn 99, never requested -- its ENTIRE reply (including the otherwise-valid
        // data for lsn 3) is discarded.
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4},
                                     JournalSlot{.lsn = 99, .is_empty = true}}},
        // Member B's reply is clean and still counts.
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 4, .is_empty = true}}},
    };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3, 4})); // 3 -> quorum-lacks-evidence, 4 -> inherited empty
    EXPECT_FALSE(journal_->has_slot(3)); // A's data was discarded along with the rest of its reply
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_TRUE(dev_->is_missing(4));
}

// ── local write failure ───────────────────────────────────────────────────────

TEST_F(CraftPreResolutionTest, WriteSlotFailureLeavesLsnMissingNotInEmptySlots) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}}};
    journal_->fail_on_write = 3;

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty()); // not verdicted Empty -- a quorum member DID have data for it
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3}));
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_EQ(journal_->free_data_calls, 1); // the successful alloc_write_data is freed back, not leaked
}

// Distinct from the write_slot-failure path above: when alloc_write_data itself fails,
// blkid_allocated stays false, so the cleanup path must NOT fire free_data (there was never a real
// allocation to free).
TEST_F(CraftPreResolutionTest, AllocWriteDataFailureLeavesLsnMissingNotInEmptySlots) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}}};
    journal_->fail_alloc = true;

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty()); // not verdicted Empty -- a quorum member DID have data for it
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3}));
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_EQ(journal_->free_data_calls, 0); // alloc never succeeded -- nothing to free
}

// ── leadership / term ─────────────────────────────────────────────────────────

TEST_F(CraftPreResolutionTest, NullRaftProposerReturnsError) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->set_raft_proposer(nullptr);
    dev_->seed_lsns(5, {3});

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::not_supported));
    EXPECT_EQ(fetcher_.call_count, 0);
}

TEST_F(CraftPreResolutionTest, FollowerReturnsNotLeaderWithoutFetching) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.leader = false;
    dev_->seed_lsns(5, {3});

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::NOT_LEADER));
    EXPECT_EQ(fetcher_.call_count, 0);
}

TEST_F(CraftPreResolutionTest, LeadershipLostDuringFetchWritesNothing) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}}};
    fetcher_.on_fetch = [this] { proposer_.leader = false; };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::NOT_LEADER));
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
}

TEST_F(CraftPreResolutionTest, TermChangedDuringFetchWritesNothing) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_term(1);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}}};
    fetcher_.on_fetch = [this] { dev_->seed_term(2); };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
}

// Term bumps while slot 3's blocks are being allocated: nothing journalled, the allocation is freed.
TEST_F(CraftPreResolutionTest, TermChangedBeforeSlotWriteFreesBlocksAndStops) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_term(1);
    dev_->seed_lsns(5, {3, 4});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4},
                                                      JournalSlot{.lsn = 4, .lba_off_bytes = 1, .len_bytes = 4}}}};
    journal_->on_alloc = [this] { dev_->seed_term(2); };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_FALSE(journal_->has_slot(4));
    EXPECT_EQ(journal_->free_data_calls, 1);
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_TRUE(dev_->is_missing(4));
}

// Term bumps during slot 3's write_slot: the entry stays journalled (and its blocks referenced), but
// missing_lsns_ is untouched and slot 4 is never written.
TEST_F(CraftPreResolutionTest, TermChangedDuringSlotWriteLeavesMissingAndStops) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_term(1);
    dev_->seed_lsns(5, {3, 4});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4},
                                                      JournalSlot{.lsn = 4, .lba_off_bytes = 1, .len_bytes = 4}}}};
    journal_->on_write = [this](int64_t lsn) {
        if (lsn == 3) dev_->seed_term(2);
    };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(journal_->has_slot(4));
    EXPECT_EQ(journal_->free_data_calls, 0);
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_TRUE(dev_->is_missing(4));
}

// ── membership-aware quorum ───────────────────────────────────────────────────

// N=5: self + 1 responder = 2 < quorum 3 -- not enough evidence to mint Empty.
TEST_F(CraftPreResolutionTest, FiveMembersOneResponderLeavesSlotUnresolved) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.members = 5;
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3}));
    EXPECT_TRUE(dev_->is_missing(3));
}

// N=5: self + 2 responders = quorum 3.
TEST_F(CraftPreResolutionTest, FiveMembersTwoRespondersMintsEmpty) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.members = 5;
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{}, QuorumSlotResponse{}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
    EXPECT_TRUE(r->unresolved.empty());
}

// A malformed reply doesn't count toward quorum: N=5, two responders but one discarded.
TEST_F(CraftPreResolutionTest, MalformedResponderDoesNotCountTowardQuorum) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.members = 5;
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{}, QuorumSlotResponse{.slots = {JournalSlot{.lsn = 99, .is_empty = true}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3}));
}

// An Empty a responder already holds from a prior verdict needs no quorum count.
TEST_F(CraftPreResolutionTest, InheritedEmptyAcceptedBelowQuorum) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.members = 5;
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .is_empty = true}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
    EXPECT_TRUE(r->unresolved.empty());
}

// Data needs only one holder, quorum or not.
TEST_F(CraftPreResolutionTest, DataFromSingleHolderWrittenBelowQuorum) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.members = 5;
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_TRUE(r->unresolved.empty());
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(dev_->is_missing(3));
}

// ── append() (SDSTOR-22908) ────────────────────────────────────────────────────
//
// append() is pre-resolution plus a SyncRSCommitLSN proposal, so it reuses this file's fixture. Proposals
// loop back through the device's own CraftRaftListener::on_commit, so each committed entry is really applied.

class CraftAppendTest : public CraftPreResolutionTest {
protected:
    void SetUp() override {
        CraftPreResolutionTest::SetUp();
        dev_->set_peer_fetcher(&fetcher_);
        fetcher_.response = {QuorumSlotResponse{}}; // one responder holding nothing
        proposer_.on_commit = [this](sisl::blob const& h, sisl::blob const& k) {
            cintrusive< homestore::repl_req_ctx > ctx{};
            dev_->test_listener().on_commit(1, h, k, {}, ctx);
        };
    }

    auto do_append(int64_t sync_to, uint64_t token = 0) {
        return homeblocks::detail::sync_get(dev_->append(sync_to, token, dev_->term()));
    }

    // One-block data write at dlsn (write() CRCs the payload, so it needs a real buffer). commit_lsn is the
    // client's piggybacked resolved prefix (-1 = not sent).
    auto do_write(int64_t dlsn, uint64_t term = 0, int64_t commit_lsn = -1) {
        static std::array< uint8_t, k_page_size > buf{};
        sisl::sg_list data;
        data.size = k_page_size;
        data.iovs.push_back(iovec{buf.data(), buf.size()});
        return homeblocks::detail::sync_get(
            dev_->write(craft::client_hdr{term, commit_lsn, -1}, dlsn, 0, k_page_size, std::move(data)));
    }

    auto do_resolve(int64_t upto, uint64_t term = 0) {
        return homeblocks::detail::sync_get(dev_->request_resolution(craft::client_hdr{term, -1, -1}, upto));
    }
};

// ── preconditions ─────────────────────────────────────────────────────────────

TEST_F(CraftAppendTest, UnwiredProposerReturnsNotSupported) {
    dev_->set_raft_proposer(nullptr);

    auto r = do_append(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::not_supported));
}

TEST_F(CraftAppendTest, FollowerReturnsNotLeaderWithoutFetchingOrProposing) {
    proposer_.leader = false;
    dev_->seed_lsns(5, {3});

    auto r = do_append(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::NOT_LEADER));
    EXPECT_EQ(fetcher_.call_count, 0);
    EXPECT_TRUE(proposer_.proposed.empty());
}

TEST_F(CraftAppendTest, NegativeSyncToRejected) {
    auto r = do_append(-1);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::invalid_argument));
}

// A caller validated term 1, then InternalLogin moved the session to 2: the round must fail rather than
// adopt the new term, and must not claim the single-flight gate on its way out.
TEST_F(CraftAppendTest, CallerTermReplacedByLoginIsRejected) {
    dev_->seed_term(2);
    dev_->seed_lsns(5, {3});

    auto r = homeblocks::detail::sync_get(dev_->append(5, 0, /*term=*/1));

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
    EXPECT_EQ(fetcher_.call_count, 0);
    EXPECT_TRUE(proposer_.proposed.empty());
    EXPECT_TRUE(do_append(5).has_value()); // the gate was never claimed: a current-session round proceeds
}

TEST_F(CraftPreResolutionTest, CallerTermReplacedByLoginIsRejected) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_term(2);
    dev_->seed_lsns(5, {3});

    auto r = do_pre_resolve(5, /*term=*/1);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
    EXPECT_EQ(fetcher_.call_count, 0);
    EXPECT_TRUE(dev_->is_missing(3));
}

// ── proposing ─────────────────────────────────────────────────────────────────

TEST_F(CraftAppendTest, ProposesWatermarkTokenAndVerdictsThenApplies) {
    dev_->seed_lsns(5, {3}); // 3 lacked by self + the one responder: quorum of 3

    auto r = do_append(5, /*token=*/42);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->resolved_upto, 5);
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
    ASSERT_EQ(proposer_.proposed.size(), 1u);
    auto const e = decode(proposer_.proposed[0]);
    EXPECT_EQ(e.type, CraftEntryType::SyncRSCommitLSN);
    EXPECT_EQ(e.rs_commit_lsn, 5);
    EXPECT_EQ(e.client_token, 42u);
    EXPECT_EQ(e.empty_slots, (std::vector< int64_t >{3}));
    EXPECT_TRUE(dev_->is_empty_slot(3));
    EXPECT_EQ(dev_->commit_lsn(), 5);
    EXPECT_EQ(dev_->last_proposed_sync_lsn(), 5);
}

// N=5, one responder: slot 3 can't be verdicted, so the watermark stops just below it.
TEST_F(CraftAppendTest, ClampsBelowFirstUnresolvedSlot) {
    proposer_.members = 5;
    dev_->seed_lsns(6, {3});

    auto r = do_append(6);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->resolved_upto, 2);
    EXPECT_TRUE(r->empty_slots.empty());
    ASSERT_EQ(proposer_.proposed.size(), 1u);
    EXPECT_EQ(decode(proposer_.proposed[0]).rs_commit_lsn, 2);
    EXPECT_EQ(dev_->commit_lsn(), 2);
    EXPECT_TRUE(dev_->is_missing(3));
}

TEST_F(CraftAppendTest, UnresolvedFirstSlotProposesNothing) {
    proposer_.members = 5;
    dev_->seed_lsns(5, {0});

    auto r = do_append(5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->resolved_upto, -1);
    EXPECT_TRUE(proposer_.proposed.empty());
}

TEST_F(CraftAppendTest, AlreadyProposedWatermarkSkipsRaftRound) {
    dev_->seed_lsns(5, {});

    ASSERT_TRUE(do_append(5).has_value());
    auto r = do_append(5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->resolved_upto, 5);
    EXPECT_EQ(proposer_.proposed.size(), 1u);
}

// The reply carries earlier rounds' verdicts too, so a client that lost a reply still learns them.
TEST_F(CraftAppendTest, ReplyIncludesEarlierRoundsVerdicts) {
    dev_->seed_lsns(5, {3});
    ASSERT_TRUE(do_append(5).has_value());

    auto r = do_append(7); // 6 and 7 are beyond the frontier and held by nobody

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->resolved_upto, 7);
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3, 6, 7}));
    ASSERT_EQ(proposer_.proposed.size(), 2u);
    EXPECT_EQ(decode(proposer_.proposed[1]).empty_slots, (std::vector< int64_t >{6, 7}));
}

// ── failures ──────────────────────────────────────────────────────────────────

TEST_F(CraftAppendTest, PreResolutionFailurePropagatesWithoutProposal) {
    dev_->seed_lsns(5, {3});
    fetcher_.should_fail = true;

    auto r = do_append(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::io_error));
    EXPECT_TRUE(proposer_.proposed.empty());
}

// Leadership lost after pre-resolution (here, while it journals a fetched slot) -- caught right before proposing.
TEST_F(CraftAppendTest, LeadershipLostBeforeProposeReturnsNotLeader) {
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 0, .len_bytes = 4096}}}};
    journal_->on_write = [this](int64_t) { proposer_.leader = false; };

    auto r = do_append(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::NOT_LEADER));
    EXPECT_TRUE(proposer_.proposed.empty());
    EXPECT_EQ(dev_->last_proposed_sync_lsn(), -1);
}

// A failed proposal propagates, doesn't advance the watermark, and releases the single-flight gate.
TEST_F(CraftAppendTest, ProposeFailurePropagatesAndReleasesGate) {
    dev_->seed_lsns(5, {});
    proposer_.fail = true;

    auto r = do_append(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::io_error));
    EXPECT_EQ(dev_->last_proposed_sync_lsn(), -1);

    proposer_.fail = false;
    EXPECT_TRUE(do_append(5).has_value());
}

// A second append() while one is in flight (here, issued from inside the first one's commit) is rejected.
TEST_F(CraftAppendTest, ConcurrentAppendRejected) {
    dev_->seed_lsns(5, {});
    std::optional< result< craft::resolution_result > > nested; // set inside on_commit
    auto const loopback = proposer_.on_commit;
    proposer_.on_commit = [&](sisl::blob const& h, sisl::blob const& k) {
        nested = homeblocks::detail::sync_get(dev_->append(5, 0, dev_->term()));
        loopback(h, k);
    };

    auto r = do_append(5);

    ASSERT_TRUE(r.has_value());
    ASSERT_TRUE(nested.has_value());
    ASSERT_FALSE(nested->has_value());
    EXPECT_EQ(nested->error(), std::make_error_condition(std::errc::operation_in_progress));
}

// ── periodic trigger (write()) ────────────────────────────────────────────────

// Each write piggybacks the previous slot as commit_lsn (one write in flight), so every periodic round
// syncs to the client's confirmed prefix, one behind the leader's last_append_lsn.
TEST_F(CraftAppendTest, PeriodicTriggerFiresEveryIntervalOnLeader) {
    dev_->set_sync_lsn_interval(4);

    for (int64_t dlsn = 0; dlsn <= 2; ++dlsn)
        ASSERT_TRUE(do_write(dlsn, 0, /*commit_lsn=*/dlsn - 1).has_value());
    EXPECT_TRUE(proposer_.proposed.empty());

    ASSERT_TRUE(do_write(3, 0, /*commit_lsn=*/2).has_value()); // 4 LSNs appended since the last attempt
    ASSERT_EQ(proposer_.proposed.size(), 1u);
    EXPECT_EQ(decode(proposer_.proposed[0]).rs_commit_lsn, 2); // the client's prefix, not last_append (3)
    EXPECT_EQ(dev_->commit_lsn(), 2);

    for (int64_t dlsn = 4; dlsn <= 7; ++dlsn)
        ASSERT_TRUE(do_write(dlsn, 0, /*commit_lsn=*/dlsn - 1).has_value());
    ASSERT_EQ(proposer_.proposed.size(), 2u);
    EXPECT_EQ(decode(proposer_.proposed[1]).rs_commit_lsn, 6);
}

// The C1 race: slot 3 is still in flight (a gap below the leader's last_append_lsn) when the round runs.
// The client's commit_lsn (2) stops below it, so the round never examines 3 and can't vote it Empty.
TEST_F(CraftAppendTest, PeriodicTriggerNeverReachesPastClientCommit) {
    dev_->set_sync_lsn_interval(4);

    for (int64_t dlsn = 0; dlsn <= 2; ++dlsn)
        ASSERT_TRUE(do_write(dlsn, 0, /*commit_lsn=*/dlsn - 1).has_value());
    ASSERT_TRUE(do_write(4, 0, /*commit_lsn=*/2).has_value()); // 3 still in flight

    ASSERT_EQ(proposer_.proposed.size(), 1u);
    auto const e = decode(proposer_.proposed[0]);
    EXPECT_EQ(e.rs_commit_lsn, 2);
    EXPECT_TRUE(e.empty_slots.empty());
    EXPECT_EQ(fetcher_.call_count, 0); // slot 3 was never a candidate
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_FALSE(dev_->is_empty_slot(3));
}

// A client that hasn't sent a commit_lsn yet has confirmed nothing: the cadence fires, but nothing is proposed.
TEST_F(CraftAppendTest, PeriodicTriggerProposesNothingWithoutClientCommit) {
    dev_->set_sync_lsn_interval(4);

    for (int64_t dlsn = 0; dlsn <= 7; ++dlsn)
        ASSERT_TRUE(do_write(dlsn).has_value());

    EXPECT_TRUE(proposer_.proposed.empty());
    EXPECT_EQ(fetcher_.call_count, 0);
}

TEST_F(CraftAppendTest, PeriodicTriggerNeverFiresOnFollower) {
    dev_->set_sync_lsn_interval(4);
    proposer_.leader = false;

    for (int64_t dlsn = 0; dlsn <= 7; ++dlsn)
        ASSERT_TRUE(do_write(dlsn).has_value());

    EXPECT_TRUE(proposer_.proposed.empty());
    EXPECT_EQ(fetcher_.call_count, 0);
}

// A lagging leader: the client confirmed slot 1, but this leader missed it and can't fetch it (N=5, one
// responder), so the proposed watermark stays below the hole. The next periodic attempt still waits a full
// interval rather than retrying on every write.
TEST_F(CraftAppendTest, PeriodicTriggerThrottledWhileHoleUnresolved) {
    dev_->set_sync_lsn_interval(4);
    proposer_.members = 5;

    ASSERT_TRUE(do_write(0).has_value());
    ASSERT_TRUE(do_write(2, 0, /*commit_lsn=*/0).has_value()); // 1 is a hole on this leader
    ASSERT_TRUE(do_write(3, 0, /*commit_lsn=*/2).has_value()); // the client has resolved 1 and 2
    EXPECT_EQ(fetcher_.call_count, 1);
    ASSERT_EQ(proposer_.proposed.size(), 1u);
    EXPECT_EQ(decode(proposer_.proposed[0]).rs_commit_lsn, 0); // clamped below the hole

    for (int64_t dlsn = 4; dlsn <= 6; ++dlsn)
        ASSERT_TRUE(do_write(dlsn, 0, /*commit_lsn=*/dlsn - 1).has_value());
    EXPECT_EQ(fetcher_.call_count, 1);

    ASSERT_TRUE(do_write(7, 0, /*commit_lsn=*/6).has_value());
    EXPECT_EQ(fetcher_.call_count, 2);
    EXPECT_EQ(proposer_.proposed.size(), 1u); // still clamped at 0: nothing new to propose
}

// The piggybacked commit_lsn never regresses on a stale/reordered message, ignores the "not sent" -1, and
// starts over when InternalLogin opens a new session.
TEST_F(CraftAppendTest, ClientCommitLsnIsMaxMonotonicAndResetByLogin) {
    EXPECT_EQ(dev_->client_commit_lsn(), -1);

    ASSERT_TRUE(do_write(0, 0, /*commit_lsn=*/-1).has_value());
    EXPECT_EQ(dev_->client_commit_lsn(), -1);
    ASSERT_TRUE(do_write(4, 0, /*commit_lsn=*/3).has_value());
    EXPECT_EQ(dev_->client_commit_lsn(), 3);
    ASSERT_TRUE(do_write(5, 0, /*commit_lsn=*/1).has_value()); // reordered: older prefix
    EXPECT_EQ(dev_->client_commit_lsn(), 3);

    std::vector< uint8_t > header(sizeof(CraftEntryHeader));
    reinterpret_cast< CraftEntryHeader* >(header.data())->type = CraftEntryType::InternalLogin;
    std::vector< uint8_t > key(sizeof(InternalLoginPayload));
    *reinterpret_cast< InternalLoginPayload* >(key.data()) = InternalLoginPayload{.client_token = 9, .term = 2};
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev_->test_listener().on_commit(1, sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                                    sisl::blob{key.data(), static_cast< uint32_t >(key.size())}, {}, ctx);
    EXPECT_EQ(dev_->client_commit_lsn(), -1);
}

// ── client-requested (request_resolution) ─────────────────────────────────────

TEST_F(CraftAppendTest, ResolveOnFollowerReturnsNotLeader) {
    proposer_.leader = false;

    auto r = do_resolve(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::NOT_LEADER));
}

TEST_F(CraftAppendTest, ResolveWithStaleTermRejected) {
    dev_->seed_term(3);
    dev_->seed_lsns(5, {3});

    auto r = do_resolve(5, /*term=*/2);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
    EXPECT_EQ(fetcher_.call_count, 0);
}

TEST_F(CraftAppendTest, ResolveNegativeUptoRejected) {
    auto r = do_resolve(-1);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::invalid_argument));
}

// The failed-write flow: the round voids slot 3, and a late write into it is then rejected.
TEST_F(CraftAppendTest, ResolveVerdictsEmptyAndLateWriteIsRejected) {
    dev_->seed_lsns(5, {3});

    auto r = do_resolve(5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->resolved_upto, 5);
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));

    auto w = do_write(3);
    ASSERT_FALSE(w.has_value());
    EXPECT_EQ(w.error(), make_error_condition(volume_error::EMPTY_SLOT));
}

// The proposed entry carries the session's client_token, set by the last InternalLogin.
TEST_F(CraftAppendTest, ResolveProposesSessionClientToken) {
    std::vector< uint8_t > header(sizeof(CraftEntryHeader));
    reinterpret_cast< CraftEntryHeader* >(header.data())->type = CraftEntryType::InternalLogin;
    std::vector< uint8_t > key(sizeof(InternalLoginPayload));
    *reinterpret_cast< InternalLoginPayload* >(key.data()) = InternalLoginPayload{.client_token = 99, .term = 4};
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev_->test_listener().on_commit(1, sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                                    sisl::blob{key.data(), static_cast< uint32_t >(key.size())}, {}, ctx);
    dev_->seed_lsns(5, {});

    auto r = do_resolve(5, /*term=*/4);

    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(proposer_.proposed.size(), 1u);
    EXPECT_EQ(decode(proposer_.proposed[0]).client_token, 99u);
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    return RUN_ALL_TESTS();
}
