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
// QuorumSlotResponse per simulated responding member; an untagged reply is tagged test_member_id(i + 1) unless
// tag_untagged is false) or an injected error. fetch_data isn't exercised
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
    // false: pass replies with no member id through as-is, as a buggy transport would.
    bool tag_untagged{true};
    std::function< void() > on_fetch; // runs inside the fetch, to change leader/term mid-await

    async_result< RsCommitLsnReply > get_rs_commit_lsn(uint64_t /* term */, bool /* is_login */) override {
        co_return RsCommitLsnReply{};
    }

    // When set, the next fetch_data() (a Sync apply's catch-up) waits for fetch_gate: the apply is held mid-flight.
    bool hold_fetch{false};
    sisl::async::value_awaitable< bool > fetch_gate;

    async_result< std::vector< JournalSlot > > fetch_data(const std::vector< int64_t >&, uint32_t) override {
        if (std::exchange(hold_fetch, false)) co_await fetch_gate;
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    // When set, the next fetch_from_quorum() (a round's pre-resolution) waits for quorum_gate: the round is held
    // mid-flight.
    bool hold_quorum_fetch{false};
    sisl::async::value_awaitable< bool > quorum_gate;

    async_result< std::vector< QuorumSlotResponse > > fetch_from_quorum(std::vector< int64_t > lsns,
                                                                        uint32_t timeout_ms) override {
        ++call_count;
        last_requested = lsns;
        last_timeout_ms = timeout_ms;
        if (std::exchange(hold_quorum_fetch, false)) co_await quorum_gate;
        if (on_fetch) on_fetch();
        if (should_fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        auto tagged = response; // an untagged reply is the i-th distinct member
        for (size_t i = 0; i < tagged.size(); ++i)
            if (tag_untagged && tagged[i].member.is_nil()) tagged[i].member = test_member_id(i + 1);
        co_return tagged;
    }

    async_result< std::vector< QuorumLsnResponse > > poll_quorum_lsns(uint64_t, bool, uint32_t) override {
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
    void TearDown() override { EXPECT_EQ(dev_->invariant_violations(), std::vector< std::string >{}); }

    // pre_resolve_slots is private and only this fixture is a friend (TEST_F bodies are subclasses, which
    // don't inherit friendship), so tests reach it through these helpers.
    auto do_pre_resolve(int64_t upto, uint64_t term) {
        return homeblocks::detail::sync_get(dev_->pre_resolve_slots(upto, term));
    }
    // Runs within the device's current session, as a well-behaved caller does.
    auto do_pre_resolve(int64_t upto) { return do_pre_resolve(upto, dev_->term()); }
    // The watermark mutex, which a login holds from its poll through its InternalLogin.
    AsyncMutex& watermark_mu() { return dev_->watermark_mu_; }

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
    EXPECT_EQ(dev_->last_append_lsn(), 3); // unchanged -- Empty verdicts are recorded by apply, not here
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
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
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
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
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
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(journal_->has_slot(4));
    EXPECT_EQ(journal_->free_data_calls, 0);
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_TRUE(dev_->is_missing(4));
}

// A slot whose allocation fails after the term moved is skipped, but the round must not go on to claim the next
// slot in the new session: the claim would turn away that session's own write to it.
TEST_F(CraftPreResolutionTest, TermChangedDuringFailedAllocStopsBeforeNextClaim) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_term(1);
    dev_->seed_lsns(5, {3, 4});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 10, .len_bytes = 4},
                                                      JournalSlot{.lsn = 4, .lba_off_bytes = 10, .len_bytes = 4}}}};
    journal_->fail_alloc = true;
    int allocs = 0;
    journal_->on_alloc = [&] {
        ++allocs;
        dev_->seed_term(2);
    };

    auto r = do_pre_resolve(/*upto=*/5, /*term=*/1);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_EQ(allocs, 1); // slot 4 was never claimed
}

// Filling a slot above the frontier would open a gap; at the missing_lsns_ cap it's left unresolved, as write()
// rejects it. Filling an existing gap is still allowed, so the set can drain.
TEST_F(CraftPreResolutionTest, MissingCapLeavesGapOpeningFillUnresolved) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {2, 3});
    dev_->set_max_missing_lsns(1); // still at the cap once 3 is filled
    fetcher_.response = {QuorumSlotResponse{
        .slots = {JournalSlot{.lsn = 3, .all_zeros = true, .lba_off_bytes = 0, .len_bytes = k_page_size},
                  JournalSlot{.lsn = 8, .all_zeros = true, .lba_off_bytes = 0, .len_bytes = k_page_size}}}};

    auto r = do_pre_resolve(/*upto=*/8);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{8}));
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(journal_->has_slot(8));
    EXPECT_EQ(dev_->last_append_lsn(), 5);
    EXPECT_EQ(dev_->missing_count(), 1u); // just 2: no gap opened for 6..7
}

// An Empty verdict applied while a fill's journal write is in flight finds the slot still missing, so it neither
// frees nor retires anything. The fill must see the verdict: no overlay entry, and its blocks freed.
TEST_F(CraftPreResolutionTest, FillVoidedByEmptyDuringWriteIsNotPublished) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3, 4});
    fetcher_.response = {QuorumSlotResponse{
        .slots = {JournalSlot{.lsn = 3, .all_zeros = true, .lba_off_bytes = 0, .len_bytes = k_page_size},
                  JournalSlot{.lsn = 4, .lba_off_bytes = k_page_size, .len_bytes = k_page_size, .csums = {7}}}}};
    // Each slot is voted Empty while its own write is in flight.
    journal_->on_write = [this](int64_t lsn) {
        if (lsn == 3) dev_->seed_empty({3});
        if (lsn == 4) dev_->seed_empty({3, 4});
    };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->unresolved.empty());
    EXPECT_EQ(dev_->overlay_lsn_for(0), -1); // slot 3 was never published
    EXPECT_EQ(journal_->free_data_calls, 1); // slot 4's blocks were freed
    EXPECT_EQ(dev_->last_append_lsn(), 5);
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

// ── distinct voters (SDSTOR-26635) ────────────────────────────────────────────
//
// Replies are tagged with their member: only voters other than this leader (test_member_id(0)) count toward
// quorum, each once. A learner's reply or a member's repeat is still used as evidence, just not counted.

// N=5: two replies from one member are one voter -- self + 1 < quorum 3.
TEST_F(CraftPreResolutionTest, RepliesFromOneMemberCountOnce) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.members = 5;
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.member = test_member_id(1)},
                         QuorumSlotResponse{.member = test_member_id(1)}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3}));
    EXPECT_TRUE(dev_->is_missing(3));
}

// A repeat's content merges with the member's first reply: self + members 1 and 2 make quorum 3 (slot 4, which no
// reply holds, is voted Empty), but member 1's second reply holds slot 3, so it's filled rather than voted Empty.
TEST_F(CraftPreResolutionTest, RepeatHoldingSlotKeepsItFromEmpty) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.members = 5;
    dev_->seed_lsns(5, {3, 4});
    fetcher_.response = {
        QuorumSlotResponse{.member = test_member_id(1)},
        QuorumSlotResponse{.member = test_member_id(1),
                           .slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}},
        QuorumSlotResponse{.member = test_member_id(2)},
    };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{4}));
    EXPECT_TRUE(r->unresolved.empty());
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(dev_->is_missing(3));
}

// N=3: the leader's own reply is dropped, leaving no usable reply -- fails closed like zero responders.
TEST_F(CraftPreResolutionTest, OwnReplyIsDiscarded) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.member = test_member_id(0)}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::not_supported));
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_FALSE(journal_->has_slot(3));
}

// N=3: a learner lacking the slot isn't a vote -- self alone < quorum 2.
TEST_F(CraftPreResolutionTest, LearnerLackDoesNotCount) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.member = test_member_id(7)}}; // not among voters 0..2

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3}));
}

// A learner's data is still real evidence: it fills the slot.
TEST_F(CraftPreResolutionTest, LearnerDataStillFills) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.member = test_member_id(7),
                                            .slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->unresolved.empty());
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(dev_->is_missing(3));
}

// An unknown voter list (members = 0) fails closed: no fresh Empty, however many reply.
TEST_F(CraftPreResolutionTest, UnknownMembershipMintsNoEmpty) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.members = 0;
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{}, QuorumSlotResponse{}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3}));
}

// A leader missing from its own voter list fails closed too, even though voters 1 and 2 both lack the slot.
TEST_F(CraftPreResolutionTest, LeaderOutsideVoterListMintsNoEmpty) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.self = test_member_id(9); // voters are 0..2
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{}, QuorumSlotResponse{}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3}));
}

// N=3: a reply with no member id isn't a vote -- self alone < quorum 2.
TEST_F(CraftPreResolutionTest, UntaggedReplyLackDoesNotCount) {
    dev_->set_peer_fetcher(&fetcher_);
    fetcher_.tag_untagged = false;
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3}));
}

// An untagged reply's evidence is still used: its data fills slot 3 and its Empty for slot 4 is inherited.
TEST_F(CraftPreResolutionTest, UntaggedReplyEvidenceIsStillUsed) {
    dev_->set_peer_fetcher(&fetcher_);
    fetcher_.tag_untagged = false;
    dev_->seed_lsns(5, {3, 4});
    fetcher_.response = {QuorumSlotResponse{
        .slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}, JournalSlot{.lsn = 4, .is_empty = true}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{4}));
    EXPECT_TRUE(r->unresolved.empty());
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(dev_->is_missing(3));
}

// ── slots with a write in flight (H1) ─────────────────────────────────────────

// A slot whose write() is still in flight is that write's to finish: pre-resolution neither journals a peer's
// copy over it nor votes it Empty.
TEST_F(CraftPreResolutionTest, InFlightSlotWithPeerDataIsUnresolvedAndNotWritten) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    dev_->seed_in_flight(3);
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 10, .len_bytes = 4}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3}));
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
}

TEST_F(CraftPreResolutionTest, InFlightSlotLackedByQuorumIsUnresolvedNotEmpty) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    dev_->seed_in_flight(3);
    fetcher_.response = {QuorumSlotResponse{}}; // self + 1 responder lack 3: a quorum of 3

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3}));
    EXPECT_TRUE(dev_->is_missing(3));
}

// A slot pre-resolution fills gets an overlay entry like a write()'s slot: commit is stalled below it at the
// unresolved slot 3, so the overlay is the only place a read can find it (E/Q).
TEST_F(CraftPreResolutionTest, FilledSlotAboveUnresolvedSlotIsInOverlay) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(4, {3});
    dev_->seed_in_flight(3);
    fetcher_.response = {QuorumSlotResponse{
        .slots = {
            JournalSlot{.lsn = 5, .all_zeros = true, .lba_off_bytes = 2 * k_page_size, .len_bytes = k_page_size}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3}));
    EXPECT_TRUE(journal_->has_slot(5));
    EXPECT_EQ(dev_->overlay_lsn_for(2), 5);
}

// A write() that finishes during the quorum fetch means this leader now holds the slot, so the round must not
// count it as lacking the slot and vote it Empty.
TEST_F(CraftPreResolutionTest, SlotWrittenDuringFetchIsNotVotedEmpty) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    // The leader's own write to 3 lands mid-fetch.
    fetcher_.on_fetch = [this] { dev_->seed_lsns(5, {}); };
    // Self + 1 responder lacking 3 would be a quorum of 3.
    fetcher_.response = {QuorumSlotResponse{}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_TRUE(r->unresolved.empty());
}

// A responder's Empty was already committed, so it wins even over a write() still in flight here: the round
// reports it instead of leaving the slot unresolved.
TEST_F(CraftPreResolutionTest, InheritedEmptyWinsOverInFlightWrite) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    dev_->seed_in_flight(3);
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .is_empty = true}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
    EXPECT_TRUE(r->unresolved.empty());
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
        proposer_.on_commit = [this](sisl::blob const& h, sisl::blob const& k) { loop_back(proposer_, *dev_, h, k); };
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

    // Commit an entry on this member as RAFT would, with no proposal of its own.
    void commit_entry(CraftEntryType type, std::vector< uint8_t > const& key) {
        std::vector< uint8_t > header(sizeof(CraftEntryHeader));
        reinterpret_cast< CraftEntryHeader* >(header.data())->type = type;
        cintrusive< homestore::repl_req_ctx > ctx{};
        dev_->raft_listener().on_commit(1, sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                                        sisl::blob{key.data(), static_cast< uint32_t >(key.size())}, {}, ctx);
    }
    void commit_sync(int64_t rs_commit_lsn, std::vector< int64_t > const& empty_slots = {}) {
        std::vector< uint8_t > key(sync_rs_commit_lsn_key_size(empty_slots.size()));
        serialize_sync_rs_commit_lsn(key.data(), rs_commit_lsn, /*client_token=*/0, empty_slots);
        commit_entry(CraftEntryType::SyncRSCommitLSN, key);
    }
    // At this member's last_append, where a login's poll would settle: never below a committed Sync's watermark.
    void commit_login(uint64_t term) {
        std::vector< uint8_t > key(sizeof(InternalLoginPayload));
        serialize_internal_login(key.data(), /*client_token=*/7, term, dev_->last_append_lsn());
        commit_entry(CraftEntryType::InternalLogin, key);
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
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_EQ(fetcher_.call_count, 0);
    EXPECT_TRUE(proposer_.proposed.empty());
    EXPECT_TRUE(do_append(5).has_value()); // the gate was never claimed: a current-session round proceeds
}

// The entry commits, but InternalLogin replaces the caller's session before append() resumes: the reply belongs to
// that session, so it's STALE_TERM, while the watermark still moves since the entry did commit.
TEST_F(CraftAppendTest, LoginDuringProposalFailsWithStaleTerm) {
    dev_->seed_term(1);
    dev_->seed_lsns(5, {});
    proposer_.on_commit = [this](sisl::blob const& h, sisl::blob const& k) {
        loop_back(proposer_, *dev_, h, k);
        dev_->seed_term(2);
    };

    auto r = do_append(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_EQ(dev_->last_proposed_sync_lsn(), 5);
}

// An allocation failure lets pre-resolution return without noticing the term moved; with nothing to propose,
// append() must still not answer for the replaced session.
TEST_F(CraftAppendTest, TermChangeBeforeSkippedProposalFailsWithStaleTerm) {
    dev_->seed_term(1);
    dev_->seed_lsns(5, {0});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 0, .lba_off_bytes = 10, .len_bytes = 4}}}};
    journal_->fail_alloc = true;
    journal_->on_alloc = [this] { dev_->seed_term(2); };

    auto r = do_append(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_TRUE(proposer_.proposed.empty());
}

// Pre-resolution returns normally after a failed allocation that moved the term, and the round still has a verdict
// to propose: append() re-checks the term just before proposing and fails STALE_TERM, proposing nothing.
TEST_F(CraftAppendTest, TermChangeBeforeProposalFailsWithStaleTerm) {
    dev_->seed_term(1);
    dev_->seed_lsns(5, {3, 4});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .is_empty = true},
                                                      JournalSlot{.lsn = 4, .lba_off_bytes = 10, .len_bytes = 4}}}};
    journal_->fail_alloc = true;
    journal_->on_alloc = [this] { dev_->seed_term(2); };

    auto r = do_append(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_TRUE(proposer_.proposed.empty());
}

TEST_F(CraftPreResolutionTest, CallerTermReplacedByLoginIsRejected) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_term(2);
    dev_->seed_lsns(5, {3});

    auto r = do_pre_resolve(5, /*term=*/1);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
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

// Slots pre-resolution fills above the frontier are recorded like write()'s, so applying the entry doesn't
// mark them missing again: commit reaches the watermark without a second fetch or write (C2a).
TEST_F(CraftAppendTest, FilledSlotsAboveFrontierCommitWithoutRefetch) {
    dev_->seed_lsns(5, {});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 6, .lba_off_bytes = 0, .len_bytes = 4},
                                                      JournalSlot{.lsn = 7, .lba_off_bytes = 0, .len_bytes = 4}}}};
    std::map< int64_t, int > writes;
    journal_->on_write = [&](int64_t lsn) { ++writes[lsn]; };

    auto r = do_append(7);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->resolved_upto, 7);
    EXPECT_EQ(dev_->last_append_lsn(), 7);
    EXPECT_EQ(dev_->missing_count(), 0u);
    EXPECT_EQ(dev_->commit_lsn(), 7);
    EXPECT_EQ(writes[6], 1);
    EXPECT_EQ(writes[7], 1);
}

// N=5, one responder: 3 stays unresolved while 6 and 7 are filled. A later round asks only about 3; it never
// fetches and writes 6 and 7 again, leaking the earlier copies' blocks (H2).
TEST_F(CraftAppendTest, UnresolvedHoleDoesNotRewriteFilledSlots) {
    proposer_.members = 5;
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 6, .lba_off_bytes = 0, .len_bytes = 4},
                                                      JournalSlot{.lsn = 7, .lba_off_bytes = 0, .len_bytes = 4}}}};
    std::map< int64_t, int > writes;
    journal_->on_write = [&](int64_t lsn) { ++writes[lsn]; };

    auto first = do_append(7);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->resolved_upto, 2);

    fetcher_.response = {QuorumSlotResponse{}}; // a responder answers only for the slots it's asked about
    auto second = do_append(7);

    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(fetcher_.last_requested, (std::vector< int64_t >{3}));
    EXPECT_EQ(writes[6], 1);
    EXPECT_EQ(writes[7], 1);
}

// A committed SyncRSCommitLSN(5) settled 4 and 5 as present, but this node's catch-up failed so it still lacks
// them. A later round where a quorum lacks them must not vote them Empty: the earlier entry already settled
// them (C2b).
TEST_F(CraftAppendTest, NoFreshEmptyAtOrBelowAppliedWatermark) {
    dev_->seed_lsns(3, {});
    std::vector< uint8_t > header(sizeof(CraftEntryHeader));
    reinterpret_cast< CraftEntryHeader* >(header.data())->type = CraftEntryType::SyncRSCommitLSN;
    std::vector< uint8_t > key(sync_rs_commit_lsn_key_size(0));
    serialize_sync_rs_commit_lsn(key.data(), /*rs_commit_lsn=*/5, /*client_token=*/0, {});
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev_->raft_listener().on_commit(1, sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                                    sisl::blob{key.data(), static_cast< uint32_t >(key.size())}, {}, ctx);
    ASSERT_TRUE(dev_->is_missing(4)); // catch-up failed: the mock's fetch_data is unsupported
    ASSERT_TRUE(dev_->is_missing(5));

    auto r = do_append(5); // the one responder lacks 4 and 5 too: self + 1 is a quorum of 3

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_EQ(r->resolved_upto, 3);
    for (auto const& entry : proposer_.proposed)
        EXPECT_TRUE(decode(entry).empty_slots.empty());
    EXPECT_FALSE(dev_->is_empty_slot(4));
    EXPECT_TRUE(dev_->is_missing(4));
    EXPECT_TRUE(dev_->is_missing(5));
}

// The same floor holds while that Sync is still waiting in the apply queue: it settled its slots as present when it
// committed, so a round must not vote them Empty before the queue reaches it. Sync(3)'s catch-up is held, Sync(5) is
// queued behind it, and the responder lacks 3..5 (self + 1 is a quorum of 3).
TEST_F(CraftAppendTest, NoFreshEmptyAtOrBelowQueuedWatermark) {
    dev_->seed_lsns(2, {});
    fetcher_.hold_fetch = true;
    commit_sync(3); // held in catch-up
    commit_sync(5); // queued behind it

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_EQ(r->unresolved, (std::vector< int64_t >{3, 4, 5}));

    fetcher_.fetch_gate.complete(true);
}

// A verdict voting slot 0 Empty applies while write(0) is inside write_slot. The write must not be acked (every
// replica discards the slot), and its blocks are freed once: apply skipped them, since the slot was still missing.
TEST_F(CraftAppendTest, WriteVotedEmptyWhileInFlightIsRejectedAndFreed) {
    std::vector< uint8_t > header(sizeof(CraftEntryHeader));
    reinterpret_cast< CraftEntryHeader* >(header.data())->type = CraftEntryType::SyncRSCommitLSN;
    std::vector< uint8_t > key(sync_rs_commit_lsn_key_size(1));
    serialize_sync_rs_commit_lsn(key.data(), /*rs_commit_lsn=*/0, /*client_token=*/0, {0});
    journal_->on_write = [&](int64_t lsn) {
        if (lsn != 0) return;
        cintrusive< homestore::repl_req_ctx > ctx{};
        dev_->raft_listener().on_commit(1, sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                                        sisl::blob{key.data(), static_cast< uint32_t >(key.size())}, {}, ctx);
    };

    auto w = do_write(0);

    ASSERT_FALSE(w.has_value());
    EXPECT_EQ(w.error(), make_error_condition(volume_error::EMPTY_SLOT));
    EXPECT_TRUE(dev_->is_empty_slot(0));
    EXPECT_FALSE(dev_->is_missing(0));
    EXPECT_EQ(journal_->free_data_calls, 1);
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

// A proposal that misses its deadline reports timed_out ("outcome unknown"), leaves the watermark alone, and releases
// the single-flight gate: the next round proposes the same watermark again rather than skipping it.
TEST_F(CraftAppendTest, ProposeTimeoutReleasesGateAndNextRoundReproposes) {
    dev_->seed_lsns(5, {});
    proposer_.time_out = true;

    auto r = do_append(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::timed_out));
    EXPECT_EQ(proposer_.proposed.size(), 1u); // it did reach the log
    EXPECT_EQ(dev_->last_proposed_sync_lsn(), -1);

    proposer_.time_out = false;
    ASSERT_TRUE(do_append(5).has_value());
    EXPECT_EQ(proposer_.proposed.size(), 2u); // re-proposed, not skipped
    EXPECT_EQ(dev_->last_proposed_sync_lsn(), 5);
}

// A timed-out entry may still commit. Two rounds time out carrying the same Empty verdict (the leader still lacks
// slot 3 when the second one resolves), then both commit in log order. The second verdict lands at or below the
// commit point the first one advanced, which must not fault the partition: the slot is already Empty.
TEST_F(CraftAppendTest, TimedOutVerdictCommittingLateThenReproposedIsHarmless) {
    dev_->seed_lsns(5, {3}); // 3 lacked by self + the one responder: quorum of 3
    proposer_.time_out = true;
    ASSERT_FALSE(do_append(5).has_value());
    ASSERT_FALSE(do_append(5).has_value()); // re-proposed before the first entry applied
    ASSERT_EQ(proposer_.proposed.size(), 2u);
    EXPECT_EQ(decode(proposer_.proposed[0]).empty_slots, (std::vector< int64_t >{3}));
    EXPECT_EQ(decode(proposer_.proposed[1]).empty_slots, (std::vector< int64_t >{3}));

    for (auto& [header, key] : proposer_.proposed) // both commit, in log order
        proposer_.on_commit(sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                            sisl::blob{key.data(), static_cast< uint32_t >(key.size())});

    EXPECT_FALSE(dev_->is_faulted());
    EXPECT_TRUE(dev_->is_empty_slot(3));
    EXPECT_EQ(dev_->commit_lsn(), 5);
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

// truncate() rolls the append frontier back below the periodic baseline: the baseline must follow it down, or
// periodic rounds stay off until writes climb back past where it was.
TEST_F(CraftAppendTest, TruncateClampsPeriodicBaseline) {
    dev_->set_sync_lsn_interval(4);
    // Nothing confirmed by the client, so two attempts move the baseline to 7 without proposing.
    for (int64_t dlsn = 0; dlsn <= 7; ++dlsn)
        ASSERT_TRUE(do_write(dlsn).has_value());
    ASSERT_TRUE(proposer_.proposed.empty());

    ASSERT_TRUE(homeblocks::detail::sync_get(dev_->truncate(1)).has_value());

    for (int64_t dlsn = 2; dlsn <= 5; ++dlsn)
        ASSERT_TRUE(do_write(dlsn, 0, /*commit_lsn=*/dlsn - 1).has_value());
    ASSERT_EQ(proposer_.proposed.size(), 1u); // 4 past the clamped baseline (1)
    EXPECT_EQ(decode(proposer_.proposed[0]).rs_commit_lsn, 4);
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
    serialize_internal_login(key.data(), /*client_token=*/9, /*term=*/2, /*rs_commit_lsn=*/-1);
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev_->raft_listener().on_commit(1, sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
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
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
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
    serialize_internal_login(key.data(), /*client_token=*/99, /*term=*/4, /*rs_commit_lsn=*/-1);
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev_->raft_listener().on_commit(1, sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                                    sisl::blob{key.data(), static_cast< uint32_t >(key.size())}, {}, ctx);
    dev_->seed_lsns(5, {});

    auto r = do_resolve(5, /*term=*/4);

    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(proposer_.proposed.size(), 1u);
    EXPECT_EQ(decode(proposer_.proposed[0]).client_token, 99u);
}

// A Resolve that arrives while a round is running waits its turn, rather than failing operation_in_progress and
// leaving the client's failed slot unresolved until its next failed write.
TEST_F(CraftAppendTest, ResolveWaitsForARunningRound) {
    dev_->seed_lsns(5, {3});
    fetcher_.hold_quorum_fetch = true;
    bool round_done = false, resolve_done = false;
    std::error_condition round_outcome, resolve_outcome;
    homeblocks::detail::detach(
        [](std::shared_ptr< CraftReplDev > dev, bool& done, std::error_condition& outcome) -> async_status {
            auto r = co_await dev->append(/*sync_to=*/5, /*client_token=*/0, /*term=*/0);
            outcome = r ? std::error_condition{} : r.error();
            done = true;
            co_return ok();
        }(dev_, round_done, round_outcome));
    ASSERT_FALSE(round_done); // held in its quorum fetch

    homeblocks::detail::detach(
        [](std::shared_ptr< CraftReplDev > dev, bool& done, std::error_condition& outcome) -> async_status {
            auto r = co_await dev->request_resolution(craft::client_hdr{0, -1, -1}, /*upto=*/5);
            outcome = r ? std::error_condition{} : r.error();
            done = true;
            co_return ok();
        }(dev_, resolve_done, resolve_outcome));
    EXPECT_FALSE(resolve_done); // waits for the round

    fetcher_.quorum_gate.complete(true);

    ASSERT_TRUE(round_done);
    EXPECT_EQ(round_outcome, std::error_condition{});
    ASSERT_TRUE(resolve_done);
    EXPECT_EQ(resolve_outcome, std::error_condition{}) << resolve_outcome.message();
}

// A Resolve that waited re-checks before it runs: the round it waited out may have changed things. Here this member
// lost its leadership meanwhile, so it neither fetches nor proposes.
TEST_F(CraftAppendTest, ResolveRechecksAfterWaiting) {
    dev_->seed_lsns(5, {3});
    std::optional< AsyncMutex::Lock > holder = homeblocks::detail::sync_get(watermark_mu().lock());
    bool done = false;
    std::error_condition outcome;
    homeblocks::detail::detach(
        [](std::shared_ptr< CraftReplDev > dev, bool& done, std::error_condition& outcome) -> async_status {
            auto r = co_await dev->request_resolution(craft::client_hdr{0, -1, -1}, /*upto=*/5);
            outcome = r ? std::error_condition{} : r.error();
            done = true;
            co_return ok();
        }(dev_, done, outcome));
    ASSERT_FALSE(done);

    proposer_.leader = false;
    holder.reset();

    ASSERT_TRUE(done);
    EXPECT_EQ(outcome, craft::make_error_condition(craft::craft_error::NOT_LEADER));
    EXPECT_EQ(fetcher_.call_count, 0);
    EXPECT_TRUE(proposer_.proposed.empty());
}

// ── the watermark mutex against a login (SDSTOR-22891) ─────────────────────────

// While a login holds the watermark, no round runs: append() fails operation_in_progress, and the periodic trigger
// skips.
TEST_F(CraftAppendTest, RoundsDontRunWhileALoginHoldsTheWatermark) {
    dev_->seed_lsns(5, {3});
    dev_->set_sync_lsn_interval(1);
    auto login = homeblocks::detail::sync_get(watermark_mu().lock());

    auto r = do_append(5);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::operation_in_progress));
    ASSERT_TRUE(do_write(6, 0, /*commit_lsn=*/5).has_value()); // would trigger a periodic round
    EXPECT_EQ(fetcher_.call_count, 0);
    EXPECT_TRUE(proposer_.proposed.empty());
}

// A login that arrives during a round waits it out, then holds the watermark.
TEST_F(CraftAppendTest, LoginWaitsOutARunningRound) {
    dev_->seed_lsns(5, {3});
    fetcher_.hold_quorum_fetch = true;
    bool round_done = false;
    std::error_condition round_outcome;
    homeblocks::detail::detach(
        [](std::shared_ptr< CraftReplDev > dev, bool& done, std::error_condition& outcome) -> async_status {
            auto r = co_await dev->append(/*sync_to=*/5, /*client_token=*/0, /*term=*/0);
            outcome = r ? std::error_condition{} : r.error();
            done = true;
            co_return ok();
        }(dev_, round_done, round_outcome));
    std::optional< AsyncMutex::Lock > login;
    homeblocks::detail::detach(
        [](AsyncMutex& mu, std::optional< AsyncMutex::Lock >& held) -> sisl::async::task< void > {
            held = co_await mu.lock();
        }(watermark_mu(), login));
    EXPECT_FALSE(login.has_value()); // the round holds it

    fetcher_.quorum_gate.complete(true);

    ASSERT_TRUE(round_done);
    EXPECT_EQ(round_outcome, std::error_condition{});
    ASSERT_TRUE(login.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 5); // the round's Sync landed before the login holds the watermark
}

// ── a committed login fences the old session (SDSTOR-26655) ───────────────────
//
// Each test holds a Sync(5) apply mid-catch-up (slots 3..5 are missing), so an InternalLogin committed behind it
// stays queued: its fence is up, but the device's term is still the old session's.

// An old-session round is rejected from the login's commit, before it fetches or proposes anything.
TEST_F(CraftAppendTest, QueuedLoginFencesOldSessionRounds) {
    dev_->seed_lsns(2, {});
    fetcher_.hold_fetch = true;
    commit_sync(5);  // held in catch-up
    commit_login(5); // queued behind it
    ASSERT_EQ(dev_->term(), 0u);
    auto const stale = craft::make_error_condition(craft::craft_error::STALE_TERM);

    auto a = do_append(5);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error(), stale);
    auto r = do_resolve(5, /*term=*/0);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), stale);
    auto p = do_pre_resolve(/*upto=*/5);
    ASSERT_FALSE(p.has_value());
    EXPECT_EQ(p.error(), stale);
    EXPECT_EQ(fetcher_.call_count, 0);
    EXPECT_TRUE(proposer_.proposed.empty());

    fetcher_.fetch_gate.complete(true);
}

// A round already past its first check when the login commits stops at the next one, writing nothing.
TEST_F(CraftAppendTest, LoginCommittedDuringFetchStopsOldSessionRound) {
    dev_->seed_lsns(2, {});
    fetcher_.hold_fetch = true;
    commit_sync(5); // held in catch-up
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}}};
    fetcher_.on_fetch = [this] { commit_login(5); }; // queued behind the Sync

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_EQ(dev_->term(), 0u); // stopped by the fence, not by the login's apply

    fetcher_.on_fetch = nullptr;
    fetcher_.fetch_gate.complete(true);
}

// ── a fence landing between a write's post-flight check and its ack (SDSTOR-22889) ──
//
// In production the window is the piggybacked commit(); here it's the periodic round write(3) triggers, whose fetch
// raises the fence. Slot 1 is a hole, so the round fetches. Either way the slot stays published; only the ack goes.

// A login poll quiesces the write's term.
TEST_F(CraftAppendTest, WriteQuiescedBeforeItsAckIsNotAcked) {
    dev_->set_sync_lsn_interval(4);
    ASSERT_TRUE(do_write(0, 0).has_value());
    ASSERT_TRUE(do_write(2, 0).has_value());
    fetcher_.on_fetch = [this] {
        ASSERT_TRUE(homeblocks::detail::sync_get(dev_->get_rs_commit_lsn(/*term=*/0, /*is_login=*/true)).has_value());
    };

    auto w = do_write(3, 0, /*commit_lsn=*/2);

    ASSERT_FALSE(w.has_value());
    EXPECT_EQ(w.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_FALSE(dev_->is_missing(3));
    EXPECT_TRUE(journal_->has_slot(3));
    fetcher_.on_fetch = nullptr;
}

// A login commits, ending the write's session.
TEST_F(CraftAppendTest, WriteWhoseSessionEndsBeforeItsAckIsNotAcked) {
    dev_->set_sync_lsn_interval(4);
    ASSERT_TRUE(do_write(0, 0).has_value());
    ASSERT_TRUE(do_write(2, 0).has_value());
    fetcher_.on_fetch = [this] { commit_login(5); };

    auto w = do_write(3, 0, /*commit_lsn=*/2);

    ASSERT_FALSE(w.has_value());
    EXPECT_EQ(w.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_FALSE(dev_->is_missing(3));
    EXPECT_TRUE(journal_->has_slot(3));
    fetcher_.on_fetch = nullptr;
}

// ── fence matrix (SDSTOR-26655, SDSTOR-22889, SDSTOR-22892) ───────────────────
//
// What each entry point does while a fence is up, and once it has settled. Each cell raises one fence and calls one
// entry point, then settles the fence and calls it again. A new fence or entry point adds its cells here, so a change
// to what a fence covers, or when, fails a cell.
//
// Start: slots 0..2 appended, session term 0. The fences:
//   queued_sync   SyncRSCommitLSN(rs=5, Empty={4}), committed with the apply queue paused; settles when it applies
//   queued_login  InternalLogin(term=5), committed with the apply queue paused; settles when it applies
//   quiesce       a login poll (GetRSCommitLSN, is_login) at term 0; settles when the login's InternalLogin applies
//   plain_poll    a watchdog poll (GetRSCommitLSN, not is_login), which fences nothing
//   stale_login   InternalLogin(term=0), not newer than the session, committed with the apply queue paused; its apply
//                 ignores it, so it fences nothing
// Outcomes that aren't errors are tested on their own: the voided slot's overlay entry
// (QueuedSyncVerdictStopsServingVoidedSlotAtCommit) and the queued watermark (NoFreshEmptyAtOrBelowQueuedWatermark).
// A cell that proposes (resolve, append) tests the entry point's checks up to its proposal: with the apply queue
// paused, the mock returns the proposal once its entry commits. That a proposal waits for its apply is tested in
// test_craft_raft_entries.cpp (InternalLoginWaitsForSuspendedSyncApply).

constexpr uint64_t k_new_term{5}; // the login's term

enum class Fence { queued_sync, queued_login, quiesce, plain_poll, stale_login };
enum class Call { write, write_voided_slot, write_new_term, read, keep_alive, resolve, append, pre_resolve };

struct FenceCell {
    char const* name;
    Fence fence;
    Call call;
    std::error_condition while_fenced; // {} = accepted
    std::error_condition once_settled;
};
void PrintTo(FenceCell const& cell, std::ostream* os) { *os << cell.name; }

std::vector< FenceCell > fence_cells() {
    auto const stale = craft::make_error_condition(craft::craft_error::STALE_TERM);
    auto const empty_slot = make_error_condition(volume_error::EMPTY_SLOT);
    auto const no_index = std::make_error_condition(std::errc::not_supported); // read() got past its term check
    return {
        // A Sync doesn't fence the session; its verdicts fence the slots they void.
        {"Sync_Write", Fence::queued_sync, Call::write, {}, {}},
        {"Sync_WriteVoidedSlot", Fence::queued_sync, Call::write_voided_slot, empty_slot, empty_slot},
        // A login fences the old session's writes and leader rounds at commit. Reads and keep_alive aren't fenced
        // (quiesce blocks writes only): like the new term's writes, they change over when the login applies.
        {"Login_Write", Fence::queued_login, Call::write, stale, stale},
        {"Login_WriteNewTerm", Fence::queued_login, Call::write_new_term, stale, {}},
        {"Login_Read", Fence::queued_login, Call::read, no_index, stale},
        {"Login_KeepAlive", Fence::queued_login, Call::keep_alive, {}, stale},
        {"Login_Resolve", Fence::queued_login, Call::resolve, stale, stale},
        {"Login_Append", Fence::queued_login, Call::append, stale, stale},
        {"Login_PreResolve", Fence::queued_login, Call::pre_resolve, stale, stale},
        // A quiesce fences the old session's writes only. The rest of its IO, and its leader rounds, go on until the
        // login applies.
        {"Quiesce_Write", Fence::quiesce, Call::write, stale, stale},
        {"Quiesce_WriteNewTerm", Fence::quiesce, Call::write_new_term, stale, {}},
        {"Quiesce_Read", Fence::quiesce, Call::read, no_index, stale},
        {"Quiesce_KeepAlive", Fence::quiesce, Call::keep_alive, {}, stale},
        {"Quiesce_Resolve", Fence::quiesce, Call::resolve, {}, stale},
        {"Quiesce_Append", Fence::quiesce, Call::append, {}, stale},
        {"Quiesce_PreResolve", Fence::quiesce, Call::pre_resolve, {}, stale},
        // A watchdog poll never quiesces.
        {"Poll_Write", Fence::plain_poll, Call::write, {}, {}},
        // A login that isn't newer neither fences the session at commit nor replaces it at apply. The entry points
        // can't see the token it leaves alone: test_craft_raft_entries.cpp checks that.
        {"StaleLogin_Write", Fence::stale_login, Call::write, {}, {}},
        {"StaleLogin_Read", Fence::stale_login, Call::read, no_index, no_index},
        {"StaleLogin_KeepAlive", Fence::stale_login, Call::keep_alive, {}, {}},
        {"StaleLogin_Resolve", Fence::stale_login, Call::resolve, {}, {}},
        {"StaleLogin_Append", Fence::stale_login, Call::append, {}, {}},
        {"StaleLogin_PreResolve", Fence::stale_login, Call::pre_resolve, {}, {}},
    };
}

class CraftFenceMatrixTest : public CraftAppendTest, public ::testing::WithParamInterface< FenceCell > {
protected:
    void raise_fence() {
        switch (GetParam().fence) {
        case Fence::queued_sync:
            dev_->pause_apply();
            commit_sync(/*rs_commit_lsn=*/5, /*empty_slots=*/{4});
            break;
        case Fence::queued_login:
            dev_->pause_apply();
            commit_login(k_new_term);
            break;
        case Fence::stale_login:
            dev_->pause_apply();
            commit_login(/*term=*/0);
            break;
        case Fence::quiesce:
        case Fence::plain_poll:
            ASSERT_TRUE(homeblocks::detail::sync_get(
                            dev_->get_rs_commit_lsn(/*term=*/0, /*is_login=*/GetParam().fence == Fence::quiesce))
                            .has_value());
            break;
        }
    }

    void settle_fence() {
        switch (GetParam().fence) {
        case Fence::queued_sync:
        case Fence::queued_login:
        case Fence::stale_login:
            dev_->resume_apply();
            break;
        case Fence::quiesce:
            commit_login(k_new_term);
            break;
        case Fence::plain_poll:
            break;
        }
    }

    // The cell's entry point, called by the old session (term 0) unless it says otherwise. Phase 0 runs while the
    // fence is up, phase 1 once it has settled; each phase writes its own dlsn.
    std::error_condition call(int phase) {
        auto const outcome = [](auto const& r) { return r ? std::error_condition{} : r.error(); };
        craft::client_hdr const old_session{0, -1, -1};
        switch (GetParam().call) {
        case Call::write:
            return outcome(do_write(/*dlsn=*/8 + phase, /*term=*/0));
        case Call::write_voided_slot:
            return outcome(do_write(/*dlsn=*/4, /*term=*/0));
        case Call::write_new_term:
            return outcome(do_write(/*dlsn=*/8 + phase, k_new_term));
        case Call::read: {
            static std::array< uint8_t, k_page_size > buf{};
            sisl::sg_list dest;
            dest.size = k_page_size;
            dest.iovs.push_back(iovec{buf.data(), buf.size()});
            return outcome(homeblocks::detail::sync_get(
                dev_->read(old_session, /*read_lsn=*/2, /*addr=*/0, k_page_size, std::move(dest))));
        }
        case Call::keep_alive:
            return outcome(homeblocks::detail::sync_get(dev_->keep_alive(old_session)));
        case Call::resolve:
            return outcome(do_resolve(/*upto=*/5, /*term=*/0));
        case Call::append:
            return outcome(homeblocks::detail::sync_get(dev_->append(/*sync_to=*/5, /*client_token=*/0, /*term=*/0)));
        case Call::pre_resolve:
            return outcome(do_pre_resolve(/*upto=*/5, /*term=*/0));
        }
        return std::make_error_condition(std::errc::invalid_argument);
    }
};

TEST_P(CraftFenceMatrixTest, WhileFencedThenOnceSettled) {
    auto const& cell = GetParam();
    dev_->seed_lsns(2, {});
    raise_fence();
    ASSERT_FALSE(HasFatalFailure());

    auto const fenced = call(0);
    EXPECT_EQ(fenced, cell.while_fenced) << "while fenced, got \"" << fenced.message() << "\", want \""
                                         << cell.while_fenced.message() << "\"";
    settle_fence();
    auto const settled = call(1);
    EXPECT_EQ(settled, cell.once_settled)
        << "once settled, got \"" << settled.message() << "\", want \"" << cell.once_settled.message() << "\"";
}

INSTANTIATE_TEST_SUITE_P(Cells, CraftFenceMatrixTest, ::testing::ValuesIn(fence_cells()),
                         [](auto const& info) { return std::string{info.param.name}; });

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    // This light suite never starts iomgr, so the watchdog's real recurring timer must stay
    // disabled -- same reasoning as every other light CRAFT test binary.
    HB_SETTINGS_FACTORY().load_json("{\"craft_watchdog_timeout_ms\": 0}");
    return RUN_ALL_TESTS();
}
