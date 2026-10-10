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

// Unit tests for CraftReplDev::pre_resolve_slots (S5 leader pre-resolution / SDSTOR-22907).
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
    int free_data_calls{0};

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const&, lba_count_t) override {
        co_return homestore::multi_blk_id{};
    }

    async_status write_slot(int64_t lsn, uint64_t /* term */, lba_t lba, lba_count_t len, homestore::multi_blk_id blkid,
                            bool all_zeros, std::vector< homestore::csum_t > const& csums) override {
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
        if (should_fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return response;
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
    }

    auto do_pre_resolve(int64_t upto) { return homeblocks::detail::sync_get(dev_->pre_resolve_slots(upto)); }

    MockCraftJournalBackend* journal_{nullptr};
    MockCraftQuorumFetcher fetcher_;
    std::shared_ptr< CraftReplDev > dev_;
};

namespace {

// ── no candidates ─────────────────────────────────────────────────────────────

TEST_F(CraftPreResolutionTest, NoUnresolvedSlotsReturnsEmptyWithoutCallingFetcher) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {});

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty());
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
    EXPECT_EQ(*r, (std::vector< int64_t >{3}));
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
    EXPECT_EQ(*r, (std::vector< int64_t >{4, 5, 6}));
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
    EXPECT_TRUE(r->empty());
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(dev_->is_missing(3));
}

TEST_F(CraftPreResolutionTest, SinglePresentZeroWriteSlotWrittenAndErasedFromMissing) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .all_zeros = true}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty());
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
    EXPECT_EQ(*r, (std::vector< int64_t >{3}));
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
    EXPECT_EQ(*r, (std::vector< int64_t >{3}));
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
    EXPECT_TRUE(r->empty());
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
    EXPECT_EQ(*r, (std::vector< int64_t >{3}));
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
    EXPECT_EQ(*r, (std::vector< int64_t >{3}));
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
    EXPECT_EQ(*r, (std::vector< int64_t >{3}));
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
    EXPECT_EQ(*r, (std::vector< int64_t >{4, 5}));
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
    EXPECT_EQ(*r, (std::vector< int64_t >{3, 4})); // 3 -> quorum-lacks-evidence, 4 -> inherited empty
    EXPECT_FALSE(journal_->has_slot(3));           // A's data was discarded along with the rest of its reply
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
    EXPECT_TRUE(r->empty()); // not verdicted Empty -- a quorum member DID have data for it
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_EQ(journal_->free_data_calls, 1); // the successful alloc_write_data is freed back, not leaked
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    return RUN_ALL_TESTS();
}
