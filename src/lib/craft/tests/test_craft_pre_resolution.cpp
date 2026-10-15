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
//   - a local write_slot failure leaves the slot unresolved (out of empty_slots, still in missing_lsns_) and
//     is reported as first_unresolved -- the only signal for a beyond-frontier slot, which was never missing
//   - Empty verdicts need a MAJORITY of the replica set (member_count/2 + 1, the leader counting itself);
//     sub-majority -> NO_QUORUM with zero local mutation; unknown membership fails closed before the fetch
//   - a term change while the quorum fetch is in flight discards the round (STALE_TERM), nothing written
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles craft_repl_dev.cpp
// directly (same pattern as test_craft_raft_entries.cpp).

#include <gtest/gtest.h>
#include <functional>
#include <map>
#include <optional>
#include <set>
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
// injects an I/O error for specific LSNs. alloc_write_data is a dumb stub (default multi_blk_id) --
// pre_resolve_slots's tests never inspect the allocated blkid's value, same posture as
// test_craft_raft_entries.cpp's own mock. alloc_write_data carries no lsn, so a targeted alloc failure is
// injected by call index instead (fail_alloc_on_call; 0-based, counting every alloc_write_data call).

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots;
    std::set< int64_t > fail_on_write;
    bool fail_alloc{false};
    std::optional< int > fail_alloc_on_call;
    int alloc_calls{0};
    int free_data_calls{0};

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const&, lba_count_t) override {
        int const call = alloc_calls++;
        if (fail_alloc || (fail_alloc_on_call && *fail_alloc_on_call == call))
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return homestore::multi_blk_id{};
    }

    async_status write_slot(int64_t lsn, uint64_t /* term */, lba_t lba, lba_count_t len, homestore::multi_blk_id blkid,
                            bool all_zeros, std::vector< homestore::csum_t > const& csums) override {
        if (fail_on_write.contains(lsn)) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
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
    // Runs inside fetch_from_quorum, i.e. while pre_resolve_slots is suspended on the broadcast -- lets a
    // test inject a state change (e.g. an InternalLogin's term bump) into exactly that window.
    std::function< void() > during_fetch;

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
        if (during_fetch) during_fetch();
        if (should_fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return response;
    }

    // Not exercised by this file -- the background GetRSCommitLSN poll is covered by test_craft_append.cpp.
    async_result< std::vector< craft::lsn_pair > > get_rs_commit_lsn_from_quorum(uint64_t, bool, uint32_t) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }
};

// ── RAFT group mock ───────────────────────────────────────────────────────────
//
// Only membership matters to pre_resolve_slots. Defaults to a 3-member replica set, where the majority is
// 2 -- this leader plus one valid responder -- so the single-responder tests below exercise the normal path.

class MockCraftRaftProposer : public CraftRaftProposer {
public:
    uint32_t members{3};
    uint32_t member_count() const override { return members; }
    // pre_resolve_slots never consults leadership or proposes -- that is propose_sync's job (test_craft_append.cpp).
    bool is_leader() const override { return true; }
    async_status propose(sisl::blob const&, sisl::blob const&) override {
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

    auto do_pre_resolve(int64_t upto) { return homeblocks::detail::sync_get(dev_->pre_resolve_slots(upto)); }

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

// The known-gap scan starts above commit_lsn: a missing_lsns_ entry at or below it (which the commit
// invariant shouldn't allow, but seed_lsns can force) is never requested.
TEST_F(CraftPreResolutionTest, KnownGapCandidatesStartAboveCommitLsn) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(8, {2, 4, 6});
    dev_->seed_commit_lsn(4);
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 6, .is_empty = true}}}};

    auto r = do_pre_resolve(/*upto=*/8);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(fetcher_.last_requested, (std::vector< int64_t >{6}));
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{6}));
    EXPECT_TRUE(dev_->is_missing(2)); // untouched
    EXPECT_TRUE(dev_->is_missing(4)); // == commit_lsn: untouched
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
// non-responding member is simply absent from the result") -- with N=3 this leader alone is 1 < majority 2,
// so it must fail closed, not be treated the same as "a majority responded, none had evidence" (which mints
// Empty below).
TEST_F(CraftPreResolutionTest, ZeroRespondingMembersReturnsNoQuorumNoLocalMutation) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {}; // succeeds, but literally zero members responded

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(craft::craft_error::NO_QUORUM));
    EXPECT_TRUE(dev_->is_missing(3)); // untouched
    EXPECT_FALSE(journal_->has_slot(3));
}

// A malformed reply counts for nothing toward the majority -- every member malformed is the same as zero
// responding members, and must not fall through to mint Empty verdicts nobody actually vouched for.
TEST_F(CraftPreResolutionTest, AllMembersMalformedReturnsNoQuorumNoLocalMutation) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 99, .is_empty = true}}}, // names an unrequested lsn
    };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(craft::craft_error::NO_QUORUM));
    EXPECT_TRUE(dev_->is_missing(3)); // untouched
    EXPECT_FALSE(journal_->has_slot(3));
}

// ── membership-aware majority (PR #183 review follow-up) ─────────────────────────

// N=5: majority is 3, so this leader + ONE responder is not enough, even though that would have been a
// majority at N=3. Nothing may be verdicted or written -- including data a responder did have.
TEST_F(CraftPreResolutionTest, FiveMembersOneResponderIsNoQuorumNoLocalMutation) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.members = 5;
    dev_->seed_lsns(5, {3, 4});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(craft::craft_error::NO_QUORUM));
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_TRUE(dev_->is_missing(4));
}

// N=5 with two valid responders + this leader = 3 = majority: the round proceeds as normal.
TEST_F(CraftPreResolutionTest, FiveMembersTwoRespondersReachesMajority) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.members = 5;
    dev_->seed_lsns(5, {3, 4});
    fetcher_.response = {
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}},
        QuorumSlotResponse{.slots = {}},
    };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{4})); // 4: a majority answered and none had it
    EXPECT_FALSE(r->first_unresolved.has_value());
    EXPECT_TRUE(journal_->has_slot(3));
}

// A malformed member is discarded before counting: two responses, one malformed, is 1 valid + leader = 2 < 3.
TEST_F(CraftPreResolutionTest, MalformedMemberDoesNotCountTowardMajority) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.members = 5;
    dev_->seed_lsns(5, {3});
    fetcher_.response = {
        QuorumSlotResponse{.slots = {}},
        QuorumSlotResponse{.slots = {JournalSlot{.lsn = 99, .is_empty = true}}}, // unrequested lsn
    };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(craft::craft_error::NO_QUORUM));
}

// Single-member replica set: majority is 1, so this leader's own lack of the slot IS the quorum's evidence
// (matches craft_client's reference N=1 behaviour).
TEST_F(CraftPreResolutionTest, SingleMemberSetLeaderAloneIsMajority) {
    dev_->set_peer_fetcher(&fetcher_);
    proposer_.members = 1;
    dev_->seed_lsns(5, {3});
    fetcher_.response = {};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
}

// Membership unknown (no RAFT group wired, or one reporting zero members): fail closed before the broadcast.
TEST_F(CraftPreResolutionTest, UnknownMembershipFailsClosedBeforeFetch) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});

    dev_->set_raft_proposer(nullptr);
    auto r1 = do_pre_resolve(/*upto=*/5);
    ASSERT_FALSE(r1.has_value());
    EXPECT_EQ(r1.error(), std::make_error_condition(std::errc::not_supported));

    dev_->set_raft_proposer(&proposer_);
    proposer_.members = 0;
    auto r2 = do_pre_resolve(/*upto=*/5);
    ASSERT_FALSE(r2.has_value());
    EXPECT_EQ(r2.error(), std::make_error_condition(std::errc::not_supported));

    EXPECT_EQ(fetcher_.call_count, 0);
    EXPECT_TRUE(dev_->is_missing(3));
}

// ── stale term across the quorum fetch (PR #183 review follow-up) ────────────────

// An InternalLogin applying while the broadcast is in flight moves state_.term; the fetched data must NOT be
// journalled under the deposed session's term -- the round is discarded with nothing written.
TEST_F(CraftPreResolutionTest, TermChangeDuringFetchDiscardsRoundNothingWritten) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_term(4);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}}};
    fetcher_.during_fetch = [this] { dev_->seed_term(5); };

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
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
    journal_->fail_on_write = {3};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty()); // not verdicted Empty -- a quorum member DID have data for it
    EXPECT_EQ(r->first_unresolved, std::optional< int64_t >{3});
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_EQ(journal_->free_data_calls, 1); // the successful alloc_write_data is freed back, not leaked
}

// The case first_unresolved exists for: a slot BEYOND this leader's own last_append_lsn was never in
// missing_lsns_, so a failed local write there leaves no trace in local state -- only first_unresolved tells
// the proposer it must not propose past it. Reports the LOWEST failure; later slots still resolve.
TEST_F(CraftPreResolutionTest, BeyondFrontierWriteFailureReportedAsLowestFirstUnresolved) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {});
    fetcher_.response = {QuorumSlotResponse{.slots = {
                                                JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4},
                                                JournalSlot{.lsn = 4, .lba_off_bytes = 2, .len_bytes = 4},
                                                JournalSlot{.lsn = 5, .lba_off_bytes = 3, .len_bytes = 4},
                                            }}};
    journal_->fail_on_write = {4};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty_slots.empty());
    EXPECT_EQ(r->first_unresolved, std::optional< int64_t >{4});
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(journal_->has_slot(4));
    EXPECT_TRUE(journal_->has_slot(5));
    EXPECT_FALSE(dev_->is_missing(4)); // never was -- hence first_unresolved
}

// ── first_unresolved is the LOWEST failure (the proposer clamps to first_unresolved - 1) ────────────

// Several failures: the lowest wins, not the last one seen -- a too-high first_unresolved would let the
// proposer propose past slot 4.
TEST_F(CraftPreResolutionTest, MultipleWriteFailuresReportLowestNotLast) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {});
    fetcher_.response = {QuorumSlotResponse{.slots = {
                                                JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4},
                                                JournalSlot{.lsn = 4, .lba_off_bytes = 2, .len_bytes = 4},
                                                JournalSlot{.lsn = 5, .lba_off_bytes = 3, .len_bytes = 4},
                                                JournalSlot{.lsn = 6, .lba_off_bytes = 4, .len_bytes = 4},
                                            }}};
    journal_->fail_on_write = {4, 6};

    auto r = do_pre_resolve(/*upto=*/6);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->first_unresolved, std::optional< int64_t >{4});
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_TRUE(journal_->has_slot(5)); // a failure doesn't stop later slots from resolving
    EXPECT_FALSE(journal_->has_slot(4));
    EXPECT_FALSE(journal_->has_slot(6));
}

// Candidates come from two sources -- known gaps (missing_lsns_) and the range beyond last_append_lsn.
// Failures in both: the known-gap one is lower and must win.
TEST_F(CraftPreResolutionTest, FailuresAcrossKnownGapAndBeyondFrontierReportLowest) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {2}); // candidates: {2} from the gap, {6, 7} beyond the frontier
    fetcher_.response = {QuorumSlotResponse{.slots = {
                                                JournalSlot{.lsn = 2, .lba_off_bytes = 1, .len_bytes = 4},
                                                JournalSlot{.lsn = 6, .lba_off_bytes = 2, .len_bytes = 4},
                                                JournalSlot{.lsn = 7, .lba_off_bytes = 3, .len_bytes = 4},
                                            }}};
    journal_->fail_on_write = {2, 7};

    auto r = do_pre_resolve(/*upto=*/7);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->first_unresolved, std::optional< int64_t >{2});
    EXPECT_TRUE(dev_->is_missing(2));
    EXPECT_TRUE(journal_->has_slot(6));
}

// Same two sources, but only the beyond-frontier slot fails: the resolved known gap below it must not
// be reported.
TEST_F(CraftPreResolutionTest, ResolvedKnownGapBelowBeyondFrontierFailureNotReported) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {2});
    fetcher_.response = {QuorumSlotResponse{.slots = {
                                                JournalSlot{.lsn = 2, .lba_off_bytes = 1, .len_bytes = 4},
                                                JournalSlot{.lsn = 6, .lba_off_bytes = 2, .len_bytes = 4},
                                            }}};
    journal_->fail_on_write = {6};

    auto r = do_pre_resolve(/*upto=*/6);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->first_unresolved, std::optional< int64_t >{6});
    EXPECT_FALSE(dev_->is_missing(2));
    EXPECT_TRUE(journal_->has_slot(2));
}

// Both failure kinds feed the same minimum: an alloc_write_data failure at 3 (alloc call 0) and a
// write_slot failure at 5 -> 3.
TEST_F(CraftPreResolutionTest, MixedAllocAndWriteFailuresReportLowest) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {});
    fetcher_.response = {QuorumSlotResponse{.slots = {
                                                JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4},
                                                JournalSlot{.lsn = 4, .lba_off_bytes = 2, .len_bytes = 4},
                                                JournalSlot{.lsn = 5, .lba_off_bytes = 3, .len_bytes = 4},
                                            }}};
    journal_->fail_alloc_on_call = 0; // lsn 3 -- the first data slot allocated
    journal_->fail_on_write = {5};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->first_unresolved, std::optional< int64_t >{3});
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(journal_->has_slot(4));
    EXPECT_FALSE(journal_->has_slot(5));
    EXPECT_EQ(journal_->free_data_calls, 1); // only lsn 5's successful alloc is freed back
}

// Empty-verdicted and zero-write slots are RESOLVED, not unresolved: a failure above them is still the
// first unresolved slot, and nothing below it counts.
TEST_F(CraftPreResolutionTest, EmptyAndZeroSlotsBelowFailureAreNotUnresolved) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {});
    fetcher_.response = {QuorumSlotResponse{.slots = {
                                                JournalSlot{.lsn = 3, .is_empty = true},
                                                JournalSlot{.lsn = 4, .all_zeros = true},
                                                JournalSlot{.lsn = 5, .lba_off_bytes = 3, .len_bytes = 4},
                                            }}};
    journal_->fail_on_write = {5};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
    EXPECT_EQ(r->first_unresolved, std::optional< int64_t >{5});
    EXPECT_TRUE(journal_->has_slot(4));
}

// A clean round reports nothing unresolved.
TEST_F(CraftPreResolutionTest, NoFailuresLeavesFirstUnresolvedEmpty) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    fetcher_.response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4}}}};

    auto r = do_pre_resolve(/*upto=*/5);

    ASSERT_TRUE(r.has_value());
    EXPECT_FALSE(r->first_unresolved.has_value());
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
    EXPECT_EQ(r->first_unresolved, std::optional< int64_t >{3});
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_EQ(journal_->free_data_calls, 0); // alloc never succeeded -- nothing to free
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    return RUN_ALL_TESTS();
}
