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

// Unit tests for CraftReplDev::append() / request_resolution() and the background SyncRSCommitLSN triggers
// (S5 / SDSTOR-22908).
//
// Verifies:
//   - append() fails closed without a RAFT group (not_supported) and on a follower (NOT_LEADER, no work done)
//   - the proposed entry is a SyncRSCommitLSN whose key round-trips {rs_commit_lsn, client_token, empty_slots}
//   - rs_commit_lsn is clamped just below pre-resolution's first_unresolved, and only verdicts <= it ride along
//   - a round that would not advance past the last committed proposal (and mints no verdicts) proposes nothing
//   - pre-resolution / propose() failures propagate; a failed propose doesn't count as "already proposed"
//   - request_resolution(): term fence, faulted-recovery gate, NOT_LEADER, and the returned verdict list
//     includes earlier rounds' Empty verdicts as well as this round's
//   - background rounds (periodic / watchdog): GetRSCommitLSN poll (is_login=false) picks max(quorum
//     last_append), needs a majority, are silent on a follower, coalesce while one is in flight, and fire every
//     sync_lsn_interval_ successful writes (0 disables)
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles craft_repl_dev.cpp
// directly (same pattern as test_craft_pre_resolution.cpp).

#include <functional>
#include <gtest/gtest.h>
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
// Same std::map-backed shape as test_craft_pre_resolution.cpp's: tests can see which LSNs were persisted, and
// fail_on_write injects an I/O error for specific LSNs.

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots;
    std::set< int64_t > fail_on_write;

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const&, lba_count_t) override {
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
    async_status free_data(homestore::multi_blk_id) override { co_return ok(); }
    async_status free_slot(int64_t lsn) override { return mock_free_slot(*this, lsn); }
    async_status read_data(homestore::multi_blk_id, sisl::sg_list&) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }
};

// ── peer mock ─────────────────────────────────────────────────────────────────
//
// fetch_from_quorum (pre-resolution) and get_rs_commit_lsn_from_quorum (the background poll) are both
// programmable. poll_response is a function so a test can report watermarks that track the device's own state.

class MockCraftPeerFetcher : public CraftPeerFetcher {
public:
    std::vector< QuorumSlotResponse > fetch_response;
    int fetch_calls{0};

    std::function< std::vector< craft::lsn_pair >() > poll_response = [] { return std::vector< craft::lsn_pair >{}; };
    bool poll_should_fail{false};
    int poll_calls{0};
    std::optional< bool > last_poll_is_login;
    uint32_t last_poll_timeout_ms{0};

    async_result< craft::lsn_pair > get_rs_commit_lsn(uint64_t, bool) override { co_return craft::lsn_pair{}; }
    async_result< std::vector< JournalSlot > > fetch_data(const std::vector< int64_t >&, uint32_t) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }
    async_result< std::vector< QuorumSlotResponse > > fetch_from_quorum(std::vector< int64_t >, uint32_t) override {
        ++fetch_calls;
        co_return fetch_response;
    }
    async_result< std::vector< craft::lsn_pair > > get_rs_commit_lsn_from_quorum(uint64_t, bool is_login,
                                                                                 uint32_t timeout_ms) override {
        ++poll_calls;
        last_poll_is_login = is_login;
        last_poll_timeout_ms = timeout_ms;
        if (poll_should_fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return poll_response();
    }
};

// ── RAFT group mock ───────────────────────────────────────────────────────────
//
// Decodes every proposed entry so tests can assert on exactly what would ride the RAFT log. With
// apply_on_commit set, a successful propose() also delivers the entry to the device's own listener, the way a
// real RAFT commit fires on_commit on the leader too -- so the leader's apply_sync_rs_commit_lsn actually runs.

struct ProposedEntry {
    CraftEntryType type;
    int64_t rs_commit_lsn;
    uint64_t client_token;
    std::vector< int64_t > empty_slots;
};

class MockCraftRaftProposer : public CraftRaftProposer {
public:
    uint32_t members{3};
    bool leader{true};
    bool should_fail{false};
    bool apply_on_commit{false};
    CraftReplDev* dev{nullptr};
    std::function< void() > during_propose;
    std::vector< ProposedEntry > proposed;

    uint32_t member_count() const override { return members; }
    bool is_leader() const override { return leader; }

    async_status propose(sisl::blob const& header, sisl::blob const& key) override {
        // Invoke a copy: a hook may reset during_propose itself, which would destroy the callable mid-call.
        if (during_propose) {
            auto hook = during_propose;
            hook();
        }
        if (should_fail) co_return std::unexpected(make_error_condition(craft::craft_error::NOT_LEADER));
        auto const* hdr = reinterpret_cast< CraftEntryHeader const* >(header.cbytes());
        auto const* p = reinterpret_cast< SyncRSCommitLSNPayload const* >(key.cbytes());
        proposed.push_back(ProposedEntry{.type = hdr->type,
                                         .rs_commit_lsn = p->rs_commit_lsn,
                                         .client_token = p->client_token,
                                         .empty_slots = parse_empty_slots(key).value_or(std::vector< int64_t >{})});
        if (apply_on_commit) {
            cintrusive< homestore::repl_req_ctx > ctx{};
            dev->test_listener().on_commit(1, header, key, {}, ctx);
        }
        co_return ok();
    }
};

} // namespace

// ── test fixture ─────────────────────────────────────────────────────────────

class CraftAppendTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto mock = std::make_unique< MockCraftJournalBackend >();
        journal_ = mock.get();
        dev_ = CraftReplDev::create(volume_id_t{}, std::move(mock), k_page_size, nullptr);
        dev_->set_peer_fetcher(&peer_);
        dev_->set_raft_proposer(&raft_);
        dev_->set_sync_lsn_interval(0); // periodic tests opt in explicitly
        raft_.dev = dev_.get();
    }

    auto do_append(int64_t sync_to, uint64_t client_token = 0) {
        return homeblocks::detail::sync_get(dev_->append(sync_to, client_token));
    }
    auto do_resolve(int64_t upto, uint64_t term = 0) {
        return homeblocks::detail::sync_get(dev_->request_resolution(craft::client_hdr{term, -1, -1}, upto));
    }
    // A zero (all_zeros) write: no payload needed, still a full successful append.
    auto do_zero_write(int64_t dlsn, uint64_t term = 0) {
        return homeblocks::detail::sync_get(
            dev_->write(craft::client_hdr{term, -1, -1}, dlsn, /*addr=*/0, /*len=*/k_page_size, sisl::sg_list{}));
    }

    MockCraftJournalBackend* journal_{nullptr};
    MockCraftPeerFetcher peer_;
    MockCraftRaftProposer raft_;
    std::shared_ptr< CraftReplDev > dev_;
};

namespace {

// ── append(): fail closed ─────────────────────────────────────────────────────

TEST_F(CraftAppendTest, NoRaftGroupIsNotSupported) {
    dev_->set_raft_proposer(nullptr);
    dev_->seed_lsns(5, {});

    auto r = do_append(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::not_supported));
}

TEST_F(CraftAppendTest, FollowerIsNotLeaderAndDoesNoWork) {
    raft_.leader = false;
    dev_->seed_lsns(5, {3});

    auto r = do_append(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(craft::craft_error::NOT_LEADER));
    EXPECT_EQ(peer_.fetch_calls, 0); // no pre-resolution on a follower
    EXPECT_TRUE(raft_.proposed.empty());
    EXPECT_TRUE(dev_->is_missing(3));
}

TEST_F(CraftAppendTest, InvalidSyncToRejected) {
    auto r = do_append(-2);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::invalid_argument));
}

// ── append(): the proposed entry ──────────────────────────────────────────────

TEST_F(CraftAppendTest, ProposesSyncRSCommitLSNCarryingVerdicts) {
    dev_->seed_lsns(5, {3});
    peer_.fetch_response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 3, .is_empty = true}}}};

    auto r = do_append(5, /*client_token=*/42);

    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(raft_.proposed.size(), 1u);
    EXPECT_EQ(raft_.proposed[0].type, CraftEntryType::SyncRSCommitLSN);
    EXPECT_EQ(raft_.proposed[0].rs_commit_lsn, 5);
    EXPECT_EQ(raft_.proposed[0].client_token, 42u);
    EXPECT_EQ(raft_.proposed[0].empty_slots, (std::vector< int64_t >{3}));
}

// Nothing unresolved: no quorum fetch, but the watermark itself is still worth proposing (lagging followers
// catch up to it on apply).
TEST_F(CraftAppendTest, NoUnresolvedSlotsStillProposesWatermark) {
    dev_->seed_lsns(5, {});

    auto r = do_append(5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(peer_.fetch_calls, 0);
    ASSERT_EQ(raft_.proposed.size(), 1u);
    EXPECT_EQ(raft_.proposed[0].rs_commit_lsn, 5);
    EXPECT_TRUE(raft_.proposed[0].empty_slots.empty());
}

// Pre-resolution left slot 4 unresolved (local write failure): the entry stops at 3, and slot 5's verdict --
// above the clamp -- must NOT ride along (apply would reject the whole entry as INVALID_ENTRY).
TEST_F(CraftAppendTest, ClampsBelowFirstUnresolvedAndDropsVerdictsAboveIt) {
    dev_->seed_lsns(2, {});
    peer_.fetch_response = {QuorumSlotResponse{.slots = {
                                                   JournalSlot{.lsn = 3, .lba_off_bytes = 1, .len_bytes = 4},
                                                   JournalSlot{.lsn = 4, .lba_off_bytes = 2, .len_bytes = 4},
                                                   JournalSlot{.lsn = 5, .is_empty = true},
                                               }}};
    journal_->fail_on_write = {4};

    auto r = do_append(5);

    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(raft_.proposed.size(), 1u);
    EXPECT_EQ(raft_.proposed[0].rs_commit_lsn, 3);
    EXPECT_TRUE(raft_.proposed[0].empty_slots.empty());
}

// Clamp lands at -1 (the very first candidate failed): nothing to propose.
TEST_F(CraftAppendTest, ClampBelowZeroProposesNothing) {
    dev_->seed_lsns(-1, {});
    peer_.fetch_response = {QuorumSlotResponse{.slots = {JournalSlot{.lsn = 0, .lba_off_bytes = 1, .len_bytes = 4}}}};
    journal_->fail_on_write = {0};

    auto r = do_append(0);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(raft_.proposed.empty());
}

// ── append(): dedup against the last committed proposal ───────────────────────

TEST_F(CraftAppendTest, NothingNewSinceLastProposalProposesNothing) {
    dev_->seed_lsns(5, {});

    ASSERT_TRUE(do_append(5).has_value());
    ASSERT_TRUE(do_append(5).has_value()); // same watermark
    ASSERT_TRUE(do_append(4).has_value()); // lower watermark
    EXPECT_EQ(raft_.proposed.size(), 1u);

    dev_->seed_lsns(7, {});
    ASSERT_TRUE(do_append(7).has_value()); // advances -- proposed
    EXPECT_EQ(raft_.proposed.size(), 2u);
}

TEST_F(CraftAppendTest, SyncToMinusOneProposesNothing) {
    auto r = do_append(-1);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(raft_.proposed.empty());
}

// ── append(): failure propagation ─────────────────────────────────────────────

TEST_F(CraftAppendTest, ProposeFailurePropagatesAndIsNotRememberedAsProposed) {
    dev_->seed_lsns(5, {});
    raft_.should_fail = true;

    auto r = do_append(5);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(craft::craft_error::NOT_LEADER));

    // The failed entry never committed, so the same watermark must be proposed again, not deduped away.
    raft_.should_fail = false;
    ASSERT_TRUE(do_append(5).has_value());
    EXPECT_EQ(raft_.proposed.size(), 1u);
}

TEST_F(CraftAppendTest, PreResolutionFailurePropagatesWithoutProposing) {
    raft_.members = 5; // majority 3: leader + 1 responder is not enough
    dev_->seed_lsns(5, {3});
    peer_.fetch_response = {QuorumSlotResponse{.slots = {}}};

    auto r = do_append(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(craft::craft_error::NO_QUORUM));
    EXPECT_TRUE(raft_.proposed.empty());
}

// ── request_resolution() ──────────────────────────────────────────────────────

TEST_F(CraftAppendTest, ResolveStaleTermRejectedBeforeAnyWork) {
    dev_->seed_term(3);
    dev_->seed_lsns(5, {3});

    auto r = do_resolve(5, /*term=*/2);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
    EXPECT_EQ(peer_.fetch_calls, 0);
    EXPECT_TRUE(raft_.proposed.empty());
}

TEST_F(CraftAppendTest, ResolveOnFollowerIsNotLeader) {
    raft_.leader = false;
    dev_->seed_lsns(5, {3});

    auto r = do_resolve(5);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(craft::craft_error::NOT_LEADER));
    EXPECT_EQ(peer_.fetch_calls, 0);
}

TEST_F(CraftAppendTest, ResolveInvalidUptoRejected) {
    auto r = do_resolve(-2);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::invalid_argument));
}

// A restart whose overlay rebuild failed faults the partition permanently -- Resolve is gated like every other
// client op (INTERNAL_ERROR). seed_lsns(2) claims slots 0..2 were appended but the journal holds none of them,
// so rebuild_overlay's first read_slot fails.
TEST_F(CraftAppendTest, ResolveRejectedOnFaultedRecovery) {
    dev_->seed_lsns(2, {});
    dev_->trigger_on_restart();
    ASSERT_TRUE(dev_->is_recovery_faulted());

    auto r = do_resolve(2);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INTERNAL_ERROR));
}

// The reply lists every Empty verdict <= resolved_upto: slot 1 from an earlier round (already in empty_lsns_)
// AND slot 3 minted now -- but only the fresh one rides the new entry.
TEST_F(CraftAppendTest, ResolveReturnsEarlierAndFreshVerdicts) {
    dev_->seed_lsns(5, {1, 3});
    dev_->seed_empty({1});
    peer_.fetch_response = {QuorumSlotResponse{.slots = {}}}; // nobody has 3 -> quorum-lacks-evidence

    auto r = do_resolve(5);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->resolved_upto, 5);
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{1, 3}));
    ASSERT_EQ(raft_.proposed.size(), 1u);
    EXPECT_EQ(raft_.proposed[0].empty_slots, (std::vector< int64_t >{3}));
}

// End to end through the leader's own apply: once the entry commits (on_commit -> apply_sync_rs_commit_lsn), a
// repeated Resolve -- e.g. the client's previous reply was lost -- proposes nothing new but still reports the
// verdict, and the leader's commit_lsn has advanced past the Empty slot.
TEST_F(CraftAppendTest, RepeatedResolveAfterApplyReportsVerdictWithoutReproposing) {
    raft_.apply_on_commit = true;
    dev_->seed_lsns(5, {3});
    peer_.fetch_response = {QuorumSlotResponse{.slots = {}}};

    auto r1 = do_resolve(5);
    ASSERT_TRUE(r1.has_value());
    EXPECT_TRUE(dev_->is_empty_slot(3));
    EXPECT_EQ(dev_->commit_lsn(), 5);

    auto r2 = do_resolve(5);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->resolved_upto, 5);
    EXPECT_EQ(r2->empty_slots, (std::vector< int64_t >{3}));
    EXPECT_EQ(raft_.proposed.size(), 1u);
}

// A clamped round reports the clamped watermark, so the client knows only the prefix below the failure is resolved.
TEST_F(CraftAppendTest, ResolveReportsClampedResolvedUpto) {
    dev_->seed_lsns(2, {});
    peer_.fetch_response = {QuorumSlotResponse{.slots = {
                                                   JournalSlot{.lsn = 3, .is_empty = true},
                                                   JournalSlot{.lsn = 4, .lba_off_bytes = 2, .len_bytes = 4},
                                               }}};
    journal_->fail_on_write = {4};

    auto r = do_resolve(5); // 5: nobody has it -> would be Empty, but it's above the clamp

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->resolved_upto, 3);
    EXPECT_EQ(r->empty_slots, (std::vector< int64_t >{3}));
}

// ── background rounds: GetRSCommitLSN poll ────────────────────────────────────

// The round proposes up to max(quorum.last_append), even past this leader's own frontier -- pre-resolution
// then resolves the slots it never received (here: Empty verdicts from the quorum).
TEST_F(CraftAppendTest, BackgroundRoundProposesQuorumMaxLastAppend) {
    dev_->seed_lsns(5, {});
    peer_.poll_response = [] { return std::vector< craft::lsn_pair >{{.commit_lsn = 2, .last_append_lsn = 8}}; };
    peer_.fetch_response = {QuorumSlotResponse{.slots = {
                                                   JournalSlot{.lsn = 6, .is_empty = true},
                                                   JournalSlot{.lsn = 7, .is_empty = true},
                                                   JournalSlot{.lsn = 8, .is_empty = true},
                                               }}};
    dev_->set_peer_fetch_timeout_ms(777);

    dev_->test_fire_background_sync();

    EXPECT_EQ(peer_.poll_calls, 1);
    EXPECT_EQ(peer_.last_poll_is_login, std::optional< bool >{false}); // background polls never quiesce
    EXPECT_EQ(peer_.last_poll_timeout_ms, 777u);
    ASSERT_EQ(raft_.proposed.size(), 1u);
    EXPECT_EQ(raft_.proposed[0].rs_commit_lsn, 8);
    EXPECT_EQ(raft_.proposed[0].empty_slots, (std::vector< int64_t >{6, 7, 8}));
    EXPECT_FALSE(dev_->background_sync_in_flight());
}

// This leader is the furthest ahead: its own last_append is the max.
TEST_F(CraftAppendTest, BackgroundRoundUsesLeaderOwnLastAppendWhenHighest) {
    dev_->seed_lsns(9, {});
    peer_.poll_response = [] { return std::vector< craft::lsn_pair >{{.commit_lsn = 2, .last_append_lsn = 4}}; };

    dev_->test_fire_background_sync();

    ASSERT_EQ(raft_.proposed.size(), 1u);
    EXPECT_EQ(raft_.proposed[0].rs_commit_lsn, 9);
}

TEST_F(CraftAppendTest, BackgroundPollSubMajorityProposesNothing) {
    raft_.members = 5; // majority 3
    dev_->seed_lsns(5, {});
    peer_.poll_response = [] { return std::vector< craft::lsn_pair >{{.commit_lsn = 5, .last_append_lsn = 5}}; };

    dev_->test_fire_background_sync();

    EXPECT_EQ(peer_.poll_calls, 1);
    EXPECT_TRUE(raft_.proposed.empty());
    EXPECT_FALSE(dev_->background_sync_in_flight());
}

// A malformed reply neither counts toward the majority nor contributes its watermark.
TEST_F(CraftAppendTest, BackgroundPollMalformedReplyDiscarded) {
    raft_.members = 5;
    dev_->seed_lsns(5, {});
    peer_.poll_response = [] {
        return std::vector< craft::lsn_pair >{{.commit_lsn = 5, .last_append_lsn = 5},
                                              {.commit_lsn = -7, .last_append_lsn = 1'000}};
    };

    dev_->test_fire_background_sync();

    EXPECT_TRUE(raft_.proposed.empty()); // 1 valid + leader = 2 < 3
}

// A failed round must release the coalescing flag, or every later trigger would be silently dropped forever.
TEST_F(CraftAppendTest, BackgroundPollFailureReleasesInFlightFlag) {
    dev_->seed_lsns(5, {});
    peer_.poll_should_fail = true;

    dev_->test_fire_background_sync();
    EXPECT_TRUE(raft_.proposed.empty());
    EXPECT_FALSE(dev_->background_sync_in_flight());

    peer_.poll_should_fail = false;
    peer_.poll_response = [] { return std::vector< craft::lsn_pair >{{.commit_lsn = 5, .last_append_lsn = 5}}; };
    dev_->test_fire_background_sync();
    EXPECT_EQ(raft_.proposed.size(), 1u);
}

// ── background rounds: gating + coalescing ────────────────────────────────────

TEST_F(CraftAppendTest, BackgroundRoundSilentOnFollower) {
    raft_.leader = false;
    dev_->seed_lsns(5, {});

    dev_->test_fire_background_sync();

    EXPECT_EQ(peer_.poll_calls, 0);
    EXPECT_TRUE(raft_.proposed.empty());
    EXPECT_FALSE(dev_->background_sync_in_flight());
}

TEST_F(CraftAppendTest, BackgroundRoundSilentWithoutRaftGroup) {
    dev_->set_raft_proposer(nullptr);
    dev_->seed_lsns(5, {});

    dev_->test_fire_background_sync();

    EXPECT_EQ(peer_.poll_calls, 0);
    EXPECT_FALSE(dev_->background_sync_in_flight());
}

// A trigger arriving while a round is mid-propose is dropped (not queued, not run concurrently); once the round
// finishes the next trigger runs normally.
TEST_F(CraftAppendTest, BackgroundTriggerCoalescedWhileRoundInFlight) {
    dev_->seed_lsns(5, {});
    peer_.poll_response = [this] {
        return std::vector< craft::lsn_pair >{{.commit_lsn = -1, .last_append_lsn = dev_->last_append_lsn()}};
    };
    bool saw_in_flight = false;
    raft_.during_propose = [this, &saw_in_flight] {
        saw_in_flight = dev_->background_sync_in_flight();
        dev_->test_fire_background_sync(); // must be coalesced away
    };

    dev_->test_fire_background_sync();

    EXPECT_TRUE(saw_in_flight);
    EXPECT_EQ(peer_.poll_calls, 1);
    EXPECT_EQ(raft_.proposed.size(), 1u);
    EXPECT_FALSE(dev_->background_sync_in_flight());

    raft_.during_propose = nullptr;
    dev_->seed_lsns(6, {});
    dev_->test_fire_background_sync();
    EXPECT_EQ(peer_.poll_calls, 2);
    EXPECT_EQ(raft_.proposed.size(), 2u);
}

// An explicit Resolve is never coalesced away by a background round in flight -- the client needs its reply.
TEST_F(CraftAppendTest, ResolveRunsEvenWhileBackgroundRoundInFlight) {
    dev_->seed_lsns(5, {});
    peer_.poll_response = [] { return std::vector< craft::lsn_pair >{{.commit_lsn = -1, .last_append_lsn = 5}}; };
    std::optional< craft::resolution_result > nested;
    raft_.during_propose = [this, &nested] {
        raft_.during_propose = nullptr; // only the outer (background) propose runs the nested Resolve
        dev_->seed_lsns(6, {});
        auto r = homeblocks::detail::sync_get(dev_->request_resolution(craft::client_hdr{0, -1, -1}, 6));
        if (r) nested = *r;
    };

    dev_->test_fire_background_sync();

    ASSERT_TRUE(nested.has_value());
    EXPECT_EQ(nested->resolved_upto, 6);
    EXPECT_EQ(raft_.proposed.size(), 2u); // the Resolve's entry, then the background round's
}

// ── periodic trigger ──────────────────────────────────────────────────────────

TEST_F(CraftAppendTest, PeriodicTriggerFiresEveryIntervalWrites) {
    dev_->set_sync_lsn_interval(4);
    peer_.poll_response = [this] {
        return std::vector< craft::lsn_pair >{{.commit_lsn = -1, .last_append_lsn = dev_->last_append_lsn()}};
    };

    for (int64_t dlsn = 0; dlsn < 8; ++dlsn) {
        ASSERT_TRUE(do_zero_write(dlsn).has_value()) << dlsn;
        EXPECT_EQ(raft_.proposed.size(), static_cast< size_t >((dlsn + 1) / 4)) << "after dlsn " << dlsn;
    }
    ASSERT_EQ(raft_.proposed.size(), 2u);
    EXPECT_EQ(raft_.proposed[0].rs_commit_lsn, 3);
    EXPECT_EQ(raft_.proposed[1].rs_commit_lsn, 7);
}

TEST_F(CraftAppendTest, PeriodicTriggerSilentOnFollower) {
    raft_.leader = false;
    dev_->set_sync_lsn_interval(2);

    for (int64_t dlsn = 0; dlsn < 6; ++dlsn)
        ASSERT_TRUE(do_zero_write(dlsn).has_value());

    EXPECT_EQ(peer_.poll_calls, 0);
    EXPECT_TRUE(raft_.proposed.empty());
}

TEST_F(CraftAppendTest, PeriodicTriggerDisabledByZeroInterval) {
    dev_->set_sync_lsn_interval(0);

    for (int64_t dlsn = 0; dlsn < 6; ++dlsn)
        ASSERT_TRUE(do_zero_write(dlsn).has_value());

    EXPECT_EQ(peer_.poll_calls, 0);
}

// Only successful appends count: a rejected write (stale term) and an idempotent re-write of an already
// appended slot don't advance the counter.
TEST_F(CraftAppendTest, PeriodicTriggerCountsOnlySuccessfulAppends) {
    dev_->set_sync_lsn_interval(2);
    peer_.poll_response = [this] {
        return std::vector< craft::lsn_pair >{{.commit_lsn = -1, .last_append_lsn = dev_->last_append_lsn()}};
    };

    ASSERT_TRUE(do_zero_write(0).has_value());
    ASSERT_FALSE(do_zero_write(1, /*term=*/9).has_value()); // stale term
    ASSERT_TRUE(do_zero_write(0).has_value());              // idempotent
    EXPECT_EQ(peer_.poll_calls, 0);

    ASSERT_TRUE(do_zero_write(1).has_value()); // 2nd successful append
    EXPECT_EQ(peer_.poll_calls, 1);
    EXPECT_EQ(raft_.proposed.size(), 1u);
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    // No iomgr in this binary: watchdog off so write() never arms a real timer (same as test_craft_write.cpp).
    HB_SETTINGS_FACTORY().load_json("{\"craft_watchdog_timeout_ms\": 0}");
    return RUN_ALL_TESTS();
}