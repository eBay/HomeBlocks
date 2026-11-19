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

// Unit tests for the leader's login steps (S7, SDSTOR-22744), one step at a time through the friended
// CraftLoginTest fixture. Step 1, login_poll() (SDSTOR-22889): quiesce and poll every member, then take the
// watermark. The replica side of the poll is in test_craft_peer_exchange.cpp, and what each entry point does under
// the quiesce is in the fence matrix (test_craft_pre_resolution.cpp). Step 2, login_resolve() (SDSTOR-22890):
// resolve every slot up to the watermark, counting only quiesced members' lacks; pre_resolve_slots() itself is
// covered in test_craft_pre_resolution.cpp.
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles craft_repl_dev.cpp
// directly (same pattern as test_craft_raft_entries.cpp).

#include <gtest/gtest.h>
#include <functional>
#include <map>
#include <vector>
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

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots;

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const&, lba_count_t) override {
        co_return homestore::multi_blk_id{};
    }
    async_status write_slot(int64_t lsn, uint64_t, lba_t lba, lba_count_t len, homestore::multi_blk_id blkid,
                            bool all_zeros, std::vector< homestore::csum_t > const& csums) override {
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
// poll_quorum_lsns returns `replies` and fetch_from_quorum returns `fetched`, each as given (already tagged with its
// member), and records what it was asked.

class MockCraftLsnPoller : public CraftPeerFetcher {
public:
    std::vector< QuorumLsnResponse > replies;
    bool should_fail{false};
    std::function< void() > on_poll; // runs inside the poll, to change leadership mid-await
    int poll_calls{0};
    uint64_t last_term{0};
    bool last_is_login{false};
    std::vector< QuorumSlotResponse > fetched;
    std::function< void() > on_fetch; // runs inside the fetch, to change state mid-await
    std::vector< int64_t > last_fetch;

    async_result< RsCommitLsnReply > get_rs_commit_lsn(uint64_t, bool) override { co_return RsCommitLsnReply{}; }
    async_result< std::vector< JournalSlot > > fetch_data(const std::vector< int64_t >&, uint32_t) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }
    async_result< std::vector< QuorumSlotResponse > > fetch_from_quorum(std::vector< int64_t > lsns,
                                                                        uint32_t) override {
        last_fetch = std::move(lsns);
        if (on_fetch) on_fetch();
        co_return fetched;
    }
    async_result< std::vector< QuorumLsnResponse > > poll_quorum_lsns(uint64_t term, bool is_login, uint32_t) override {
        ++poll_calls;
        last_term = term;
        last_is_login = is_login;
        if (on_poll) on_poll();
        if (should_fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return replies;
    }
};

// One member's poll reply: test_member_id(member) at `term`, with last_append_lsn = last_append.
QuorumLsnResponse reply(uint32_t member, int64_t last_append, uint64_t term = 0) {
    return QuorumLsnResponse{.member = test_member_id(member),
                             .reply = {.lsns = {.commit_lsn = -1, .last_append_lsn = last_append}, .term = term}};
}

// One member's fetch reply: test_member_id(member) holding `slots`, and lacking every other slot asked for.
QuorumSlotResponse holding(uint32_t member, std::vector< JournalSlot > slots = {}) {
    return QuorumSlotResponse{.member = test_member_id(member), .slots = std::move(slots)};
}

// A one-block all_zeros slot at lsn: a fill with no payload to allocate.
JournalSlot zeros(int64_t lsn) {
    return JournalSlot{.lsn = lsn, .all_zeros = true, .lba_off_bytes = 0, .len_bytes = k_page_size};
}

} // namespace

// ── test fixture ─────────────────────────────────────────────────────────────
//
// craft_repl_dev.hpp friends this exact type, so it must sit outside the anonymous namespace. TEST_F bodies are
// subclasses and don't inherit the friendship, so they reach the login steps through these helpers.

class CraftLoginTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto mock = std::make_unique< MockCraftJournalBackend >();
        journal_ = mock.get();
        dev_ = CraftReplDev::create(volume_id_t{}, std::move(mock), k_page_size, nullptr);
        dev_->set_raft_proposer(&proposer_);
        dev_->set_peer_fetcher(&poller_);
    }
    void TearDown() override { EXPECT_EQ(dev_->invariant_violations(), std::vector< std::string >{}); }

    auto do_login_poll(uint64_t term) { return homeblocks::detail::sync_get(dev_->login_poll(term)); }
    auto do_login_resolve(int64_t rs_commit_lsn, std::vector< craft::peer_id_t > quiesced, uint64_t term = 0) {
        return homeblocks::detail::sync_get(dev_->login_resolve(rs_commit_lsn, term, std::move(quiesced)));
    }

    // Commit an entry on this member as RAFT would, with no proposal of its own.
    void commit_entry(CraftEntryType type, std::vector< uint8_t > const& key) {
        std::vector< uint8_t > header(sizeof(CraftEntryHeader));
        reinterpret_cast< CraftEntryHeader* >(header.data())->type = type;
        cintrusive< homestore::repl_req_ctx > ctx{};
        dev_->raft_listener().on_commit(1, sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                                        sisl::blob{key.data(), static_cast< uint32_t >(key.size())}, {}, ctx);
    }
    void commit_login(uint64_t term) {
        std::vector< uint8_t > key(sizeof(InternalLoginPayload));
        *reinterpret_cast< InternalLoginPayload* >(key.data()) = InternalLoginPayload{.client_token = 7, .term = term};
        commit_entry(CraftEntryType::InternalLogin, key);
    }
    void commit_sync(int64_t rs_commit_lsn, std::vector< int64_t > const& empty_slots = {}) {
        std::vector< uint8_t > key(sync_rs_commit_lsn_key_size(empty_slots.size()));
        serialize_sync_rs_commit_lsn(key.data(), rs_commit_lsn, /*client_token=*/0, empty_slots);
        commit_entry(CraftEntryType::SyncRSCommitLSN, key);
    }
    // Faults the partition: a verdict for a slot it has already committed.
    void fault_partition() {
        dev_->seed_lsns(5, {});
        dev_->seed_commit_lsn(5);
        commit_sync(/*rs_commit_lsn=*/5, /*empty_slots=*/{3});
    }

    MockCraftJournalBackend* journal_{nullptr};
    MockCraftLsnPoller poller_;
    MockCraftRaftProposer proposer_;
    std::shared_ptr< CraftReplDev > dev_;
};

namespace {

std::error_condition stale_term() { return craft::make_error_condition(craft::craft_error::STALE_TERM); }

// ── login_poll: the watermark ─────────────────────────────────────────────────

// The poll quiesces at the session's term, and the watermark is the max over every reply it got back: members 1 and
// 2 already make a quorum, but the max comes from member 3. Waiting for every member up to the timeout is the
// transport's part (poll_quorum_lsns()), not tested here.
TEST_F(CraftLoginTest, PollTakesMaxLastAppendOverEveryReply) {
    proposer_.members = 5;
    dev_->seed_lsns(4, {});
    poller_.replies = {reply(1, 7), reply(2, 6), reply(3, 9), reply(4, 8)};

    auto r = do_login_poll(/*term=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->rs_commit_lsn, 9);
    EXPECT_EQ(r->quiesced.size(), 4u);
    EXPECT_EQ(poller_.last_term, 0u);
    EXPECT_TRUE(poller_.last_is_login);
}

// The leader counts itself locally.
TEST_F(CraftLoginTest, PollCountsTheLeadersOwnLastAppend) {
    dev_->seed_lsns(12, {});
    poller_.replies = {reply(1, 7), reply(2, 9)};

    auto r = do_login_poll(/*term=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->rs_commit_lsn, 12);
}

// A Sync committed while the poll is out (an old-session Resolve's, say) moves this leader past what it read before
// the poll. The watermark must cover it: the login truncates to it, and can't truncate below the committed prefix.
TEST_F(CraftLoginTest, PollWatermarkCoversASyncCommittedDuringThePoll) {
    dev_->seed_lsns(4, {});
    poller_.replies = {reply(1, 7), reply(2, 7)};
    poller_.on_poll = [this] { commit_sync(/*rs_commit_lsn=*/9, /*empty_slots=*/{5, 6, 7, 8, 9}); };

    auto r = do_login_poll(/*term=*/0);

    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(dev_->commit_lsn(), 9);
    EXPECT_EQ(r->rs_commit_lsn, 9);
}

// A reply from the leader itself is dropped: it neither counts nor feeds the watermark.
TEST_F(CraftLoginTest, PollDropsTheLeadersOwnReply) {
    dev_->seed_lsns(4, {});
    poller_.replies = {reply(1, 5), reply(0, 50)};

    auto r = do_login_poll(/*term=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->rs_commit_lsn, 5);
    EXPECT_EQ(r->quiesced, (std::vector< craft::peer_id_t >{test_member_id(1)}));
}

// ── login_poll: failures ──────────────────────────────────────────────────────

TEST_F(CraftLoginTest, PollBelowMajorityFailsNoQuorum) {
    proposer_.members = 5;
    poller_.replies = {reply(1, 7)};

    auto r = do_login_poll(/*term=*/0);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::NO_QUORUM));
}

// N=5: a repeat, the leader's own reply and a learner's don't count, so one distinct voter + self < quorum 3.
TEST_F(CraftLoginTest, PollCountsEachVoterOnce) {
    proposer_.members = 5;
    poller_.replies = {reply(1, 7), reply(1, 7), reply(0, 7), reply(7, 7)}; // member 7 isn't a voter

    auto r = do_login_poll(/*term=*/0);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::NO_QUORUM));
}

// A member already past the poll's term has applied a newer login.
TEST_F(CraftLoginTest, PollFailsStaleTermWhenAMemberIsPastTheTerm) {
    dev_->seed_term(3);
    poller_.replies = {reply(1, 7, /*term=*/3), reply(2, 7, /*term=*/4)};

    auto r = do_login_poll(/*term=*/3);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), stale_term());
}

// ...and so is this leader, if the caller's term is behind its own.
TEST_F(CraftLoginTest, PollFailsStaleTermWhenTheLeaderIsPastTheTerm) {
    dev_->seed_term(4);
    poller_.replies = {reply(1, 7, /*term=*/3), reply(2, 7, /*term=*/3)};

    auto r = do_login_poll(/*term=*/3);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), stale_term());
    EXPECT_EQ(poller_.poll_calls, 0); // known before polling: no member is quiesced, nothing waited for
}

TEST_F(CraftLoginTest, PollFailurePropagates) {
    poller_.should_fail = true;

    auto r = do_login_poll(/*term=*/0);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::io_error));
}

TEST_F(CraftLoginTest, PollOnFollowerFailsNotLeaderWithoutPolling) {
    proposer_.leader = false;

    auto r = do_login_poll(/*term=*/0);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::NOT_LEADER));
    EXPECT_EQ(poller_.poll_calls, 0);
}

TEST_F(CraftLoginTest, PollLosingLeadershipMidPollFailsNotLeader) {
    poller_.replies = {reply(1, 7), reply(2, 7)};
    poller_.on_poll = [this] { proposer_.leader = false; };

    auto r = do_login_poll(/*term=*/0);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::NOT_LEADER));
}

TEST_F(CraftLoginTest, PollUnwiredFailsNotSupported) {
    dev_->set_peer_fetcher(nullptr);

    auto r = do_login_poll(/*term=*/0);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::not_supported));
}

TEST_F(CraftLoginTest, PollOnFaultedLeaderFailsInternalError) {
    fault_partition();
    ASSERT_TRUE(dev_->is_faulted());

    auto r = do_login_poll(/*term=*/0);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INTERNAL_ERROR));
    EXPECT_EQ(poller_.poll_calls, 0);
}

// ...and one that faults while the poll is out doesn't report its result.
TEST_F(CraftLoginTest, PollFaultedDuringThePollFailsInternalError) {
    poller_.replies = {reply(1, 7), reply(2, 7)};
    poller_.on_poll = [this] { fault_partition(); };

    auto r = do_login_poll(/*term=*/0);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INTERNAL_ERROR));
}

// A login committed while the poll is out (say, an earlier attempt's InternalLogin whose proposal timed out) makes
// the round's term stale, even while it still waits to apply.
TEST_F(CraftLoginTest, PollFailsStaleTermWhenALoginCommitsDuringThePoll) {
    poller_.replies = {reply(1, 7), reply(2, 7)};
    poller_.on_poll = [this] {
        dev_->pause_apply();
        commit_login(/*term=*/1);
    };

    auto r = do_login_poll(/*term=*/0);
    EXPECT_EQ(dev_->term(), 0u); // committed, not yet applied
    dev_->resume_apply();

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), stale_term());
}

// ── login_poll: the leader's own quiesce ──────────────────────────────────────

// The leader quiesces its own old-session writes before reading its own watermark.
TEST_F(CraftLoginTest, PollQuiescesTheLeadersOwnWrites) {
    dev_->seed_lsns(2, {});
    poller_.replies = {reply(1, 2), reply(2, 2)};
    ASSERT_TRUE(do_login_poll(/*term=*/0).has_value());

    auto w = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{0, -1, -1}, /*dlsn=*/3, /*addr=*/0, k_page_size, sisl::sg_list{}));

    ASSERT_FALSE(w.has_value());
    EXPECT_EQ(w.error(), stale_term());
    EXPECT_FALSE(journal_->slots.contains(3));
}

// ── login_resolve (SDSTOR-22890) ──────────────────────────────────────────────
//
// N=3 unless set: this leader is member 0, the voters are 0..2. `quiesced` is what the login's poll returned.

std::error_condition no_quorum() { return craft::make_error_condition(craft::craft_error::NO_QUORUM); }

// A member the poll didn't quiesce could still ack an old-session write after reporting it lacks the slot, so its
// lack doesn't count: the slot stays unresolved, and the login fails.
TEST_F(CraftLoginTest, ResolveCountsOnlyQuiescedLacks) {
    dev_->seed_lsns(2, {});
    poller_.fetched = {holding(2)};

    auto r = do_login_resolve(/*rs_commit_lsn=*/3, /*quiesced=*/{test_member_id(1)});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), no_quorum());
    EXPECT_FALSE(journal_->slots.contains(3));
}

// ...while the same lack from a quiesced member mints the verdict.
TEST_F(CraftLoginTest, ResolveVerdictsEmptyOnQuiescedLacks) {
    dev_->seed_lsns(2, {});
    poller_.fetched = {holding(2)};

    auto r = do_login_resolve(/*rs_commit_lsn=*/3, /*quiesced=*/{test_member_id(2)});

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, (std::vector< int64_t >{3}));
}

TEST_F(CraftLoginTest, ResolveVerdictsEmptyAtFiveMembers) {
    proposer_.members = 5;
    dev_->seed_lsns(2, {});
    poller_.fetched = {holding(1), holding(2)};

    auto r = do_login_resolve(/*rs_commit_lsn=*/3, /*quiesced=*/{test_member_id(1), test_member_id(2)});

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, (std::vector< int64_t >{3}));
}

// A hole below this leader's own last_append is resolved too, not just the tail above it.
TEST_F(CraftLoginTest, ResolveFillsAHoleBelowTheLeadersLastAppend) {
    dev_->seed_lsns(5, {3});
    poller_.fetched = {holding(1, {zeros(3)})};

    auto r = do_login_resolve(/*rs_commit_lsn=*/5, /*quiesced=*/{test_member_id(1)});

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty());
    EXPECT_EQ(poller_.last_fetch, (std::vector< int64_t >{3}));
    EXPECT_TRUE(journal_->slots.contains(3));
    EXPECT_FALSE(dev_->is_missing(3));
}

// A responder's committed Empty is inherited whether or not the poll quiesced it.
TEST_F(CraftLoginTest, ResolveInheritsACommittedEmptyFromAnyResponder) {
    dev_->seed_lsns(2, {});
    poller_.fetched = {holding(2, {JournalSlot{.lsn = 3, .is_empty = true}})};

    auto r = do_login_resolve(/*rs_commit_lsn=*/3, /*quiesced=*/{test_member_id(1)});

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, (std::vector< int64_t >{3}));
}

// All or nothing: one unresolved slot fails the login, though another was filled (that fill stays: it's real data).
TEST_F(CraftLoginTest, ResolveFailsIfAnySlotStaysUnresolved) {
    dev_->seed_lsns(2, {});
    poller_.fetched = {holding(2, {zeros(3)})}; // member 2 holds 3 but isn't quiesced, so its lack of 4 doesn't count

    auto r = do_login_resolve(/*rs_commit_lsn=*/4, /*quiesced=*/{test_member_id(1)});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), no_quorum());
    EXPECT_TRUE(journal_->slots.contains(3));
}

// A fetch no member answered leaves no evidence at all: at login that's no quorum (a background round reports
// not_supported, as before).
TEST_F(CraftLoginTest, ResolveWithNoFetchReplyFailsNoQuorum) {
    dev_->seed_lsns(2, {});
    poller_.fetched = {};

    auto r = do_login_resolve(/*rs_commit_lsn=*/3, /*quiesced=*/{test_member_id(1), test_member_id(2)});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), no_quorum());
}

// At or below an applied Sync's watermark, a slot was settled present: no fresh Empty, even on quiesced lacks (C2b).
TEST_F(CraftLoginTest, ResolveNeverVerdictsEmptyAtOrBelowTheSettledFloor) {
    dev_->seed_lsns(2, {});
    commit_sync(/*rs_commit_lsn=*/3); // its catch-up fails, so 3 stays missing
    ASSERT_TRUE(dev_->is_missing(3));
    poller_.fetched = {holding(1), holding(2)};

    auto r = do_login_resolve(/*rs_commit_lsn=*/3, /*quiesced=*/{test_member_id(1), test_member_id(2)});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), no_quorum());
}

// A faulted leader can't run a login: its state, the settled floor included, can't be trusted.
TEST_F(CraftLoginTest, ResolveOnFaultedLeaderFailsInternalError) {
    fault_partition();
    ASSERT_TRUE(dev_->is_faulted());

    auto r = do_login_resolve(/*rs_commit_lsn=*/5, /*quiesced=*/{test_member_id(1), test_member_id(2)});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INTERNAL_ERROR));
}

// ...and verdicts computed while it faulted aren't returned: the Sync that faulted it never raised the settled floor.
TEST_F(CraftLoginTest, ResolveFaultedDuringTheFetchFailsInternalError) {
    dev_->seed_lsns(2, {});
    poller_.fetched = {holding(1), holding(2)};
    poller_.on_fetch = [this] { fault_partition(); };

    auto r = do_login_resolve(/*rs_commit_lsn=*/3, /*quiesced=*/{test_member_id(1), test_member_id(2)});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INTERNAL_ERROR));
}

// An old-session write still in flight on this leader owns its slot; until that drains, the login fails and retries.
TEST_F(CraftLoginTest, ResolveLeavesALeadersInFlightWriteUnresolved) {
    dev_->seed_lsns(5, {3});
    dev_->seed_in_flight(3);
    poller_.fetched = {holding(1), holding(2)};

    auto r = do_login_resolve(/*rs_commit_lsn=*/5, /*quiesced=*/{test_member_id(1), test_member_id(2)});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), no_quorum());
}

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
