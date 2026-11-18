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
// the quiesce is in the fence matrix (test_craft_pre_resolution.cpp).
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
// poll_quorum_lsns returns `replies` as given (each already tagged with its member) and records what it was asked.

class MockCraftLsnPoller : public CraftPeerFetcher {
public:
    std::vector< QuorumLsnResponse > replies;
    bool should_fail{false};
    std::function< void() > on_poll; // runs inside the poll, to change leadership mid-await
    int poll_calls{0};
    uint64_t last_term{0};
    bool last_is_login{false};

    async_result< RsCommitLsnReply > get_rs_commit_lsn(uint64_t, bool) override { co_return RsCommitLsnReply{}; }
    async_result< std::vector< JournalSlot > > fetch_data(const std::vector< int64_t >&, uint32_t) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }
    async_result< std::vector< QuorumSlotResponse > > fetch_from_quorum(std::vector< int64_t >, uint32_t) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
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

// A faulted leader (here, by a verdict for a slot it already committed) can't run a login.
TEST_F(CraftLoginTest, PollOnFaultedLeaderFailsInternalError) {
    dev_->seed_lsns(5, {});
    dev_->seed_commit_lsn(5);
    std::vector< uint8_t > header(sizeof(CraftEntryHeader));
    reinterpret_cast< CraftEntryHeader* >(header.data())->type = CraftEntryType::SyncRSCommitLSN;
    std::vector< uint8_t > key(sync_rs_commit_lsn_key_size(1));
    serialize_sync_rs_commit_lsn(key.data(), /*rs_commit_lsn=*/5, /*client_token=*/0, {3});
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev_->raft_listener().on_commit(1, sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                                    sisl::blob{key.data(), static_cast< uint32_t >(key.size())}, {}, ctx);
    ASSERT_TRUE(dev_->is_faulted());

    auto r = do_login_poll(/*term=*/0);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INTERNAL_ERROR));
    EXPECT_EQ(poller_.poll_calls, 0);
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
