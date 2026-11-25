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
// covered in test_craft_pre_resolution.cpp. Step 3, login_sync() (SDSTOR-22891): commit the watermark under the
// watermark mutex; the mutex against rounds is covered in test_craft_pre_resolution.cpp, and AsyncMutex itself in
// test_craft_concurrency.cpp. Step 4, login_install() (SDSTOR-22892): commit the new session; the InternalLogin
// entry and its apply are covered in test_craft_raft_entries.cpp.
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles craft_repl_dev.cpp
// directly (same pattern as test_craft_raft_entries.cpp).

#include <gtest/gtest.h>
#include <functional>
#include <latch>
#include <map>
#include <thread>
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
    // When set, the next write_slot() waits for write_gate before it lands: a journal write in flight.
    bool hold_write{false};
    sisl::async::value_awaitable< bool > write_gate;
    std::optional< int64_t > fail_on_read; // read_slot() of this lsn fails with an I/O error
    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const&, lba_count_t) override {
        co_return homestore::multi_blk_id{};
    }
    async_status write_slot(int64_t lsn, uint64_t, lba_t lba, lba_count_t len, homestore::multi_blk_id blkid,
                            bool all_zeros, std::vector< homestore::csum_t > const& csums) override {
        if (std::exchange(hold_write, false)) co_await write_gate;
        slots[lsn] = JournalSlot{
            .lsn = lsn, .all_zeros = all_zeros, .lba_off_bytes = lba, .len_bytes = len, .blkid = blkid, .csums = csums};
        co_return ok();
    }
    async_result< JournalSlot > read_slot(int64_t lsn) override {
        if (fail_on_read && *fail_on_read == lsn)
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return co_await mock_read_slot(*this, lsn);
    }
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

// Receives a coroutine's completion and drops it: lets a test start one and destroy it mid-await.
struct DropReceiver {
    using receiver_concept = stdexec::receiver_t;
    void set_value(auto&&...) && noexcept {}
    void set_error(auto&&) && noexcept {}
    void set_stopped() && noexcept {}
};

// A coroutine started and held, so a test can destroy it while it waits.
struct HeldTask {
    explicit HeldTask(sisl::async::task< void > task) :
            op{stdexec::connect(
                stdexec::write_env(std::move(task), stdexec::prop{stdexec::get_scheduler, exec::inline_scheduler{}}),
                DropReceiver{})} {
        stdexec::start(op);
    }
    decltype(stdexec::connect(stdexec::write_env(std::declval< sisl::async::task< void > >(),
                                                 stdexec::prop{stdexec::get_scheduler, exec::inline_scheduler{}}),
                              DropReceiver{})) op;
};

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
    // Holds the watermark as a login does, for the call.
    auto do_login_sync(int64_t rs_commit_lsn, uint64_t client_token, std::vector< int64_t > empty_slots,
                       uint64_t term = 0) {
        auto lock = homeblocks::detail::sync_get(dev_->watermark_mu_.lock());
        return homeblocks::detail::sync_get(
            dev_->login_sync(lock, rs_commit_lsn, client_token, std::move(empty_slots), term));
    }
    auto do_login_install(int64_t rs_commit_lsn, uint64_t client_token, uint64_t term = 0) {
        auto lock = homeblocks::detail::sync_get(dev_->watermark_mu_.lock());
        return homeblocks::detail::sync_get(dev_->login_install(lock, rs_commit_lsn, client_token, term));
    }
    // Proposals commit and apply on this member, as RAFT would, and return the apply's outcome.
    void loop_back_proposals() {
        proposer_.on_commit = [this](sisl::blob const& h, sisl::blob const& k) { loop_back(proposer_, *dev_, h, k); };
    }

    // Commit an entry on this member as RAFT would, with no proposal of its own.
    void commit_entry(CraftEntryType type, std::vector< uint8_t > const& key) {
        std::vector< uint8_t > header(sizeof(CraftEntryHeader));
        reinterpret_cast< CraftEntryHeader* >(header.data())->type = type;
        cintrusive< homestore::repl_req_ctx > ctx{};
        dev_->raft_listener().on_commit(1, sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                                        sisl::blob{key.data(), static_cast< uint32_t >(key.size())}, {}, ctx);
    }
    // At this member's last_append, where a login's poll would settle: never below a committed Sync's watermark.
    void commit_login(uint64_t term) { commit_login(term, dev_->last_append_lsn()); }
    void commit_login(uint64_t term, int64_t rs_commit_lsn) {
        std::vector< uint8_t > key(sizeof(InternalLoginPayload));
        serialize_internal_login(key.data(), /*client_token=*/7, term, rs_commit_lsn);
        commit_entry(CraftEntryType::InternalLogin, key);
    }
    // A Sync from this member's current session, or from `term` (a newer session's, ahead of its login's apply).
    void commit_sync(int64_t rs_commit_lsn, std::vector< int64_t > const& empty_slots = {},
                     std::optional< uint64_t > term = std::nullopt) {
        std::vector< uint8_t > key(sync_rs_commit_lsn_key_size(empty_slots.size()));
        serialize_sync_rs_commit_lsn(key.data(), rs_commit_lsn, /*client_token=*/0, term.value_or(dev_->term()),
                                     empty_slots);
        commit_entry(CraftEntryType::SyncRSCommitLSN, key);
    }
    // Starts a one-block all-zeros write at dlsn without waiting for it. The returned result is set when it finishes;
    // the coroutine shares it, so one still suspended when a test returns writes live memory.
    std::shared_ptr< std::optional< result< craft::lsn_pair > > > start_write(int64_t dlsn, uint64_t term) {
        auto outcome = std::make_shared< std::optional< result< craft::lsn_pair > > >();
        homeblocks::detail::detach(
            [](std::shared_ptr< CraftReplDev > dev, int64_t dlsn, uint64_t term,
               std::shared_ptr< std::optional< result< craft::lsn_pair > > > out) -> sisl::async::task< void > {
                craft::client_hdr const hdr{term, -1, -1};
                *out = co_await dev->write(hdr, dlsn, /*addr=*/0, k_page_size, sisl::sg_list{});
            }(dev_, dlsn, term, outcome));
        return outcome;
    }
    // Starts login_resolve() at term 0 without waiting for it, sharing its result the same way.
    std::shared_ptr< std::optional< result< std::vector< int64_t > > > >
    start_login_resolve(int64_t rs_commit_lsn, std::vector< craft::peer_id_t > quiesced) {
        auto outcome = std::make_shared< std::optional< result< std::vector< int64_t > > > >();
        homeblocks::detail::detach(
            [](std::shared_ptr< CraftReplDev > dev, int64_t rs, std::vector< craft::peer_id_t > quiesced,
               std::shared_ptr< std::optional< result< std::vector< int64_t > > > > out) -> sisl::async::task< void > {
                *out = co_await dev->login_resolve(rs, /*term=*/0, std::move(quiesced));
            }(dev_, rs_commit_lsn, std::move(quiesced), outcome));
        return outcome;
    }
    // Starts drain_writes() and destroys it while it waits, as a caller that gives up would.
    void abandon_drain() { HeldTask held{dev_->drain_writes()}; }
    // Starts drain_writes() and holds it, so a test can destroy it while it waits.
    std::unique_ptr< HeldTask > hold_drain() { return std::make_unique< HeldTask >(dev_->drain_writes()); }
    // Starts drain_writes() and runs `then` once it returns. `then` should capture by value (shared state, say): the
    // drain may still be waiting when a test returns.
    void start_drain_then(std::function< void() > then) {
        homeblocks::detail::detach(
            [](std::shared_ptr< CraftReplDev > dev, std::function< void() > then) -> sisl::async::task< void > {
                co_await dev->drain_writes();
                then();
            }(dev_, std::move(then)));
    }
    size_t drain_waiters() {
        std::lock_guard lk{dev_->state_mu_};
        return dev_->drain_waiters_.size();
    }
    // Faults the partition: a verdict for a slot it has already committed.
    void fault_partition() {
        dev_->seed_lsns(5, {});
        dev_->seed_commit_lsn(5);
        commit_sync(/*rs_commit_lsn=*/5, /*empty_slots=*/{3});
    }
    // Backs the device's own commits with a test index that records each LBA it unmaps (every write here is
    // all-zeros), and fails at fail_at.
    void use_counting_index(std::optional< lba_t > fail_at = std::nullopt) {
        dev_->use_test_index([](lba_t, lba_t, std::unordered_map< lba_t, BlockInfo >&) -> status { return ok(); },
                             [this, fail_at](lba_t s, lba_t e, std::vector< homestore::blk_id >&) -> status {
                                 if (fail_at && s <= *fail_at && *fail_at <= e)
                                     return std::unexpected(volume_error::INDEX_ERROR);
                                 for (lba_t lba = s; lba <= e; ++lba)
                                     unmapped_.push_back(lba);
                                 return ok();
                             });
    }
    // A one-block all-zeros write at dlsn, to LBA dlsn, with no piggybacked commit.
    auto write_zeros(int64_t dlsn) {
        return homeblocks::detail::sync_get(dev_->write(craft::client_hdr{0, -1, -1}, dlsn,
                                                        /*addr=*/static_cast< uint64_t >(dlsn) * k_page_size,
                                                        k_page_size, sisl::sg_list{}));
    }
    // Starts login_sync() at term 0, holding the watermark as a login does, without waiting for it; `outcome` gets its
    // result when it finishes.
    void start_login_sync(int64_t rs_commit_lsn, std::optional< status >& outcome) {
        homeblocks::detail::detach([](std::shared_ptr< CraftReplDev > dev, int64_t rs,
                                      std::optional< status >& out) -> sisl::async::task< void > {
            auto lock = co_await dev->watermark_mu_.lock();
            out = co_await dev->login_sync(lock, rs, /*client_token=*/42, {}, /*term=*/0);
        }(dev_, rs_commit_lsn, outcome));
    }

    std::vector< lba_t > unmapped_;
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

// An empty quiesced set counts no member's lack (it isn't "no filter"), though replies still supply data: slot 3 is
// filled from member 1, but no lack of 4 counts, so the login fails.
TEST_F(CraftLoginTest, ResolveWithNoQuiescedMemberCountsNoLack) {
    dev_->seed_lsns(2, {});
    poller_.fetched = {holding(1, {zeros(3)}), holding(2)};

    auto r = do_login_resolve(/*rs_commit_lsn=*/4, /*quiesced=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), no_quorum());
    EXPECT_TRUE(journal_->slots.contains(3));
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

// The poll quiesced this leader's own write while it was still in flight: resolution waits for it to land, published
// though not acked, so its slot needs no fetch and the login goes on.
TEST_F(CraftLoginTest, ResolveWaitsForTheLeadersInFlightWrite) {
    dev_->seed_lsns(2, {});
    journal_->hold_write = true;
    auto const w = start_write(/*dlsn=*/3, /*term=*/0);
    ASSERT_TRUE(homeblocks::detail::sync_get(dev_->get_rs_commit_lsn(/*term=*/0, /*is_login=*/true)).has_value());

    auto const resolved = start_login_resolve(/*rs_commit_lsn=*/3, {test_member_id(1), test_member_id(2)});
    EXPECT_FALSE(resolved->has_value()); // waiting for the write

    journal_->write_gate.complete(true);

    ASSERT_TRUE(w->has_value());
    EXPECT_FALSE((*w)->has_value()); // quiesced: kept, not acked
    ASSERT_TRUE(resolved->has_value());
    ASSERT_TRUE((*resolved)->has_value()) << (*resolved)->error().message();
    EXPECT_TRUE((*resolved)->value().empty()); // no Empty verdict
    EXPECT_FALSE(dev_->is_missing(3));
    EXPECT_TRUE(poller_.last_fetch.empty()); // nothing left to fetch
}

// ── login_sync (SDSTOR-22891) ─────────────────────────────────────────────────

// The entry carries the login's watermark, the new client's token and the verdicts, and the leader ends exactly at
// the watermark (slot 5 is Empty, so commit_lsn passes it).
TEST_F(CraftLoginTest, SyncProposesTheWatermarkTokenAndVerdicts) {
    loop_back_proposals();
    dev_->seed_lsns(4, {});

    auto r = do_login_sync(/*rs_commit_lsn=*/5, /*client_token=*/42, /*empty_slots=*/{5});

    ASSERT_TRUE(r.has_value()) << r.error().message();
    ASSERT_EQ(proposer_.proposed.size(), 1u);
    auto const sync = decode(proposer_.proposed[0]);
    EXPECT_EQ(sync.type, CraftEntryType::SyncRSCommitLSN);
    EXPECT_EQ(sync.rs_commit_lsn, 5);
    EXPECT_EQ(sync.client_token, 42u);
    EXPECT_EQ(sync.empty_slots, (std::vector< int64_t >{5}));
    EXPECT_EQ(dev_->commit_lsn(), 5);
}

// Unlike append(), it proposes even when the watermark is already covered.
TEST_F(CraftLoginTest, SyncProposesEvenWhenTheWatermarkIsCovered) {
    loop_back_proposals();
    dev_->seed_lsns(5, {});
    ASSERT_TRUE(do_login_sync(/*rs_commit_lsn=*/5, /*client_token=*/42, /*empty_slots=*/{}).has_value());

    auto r = do_login_sync(/*rs_commit_lsn=*/5, /*client_token=*/43, /*empty_slots=*/{});

    ASSERT_TRUE(r.has_value()) << r.error().message();
    ASSERT_EQ(proposer_.proposed.size(), 2u);
    EXPECT_EQ(decode(proposer_.proposed[1]).client_token, 43u);
}

// An earlier round's Sync past the watermark (a timed-out proposal that committed late) leaves it stale: InternalLogin
// would truncate below the committed prefix.
TEST_F(CraftLoginTest, SyncFailsNoQuorumWhenALateSyncPassedTheWatermark) {
    loop_back_proposals();
    dev_->seed_lsns(7, {});
    commit_sync(/*rs_commit_lsn=*/7);
    ASSERT_EQ(dev_->commit_lsn(), 7);

    auto r = do_login_sync(/*rs_commit_lsn=*/5, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), no_quorum());
}

// A slot below the watermark still missing after the apply stops commit_lsn short of it.
TEST_F(CraftLoginTest, SyncFailsNoQuorumWhenASlotStaysMissing) {
    loop_back_proposals();
    dev_->seed_lsns(5, {3}); // the apply's catch-up can't fetch it

    auto r = do_login_sync(/*rs_commit_lsn=*/5, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), no_quorum());
    EXPECT_EQ(dev_->commit_lsn(), 2);
}

// A late Sync past the watermark whose catch-up can't fetch its new slots leaves commit_lsn at the watermark, but
// InternalLogin would still truncate slots that Sync settled.
TEST_F(CraftLoginTest, SyncFailsNoQuorumBelowAnAppliedSyncsWatermark) {
    loop_back_proposals();
    dev_->seed_lsns(5, {});
    commit_sync(/*rs_commit_lsn=*/7); // nobody serves slots 6 and 7
    ASSERT_EQ(dev_->commit_lsn(), 5);

    auto r = do_login_sync(/*rs_commit_lsn=*/5, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), no_quorum());
}

// An earlier login's timed-out InternalLogin commits ahead of the Sync: the Sync's session is replaced, so it's
// rejected at commit and the watermark doesn't move.
TEST_F(CraftLoginTest, SyncFailsStaleTermWhenALoginCommitsAheadOfIt) {
    dev_->seed_lsns(5, {});
    proposer_.on_commit = [this](sisl::blob const& h, sisl::blob const& k) {
        commit_login(/*term=*/1);
        loop_back(proposer_, *dev_, h, k);
    };

    auto r = do_login_sync(/*rs_commit_lsn=*/5, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), stale_term());
    EXPECT_EQ(dev_->commit_lsn(), -1);
}

// Another leader's InternalLogin commits right behind the Sync, before login_sync() resumes: the Sync applied, but the
// session it was for is gone.
TEST_F(CraftLoginTest, SyncFailsStaleTermWhenALoginCommitsBehindIt) {
    dev_->seed_lsns(5, {});
    proposer_.on_commit = [this](sisl::blob const& h, sisl::blob const& k) {
        loop_back(proposer_, *dev_, h, k);
        commit_login(/*term=*/1);
    };

    auto r = do_login_sync(/*rs_commit_lsn=*/5, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), stale_term());
    EXPECT_EQ(dev_->commit_lsn(), 5);
}

TEST_F(CraftLoginTest, SyncProposalFailurePropagates) {
    proposer_.fail = true;
    dev_->seed_lsns(5, {});

    auto r = do_login_sync(/*rs_commit_lsn=*/5, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::io_error));
}

TEST_F(CraftLoginTest, SyncProposalTimeoutPropagates) {
    proposer_.time_out = true;
    dev_->seed_lsns(5, {});

    auto r = do_login_sync(/*rs_commit_lsn=*/5, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::timed_out));
}

TEST_F(CraftLoginTest, SyncOnFaultedLeaderFailsInternalErrorWithoutProposing) {
    fault_partition();

    auto r = do_login_sync(/*rs_commit_lsn=*/5, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INTERNAL_ERROR));
    EXPECT_TRUE(proposer_.proposed.empty());
}

// The partition faults while the Sync commits: the check after the proposal can't be trusted.
TEST_F(CraftLoginTest, SyncFaultedDuringTheProposalFailsInternalError) {
    dev_->seed_lsns(2, {});
    proposer_.on_commit = [this](sisl::blob const&, sisl::blob const&) { fault_partition(); };

    auto r = do_login_sync(/*rs_commit_lsn=*/2, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INTERNAL_ERROR));
}

// The partition faults while the proposal is out, and the proposal then times out: the permanent fault wins.
TEST_F(CraftLoginTest, SyncFaultedDuringATimedOutProposalFailsInternalError) {
    dev_->seed_lsns(5, {});
    proposer_.on_propose = [this] { fault_partition(); };
    proposer_.time_out = true;

    auto r = do_login_sync(/*rs_commit_lsn=*/5, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INTERNAL_ERROR));
}

// ── login_sync over an index (SDSTOR-26853) ───────────────────────────────────
//
// The tests above run with no index, so the Sync's advance only steps over slots. These back it with one, as a
// volume's would.

// Writes acked with no piggybacked commit: the login's Sync indexes every slot up to its watermark before
// login_sync() returns.
TEST_F(CraftLoginTest, SyncIndexesEverySlotUpToTheWatermark) {
    use_counting_index();
    for (int64_t dlsn = 0; dlsn <= 2; ++dlsn)
        ASSERT_TRUE(write_zeros(dlsn).has_value());
    ASSERT_EQ(dev_->commit_lsn(), -1);
    loop_back_proposals();

    auto r = do_login_sync(/*rs_commit_lsn=*/2, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_TRUE(r.has_value()) << r.error().message();
    EXPECT_EQ(dev_->commit_lsn(), 2);
    EXPECT_EQ(unmapped_, (std::vector< lba_t >{0, 1, 2}));
}

// A slot at or below the watermark that can't be indexed stops the advance below it: the login fails NO_QUORUM, with
// commit_lsn just below that slot.
TEST_F(CraftLoginTest, SyncFailsNoQuorumWhenASlotCantBeIndexed) {
    use_counting_index(/*fail_at=*/1);
    for (int64_t dlsn = 0; dlsn <= 2; ++dlsn)
        ASSERT_TRUE(write_zeros(dlsn).has_value());
    loop_back_proposals();

    auto r = do_login_sync(/*rs_commit_lsn=*/2, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), no_quorum());
    EXPECT_EQ(dev_->commit_lsn(), 0);
    auto const stop = dev_->last_commit_stop(); // the slot login_sync()'s failure names
    ASSERT_TRUE(stop.has_value());
    EXPECT_EQ(stop->lsn, 1);
}

// The same for a slot that can't be read back from the journal.
TEST_F(CraftLoginTest, SyncFailsNoQuorumWhenASlotCantBeReadBack) {
    use_counting_index();
    for (int64_t dlsn = 0; dlsn <= 2; ++dlsn)
        ASSERT_TRUE(write_zeros(dlsn).has_value());
    journal_->fail_on_read = 1;
    loop_back_proposals();

    auto r = do_login_sync(/*rs_commit_lsn=*/2, /*client_token=*/42, /*empty_slots=*/{});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), no_quorum());
    EXPECT_EQ(dev_->commit_lsn(), 0);
    auto const stop = dev_->last_commit_stop(); // the slot login_sync()'s failure names
    ASSERT_TRUE(stop.has_value());
    EXPECT_EQ(stop->lsn, 1);
    EXPECT_EQ(stop->error, std::make_error_condition(std::errc::io_error));
}

// A commit run is still indexing past the watermark when the login's Sync applies: the Sync's own advance returns at
// once, as commit_lsn already covers it, but login_sync() waits for that run to end and then sees commit_lsn past the
// watermark. Not reachable through the protocol (a leader's last_append stays at or below the watermark), so this is
// the defence the early return mustn't remove.
TEST_F(CraftLoginTest, SyncWaitsForARunningCommitBeforeItChecks) {
    use_counting_index();
    for (int64_t dlsn = 0; dlsn <= 3; ++dlsn)
        ASSERT_TRUE(write_zeros(dlsn).has_value());
    auto const del_ok = [](lba_t, lba_t, std::vector< homestore::blk_id >&) -> status { return ok(); };
    auto const write_ok = [](lba_t, lba_t, std::unordered_map< lba_t, BlockInfo >&) -> status { return ok(); };
    ASSERT_TRUE(homeblocks::detail::sync_get(dev_->commit_with(2, write_ok, del_ok)).has_value());
    ASSERT_EQ(dev_->commit_lsn(), 2);
    loop_back_proposals();
    std::optional< status > outcome;

    auto r = homeblocks::detail::sync_get(
        dev_->commit_with(3, write_ok, [&](lba_t s, lba_t, std::vector< homestore::blk_id >&) -> status {
            if (s == 3 && !outcome) {
                start_login_sync(/*rs_commit_lsn=*/2, outcome);
                EXPECT_FALSE(outcome.has_value()); // it waits for this run
            }
            return ok();
        }));

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 3);
    ASSERT_TRUE(outcome.has_value());
    ASSERT_FALSE(outcome->has_value());
    EXPECT_EQ(outcome->error(), no_quorum()); // past the watermark: a late advance
}

// ── login_install (SDSTOR-22892) ──────────────────────────────────────────────

// The entry carries the new client's token, the next term and the watermark, and once it applies the session is
// the new client's.
TEST_F(CraftLoginTest, InstallProposesTheNewSession) {
    loop_back_proposals();

    auto r = do_login_install(/*rs_commit_lsn=*/5, /*client_token=*/42);

    ASSERT_TRUE(r.has_value()) << r.error().message();
    ASSERT_EQ(proposer_.proposed.size(), 1u);
    auto& [header, key] = proposer_.proposed[0];
    EXPECT_EQ(reinterpret_cast< CraftEntryHeader const* >(header.data())->type, CraftEntryType::InternalLogin);
    auto const login = parse_internal_login(sisl::blob{key.data(), static_cast< uint32_t >(key.size())});
    ASSERT_TRUE(login);
    EXPECT_EQ(login->client_token, 42u);
    EXPECT_EQ(login->term, 1u);
    EXPECT_EQ(login->rs_commit_lsn, 5);
    EXPECT_EQ(dev_->term(), 1u);
    EXPECT_EQ(dev_->client_token(), 42u);
}

// An earlier attempt's InternalLogin for the same term commits just ahead: it takes the term, ours is ignored.
TEST_F(CraftLoginTest, InstallFailsStaleTermWhenAnEarlierLoginTookTheTerm) {
    proposer_.on_commit = [this](sisl::blob const& h, sisl::blob const& k) {
        commit_login(/*term=*/1); // token 7
        loop_back(proposer_, *dev_, h, k);
    };

    auto r = do_login_install(/*rs_commit_lsn=*/5, /*client_token=*/42);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), stale_term());
    EXPECT_EQ(dev_->term(), 1u);
    EXPECT_EQ(dev_->client_token(), 7u);
}

// A login that committed since the caller read its term: nothing is proposed.
TEST_F(CraftLoginTest, InstallFailsStaleTermWithoutProposingWhenTheSessionMovedOn) {
    commit_login(/*term=*/1);

    auto r = do_login_install(/*rs_commit_lsn=*/5, /*client_token=*/42, /*term=*/0);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), stale_term());
    EXPECT_TRUE(proposer_.proposed.empty());
}

// Ours applies, but another leader's newer InternalLogin has already committed behind it: the new session is fenced
// before login_install() returns.
TEST_F(CraftLoginTest, InstallFailsStaleTermWhenANewerLoginCommittedBehindIt) {
    proposer_.on_commit = [this](sisl::blob const& h, sisl::blob const& k) {
        loop_back(proposer_, *dev_, h, k);
        dev_->pause_apply();
        commit_login(/*term=*/2);
    };

    auto r = do_login_install(/*rs_commit_lsn=*/5, /*client_token=*/42);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), stale_term());
    EXPECT_EQ(dev_->term(), 1u); // ours did install
    EXPECT_EQ(dev_->client_token(), 42u);
    dev_->resume_apply();
}

// Across a leadership change, another leader's Sync past the login's watermark commits ahead of its InternalLogin:
// the session would start below committed slots, so the entry is rejected.
TEST_F(CraftLoginTest, InstallFailsNoQuorumWhenASyncPassedTheWatermarkAheadOfIt) {
    dev_->seed_lsns(7, {});
    proposer_.on_commit = [this](sisl::blob const& h, sisl::blob const& k) {
        commit_sync(/*rs_commit_lsn=*/7);
        loop_back(proposer_, *dev_, h, k);
    };

    auto r = do_login_install(/*rs_commit_lsn=*/5, /*client_token=*/42);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), no_quorum());
    EXPECT_EQ(dev_->term(), 0u);
}

// A newer login has committed but not yet applied: the fence has risen though the session term hasn't.
TEST_F(CraftLoginTest, InstallFailsStaleTermWithoutProposingWhenALoginIsQueued) {
    dev_->pause_apply();
    commit_login(/*term=*/1);
    ASSERT_EQ(dev_->term(), 0u);

    auto r = do_login_install(/*rs_commit_lsn=*/5, /*client_token=*/42, /*term=*/0);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), stale_term());
    EXPECT_TRUE(proposer_.proposed.empty());
    dev_->resume_apply();
}

TEST_F(CraftLoginTest, InstallOnAFollowerFailsNotLeaderWithoutProposing) {
    proposer_.leader = false;

    auto r = do_login_install(/*rs_commit_lsn=*/5, /*client_token=*/42);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::NOT_LEADER));
    EXPECT_TRUE(proposer_.proposed.empty());
}

TEST_F(CraftLoginTest, InstallProposalFailurePropagates) {
    proposer_.fail = true;

    auto r = do_login_install(/*rs_commit_lsn=*/5, /*client_token=*/42);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::io_error));
    EXPECT_EQ(dev_->term(), 0u);
}

// The entry may still commit later; the client logs in again either way.
TEST_F(CraftLoginTest, InstallProposalTimeoutPropagates) {
    proposer_.time_out = true;

    auto r = do_login_install(/*rs_commit_lsn=*/5, /*client_token=*/42);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::timed_out));
    EXPECT_EQ(dev_->term(), 0u);
}

TEST_F(CraftLoginTest, InstallOnFaultedLeaderFailsInternalErrorWithoutProposing) {
    fault_partition();

    auto r = do_login_install(/*rs_commit_lsn=*/5, /*client_token=*/42);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INTERNAL_ERROR));
    EXPECT_TRUE(proposer_.proposed.empty());
}

// The partition faults while the proposal is out, and the proposal then times out: the permanent fault wins.
TEST_F(CraftLoginTest, InstallFaultedDuringATimedOutProposalFailsInternalError) {
    proposer_.on_propose = [this] { fault_partition(); };
    proposer_.time_out = true;

    auto r = do_login_install(/*rs_commit_lsn=*/5, /*client_token=*/42);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INTERNAL_ERROR));
}

// ── InternalLogin truncation (SDSTOR-22909) ───────────────────────────────────
//
// A login drops this member's slots above its watermark when it commits, in log order with every other entry's
// commit-time changes. Its apply waits for writes still in flight, then starts the session.

// The new session's Sync commits while the login that starts the session still waits to apply (a lagging member):
// the login's truncation mustn't undo what the Sync installed at its commit.
TEST_F(CraftLoginTest, ASyncCommittedBehindAQueuedLoginKeepsItsInstall) {
    dev_->seed_lsns(5, {});
    dev_->pause_apply();
    commit_login(/*term=*/1, /*rs_commit_lsn=*/5);
    commit_sync(/*rs_commit_lsn=*/8, /*empty_slots=*/{7}, /*term=*/1);

    dev_->resume_apply();

    EXPECT_EQ(dev_->term(), 1u);
    EXPECT_EQ(dev_->last_append_lsn(), 8);
    EXPECT_TRUE(dev_->is_missing(6)); // no peer copy to fetch: still missing, not dropped
    EXPECT_TRUE(dev_->is_empty_slot(7));
    EXPECT_TRUE(dev_->is_missing(8));
}

// An old-session write is still in flight when a login commits with a watermark below it: the slot is dropped, the
// write is neither published nor acked when it lands, and the session starts only after it has landed.
TEST_F(CraftLoginTest, AWriteInFlightAboveTheWatermarkIsDropped) {
    dev_->seed_lsns(5, {});
    journal_->hold_write = true;
    auto const w = start_write(/*dlsn=*/7, /*term=*/0);
    ASSERT_FALSE(w->has_value());

    commit_login(/*term=*/1, /*rs_commit_lsn=*/5);

    EXPECT_EQ(dev_->term(), 0u); // the apply waits for the write
    EXPECT_EQ(dev_->last_append_lsn(), 5);
    EXPECT_FALSE(dev_->is_missing(6));

    journal_->write_gate.complete(true);

    ASSERT_TRUE(w->has_value());
    ASSERT_FALSE((*w)->has_value());
    EXPECT_EQ((*w)->error(), stale_term());
    EXPECT_EQ(dev_->term(), 1u);
    EXPECT_EQ(dev_->last_append_lsn(), 5);
    EXPECT_FALSE(dev_->is_missing(6));
    EXPECT_FALSE(dev_->is_missing(7));
    EXPECT_EQ(dev_->overlay_lsn_for(0), -1);
}

// The same write at or below the watermark is the old session's copy of a slot the login settled: it's published when
// it lands, but not acked.
TEST_F(CraftLoginTest, AWriteInFlightAtOrBelowTheWatermarkIsKeptButNotAcked) {
    dev_->seed_lsns(5, {4});
    journal_->hold_write = true;
    auto const w = start_write(/*dlsn=*/4, /*term=*/0);

    commit_login(/*term=*/1, /*rs_commit_lsn=*/5);
    EXPECT_EQ(dev_->term(), 0u); // the apply waits for the write

    journal_->write_gate.complete(true);

    ASSERT_TRUE(w->has_value());
    ASSERT_FALSE((*w)->has_value());
    EXPECT_EQ((*w)->error(), stale_term());
    EXPECT_EQ(dev_->term(), 1u);
    EXPECT_FALSE(dev_->is_missing(4));
    EXPECT_EQ(dev_->overlay_lsn_for(0), 4);
}

// A member the login's poll missed holds old-session slots above the watermark. They go when the login commits there
// (the frontier, the gaps above it, an Empty verdict above it), and the session starts once it applies.
TEST_F(CraftLoginTest, AMemberThatMissedTheLoginTruncatesWhenItCommits) {
    dev_->seed_lsns(10, {7});
    dev_->seed_empty({9});
    dev_->pause_apply();

    commit_login(/*term=*/1, /*rs_commit_lsn=*/5);

    EXPECT_EQ(dev_->last_append_lsn(), 5);
    EXPECT_FALSE(dev_->is_missing(7));
    EXPECT_FALSE(dev_->is_empty_slot(9));
    EXPECT_EQ(dev_->term(), 0u); // not applied yet

    dev_->resume_apply();

    EXPECT_EQ(dev_->term(), 1u);
}

// A login whose term isn't newer than the session's is ignored when it applies, so it truncates nothing either.
TEST_F(CraftLoginTest, ALoginThatIsntNewerTruncatesNothing) {
    dev_->seed_term(1);
    dev_->seed_lsns(10, {7});

    commit_login(/*term=*/1, /*rs_commit_lsn=*/5);

    EXPECT_EQ(dev_->last_append_lsn(), 10);
    EXPECT_TRUE(dev_->is_missing(7));
    EXPECT_EQ(dev_->term(), 1u);
}

// A pre-resolution fill is in flight when another leader's login commits with a watermark below it: the fill is
// dropped when it lands, the round stops, and the login applies once it has landed.
TEST_F(CraftLoginTest, AFillInFlightAboveTheWatermarkIsDropped) {
    dev_->seed_lsns(5, {4});
    poller_.fetched = {holding(1, {zeros(4)}), holding(2)};
    journal_->hold_write = true;
    auto const resolved = start_login_resolve(/*rs_commit_lsn=*/5, {test_member_id(1), test_member_id(2)});
    ASSERT_FALSE(resolved->has_value()); // the fill of 4 is in write_slot

    commit_login(/*term=*/1, /*rs_commit_lsn=*/3);
    EXPECT_EQ(dev_->term(), 0u); // the apply waits for the fill

    journal_->write_gate.complete(true);

    ASSERT_TRUE(resolved->has_value());
    ASSERT_FALSE((*resolved)->has_value());
    EXPECT_EQ((*resolved)->error(), stale_term());
    EXPECT_EQ(dev_->term(), 1u);
    EXPECT_EQ(dev_->last_append_lsn(), 3);
    EXPECT_FALSE(dev_->is_missing(4));
    EXPECT_EQ(dev_->overlay_lsn_for(0), -1);
}

// At or below the login's watermark, the same fill is published when it lands; the round still stops.
TEST_F(CraftLoginTest, AFillInFlightAtOrBelowTheWatermarkIsKept) {
    dev_->seed_lsns(5, {4});
    poller_.fetched = {holding(1, {zeros(4)}), holding(2)};
    journal_->hold_write = true;
    auto const resolved = start_login_resolve(/*rs_commit_lsn=*/5, {test_member_id(1), test_member_id(2)});

    commit_login(/*term=*/1, /*rs_commit_lsn=*/5);
    EXPECT_EQ(dev_->term(), 0u); // the apply waits for the fill

    journal_->write_gate.complete(true);

    ASSERT_TRUE(resolved->has_value());
    ASSERT_FALSE((*resolved)->has_value());
    EXPECT_EQ((*resolved)->error(), stale_term());
    EXPECT_EQ(dev_->term(), 1u);
    EXPECT_FALSE(dev_->is_missing(4));
    EXPECT_EQ(dev_->overlay_lsn_for(0), 4);
}

// Writes 0, 3 and 5 hit the same LBA, with 1, 2 and 4 missing, so none is indexed. A login at 4 drops 5, the LBA's
// newest overlay entry, at commit; its apply makes 3 readable again.
TEST_F(CraftLoginTest, ALoginRepublishesTheWriteItsTruncationUncovered) {
    for (int64_t dlsn : {0, 3, 5}) {
        auto w = homeblocks::detail::sync_get(
            dev_->write(craft::client_hdr{0, -1, -1}, dlsn, /*addr=*/0, k_page_size, sisl::sg_list{}));
        ASSERT_TRUE(w.has_value());
    }
    ASSERT_EQ(dev_->overlay_lsn_for(0), 5);
    dev_->pause_apply();

    commit_login(/*term=*/1, /*rs_commit_lsn=*/4);
    EXPECT_EQ(dev_->overlay_lsn_for(0), -1); // dropped at commit

    dev_->resume_apply();

    EXPECT_EQ(dev_->overlay_lsn_for(0), 3);
    EXPECT_EQ(dev_->term(), 1u);
}

// The same login, but the journal read of slot 3 fails while it re-publishes: the overlay can't be trusted to hold an
// LBA's surviving write, so the partition faults and the session isn't started.
TEST_F(CraftLoginTest, ALoginWhoseRepublishFailsFaultsThePartition) {
    for (int64_t dlsn : {0, 3, 5}) {
        auto w = homeblocks::detail::sync_get(
            dev_->write(craft::client_hdr{0, -1, -1}, dlsn, /*addr=*/0, k_page_size, sisl::sg_list{}));
        ASSERT_TRUE(w.has_value());
    }
    journal_->fail_on_read = 3;

    commit_login(/*term=*/1, /*rs_commit_lsn=*/4);

    EXPECT_TRUE(dev_->is_faulted());
    EXPECT_EQ(dev_->term(), 0u);
}

// ── the write drain ───────────────────────────────────────────────────────────

// The leader's resolution and a login's apply both wait for the same write; when it lands, both go on. The login
// fenced the resolution's session, so that round stops.
TEST_F(CraftLoginTest, TwoDrainsWaitingOnTheSameWriteBothResume) {
    dev_->seed_lsns(2, {});
    journal_->hold_write = true;
    auto const w = start_write(/*dlsn=*/3, /*term=*/0);
    auto const resolved = start_login_resolve(/*rs_commit_lsn=*/3, {test_member_id(1), test_member_id(2)});
    commit_login(/*term=*/1, /*rs_commit_lsn=*/3);
    EXPECT_EQ(drain_waiters(), 2u);

    journal_->write_gate.complete(true);

    ASSERT_TRUE(w->has_value());
    ASSERT_TRUE(resolved->has_value());
    ASSERT_FALSE((*resolved)->has_value());
    EXPECT_EQ((*resolved)->error(), stale_term());
    EXPECT_EQ(dev_->term(), 1u);
    EXPECT_EQ(drain_waiters(), 0u);
}

// A drain whose caller gives up while it waits leaves the waiter list, so the write landing later resumes nothing.
TEST_F(CraftLoginTest, AnAbandonedDrainLeavesTheWaiterList) {
    dev_->seed_lsns(3, {3});
    dev_->seed_in_flight(3);

    abandon_drain();
    EXPECT_EQ(drain_waiters(), 0u);

    dev_->release_in_flight(3); // would resume a destroyed frame if the waiter were still listed
}

// The first drain resumed destroys a second one still waiting: the second has stayed listed, so its own guard takes it
// off the list and nothing resumes its destroyed frame.
TEST_F(CraftLoginTest, ADrainResumedFirstCanDestroyAnotherStillWaiting) {
    dev_->seed_lsns(3, {3});
    dev_->seed_in_flight(3);
    auto const second = std::make_shared< std::unique_ptr< HeldTask > >();
    start_drain_then([second] { second->reset(); });
    *second = hold_drain();
    ASSERT_EQ(drain_waiters(), 2u);

    dev_->release_in_flight(3);

    EXPECT_FALSE(*second);
    EXPECT_EQ(drain_waiters(), 0u);
}

// The drain resumed first starts another write before the second one resumes: the second keeps waiting, for that write.
TEST_F(CraftLoginTest, ADrainWaitsForAWriteStartedByOneResumedAheadOfIt) {
    dev_->seed_lsns(4, {3, 4});
    dev_->seed_in_flight(3);
    auto const second_done = std::make_shared< bool >(false);
    start_drain_then([dev = dev_] { dev->seed_in_flight(4); });
    start_drain_then([second_done] { *second_done = true; });
    ASSERT_EQ(drain_waiters(), 2u);

    dev_->release_in_flight(3);

    EXPECT_FALSE(*second_done);
    EXPECT_EQ(drain_waiters(), 1u);

    dev_->release_in_flight(4);

    EXPECT_TRUE(*second_done);
    EXPECT_EQ(drain_waiters(), 0u);
}

// A write lands on another thread while the drain resumed first is still running: that release leaves the rest to the
// thread already resuming drains, so two releases can't each take a waiter off the list before either runs. The first
// then destroys the second, whose guard takes it off the list, and nothing resumes it.
TEST_F(CraftLoginTest, AReleaseWhileADrainIsResumingLeavesTheRestToIt) {
    dev_->seed_lsns(4, {3, 4});
    dev_->seed_in_flight(3);
    auto const first_running = std::make_shared< std::latch >(1);
    auto const first_may_go_on = std::make_shared< std::latch >(1);
    auto const second = std::make_shared< std::unique_ptr< HeldTask > >();
    start_drain_then([dev = dev_, first_running, first_may_go_on, second] {
        dev->seed_in_flight(4);
        first_running->count_down();
        first_may_go_on->wait();
        second->reset();
    });
    *second = hold_drain();
    ASSERT_EQ(drain_waiters(), 2u);

    std::thread releaser{[dev = dev_] { dev->release_in_flight(3); }};
    first_running->wait();
    dev_->release_in_flight(4); // nothing in flight now, but the first drain is still resuming

    EXPECT_EQ(drain_waiters(), 1u); // the second still waits
    first_may_go_on->count_down();
    releaser.join();

    EXPECT_FALSE(*second);
    EXPECT_EQ(drain_waiters(), 0u);
}

// A login has already replaced this round's session, and a write is in flight (the new session's, say): resolution
// fails STALE_TERM at once rather than wait for it.
TEST_F(CraftLoginTest, ResolveForAReplacedSessionDoesNotWaitForWrites) {
    dev_->seed_term(1);
    dev_->seed_lsns(3, {3});
    dev_->seed_in_flight(3);
    auto const resolved = start_login_resolve(/*rs_commit_lsn=*/3, {test_member_id(1), test_member_id(2)}); // term 0

    EXPECT_TRUE(resolved->has_value()); // didn't wait for the write
    dev_->release_in_flight(3);

    ASSERT_TRUE(resolved->has_value());
    ASSERT_FALSE((*resolved)->has_value());
    EXPECT_EQ((*resolved)->error(), stale_term());
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
