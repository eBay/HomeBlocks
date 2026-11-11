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

// Unit tests for CraftReplDev's two RAFT entry applies -- SyncRSCommitLSN (S5 / SDSTOR-22886) and
// InternalLogin (S5 / SDSTOR-22887) -- and their on_commit dispatch.
//
// SyncRSCommitLSN tests verify:
//   - client_token is carried on the entry for observability only -- it is NOT gated against local state,
//     regardless of what InternalLogin has (or hasn't) established, since SyncRSCommitLSN applies before
//     the InternalLogin that would set it (see the inline comment at the call site for why)
//   - empty_slots are range-validated against rs_commit_lsn and reconciled into empty_lsns_/missing_lsns_
//   - commit_lsn/last_append_lsn advance directly when there's no gap to catch up on
//   - fetch_data is invoked with exactly the missing LSNs when behind, and its response is persisted
//   - a peer response naming an unrequested or duplicate LSN is rejected as a whole batch
//   - catch-up is best-effort: a failed fetch, a failed write_slot, or no peer_fetcher_ at all leaves the
//     affected LSN(s) missing; commit_lsn stalls just below the first unresolved Missing slot (Empty
//     slots are skipped over), never advancing past it regardless of rs_commit_lsn
//   - commit_lsn never decrements
//   - on_commit parses a real serialized SyncRSCommitLSN entry and dispatches correctly (and rejects
//     malformed header/key blobs without touching state)
//   - a checkpoint trigger (SDSTOR-22888) fires once commit_lsn has advanced by at least
//     checkpoint_lsn_interval_ since the last trigger (accumulating across calls, not just within
//     one), is a no-op when unwired, and a trigger failure is logged but never fails the apply
//
// InternalLogin tests verify:
//   - on_commit dispatches to apply_internal_login, which sets client_token/term
//   - a wrong-size key (too short or too long) is rejected, state untouched
//   - a second InternalLogin replaces client_token outright but never regresses term
//   - once applied, write()'s term-fence check reflects the new term end-to-end
//   - InternalLogin establishing a new client_token has no bearing on SyncRSCommitLSN applies -- old or
//     new token, the apply proceeds the same either way
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles craft_repl_dev.cpp
// directly (same pattern as test_craft_truncate.cpp).

#include <gtest/gtest.h>
#include <deque>
#include <map>
#include <optional>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "craft/craft_repl_dev.hpp"
#include "coro_helpers.hpp"
#include "home_blks_config.hpp"
#include "mock_journal_backend.hpp"

SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)
SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)

namespace homeblocks {
namespace {

static constexpr uint32_t k_page_size = 4096;

// A test-controlled suspension point: an awaited gate stays suspended until open() is called (then the
// suspended coroutine resumes inline on the test thread). Lets a test hold an apply or a fetch mid-flight.
struct TestGate {
    bool is_open{false};
    homeblocks::detail::wait_list waiters;
    auto wait() {
        return waiters.wait_until([this] { return is_open; });
    }
    void open() {
        is_open = true;
        waiters.notify_all();
    }
};

// ── journal mock ──────────────────────────────────────────────────────────────
//
// Backed by a std::map so tests can inspect exactly which LSNs got persisted during catch-up.
// fail_on_write optionally injects an I/O error for a specific LSN.

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots;
    std::optional< int64_t > fail_on_write;
    int free_data_calls{0};
    std::optional< int64_t > truncated_to; // last truncate_to() argument, if any
    bool fail_truncate{false};
    TestGate* write_gate{nullptr};              // when set, write_slot suspends until the gate opens
    std::map< int64_t, TestGate* > write_gates; // per-lsn variant of write_gate

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const&, lba_count_t) override {
        co_return homestore::multi_blk_id{};
    }

    async_status write_slot(int64_t lsn, uint64_t /* term */, lba_t lba, lba_count_t len,
                            homestore::multi_blk_id /* blkid */, bool all_zeros,
                            std::vector< homestore::csum_t > const& csums) override {
        if (write_gate) co_await write_gate->wait();
        if (auto it = write_gates.find(lsn); it != write_gates.end()) co_await it->second->wait();
        if (fail_on_write && *fail_on_write == lsn)
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        slots[lsn] =
            JournalSlot{.lsn = lsn, .all_zeros = all_zeros, .lba_off_bytes = lba, .len_bytes = len, .csums = csums};
        co_return ok();
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override { return mock_read_slot(*this, lsn); }

    async_status truncate_to(int64_t lsn) override {
        if (fail_truncate) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        truncated_to = lsn;
        co_return ok();
    }

    async_status free_data(homestore::multi_blk_id) override {
        ++free_data_calls;
        co_return ok();
    }

    TestGate* free_gate{nullptr}; // when set, free_slot suspends until the gate opens
    int free_slot_calls{0};
    async_status free_slot(int64_t lsn) override {
        ++free_slot_calls;
        if (free_gate) co_await free_gate->wait();
        co_return co_await mock_free_slot(*this, lsn);
    }

    async_status read_data(homestore::multi_blk_id, sisl::sg_list&) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    bool has_slot(int64_t lsn) const { return slots.count(lsn) > 0; }
};

// ── peer fetcher mock ─────────────────────────────────────────────────────────
//
// Records the LSN list it was last called with; returns a programmable response or an injected error.

class MockCraftPeerFetcher : public CraftPeerFetcher {
public:
    std::vector< int64_t > last_requested;
    uint32_t last_timeout_ms{0};
    std::vector< JournalSlot > response;
    bool should_fail{false};

    async_result< craft::lsn_pair > get_rs_commit_lsn(uint64_t /* term */, bool /* is_login */) override {
        co_return craft::lsn_pair{};
    }

    TestGate* gate{nullptr}; // when set, fetch_data suspends until the gate opens
    async_result< std::vector< JournalSlot > > fetch_data(const std::vector< int64_t >& lsns,
                                                          uint32_t timeout_ms) override {
        last_requested = lsns;
        last_timeout_ms = timeout_ms;
        if (gate) co_await gate->wait();
        if (should_fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return response;
    }

    // fetch_range: a scripted response queue when non-empty; otherwise the default (delegates to fetch_data).
    // Every request's ranges are recorded.
    std::deque< result< FetchRangeResult > > range_script;
    std::vector< std::vector< LsnRange > > range_requests;
    TestGate* range_gate{nullptr}; // when set, fetch_range suspends until the gate opens
    async_result< FetchRangeResult > fetch_range(std::vector< LsnRange > ranges, uint64_t max_bytes,
                                                 uint32_t timeout_ms) override {
        range_requests.push_back(ranges);
        if (range_gate) co_await range_gate->wait();
        if (!range_script.empty()) {
            auto r = std::move(range_script.front());
            range_script.pop_front();
            co_return r;
        }
        co_return co_await CraftPeerFetcher::fetch_range(std::move(ranges), max_bytes, timeout_ms);
    }

    // Not exercised by this file -- pre_resolve_slots's quorum broadcast is covered by
    // test_craft_pre_resolution.cpp's own mock.
    async_result< std::vector< QuorumSlotResponse > > fetch_from_quorum(std::vector< int64_t >, uint32_t) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }
};

// ── checkpoint trigger mock ───────────────────────────────────────────────────
//
// Records call count / last `force` value; fail_next injects a one-shot error.

struct MockCraftCheckpointTrigger {
    int call_count{0};
    bool last_force{false};
    bool fail_next{false};

    async_status operator()(bool force) {
        ++call_count;
        last_force = force;
        if (fail_next) {
            fail_next = false;
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        co_return ok();
    }
};

// ── wire-format helpers for the on_commit dispatch tests ─────────────────────

sisl::blob as_blob(std::vector< uint8_t >& buf) { return sisl::blob{buf.data(), static_cast< uint32_t >(buf.size())}; }

std::vector< uint8_t > make_header(CraftEntryType type) {
    std::vector< uint8_t > buf(sizeof(CraftEntryHeader));
    reinterpret_cast< CraftEntryHeader* >(buf.data())->type = type;
    return buf;
}

std::vector< uint8_t > make_sync_rs_commit_lsn_key(int64_t rs_commit_lsn, uint64_t client_token,
                                                   const std::vector< int64_t >& empty_slots) {
    std::vector< uint8_t > buf(sync_rs_commit_lsn_key_size(empty_slots.size()));
    serialize_sync_rs_commit_lsn(buf.data(), rs_commit_lsn, client_token, empty_slots);
    return buf;
}

std::vector< uint8_t > make_internal_login_v2_key(uint64_t client_token, uint64_t term, int64_t rs_commit_lsn) {
    std::vector< uint8_t > buf(sizeof(InternalLoginPayloadV2));
    auto* p = reinterpret_cast< InternalLoginPayloadV2* >(buf.data());
    p->client_token = client_token;
    p->term = term;
    p->rs_commit_lsn = rs_commit_lsn;
    return buf;
}

std::vector< uint8_t > make_login_aborted_key(uint64_t abort_upto) {
    std::vector< uint8_t > buf(sizeof(LoginAbortedPayload));
    reinterpret_cast< LoginAbortedPayload* >(buf.data())->abort_upto = abort_upto;
    return buf;
}

std::vector< uint8_t > make_internal_login_key(uint64_t client_token, uint64_t term) {
    std::vector< uint8_t > buf(sizeof(InternalLoginPayload));
    auto* p = reinterpret_cast< InternalLoginPayload* >(buf.data());
    p->client_token = client_token;
    p->term = term;
    return buf;
}

} // namespace

// craft_repl_dev.hpp friends this exact type (homeblocks::CraftRaftEntriesTest) so it can call the
// private apply_sync_rs_commit_lsn directly -- it must NOT sit in the anonymous namespace above, or it
// would be a distinct, unrelated type from the friend's perspective.

// ── test fixture ─────────────────────────────────────────────────────────────

class CraftRaftEntriesTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto mock = std::make_unique< MockCraftJournalBackend >();
        journal_ = mock.get();
        dev_ = CraftReplDev::create(volume_id_t{}, std::move(mock), k_page_size, nullptr);
    }

    auto install(JournalSlot slot, uint64_t term) {
        return homeblocks::detail::sync_get(dev_->install_fetched_slot(slot, term));
    }

    auto do_apply(int64_t rs_commit_lsn, uint64_t client_token, std::vector< int64_t > empty_slots = {}) {
        return homeblocks::detail::sync_get(
            dev_->apply_sync_rs_commit_lsn(rs_commit_lsn, client_token, std::move(empty_slots)));
    }

    MockCraftJournalBackend* journal_{nullptr};
    MockCraftPeerFetcher fetcher_;
    MockCraftCheckpointTrigger trigger_;
    std::shared_ptr< CraftReplDev > dev_;
};

namespace {

// ── client_token is not gated ─────────────────────────────────────────────────

// client_token is carried for observability only -- a mismatch must NOT block the apply. See the inline
// comment in apply_sync_rs_commit_lsn (under state_mu_) for why: SyncRSCommitLSN applies before the
// InternalLogin that would establish state_.client_token, so an equality-fence here would make login
// itself unreachable.
TEST_F(CraftRaftEntriesTest, ClientTokenMismatchDoesNotBlockApply) {
    dev_->seed_lsns(5, {3});
    auto r = do_apply(/*rs_commit_lsn=*/100, /*client_token=*/999);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 100);
    EXPECT_EQ(dev_->commit_lsn(), 2); // stalls at lsn=3, still missing -- no peer_fetcher_ wired
}

// ── empty_slots range validation ──────────────────────────────────────────────

// A negative LSN in empty_slots is nonsensical for a SyncRSCommitLSN verdict -- reject the whole apply,
// the same all-or-nothing gate as a token mismatch, before any state is touched.
TEST_F(CraftRaftEntriesTest, RejectsEmptySlotWithNegativeLSN) {
    dev_->seed_lsns(5, {3});
    auto r = do_apply(/*rs_commit_lsn=*/10, /*client_token=*/0, /*empty_slots=*/{-1});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INVALID_ENTRY));
    EXPECT_EQ(dev_->commit_lsn(), -1);
    EXPECT_EQ(dev_->last_append_lsn(), 5);
    EXPECT_EQ(dev_->missing_count(), 1u);
}

// An empty_slots entry above rs_commit_lsn names a slot the leader never pre-resolved (S5 only resolves
// up to the LSN it proposes) -- reject the whole apply rather than let it poison empty_lsns_ for a slot
// that hasn't even been reached yet.
TEST_F(CraftRaftEntriesTest, RejectsEmptySlotAboveRSCommitLSN) {
    dev_->seed_lsns(5, {3});
    auto r = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{6});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INVALID_ENTRY));
    EXPECT_EQ(dev_->commit_lsn(), -1);
    EXPECT_EQ(dev_->last_append_lsn(), 5);
    EXPECT_EQ(dev_->missing_count(), 1u);
}

// ── empty_slots reconciliation ────────────────────────────────────────────────

TEST_F(CraftRaftEntriesTest, EmptySlotsReconciled) {
    dev_->seed_lsns(5, {3});
    auto r = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3});

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(dev_->is_empty_slot(3));
    EXPECT_FALSE(dev_->is_missing(3));
    EXPECT_EQ(dev_->commit_lsn(), 5);
    // lsn=3 was missing -- never had local data, so verdicting it Empty must not free anything.
    EXPECT_EQ(journal_->free_data_calls, 0);
}

TEST_F(CraftRaftEntriesTest, EmptySlotOverLocalDataFreesBlock) {
    dev_->seed_lsns(5, {});
    journal_->slots[3] = JournalSlot{.lsn = 3, .all_zeros = false, .lba_off_bytes = 0, .len_bytes = 4};

    auto r = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3});

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(dev_->is_empty_slot(3));
    EXPECT_EQ(journal_->free_data_calls, 1);
}

// Double-free guard: an lsn already verdicted Empty by a prior apply must not be re-freed if it
// appears again in a later (redundant/overlapping) SyncRSCommitLSN's empty_slots -- nothing in the
// protocol strictly forbids this, and re-adding it here would double-free the same blkid.
TEST_F(CraftRaftEntriesTest, EmptySlotAlreadyVerdictedNotFreedAgain) {
    dev_->seed_lsns(5, {});
    journal_->slots[3] = JournalSlot{.lsn = 3, .all_zeros = false, .lba_off_bytes = 0, .len_bytes = 4};

    auto first = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3});
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(journal_->free_data_calls, 1);

    auto second = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3});
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(journal_->free_data_calls, 1); // unchanged -- not freed a second time
}

// Same double-free guard, but for a duplicate *within* one empty_slots list rather than across two
// calls
TEST_F(CraftRaftEntriesTest, EmptySlotDuplicatedWithinSameBatchFreedOnce) {
    dev_->seed_lsns(5, {});
    journal_->slots[3] = JournalSlot{.lsn = 3, .all_zeros = false, .lba_off_bytes = 0, .len_bytes = 4};

    auto r = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3, 3});

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(journal_->free_data_calls, 1);
}

// An empty_slots entry can also fall inside the range this same apply newly opens (rather than being
// an already-missing LSN from before) -- it must end up ONLY in empty_lsns_, not re-added to
// missing_lsns_ by the gap-marking step that runs right after reconciliation.
TEST_F(CraftRaftEntriesTest, EmptySlotWithinNewGapRangeNotDoubleTracked) {
    dev_->seed_lsns(0, {});
    auto r = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3});

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(dev_->is_empty_slot(3));
    EXPECT_FALSE(dev_->is_missing(3));
    // The rest of the newly-opened gap range (1, 2, 4, 5) is still missing -- no peer_fetcher_ wired.
    EXPECT_TRUE(dev_->is_missing(1));
    EXPECT_TRUE(dev_->is_missing(2));
    EXPECT_TRUE(dev_->is_missing(4));
    EXPECT_TRUE(dev_->is_missing(5));
    EXPECT_EQ(dev_->missing_count(), 4u);
    EXPECT_EQ(dev_->commit_lsn(), 0); // stalls at lsn=1, still missing -- no peer_fetcher_ wired
    // lsn=3 was beyond last_append_lsn (0) at apply time -- never locally appended, so nothing to free.
    EXPECT_EQ(journal_->free_data_calls, 0);
}

// ── watermark advance ──────────────────────────────────────────────────────────

// last_append_lsn already covers rs_commit_lsn: nothing to fetch, commit_lsn advances directly.
TEST_F(CraftRaftEntriesTest, NoGapAdvancesDirectly) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(10, {});

    auto r = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 5);
    EXPECT_TRUE(fetcher_.last_requested.empty());
}

TEST_F(CraftRaftEntriesTest, CommitLsnNeverDecrements) {
    dev_->seed_lsns(10, {});
    dev_->seed_commit_lsn(8);

    auto r = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 8);
    EXPECT_EQ(dev_->last_append_lsn(), 10);
}

// ── catch-up ───────────────────────────────────────────────────────────────────

// Behind rs_commit_lsn: fetch_data is called with exactly the missing LSNs, and its response
// (one present slot, one Empty slot) is persisted/marked correctly.
TEST_F(CraftRaftEntriesTest, BehindWithPeerFetcherAppliesFetchedSlots) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(0, {});
    fetcher_.response = {
        JournalSlot{.lsn = 1, .lba_off_bytes = 10, .len_bytes = 4},
        JournalSlot{.lsn = 2, .is_empty = true},
    };

    auto r = do_apply(/*rs_commit_lsn=*/2, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(fetcher_.last_requested, (std::vector< int64_t >{1, 2}));
    EXPECT_TRUE(journal_->has_slot(1));
    EXPECT_TRUE(dev_->is_empty_slot(2));
    EXPECT_FALSE(dev_->is_missing(1));
    EXPECT_FALSE(dev_->is_missing(2));
    EXPECT_EQ(dev_->commit_lsn(), 2);
    EXPECT_EQ(dev_->last_append_lsn(), 2);
}

// set_peer_fetch_timeout_ms() threads the configured deadline through to fetch_data verbatim.
TEST_F(CraftRaftEntriesTest, BehindPassesConfiguredTimeoutToPeerFetcher) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->set_peer_fetch_timeout_ms(1234);
    dev_->seed_lsns(0, {});
    fetcher_.response = {JournalSlot{.lsn = 1, .lba_off_bytes = 10, .len_bytes = 4}};

    auto r = do_apply(/*rs_commit_lsn=*/1, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(fetcher_.last_timeout_ms, 1234u);
}

// fetch_data's contract is one entry per requested LSN. A response naming an LSN we never asked for (a
// buggy/misbehaving peer) can't be partially trusted -- since gap-marking already advanced by this point
// in the apply, this can't gate the whole apply the way the upfront empty_slots check does, but it CAN
// still refuse the batch: none of the response is applied (same outcome as a fetch failure), and commit_lsn
// stalls at the first still-missing lsn (best-effort forward progress, not a jump to rs_commit_lsn).
TEST_F(CraftRaftEntriesTest, BehindRejectsPeerResponseWithUnrequestedLSN) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(0, {});
    fetcher_.response = {
        JournalSlot{.lsn = 1, .lba_off_bytes = 10, .len_bytes = 4}, JournalSlot{.lsn = 2, .is_empty = true},
        JournalSlot{.lsn = 99, .is_empty = true}, // never requested -- only 1 and 2 were
    };

    auto r = do_apply(/*rs_commit_lsn=*/2, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_FALSE(journal_->has_slot(1));
    EXPECT_FALSE(dev_->is_empty_slot(2));
    EXPECT_FALSE(dev_->is_empty_slot(99));
    EXPECT_EQ(dev_->missing_count(), 2u); // 1 and 2 both remain missing
    EXPECT_EQ(dev_->commit_lsn(), 0);     // stalls at lsn=1, still missing
}

// A duplicate entry for an actually-requested LSN is just as much a contract violation as an
// unrequested one (validate_fetch_response catches both the same way) -- same whole-batch rejection.
TEST_F(CraftRaftEntriesTest, BehindRejectsPeerResponseWithDuplicateLSN) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(0, {});
    fetcher_.response = {
        JournalSlot{.lsn = 1, .lba_off_bytes = 10, .len_bytes = 4},
        JournalSlot{.lsn = 1, .lba_off_bytes = 20, .len_bytes = 4}, // duplicate
    };

    auto r = do_apply(/*rs_commit_lsn=*/1, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_FALSE(journal_->has_slot(1));
    EXPECT_EQ(dev_->missing_count(), 1u);
    EXPECT_EQ(dev_->commit_lsn(), 0); // stalls at lsn=1, still missing
}

// fetch_data fails outright: commit_lsn stalls at the first missing lsn (best-effort, not a jump to
// rs_commit_lsn); every spanned LSN remains missing.
TEST_F(CraftRaftEntriesTest, BehindFetchFailsCommitLsnStallsAtFirstMissing) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(0, {});
    fetcher_.should_fail = true;

    auto r = do_apply(/*rs_commit_lsn=*/3, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 0); // stalls at lsn=1, still missing
    EXPECT_EQ(dev_->last_append_lsn(), 3);
    EXPECT_EQ(dev_->missing_count(), 3u);
}

// No peer_fetcher_ wired at all (S9 not wired yet): same best-effort outcome as a fetch failure.
TEST_F(CraftRaftEntriesTest, BehindNoPeerFetcherCommitLsnStallsAtFirstMissing) {
    dev_->seed_lsns(0, {});

    auto r = do_apply(/*rs_commit_lsn=*/2, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 0); // stalls at lsn=1, still missing
    EXPECT_EQ(dev_->missing_count(), 2u);
}

// A fetched slot's write_slot fails: that LSN alone stays missing; the rest of catch-up still applies,
// and commit_lsn advances up to (but not past) it.
TEST_F(CraftRaftEntriesTest, WriteSlotFailureDuringCatchupLeavesLsnMissing) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(0, {});
    fetcher_.response = {
        JournalSlot{.lsn = 1, .lba_off_bytes = 1, .len_bytes = 4},
        JournalSlot{.lsn = 2, .lba_off_bytes = 2, .len_bytes = 4},
    };
    journal_->fail_on_write = 2;

    auto r = do_apply(/*rs_commit_lsn=*/2, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(journal_->has_slot(1));
    EXPECT_FALSE(journal_->has_slot(2));
    EXPECT_FALSE(dev_->is_missing(1));
    EXPECT_TRUE(dev_->is_missing(2));
    EXPECT_EQ(dev_->commit_lsn(), 1); // lsn=1 resolved; stalls at lsn=2, still missing
}

// ── checkpoint trigger (SDSTOR-22888) ─────────────────────────────────────────

TEST_F(CraftRaftEntriesTest, CheckpointTriggerFiresOnceIntervalCrossed) {
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(5);
    dev_->seed_lsns(10, {});

    auto r = do_apply(/*rs_commit_lsn=*/10, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 10);
    EXPECT_EQ(trigger_.call_count, 1);
    EXPECT_FALSE(trigger_.last_force);
}

TEST_F(CraftRaftEntriesTest, CheckpointTriggerDoesNotFireBelowInterval) {
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(5);
    dev_->seed_lsns(3, {});

    auto r = do_apply(/*rs_commit_lsn=*/3, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 3);
    EXPECT_EQ(trigger_.call_count, 0);
}

// Two applies whose individual advances each stay below the interval on their own, but whose
// combined progress since the last trigger crosses it on the second call -- the interval tracks
// cumulative distance from last_checkpoint_lsn_, not distance moved within a single apply.
TEST_F(CraftRaftEntriesTest, CheckpointTriggerAccumulatesAcrossCalls) {
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(5);
    dev_->seed_lsns(10, {});

    auto r1 = do_apply(/*rs_commit_lsn=*/3, /*client_token=*/0);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 3);
    EXPECT_EQ(trigger_.call_count, 0);

    auto r2 = do_apply(/*rs_commit_lsn=*/4, /*client_token=*/0);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 4);
    EXPECT_EQ(trigger_.call_count, 1);
}

// Pins down the exact boundary (>=, not >): delta from last_checkpoint_lsn_ (-1) to commit_lsn (4)
// is exactly 5, equal to the interval, not one past it.
TEST_F(CraftRaftEntriesTest, CheckpointTriggerFiresExactlyAtIntervalBoundary) {
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(5);
    dev_->seed_lsns(4, {});

    auto r = do_apply(/*rs_commit_lsn=*/4, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 4);
    EXPECT_EQ(trigger_.call_count, 1);
}

// A single call can advance commit_lsn by far more than one interval's width (e.g. a large
// catch-up). The claim must reset last_checkpoint_lsn_ to the ACTUAL commit_lsn reached (50), not
// to last_checkpoint_lsn_ + interval (-1 + 5 = 4) -- the two are indistinguishable in
// CheckpointTriggerAccumulatesAcrossCalls above (both land on 4 there), so this pins it down with a
// jump big enough to tell them apart: a second, small follow-up advance must NOT refire, which it
// would if the baseline had been left at 4 instead of 50.
TEST_F(CraftRaftEntriesTest, CheckpointTriggerBaselineTracksActualReachedValue) {
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(5);
    dev_->seed_lsns(60, {});

    auto r1 = do_apply(/*rs_commit_lsn=*/50, /*client_token=*/0);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 50);
    EXPECT_EQ(trigger_.call_count, 1);

    auto r2 = do_apply(/*rs_commit_lsn=*/51, /*client_token=*/0);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 51);
    EXPECT_EQ(trigger_.call_count, 1); // delta since the real baseline (50) is only 1 -- must not refire
}

// No checkpoint_trigger_ wired (production not yet wired, same posture as peer_fetcher_): crossing
// the interval must not crash or fail the apply, just skip the trigger.
TEST_F(CraftRaftEntriesTest, CheckpointTriggerNoOpsWhenUnwired) {
    dev_->set_checkpoint_lsn_interval(5);
    dev_->seed_lsns(10, {});

    auto r = do_apply(/*rs_commit_lsn=*/10, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 10);
}

// A checkpoint trigger failure is logged, not propagated -- best-effort, same posture as this
// function's catch-up/fetch failure handling.
TEST_F(CraftRaftEntriesTest, CheckpointTriggerFailureDoesNotFailApply) {
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(5);
    trigger_.fail_next = true;
    dev_->seed_lsns(10, {});

    auto r = do_apply(/*rs_commit_lsn=*/10, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 10);
    EXPECT_EQ(trigger_.call_count, 1);
}

// ── on_commit dispatch ─────────────────────────────────────────────────────────

TEST_F(CraftRaftEntriesTest, OnCommitDispatchesSyncRSCommitLSN) {
    auto header_buf = make_header(CraftEntryType::SyncRSCommitLSN);
    auto key_buf = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/7, /*client_token=*/0, /*empty_slots=*/{});
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    // last_append_lsn advances unconditionally
    EXPECT_EQ(dev_->last_append_lsn(), 7);
}

TEST_F(CraftRaftEntriesTest, OnCommitRejectsHeaderTooSmall) {
    std::vector< uint8_t > empty_header;
    auto key_buf = make_sync_rs_commit_lsn_key(7, 0, {});
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->test_listener().on_commit(1, as_blob(empty_header), as_blob(key_buf), {}, ctx);

    EXPECT_EQ(dev_->commit_lsn(), -1); // untouched
}

TEST_F(CraftRaftEntriesTest, OnCommitRejectsMalformedSyncRSCommitLSNKey) {
    auto header_buf = make_header(CraftEntryType::SyncRSCommitLSN);
    std::vector< uint8_t > short_key(sizeof(SyncRSCommitLSNPayload) - 1, 0);
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(short_key), {}, ctx);

    EXPECT_EQ(dev_->commit_lsn(), -1); // untouched
}

// Distinct from the too-short case above: this key is large enough for the fixed prefix (and even
// carries 2 real trailing slots), but lies about how many follow -- parse_empty_slots's exact-size
// check (not on_commit's coarser size check) is what rejects it.
TEST_F(CraftRaftEntriesTest, OnCommitRejectsMismatchedEmptySlotsCount) {
    auto header_buf = make_header(CraftEntryType::SyncRSCommitLSN);
    auto key_buf = make_sync_rs_commit_lsn_key(7, 0, {10, 20});
    reinterpret_cast< SyncRSCommitLSNPayload* >(key_buf.data())->num_empty_slots = 5;
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    EXPECT_EQ(dev_->commit_lsn(), -1); // untouched
}

TEST_F(CraftRaftEntriesTest, OnCommitIgnoresUnrecognizedEntryType) {
    std::vector< uint8_t > header_buf(sizeof(CraftEntryHeader));
    reinterpret_cast< CraftEntryHeader* >(header_buf.data())->type = static_cast< CraftEntryType >(99);
    std::vector< uint8_t > key_buf;
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    EXPECT_EQ(dev_->commit_lsn(), -1); // untouched
}

// ── InternalLogin apply (SDSTOR-22887) ────────────────────────────────────────

TEST_F(CraftRaftEntriesTest, OnCommitDispatchesInternalLogin) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    auto key_buf = make_internal_login_key(/*client_token=*/42, /*term=*/5);
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    EXPECT_EQ(dev_->client_token(), 42u);
    EXPECT_EQ(dev_->term(), 5u);
}

TEST_F(CraftRaftEntriesTest, OnCommitRejectsInternalLoginWrongSize) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    std::vector< uint8_t > short_key(sizeof(InternalLoginPayload) - 1, 0);
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(short_key), {}, ctx);

    EXPECT_EQ(dev_->client_token(), 0u); // untouched
    EXPECT_EQ(dev_->term(), 0u);         // untouched
}

// Unlike SyncRSCommitLSN's coarser "at least the fixed prefix" check (it has variable trailing data),
// InternalLoginPayload never does -- on_commit's check is an exact-size `!=`, so a key that's too LARGE
// must be rejected just as much as one that's too small.
TEST_F(CraftRaftEntriesTest, OnCommitRejectsInternalLoginKeyTooLarge) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    std::vector< uint8_t > long_key(sizeof(InternalLoginPayload) + 1, 0);
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(long_key), {}, ctx);

    EXPECT_EQ(dev_->client_token(), 0u); // untouched
    EXPECT_EQ(dev_->term(), 0u);         // untouched
}

// "A second InternalLogin invalidates any existing session before establishing the new one" (ticket) --
// the second apply's values must win outright, not merge with the first's. Driven through on_commit
// (apply_internal_login itself is private, and TEST_F bodies live in a class derived from
// CraftRaftEntriesTest -- friendship doesn't propagate to it, so the public dispatch path is used here).
TEST_F(CraftRaftEntriesTest, SecondInternalLoginReplacesSession) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    cintrusive< homestore::repl_req_ctx > ctx{};

    auto key1 = make_internal_login_key(/*client_token=*/1, /*term=*/1);
    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key1), {}, ctx);
    auto key2 = make_internal_login_key(/*client_token=*/2, /*term=*/2);
    dev_->test_listener().on_commit(2, as_blob(header_buf), as_blob(key2), {}, ctx);

    EXPECT_EQ(dev_->client_token(), 2u);
    EXPECT_EQ(dev_->term(), 2u);
}

TEST_F(CraftRaftEntriesTest, InternalLoginTermNeverRegresses) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    cintrusive< homestore::repl_req_ctx > ctx{};

    auto key1 = make_internal_login_key(/*client_token=*/1, /*term=*/5);
    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key1), {}, ctx);
    auto key2 = make_internal_login_key(/*client_token=*/2, /*term=*/3);
    dev_->test_listener().on_commit(2, as_blob(header_buf), as_blob(key2), {}, ctx);

    EXPECT_EQ(dev_->term(), 5u);
}

// A stale InternalLogin (term at or below the current session) is a replayed or superseded entry and
// changes nothing -- not even client_token. (Previously the token was overwritten regardless; that let a
// replayed old login rebind the session to a deposed client.)
TEST_F(CraftRaftEntriesTest, InternalLoginIgnoredEntirelyWhenTermRegresses) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    cintrusive< homestore::repl_req_ctx > ctx{};

    auto key1 = make_internal_login_key(/*client_token=*/1, /*term=*/5);
    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key1), {}, ctx);
    auto key2 = make_internal_login_key(/*client_token=*/99, /*term=*/3); // lower term, different token
    dev_->test_listener().on_commit(2, as_blob(header_buf), as_blob(key2), {}, ctx);

    EXPECT_EQ(dev_->term(), 5u);         // guarded against regression
    EXPECT_EQ(dev_->client_token(), 1u); // NOT overwritten: the whole entry is ignored
}

// Happy-path regression check for the write()-term-check-under-lock reorg: a matching term still
// succeeds normally (only the mismatch path changed).
TEST_F(CraftRaftEntriesTest, WriteSucceedsWithMatchingTermAfterInternalLogin) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    auto key_buf = make_internal_login_key(/*client_token=*/1, /*term=*/5);
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    auto r = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{.term = 5, .commit_lsn = -1, .all_committed_lsn = -1}, /*dlsn=*/1,
                    /*addr=*/0, /*len=*/k_page_size, sisl::sg_list{})); // empty data -> all_zeros write

    ASSERT_TRUE(r.has_value());
}

// End-to-end: once InternalLogin moves state_.term forward, a write() still presenting the old term is
// fenced out on its very next call -- the "invalidation" this ticket calls for, and the regression test
// for write()'s term-check-under-lock fix.
TEST_F(CraftRaftEntriesTest, WriteRejectsStaleTermAfterInternalLogin) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    auto key_buf = make_internal_login_key(/*client_token=*/1, /*term=*/5);
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    auto r = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{.term = 4, .commit_lsn = -1, .all_committed_lsn = -1}, /*dlsn=*/1,
                    /*addr=*/0, /*len=*/k_page_size, sisl::sg_list{})); // empty data -> all_zeros write

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
}

// InternalLogin establishing a new client_token has no bearing on SyncRSCommitLSN applies -- neither the
// stale/old token nor the newly-established one gates the apply; both succeed identically. Guards against
// reintroducing an equality-fence keyed off InternalLogin's client_token.
TEST_F(CraftRaftEntriesTest, SyncRSCommitLSNAppliesRegardlessOfInternalLoginToken) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    auto key_buf = make_internal_login_key(/*client_token=*/7, /*term=*/1);
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    auto stale = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0); // the old default -- would have been a mismatch
    ASSERT_TRUE(stale.has_value());

    auto fresh = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/7); // the token InternalLogin just set
    ASSERT_TRUE(fresh.has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 5);
}

// ── InternalLogin v2, ordered apply, fence, LoginAborted (docs/craft/resync.md §4.3, §4.6) ──────────

namespace {
void commit_entry(CraftReplDev& dev, int64_t raft_lsn, CraftEntryType type, std::vector< uint8_t > key) {
    auto header_buf = make_header(type);
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev.test_listener().on_commit(raft_lsn, as_blob(header_buf), as_blob(key), {}, ctx);
}

auto write_at(CraftReplDev& dev, uint64_t term, int64_t dlsn) {
    return homeblocks::detail::sync_get(
        dev.write(craft::client_hdr{.term = term, .commit_lsn = -1, .all_committed_lsn = -1}, dlsn, /*addr=*/0,
                  /*len=*/k_page_size, sisl::sg_list{})); // empty data -> all_zeros write
}
} // namespace

// v2 carries rs_commit_lsn: the stale-session tail above it is truncated before the new term is admitted.
TEST_F(CraftRaftEntriesTest, InternalLoginV2TruncatesAboveRsCommitLsn) {
    dev_->seed_lsns(/*last_append=*/10, {});
    commit_entry(*dev_, 1, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 5, /*rs=*/6));

    ASSERT_TRUE(journal_->truncated_to.has_value());
    EXPECT_EQ(*journal_->truncated_to, 6);
    EXPECT_EQ(dev_->last_append_lsn(), 6);
    EXPECT_EQ(dev_->term(), 5u);
    EXPECT_EQ(dev_->client_token(), 42u);
}

TEST_F(CraftRaftEntriesTest, InternalLoginV2NoTruncateWhenNothingAboveRs) {
    dev_->seed_lsns(/*last_append=*/6, {});
    commit_entry(*dev_, 1, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 5, /*rs=*/6));

    EXPECT_FALSE(journal_->truncated_to.has_value());
    EXPECT_EQ(dev_->term(), 5u);
}

// v1 entries (no rs_commit_lsn) still decode and admit the session, but cannot truncate.
TEST_F(CraftRaftEntriesTest, InternalLoginV1StillAppliesWithoutTruncation) {
    dev_->seed_lsns(/*last_append=*/10, {});
    commit_entry(*dev_, 1, CraftEntryType::InternalLogin, make_internal_login_key(42, 5));

    EXPECT_FALSE(journal_->truncated_to.has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 10);
    EXPECT_EQ(dev_->term(), 5u);
}

// If truncation fails, the new session is NOT admitted: admitting it over an untruncated stale tail could
// put two sessions' data in one slot.
TEST_F(CraftRaftEntriesTest, InternalLoginTruncateFailureDoesNotAdmitSession) {
    dev_->seed_lsns(/*last_append=*/10, {});
    journal_->fail_truncate = true;
    commit_entry(*dev_, 1, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 5, /*rs=*/6));

    EXPECT_EQ(dev_->term(), 0u);
    EXPECT_EQ(dev_->client_token(), 0u);
    auto r = write_at(*dev_, 5, 7);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
}

// commit_lsn above rs_commit_lsn cannot happen with a correct leader; the apply refuses to truncate committed
// data but still admits the session.
TEST_F(CraftRaftEntriesTest, InternalLoginSkipsTruncationBelowCommitLsn) {
    dev_->seed_lsns(/*last_append=*/10, {});
    dev_->seed_commit_lsn(8);
    commit_entry(*dev_, 1, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 5, /*rs=*/6));

    EXPECT_FALSE(journal_->truncated_to.has_value());
    EXPECT_EQ(dev_->term(), 5u);
}

// A RAFT entry delivered again at an already-applied index (restart replay, duplicate callback) is dropped.
TEST_F(CraftRaftEntriesTest, ReplayedRaftIndexIsSkipped) {
    commit_entry(*dev_, 7, CraftEntryType::InternalLogin, make_internal_login_v2_key(1, 2, -1));
    commit_entry(*dev_, 7, CraftEntryType::InternalLogin, make_internal_login_v2_key(9, 9, -1)); // same index
    commit_entry(*dev_, 6, CraftEntryType::InternalLogin, make_internal_login_v2_key(9, 9, -1)); // older index

    EXPECT_EQ(dev_->term(), 2u);
    EXPECT_EQ(dev_->client_token(), 1u);
    EXPECT_EQ(dev_->last_applied_raft_idx(), 7);
}

// Entries apply in RAFT order: the login's SyncRSCommitLSN installs its Empty verdict before the following
// InternalLogin truncates, so the verdict survives and the truncation point is the login watermark.
TEST_F(CraftRaftEntriesTest, SyncVerdictsApplyBeforeFollowingLoginTruncates) {
    dev_->seed_lsns(/*last_append=*/10, {});
    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(8, 0, {8}));
    commit_entry(*dev_, 2, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 5, /*rs=*/8));

    EXPECT_TRUE(dev_->is_empty_slot(8));
    ASSERT_TRUE(journal_->truncated_to.has_value());
    EXPECT_EQ(*journal_->truncated_to, 8);
    EXPECT_EQ(dev_->last_append_lsn(), 8);
    EXPECT_EQ(dev_->term(), 5u);
    EXPECT_EQ(dev_->last_applied_raft_idx(), 2);
}

// A malformed entry is rejected at parse time and never enqueued, so a later valid entry still applies.
TEST_F(CraftRaftEntriesTest, MalformedEntryDoesNotBlockLaterEntries) {
    std::vector< uint8_t > bad(sizeof(InternalLoginPayload) + 3, 0);
    commit_entry(*dev_, 1, CraftEntryType::InternalLogin, bad);
    commit_entry(*dev_, 2, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 5, -1));

    EXPECT_EQ(dev_->term(), 5u);
}

// While fenced (fence_term > session term) the replica admits neither the old session nor the new one.
TEST_F(CraftRaftEntriesTest, FencedReplicaRejectsOldAndNewSessions) {
    dev_->seed_term(3);
    dev_->seed_fence_term(4);

    auto old_w = write_at(*dev_, 3, 1);
    ASSERT_FALSE(old_w.has_value());
    EXPECT_EQ(old_w.error(), make_error_condition(volume_error::STALE_TERM));
    auto new_w = write_at(*dev_, 4, 1);
    ASSERT_FALSE(new_w.has_value());
    EXPECT_EQ(new_w.error(), make_error_condition(volume_error::STALE_TERM));
}

// Applying the login's InternalLogin lifts the fence and admits the new session in one step.
TEST_F(CraftRaftEntriesTest, InternalLoginLiftsFenceAndAdmitsNewSession) {
    dev_->seed_term(3);
    dev_->seed_fence_term(4);
    commit_entry(*dev_, 1, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 4, -1));

    EXPECT_EQ(dev_->term(), 4u);
    EXPECT_TRUE(write_at(*dev_, 4, 1).has_value());
    EXPECT_FALSE(write_at(*dev_, 3, 2).has_value());
}

// LoginAborted lifts a fence left by a login that never committed: the old session is admitted again.
TEST_F(CraftRaftEntriesTest, LoginAbortedLiftsFenceAndReadmitsOldSession) {
    dev_->seed_term(3);
    dev_->seed_fence_term(4);
    commit_entry(*dev_, 1, CraftEntryType::LoginAborted, make_login_aborted_key(4));

    EXPECT_EQ(dev_->fence_term(), 3u);
    EXPECT_EQ(dev_->aborted_upto(), 4u);
    EXPECT_TRUE(write_at(*dev_, 3, 1).has_value());
}

// A fence above abort_upto belongs to a newer login and is left alone.
TEST_F(CraftRaftEntriesTest, LoginAbortedLeavesNewerFence) {
    dev_->seed_term(3);
    dev_->seed_fence_term(6);
    commit_entry(*dev_, 1, CraftEntryType::LoginAborted, make_login_aborted_key(4));

    EXPECT_EQ(dev_->fence_term(), 6u);
    EXPECT_FALSE(write_at(*dev_, 3, 1).has_value());
}

// An abort is final: a late InternalLogin for an aborted term is ignored.
TEST_F(CraftRaftEntriesTest, InternalLoginForAbortedTermIsIgnored) {
    dev_->seed_term(3);
    commit_entry(*dev_, 1, CraftEntryType::LoginAborted, make_login_aborted_key(4));
    commit_entry(*dev_, 2, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 4, -1));

    EXPECT_EQ(dev_->term(), 3u);
    EXPECT_NE(dev_->client_token(), 42u);
}

TEST_F(CraftRaftEntriesTest, AbortedUptoNeverRegresses) {
    commit_entry(*dev_, 1, CraftEntryType::LoginAborted, make_login_aborted_key(9));
    commit_entry(*dev_, 2, CraftEntryType::LoginAborted, make_login_aborted_key(4));

    EXPECT_EQ(dev_->aborted_upto(), 9u);
}

TEST_F(CraftRaftEntriesTest, OnCommitRejectsLoginAbortedWrongSize) {
    std::vector< uint8_t > bad(sizeof(LoginAbortedPayload) + 1, 0);
    dev_->seed_term(3);
    dev_->seed_fence_term(4);
    commit_entry(*dev_, 1, CraftEntryType::LoginAborted, bad);

    EXPECT_EQ(dev_->fence_term(), 4u); // untouched
    EXPECT_EQ(dev_->aborted_upto(), 0u);
}

// ── Copilot review follow-ups (PR #196) ───────────────────────────────────────────────────────────

// A v2 watermark of -1 is legitimate (the quorum had appended nothing): a replica holding an old-session tail
// must still truncate all of it. Only v1 entries skip truncation.
TEST_F(CraftRaftEntriesTest, InternalLoginV2TruncatesToMinusOne) {
    dev_->seed_lsns(/*last_append=*/4, {});
    commit_entry(*dev_, 1, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 5, /*rs=*/-1));

    ASSERT_TRUE(journal_->truncated_to.has_value());
    EXPECT_EQ(*journal_->truncated_to, -1);
    EXPECT_EQ(dev_->last_append_lsn(), -1);
    EXPECT_EQ(dev_->term(), 5u);
}

// Ordering under suspension: while a SyncRSCommitLSN apply is suspended (reclaiming a slot it verdicts Empty),
// the following InternalLogin must not truncate or admit. With per-entry detached applies it would.
TEST_F(CraftRaftEntriesTest, LoginWaitsForSuspendedEarlierSyncApply) {
    TestGate gate;
    journal_->free_gate = &gate;
    dev_->seed_lsns(/*last_append=*/10, {});
    journal_->slots[8] = JournalSlot{.lsn = 8, .lba_off_bytes = 0, .len_bytes = 1}; // held -> reclaimed on Empty

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(8, 0, {8}));
    commit_entry(*dev_, 2, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 5, /*rs=*/8));

    EXPECT_EQ(journal_->free_slot_calls, 1);
    EXPECT_FALSE(journal_->truncated_to.has_value()); // login has not run yet
    EXPECT_EQ(dev_->term(), 0u);
    EXPECT_EQ(dev_->last_applied_raft_idx(), -1);

    gate.open(); // the Sync apply finishes, then the queue runs the login

    ASSERT_TRUE(journal_->truncated_to.has_value());
    EXPECT_EQ(*journal_->truncated_to, 8);
    EXPECT_EQ(dev_->term(), 5u);
    EXPECT_EQ(dev_->last_applied_raft_idx(), 2);
}

// Catch-up runs off the apply queue: a peer fetch held pending does not block the next entry.
TEST_F(CraftRaftEntriesTest, PendingCatchUpDoesNotBlockLogin) {
    TestGate gate;
    fetcher_.gate = &gate;
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(/*last_append=*/2, {});

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(5, 0, {}));
    commit_entry(*dev_, 2, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 5, /*rs=*/5));

    EXPECT_EQ(dev_->term(), 5u); // login applied while the fetch for 3..5 is still pending
    EXPECT_EQ(fetcher_.last_requested, (std::vector< int64_t >{3, 4, 5}));
    gate.open();
}

// A slot verdicted Empty while its catch-up fetch is in flight is not installed (Empty wins).
TEST_F(CraftRaftEntriesTest, CatchUpSkipsSlotVerdictedEmptyMidFetch) {
    TestGate gate;
    fetcher_.gate = &gate;
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(/*last_append=*/2, {});
    fetcher_.response = {JournalSlot{.lsn = 3, .all_zeros = true, .lba_off_bytes = 0, .len_bytes = 1}};

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(3, 0, {}));
    commit_entry(*dev_, 2, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(3, 0, {3})); // voids 3
    gate.open();

    EXPECT_TRUE(dev_->is_empty_slot(3));
    EXPECT_FALSE(journal_->has_slot(3)); // the stale response was not installed
    EXPECT_EQ(dev_->commit_lsn(), 3);
}

// A replica that missed the login poll is fenced by the InternalLogin apply itself: while the login waits for an
// already-admitted write above the watermark, a new old-session write is rejected.
TEST_F(CraftRaftEntriesTest, LoginFencesAndWaitsForInFlightWriteAboveWatermark) {
    TestGate gate;
    journal_->write_gate = &gate;
    dev_->seed_term(3);
    dev_->seed_lsns(/*last_append=*/5, {});

    // An old-session write to slot 7 is admitted and suspends inside write_slot.
    auto in_flight = dev_->write(craft::client_hdr{.term = 3, .commit_lsn = -1, .all_committed_lsn = -1}, 7, 0,
                                 k_page_size, sisl::sg_list{});
    homeblocks::detail::detach([](auto t) -> async_status {
        (void)co_await std::move(t);
        co_return ok();
    }(std::move(in_flight)));

    commit_entry(*dev_, 1, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 4, /*rs=*/5));
    EXPECT_EQ(dev_->fence_term(), 4u);
    EXPECT_FALSE(journal_->truncated_to.has_value()); // waiting for the in-flight write
    journal_->write_gate = nullptr;
    auto r = write_at(*dev_, 3, 6); // a new old-session write is fenced
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));

    gate.open(); // the in-flight write finishes (unacked), then truncation and admission proceed
    ASSERT_TRUE(journal_->truncated_to.has_value());
    EXPECT_EQ(*journal_->truncated_to, 5);
    EXPECT_EQ(dev_->term(), 4u);
}

// ── second review round (PR #196) ─────────────────────────────────────────────────────────────────

namespace {
// Starts a write and records its result once it completes (it may suspend on a gate first).
void start_write(CraftReplDev& dev, uint64_t term, int64_t dlsn, std::optional< result< craft::lsn_pair > >& out) {
    homeblocks::detail::detach(
        [](CraftReplDev& d, uint64_t t, int64_t l, std::optional< result< craft::lsn_pair > >& o) -> async_status {
            o = co_await d.write(craft::client_hdr{.term = t, .commit_lsn = -1, .all_committed_lsn = -1}, l, 0,
                                 k_page_size, sisl::sg_list{});
            co_return ok();
        }(dev, term, dlsn, out));
}
} // namespace

// Two writes in flight above the watermark: the first one finishing must NOT start the truncation.
TEST_F(CraftRaftEntriesTest, LoginDrainsEveryInFlightWriteNotJustTheFirst) {
    TestGate g7, g8;
    journal_->write_gates = {{7, &g7}, {8, &g8}};
    dev_->seed_term(3);
    dev_->seed_lsns(/*last_append=*/5, {});
    std::optional< result< craft::lsn_pair > > w7, w8;
    start_write(*dev_, 3, 7, w7);
    start_write(*dev_, 3, 8, w8);

    commit_entry(*dev_, 1, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 4, /*rs=*/5));
    g7.open();
    ASSERT_TRUE(w7.has_value());
    EXPECT_FALSE(journal_->truncated_to.has_value()); // write 8 still in flight

    g8.open();
    ASSERT_TRUE(w8.has_value());
    ASSERT_TRUE(journal_->truncated_to.has_value());
    EXPECT_EQ(*journal_->truncated_to, 5);
    EXPECT_FALSE(w7->has_value()); // neither fenced write reported success
    EXPECT_FALSE(w8->has_value());
}

// A write that passed its checks and is suspended in its piggyback commit when the login fences must not
// report success: the login is about to truncate it.
TEST_F(CraftRaftEntriesTest, WriteFencedDuringPiggybackCommitReportsStaleTerm) {
    TestGate gate;
    dev_->test_post_commit_hook_ = [&gate]() -> async_status {
        co_await gate.wait();
        co_return ok();
    };
    dev_->seed_term(3);
    dev_->seed_lsns(/*last_append=*/5, {});
    std::optional< result< craft::lsn_pair > > w;
    start_write(*dev_, 3, 6, w);
    ASSERT_FALSE(w.has_value()); // held right after its piggyback commit

    commit_entry(*dev_, 1, CraftEntryType::InternalLogin, make_internal_login_v2_key(42, 4, /*rs=*/5));
    EXPECT_FALSE(journal_->truncated_to.has_value()); // waiting for the held write
    gate.open();

    ASSERT_TRUE(w.has_value());
    ASSERT_FALSE(w->has_value());
    EXPECT_EQ(w->error(), make_error_condition(volume_error::STALE_TERM));
    ASSERT_TRUE(journal_->truncated_to.has_value());
    EXPECT_EQ(*journal_->truncated_to, 5);
    dev_->test_post_commit_hook_ = nullptr;
}

// While catch-up installs a slot, a client write for the same dLSN is rejected rather than racing it.
TEST_F(CraftRaftEntriesTest, ClientWriteRejectedWhileCatchUpInstallsSameSlot) {
    TestGate gate;
    journal_->write_gates = {{3, &gate}};
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_term(1);
    dev_->seed_lsns(/*last_append=*/2, {});
    fetcher_.response = {JournalSlot{.lsn = 3, .all_zeros = true, .lba_off_bytes = 0, .len_bytes = 1}};

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(3, 0, {}));
    // catch-up is now suspended inside write_slot(3), holding the claim on slot 3
    auto r = write_at(*dev_, 1, 3);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::operation_in_progress));

    gate.open();
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_FALSE(dev_->is_missing(3));
}

// ── catch-up retry, backoff and ranged fetch (PR E1, docs/craft/resync.md §4.3) ──────────────────────

namespace {
result< FetchRangeResult > range_error() { return std::unexpected(std::make_error_condition(std::errc::io_error)); }
JournalSlot zero_slot(int64_t lsn) {
    return JournalSlot{.lsn = lsn, .all_zeros = true, .lba_off_bytes = 0, .len_bytes = 1};
}
} // namespace

// A failed pass is retried after a backoff delay, and the retry fills the slot.
TEST_F(CraftRaftEntriesTest, CatchUpRetriesAfterAFailedFetch) {
    std::vector< std::chrono::milliseconds > delays;
    dev_->set_retry_delay_fn([&delays](std::chrono::milliseconds d) -> async_status {
        delays.push_back(d);
        co_return ok();
    });
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(/*last_append=*/2, {});
    fetcher_.range_script = {range_error(), FetchRangeResult{.slots = {zero_slot(3)}}};

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(3, 0, {}));

    EXPECT_EQ(delays, (std::vector< std::chrono::milliseconds >{CraftReplDev::catch_up_min_backoff}));
    EXPECT_TRUE(journal_->has_slot(3));
    EXPECT_EQ(dev_->commit_lsn(), 3);
}

// With no progress the backoff doubles up to the cap, and after catch_up_max_failed_passes the runner stops; the
// next SyncRSCommitLSN restarts it.
TEST_F(CraftRaftEntriesTest, CatchUpBacksOffThenStopsAndRestartsOnNextSync) {
    std::vector< std::chrono::milliseconds > delays;
    dev_->set_retry_delay_fn([&delays](std::chrono::milliseconds d) -> async_status {
        delays.push_back(d);
        co_return ok();
    });
    dev_->set_peer_fetcher(&fetcher_);
    fetcher_.should_fail = true; // default fetch_range delegates to the failing fetch_data
    dev_->seed_lsns(/*last_append=*/2, {});

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(3, 0, {}));

    ASSERT_EQ(delays.size(), static_cast< size_t >(CraftReplDev::catch_up_max_failed_passes - 1));
    EXPECT_EQ(delays.front(), CraftReplDev::catch_up_min_backoff);
    EXPECT_EQ(delays[1], CraftReplDev::catch_up_min_backoff * 2);
    EXPECT_EQ(delays.back(), CraftReplDev::catch_up_max_backoff); // capped
    auto const passes = fetcher_.range_requests.size();
    EXPECT_EQ(passes, static_cast< size_t >(CraftReplDev::catch_up_max_failed_passes));

    fetcher_.should_fail = false;
    fetcher_.response = {zero_slot(3)};
    commit_entry(*dev_, 2, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(3, 0, {}));
    EXPECT_GT(fetcher_.range_requests.size(), passes); // restarted
    EXPECT_EQ(dev_->commit_lsn(), 3);
}

// Without a delay function (light tests, unwired production) a pass that makes no progress ends the runner: no
// retry, as before E1.
TEST_F(CraftRaftEntriesTest, CatchUpDoesNotRetryWithoutDelayFn) {
    dev_->set_peer_fetcher(&fetcher_);
    fetcher_.should_fail = true;
    dev_->seed_lsns(/*last_append=*/2, {});

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(3, 0, {}));

    EXPECT_EQ(fetcher_.range_requests.size(), 1u);
    EXPECT_TRUE(dev_->is_missing(3));
}

// The responder's continuation (`next`) is followed until the request is covered.
TEST_F(CraftRaftEntriesTest, CatchUpFollowsTheContinuation) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(/*last_append=*/2, {});
    fetcher_.range_script = {FetchRangeResult{.slots = {zero_slot(3)}, .next = 4},
                             FetchRangeResult{.slots = {zero_slot(4), zero_slot(5)}, .next = -1}};

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(5, 0, {}));

    ASSERT_EQ(fetcher_.range_requests.size(), 2u);
    EXPECT_EQ(fetcher_.range_requests[0], (std::vector< LsnRange >{{3, 5}}));
    EXPECT_EQ(fetcher_.range_requests[1], (std::vector< LsnRange >{{4, 5}}));
    EXPECT_EQ(dev_->commit_lsn(), 5);
}

// Missing slots are sent as ranges, and each request is bounded by catch_up_max_slots.
TEST_F(CraftRaftEntriesTest, CatchUpChunksRequestsByMaxSlots) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->set_catch_up_chunk(/*max_bytes=*/1 << 20, /*max_slots=*/2);
    dev_->seed_lsns(/*last_append=*/2, {});
    fetcher_.range_script = {FetchRangeResult{.slots = {zero_slot(3), zero_slot(4)}},
                             FetchRangeResult{.slots = {zero_slot(5), zero_slot(6)}},
                             FetchRangeResult{.slots = {zero_slot(7)}}};

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(7, 0, {}));

    ASSERT_GE(fetcher_.range_requests.size(), 3u);
    EXPECT_EQ(fetcher_.range_requests[0], (std::vector< LsnRange >{{3, 4}}));
    EXPECT_EQ(fetcher_.range_requests[1], (std::vector< LsnRange >{{5, 6}}));
    EXPECT_EQ(fetcher_.range_requests[2], (std::vector< LsnRange >{{7, 7}}));
    EXPECT_EQ(dev_->commit_lsn(), 7);
}

// Non-contiguous missing slots become separate ranges in one request.
TEST_F(CraftRaftEntriesTest, CatchUpSendsMissingSlotsAsRanges) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(/*last_append=*/8, {2, 3, 6});
    fetcher_.response = {zero_slot(2), zero_slot(3), zero_slot(6)};

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(8, 0, {}));

    ASSERT_GE(fetcher_.range_requests.size(), 1u);
    EXPECT_EQ(fetcher_.range_requests[0], (std::vector< LsnRange >{{2, 3}, {6, 6}}));
    EXPECT_FALSE(dev_->is_missing(6));
}

// A response with a slot outside the request, or at/after its own `next`, is rejected whole.
TEST_F(CraftRaftEntriesTest, CatchUpRejectsResponsePastItsContinuation) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(/*last_append=*/2, {});
    fetcher_.range_script = {FetchRangeResult{.slots = {zero_slot(3), zero_slot(5)}, .next = 4}};

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(5, 0, {}));

    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
}

// A responder that reports no progress (empty response, continuation at the start) does not loop forever.
TEST_F(CraftRaftEntriesTest, CatchUpAbandonsABatchThatMakesNoProgress) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(/*last_append=*/2, {});
    fetcher_.range_script = {FetchRangeResult{.slots = {}, .next = 3}};

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(5, 0, {}));

    EXPECT_EQ(fetcher_.range_requests.size(), 1u);
    EXPECT_TRUE(dev_->is_missing(3));
}

// A responder past retention reports reclaimed_below; the slots stay missing (baseline recovery, not retry).
TEST_F(CraftRaftEntriesTest, CatchUpReportsReclaimedResponder) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(/*last_append=*/2, {});
    fetcher_.range_script = {FetchRangeResult{.slots = {}, .next = -1, .reclaimed_below = 10}};

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(5, 0, {}));

    EXPECT_TRUE(dev_->is_missing(3));
}

// ── review on #200 ──

// A slot an in-flight client write still owns is not resolved: install reports it as no progress.
TEST_F(CraftRaftEntriesTest, CatchUpSlotOwnedByAClientWriteIsNotProgress) {
    dev_->seed_term(1);
    TestGate gate;
    journal_->write_gates[3] = &gate;
    std::optional< result< craft::lsn_pair > > w;
    homeblocks::detail::detach([](CraftReplDev& d, std::optional< result< craft::lsn_pair > >& o) -> async_status {
        o = co_await d.write(craft::client_hdr{.term = 1, .commit_lsn = -1, .all_committed_lsn = -1}, 3, 0, k_page_size,
                             sisl::sg_list{});
        co_return ok();
    }(*dev_, w));
    ASSERT_FALSE(w.has_value());
    ASSERT_TRUE(dev_->is_missing(3));

    auto r = install(zero_slot(3), 1);
    ASSERT_TRUE(r.has_value());
    EXPECT_FALSE(*r);

    journal_->write_gates.clear();
    gate.open();
    ASSERT_TRUE(w.has_value());
    EXPECT_TRUE(w->has_value());
    r = install(zero_slot(3), 1); // filled by the write now: resolved
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(*r);
}

// A Sync that raises the target while a pass is failing is not lost when that pass gives up.
TEST_F(CraftRaftEntriesTest, CatchUpKeepsATargetRaisedDuringAFailedPass) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(/*last_append=*/2, {});
    TestGate gate;
    fetcher_.range_gate = &gate;
    fetcher_.range_script = {range_error(), range_error()};

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(3, 0, {}));
    ASSERT_EQ(fetcher_.range_requests.size(), 1u); // held in the first fetch
    commit_entry(*dev_, 2, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(6, 0, {}));
    EXPECT_EQ(fetcher_.range_requests.size(), 1u); // the running runner owns the raised target

    fetcher_.range_gate = nullptr;
    gate.open();
    ASSERT_EQ(fetcher_.range_requests.size(), 2u);
    EXPECT_EQ(fetcher_.range_requests[1], (std::vector< LsnRange >{{3, 6}}));
}

// Slots a responder reports as reclaimed are left for baseline recovery: no retry and no backoff for them, while
// the recoverable part of the range continues.
TEST_F(CraftRaftEntriesTest, CatchUpLeavesReclaimedSlotsForBaseline) {
    dev_->set_peer_fetcher(&fetcher_);
    int delays = 0;
    dev_->set_retry_delay_fn([&delays](std::chrono::milliseconds) -> async_status {
        ++delays;
        co_return ok();
    });
    dev_->seed_lsns(/*last_append=*/2, {});
    fetcher_.range_script = {FetchRangeResult{.slots = {}, .next = -1, .reclaimed_below = 5},
                             FetchRangeResult{.slots = {zero_slot(5), zero_slot(6)}, .next = -1}};

    commit_entry(*dev_, 1, CraftEntryType::SyncRSCommitLSN, make_sync_rs_commit_lsn_key(6, 0, {}));
    ASSERT_EQ(fetcher_.range_requests.size(), 2u);
    EXPECT_EQ(fetcher_.range_requests[1], (std::vector< LsnRange >{{5, 6}}));
    EXPECT_EQ(delays, 0);
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_TRUE(dev_->is_missing(4));
    EXPECT_FALSE(dev_->is_missing(5));
    EXPECT_FALSE(dev_->is_missing(6));
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
