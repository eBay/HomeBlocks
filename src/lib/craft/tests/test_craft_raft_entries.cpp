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
// CreateVolume (SDSTOR-22894) codec tests verify a serialized entry round-trips, and a key whose size
// disagrees with its declared member/name counts is rejected.
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles craft_repl_dev.cpp
// directly (same pattern as test_craft_truncate.cpp).

#include <boost/uuid/random_generator.hpp>
#include <gtest/gtest.h>
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

// ── journal mock ──────────────────────────────────────────────────────────────
//
// Backed by a std::map so tests can inspect exactly which LSNs got persisted during catch-up.
// fail_on_write optionally injects an I/O error for a specific LSN.

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots;
    std::optional< int64_t > fail_on_write;
    int free_data_calls{0};
    std::function< void(int64_t) > on_write; // runs inside write_slot, to apply an Empty verdict mid-write
    std::function< void() > on_free;         // runs inside free_data, to look at the durable image at that moment
    bool fail_tail{false};

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const&, lba_count_t) override {
        co_return homestore::multi_blk_id{};
    }

    async_status write_slot(int64_t lsn, uint64_t /* term */, lba_t lba, lba_count_t len,
                            homestore::multi_blk_id /* blkid */, bool all_zeros,
                            std::vector< homestore::csum_t > const& csums) override {
        if (on_write) on_write(lsn);
        if (fail_on_write && *fail_on_write == lsn)
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        slots[lsn] =
            JournalSlot{.lsn = lsn, .all_zeros = all_zeros, .lba_off_bytes = lba, .len_bytes = len, .csums = csums};
        co_return ok();
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override { return mock_read_slot(*this, lsn); }

    async_status truncate_to(int64_t) override { co_return ok(); }

    async_status free_data(homestore::multi_blk_id) override {
        ++free_data_calls;
        if (on_free) on_free();
        co_return ok();
    }

    async_status free_slot(int64_t lsn) override { return mock_free_slot(*this, lsn); }

    async_status read_data(homestore::multi_blk_id, sisl::sg_list&) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    // The blocks committed, as {first block, recovering}.
    std::vector< std::pair< homestore::blk_num_t, bool > > commits;
    bool fail_commit{false};
    status commit_data(homestore::multi_blk_id const& blkid, bool recovering) override {
        if (fail_commit) return std::unexpected(std::make_error_condition(std::errc::io_error));
        if (!blkid.is_valid()) return ok(); // as the HomeStore backend: nothing allocated
        commits.emplace_back(blkid.to_single_blkid().blk_num(), recovering);
        return ok();
    }

    async_result< int64_t > tail_lsn() override {
        if (fail_tail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return slots.empty() ? -1 : slots.rbegin()->first;
    }

    // Journal reclaim: drops the slots <= upto.
    int64_t reclaimed{-1};
    int reclaim_calls{0};
    bool reclaim_unsupported{false};
    bool fail_reclaim{false};
    bool hold_reclaim{false}; // the next reclaim waits at reclaim_gate
    sisl::async::value_awaitable< bool > reclaim_gate;
    async_status reclaim_upto(int64_t upto) override {
        ++reclaim_calls;
        if (std::exchange(hold_reclaim, false)) co_await reclaim_gate;
        if (reclaim_unsupported)
            co_return std::unexpected(std::make_error_condition(std::errc::operation_not_supported));
        if (fail_reclaim) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        slots.erase(slots.begin(), slots.upper_bound(upto));
        reclaimed = std::max(reclaimed, upto);
        co_return ok();
    }
    int64_t reclaimed_upto() const override { return reclaimed; }

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
    // When set, the next fetch_data() waits for fetch_gate before replying: an apply suspended mid-catch-up.
    bool hold_fetch{false};
    sisl::async::value_awaitable< bool > fetch_gate;
    bool should_throw{false}; // fetch_data() throws, as a transport bug might

    async_result< craft::lsn_pair > get_rs_commit_lsn(uint64_t /* term */, bool /* is_login */) override {
        co_return craft::lsn_pair{};
    }

    async_result< std::vector< JournalSlot > > fetch_data(const std::vector< int64_t >& lsns,
                                                          uint32_t timeout_ms) override {
        last_requested = lsns;
        last_timeout_ms = timeout_ms;
        if (std::exchange(hold_fetch, false)) co_await fetch_gate;
        if (should_throw) throw std::runtime_error("fetch_data threw");
        if (should_fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return response;
    }

    // Not exercised by this file -- pre_resolve_slots's quorum broadcast is covered by
    // test_craft_pre_resolution.cpp's own mock.
    async_result< std::vector< QuorumSlotResponse > > fetch_from_quorum(std::vector< int64_t >, uint32_t) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    async_result< std::vector< QuorumLsnResponse > > poll_quorum_lsns(uint64_t, bool, uint32_t) override {
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
    bool flushed{true}; // false: the request coalesced with a flush already running
    bool hold{false};   // the next trigger waits at gate
    sisl::async::value_awaitable< bool > gate;

    async_result< bool > operator()(bool force) {
        ++call_count;
        last_force = force;
        if (std::exchange(hold, false)) co_await gate;
        if (fail_next) {
            fail_next = false;
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        co_return flushed;
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
    void TearDown() override { EXPECT_EQ(dev_->invariant_violations(), std::vector< std::string >{}); }

    auto do_apply(int64_t rs_commit_lsn, uint64_t client_token, std::vector< int64_t > empty_slots = {}) {
        return homeblocks::detail::sync_get(
            dev_->apply_sync_rs_commit_lsn(rs_commit_lsn, client_token, std::move(empty_slots)));
    }

    // ── durable state (S8) ──
    // A device with a state store, over a fresh journal mock: replaces dev_ and journal_.
    MockCraftStateStore* with_store() {
        auto store = std::make_unique< MockCraftStateStore >();
        auto* raw = store.get();
        auto mock = std::make_unique< MockCraftJournalBackend >();
        journal_ = mock.get();
        dev_ = CraftReplDev::create(volume_id_t{}, std::move(mock), k_page_size, nullptr, std::move(store));
        return raw;
    }

    // A restart: a device over this journal content and this image (nullopt: none ever persisted), recovered through
    // the production restart path. Replaces dev_ and journal_; every mock completes inline, so recovery has finished
    // on return.
    MockCraftStateStore* restart(std::optional< CraftDurableState > image, std::map< int64_t, JournalSlot > slots) {
        auto* store = with_store();
        store->saved = std::move(image);
        journal_->slots = std::move(slots);
        dev_->trigger_on_restart();
        return store;
    }

    // Commits one RAFT entry at raft_lsn through the listener, as HomeStore does.
    void commit(int64_t raft_lsn, std::vector< uint8_t > header, std::vector< uint8_t > key) {
        cintrusive< homestore::repl_req_ctx > ctx{};
        dev_->raft_listener().on_commit(raft_lsn, as_blob(header), as_blob(key), {}, ctx);
    }

public:
    // Public: a detached coroutine in a test body reaches it through a pointer to this fixture.
    async_status persist() { return dev_->persist_state(); }

protected:
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

// A verdict for a slot this replica already applied (<= commit_lsn) must not free its blocks: the index still
// points at them. Apply refuses and faults the partition instead (C3).
TEST_F(CraftRaftEntriesTest, EmptySlotAtOrBelowCommitLsnFaultsInsteadOfFreeing) {
    dev_->seed_lsns(5, {});
    dev_->seed_commit_lsn(5);
    journal_->slots[3] = JournalSlot{.lsn = 3, .all_zeros = false, .lba_off_bytes = 0, .len_bytes = 4};

    auto r = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3});

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::INTERNAL_ERROR));
    EXPECT_EQ(journal_->free_data_calls, 0);
    EXPECT_FALSE(dev_->is_empty_slot(3));
    EXPECT_TRUE(dev_->is_faulted());
}

// A slot commit_impl() has started indexing is covered too, though commit_lsn only moves when the run ends: a
// verdict landing mid-run must fault, not free blocks the index references (F).
TEST_F(CraftRaftEntriesTest, EmptySlotBeingIndexedFaultsInsteadOfFreeing) {
    dev_->seed_lsns(5, {});
    dev_->seed_commit_lsn(2);
    journal_->slots[3] = JournalSlot{.lsn = 3,
                                     .lba_off_bytes = 0,
                                     .len_bytes = k_page_size,
                                     .blkid = homestore::multi_blk_id{500, 1, 1},
                                     .csums = {7}};

    std::optional< status > apply_r;
    auto c = homeblocks::detail::sync_get(dev_->commit_with(
        3,
        [&](lba_t, lba_t, std::unordered_map< lba_t, BlockInfo >&) -> status {
            // lsn 3 is being indexed; commit_lsn is still 2.
            apply_r = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3});
            return ok();
        },
        [](lba_t, lba_t, std::vector< homestore::blk_id >&) -> status { return ok(); }));

    ASSERT_TRUE(c.has_value());
    EXPECT_EQ(*c, 3);
    ASSERT_TRUE(apply_r.has_value());
    ASSERT_FALSE(apply_r->has_value());
    EXPECT_EQ(apply_r->error(), make_error_condition(volume_error::INTERNAL_ERROR));
    EXPECT_EQ(journal_->free_data_calls, 0);
    EXPECT_FALSE(dev_->is_empty_slot(3));
    EXPECT_TRUE(dev_->is_faulted());
}

// Once faulted, the partition stops answering peers and stops proposing: its slots and watermarks no longer
// match the replica set's.
TEST_F(CraftRaftEntriesTest, FaultedPartitionStopsAnsweringPeersAndProposing) {
    dev_->seed_lsns(5, {});
    dev_->seed_commit_lsn(5);
    journal_->slots[3] = JournalSlot{.lsn = 3, .all_zeros = false, .lba_off_bytes = 0, .len_bytes = 4};
    ASSERT_FALSE(do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3}).has_value());
    ASSERT_TRUE(dev_->is_faulted());

    auto const faulted = make_error_condition(volume_error::INTERNAL_ERROR);
    auto fetched = homeblocks::detail::sync_get(dev_->fetch_data({3}));
    ASSERT_FALSE(fetched.has_value());
    EXPECT_EQ(fetched.error(), faulted);
    auto polled = homeblocks::detail::sync_get(dev_->get_rs_commit_lsn(/*term=*/0, /*is_login=*/false));
    ASSERT_FALSE(polled.has_value());
    EXPECT_EQ(polled.error(), faulted);
    auto appended = homeblocks::detail::sync_get(dev_->append(/*sync_to=*/5, /*client_token=*/0, dev_->term()));
    ASSERT_FALSE(appended.has_value());
    EXPECT_EQ(appended.error(), faulted);
}

// Freeing an Empty slot's blocks also retires its overlay entry: apply moves commit_lsn past the slot directly,
// so commit never visits it to retire the entry, and reads would keep serving the slot (C3).
TEST_F(CraftRaftEntriesTest, EmptySlotOverLocalDataRetiresOverlay) {
    auto w = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{.term = 0, .commit_lsn = -1, .all_committed_lsn = -1}, /*dlsn=*/0,
                    /*addr=*/5 * k_page_size, /*len=*/k_page_size, sisl::sg_list{})); // empty data -> all_zeros write
    ASSERT_TRUE(w.has_value());
    ASSERT_EQ(dev_->overlay_lsn_for(5), 0);

    auto r = do_apply(/*rs_commit_lsn=*/0, /*client_token=*/0, /*empty_slots=*/{0});

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(dev_->is_empty_slot(0));
    EXPECT_EQ(dev_->overlay_lsn_for(5), -1);
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

// Catch-up leaves a slot whose write() is still in flight to that write, rather than journaling a peer's copy
// alongside it (H1).
TEST_F(CraftRaftEntriesTest, CatchUpSkipsSlotWithWriteInFlight) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3});
    dev_->seed_in_flight(3);
    fetcher_.response = {JournalSlot{.lsn = 3, .lba_off_bytes = 10, .len_bytes = 4}};

    auto r = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_FALSE(journal_->has_slot(3));
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_EQ(dev_->commit_lsn(), 2);
}

// A slot catch-up fills above one still missing gets an overlay entry: commit stops at the hole, so the overlay
// is the only place a read can find it (E/Q).
TEST_F(CraftRaftEntriesTest, CatchUpFilledSlotAboveHoleIsInOverlay) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3, 5});
    fetcher_.response = {
        JournalSlot{.lsn = 5, .all_zeros = true, .lba_off_bytes = 2 * k_page_size, .len_bytes = k_page_size}};

    auto r = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_EQ(dev_->commit_lsn(), 2);
    EXPECT_EQ(dev_->overlay_lsn_for(2), 5);
}

// Same race in catch-up: an Empty verdict applied during the fill's journal write leaves the fill void.
TEST_F(CraftRaftEntriesTest, CatchUpFillVoidedByEmptyDuringWriteIsNotPublished) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {3, 4});
    fetcher_.response = {JournalSlot{.lsn = 3, .all_zeros = true, .lba_off_bytes = 0, .len_bytes = k_page_size},
                         JournalSlot{.lsn = 4, .lba_off_bytes = k_page_size, .len_bytes = k_page_size, .csums = {7}}};
    // Each slot is voted Empty while its own write is in flight.
    journal_->on_write = [this](int64_t lsn) {
        if (lsn == 3) dev_->seed_empty({3});
        if (lsn == 4) dev_->seed_empty({3, 4});
    };

    auto r = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->overlay_lsn_for(0), -1); // slot 3 was never published
    EXPECT_EQ(journal_->free_data_calls, 1); // slot 4's blocks were freed
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

    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    // last_append_lsn advances unconditionally
    EXPECT_EQ(dev_->last_append_lsn(), 7);
}

TEST_F(CraftRaftEntriesTest, OnCommitRejectsHeaderTooSmall) {
    std::vector< uint8_t > empty_header;
    auto key_buf = make_sync_rs_commit_lsn_key(7, 0, {});
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->raft_listener().on_commit(1, as_blob(empty_header), as_blob(key_buf), {}, ctx);

    EXPECT_EQ(dev_->commit_lsn(), -1); // untouched
}

TEST_F(CraftRaftEntriesTest, OnCommitRejectsMalformedSyncRSCommitLSNKey) {
    auto header_buf = make_header(CraftEntryType::SyncRSCommitLSN);
    std::vector< uint8_t > short_key(sizeof(SyncRSCommitLSNPayload) - 1, 0);
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(short_key), {}, ctx);

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

    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    EXPECT_EQ(dev_->commit_lsn(), -1); // untouched
}

TEST_F(CraftRaftEntriesTest, OnCommitIgnoresUnrecognizedEntryType) {
    std::vector< uint8_t > header_buf(sizeof(CraftEntryHeader));
    reinterpret_cast< CraftEntryHeader* >(header_buf.data())->type = static_cast< CraftEntryType >(99);
    std::vector< uint8_t > key_buf;
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    EXPECT_EQ(dev_->commit_lsn(), -1); // untouched
}

// ── InternalLogin apply (SDSTOR-22887) ────────────────────────────────────────

TEST_F(CraftRaftEntriesTest, OnCommitDispatchesInternalLogin) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    auto key_buf = make_internal_login_key(/*client_token=*/42, /*term=*/5);
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    EXPECT_EQ(dev_->client_token(), 42u);
    EXPECT_EQ(dev_->term(), 5u);
}

TEST_F(CraftRaftEntriesTest, OnCommitRejectsInternalLoginWrongSize) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    std::vector< uint8_t > short_key(sizeof(InternalLoginPayload) - 1, 0);
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(short_key), {}, ctx);

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

    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(long_key), {}, ctx);

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
    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key1), {}, ctx);
    auto key2 = make_internal_login_key(/*client_token=*/2, /*term=*/2);
    dev_->raft_listener().on_commit(2, as_blob(header_buf), as_blob(key2), {}, ctx);

    EXPECT_EQ(dev_->client_token(), 2u);
    EXPECT_EQ(dev_->term(), 2u);
}

TEST_F(CraftRaftEntriesTest, InternalLoginTermNeverRegresses) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    cintrusive< homestore::repl_req_ctx > ctx{};

    auto key1 = make_internal_login_key(/*client_token=*/1, /*term=*/5);
    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key1), {}, ctx);
    auto key2 = make_internal_login_key(/*client_token=*/2, /*term=*/3);
    dev_->raft_listener().on_commit(2, as_blob(header_buf), as_blob(key2), {}, ctx);

    EXPECT_EQ(dev_->term(), 5u);
}

// client_token has no ordering semantics (it's an opaque id, unlike term) -- it's a plain overwrite even
// when the accompanying term regresses and is guarded. Pins down the intentional asymmetry between the
// two fields so it doesn't read as an oversight to a future reader.
TEST_F(CraftRaftEntriesTest, InternalLoginClientTokenOverwrittenEvenWhenTermRegresses) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    cintrusive< homestore::repl_req_ctx > ctx{};

    auto key1 = make_internal_login_key(/*client_token=*/1, /*term=*/5);
    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key1), {}, ctx);
    auto key2 = make_internal_login_key(/*client_token=*/99, /*term=*/3); // lower term, different token
    dev_->raft_listener().on_commit(2, as_blob(header_buf), as_blob(key2), {}, ctx);

    EXPECT_EQ(dev_->term(), 5u);          // guarded against regression
    EXPECT_EQ(dev_->client_token(), 99u); // overwritten regardless
}

// Happy-path regression check for the write()-term-check-under-lock reorg: a matching term still
// succeeds normally (only the mismatch path changed).
TEST_F(CraftRaftEntriesTest, WriteSucceedsWithMatchingTermAfterInternalLogin) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    auto key_buf = make_internal_login_key(/*client_token=*/1, /*term=*/5);
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

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
    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    auto r = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{.term = 4, .commit_lsn = -1, .all_committed_lsn = -1}, /*dlsn=*/1,
                    /*addr=*/0, /*len=*/k_page_size, sisl::sg_list{})); // empty data -> all_zeros write

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
}

// InternalLogin applies while a write's journal append is in flight: write()'s post-flight sees the new term and
// fails STALE_TERM. The entry stays journalled and its blocks aren't freed (the journal references them; the next
// login truncates it), and the slot stays missing.
TEST_F(CraftRaftEntriesTest, WriteFailsStaleTermWhenLoginLandsDuringWriteSlot) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    auto key_buf = make_internal_login_key(/*client_token=*/1, /*term=*/1);
    journal_->on_write = [&](int64_t) {
        cintrusive< homestore::repl_req_ctx > ctx{};
        dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);
    };
    std::vector< uint8_t > buf(k_page_size);
    sisl::sg_list data;
    data.size = k_page_size;
    data.iovs.push_back(iovec{buf.data(), buf.size()});

    auto r = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{.term = 0, .commit_lsn = -1, .all_committed_lsn = -1}, /*dlsn=*/0,
                    /*addr=*/0, /*len=*/k_page_size, std::move(data)));

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_TRUE(journal_->has_slot(0));
    EXPECT_EQ(journal_->free_data_calls, 0);
    EXPECT_TRUE(dev_->is_missing(0));
}

// InternalLogin establishing a new client_token has no bearing on SyncRSCommitLSN applies -- neither the
// stale/old token nor the newly-established one gates the apply; both succeed identically. Guards against
// reintroducing an equality-fence keyed off InternalLogin's client_token.
TEST_F(CraftRaftEntriesTest, SyncRSCommitLSNAppliesRegardlessOfInternalLoginToken) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    auto key_buf = make_internal_login_key(/*client_token=*/7, /*term=*/1);
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    auto stale = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0); // the old default -- would have been a mismatch
    ASSERT_TRUE(stale.has_value());

    auto fresh = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/7); // the token InternalLogin just set
    ASSERT_TRUE(fresh.has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 5);
}

// ── CreateVolume codec (SDSTOR-22894) ─────────────────────────────────────────

volume_info make_craft_volume_info() {
    auto gen = boost::uuids::random_generator();
    volume_info v{gen(), 1 << 30, 4096, "vol_7", 3 /* ordinal: each node's own, not replicated */};
    v.repl_mode = replication_mode::CRAFT;
    v.members = {gen(), gen(), gen()};
    return v;
}

std::vector< uint8_t > serialize(volume_info const& v) {
    std::vector< uint8_t > buf(create_volume_key_size(v.members.size(), v.name.size()));
    serialize_create_volume(buf.data(), v);
    return buf;
}

TEST(CreateVolumeCodecTest, RoundTrip) {
    auto const in = make_craft_volume_info();
    auto buf = serialize(in);
    auto const out = parse_create_volume(as_blob(buf));
    ASSERT_TRUE(out);
    EXPECT_EQ(out->id, in.id);
    EXPECT_EQ(out->size_bytes, in.size_bytes);
    EXPECT_EQ(out->page_size, in.page_size);
    EXPECT_EQ(out->members, in.members);
    EXPECT_EQ(out->name, in.name);
    EXPECT_EQ(out->repl_mode, replication_mode::CRAFT);
    EXPECT_EQ(out->ordinal, 0u);
}

TEST(CreateVolumeCodecTest, RejectsKeyWhoseSizeDisagreesWithItsCounts) {
    auto buf = serialize(make_craft_volume_info());
    auto truncated = std::vector< uint8_t >(buf.begin(), buf.end() - 1);
    EXPECT_FALSE(parse_create_volume(as_blob(truncated)));
    auto padded = buf;
    padded.push_back(0);
    EXPECT_FALSE(parse_create_volume(as_blob(padded)));
    auto prefix_only = std::vector< uint8_t >(buf.begin(), buf.begin() + sizeof(CreateVolumeHeader) - 1);
    EXPECT_FALSE(parse_create_volume(as_blob(prefix_only)));
}

TEST(CreateVolumeCodecTest, RejectsAnotherVersion) {
    auto buf = serialize(make_craft_volume_info());
    ASSERT_EQ(reinterpret_cast< CreateVolumeHeader const* >(buf.data())->version, CreateVolumeHeader::VERSION);
    for (int const version : {0, CreateVolumeHeader::VERSION + 1}) {
        reinterpret_cast< CreateVolumeHeader* >(buf.data())->version = static_cast< uint8_t >(version);
        EXPECT_FALSE(parse_create_volume(as_blob(buf))) << "version " << version;
    }
}

// ── proposal completion (make_homestore_raft_proposer's ctx) ──────────────────

TEST_F(CraftRaftEntriesTest, OnCommitCompletesOwnProposalOk) {
    auto header_buf = make_header(CraftEntryType::SyncRSCommitLSN);
    auto key_buf = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/3, /*client_token=*/0, /*empty_slots=*/{});
    auto const ctx = make_test_proposal_ctx();

    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    auto const outcome = take_test_proposal_outcome(ctx);
    ASSERT_TRUE(outcome.has_value());
    EXPECT_TRUE(outcome->has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 3); // and it was applied
}

TEST_F(CraftRaftEntriesTest, OnCommitMalformedEntryFailsProposal) {
    std::vector< uint8_t > empty_header;
    auto key_buf = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/3, /*client_token=*/0, /*empty_slots=*/{});
    auto const ctx = make_test_proposal_ctx();

    dev_->raft_listener().on_commit(1, as_blob(empty_header), as_blob(key_buf), {}, ctx);

    auto const outcome = take_test_proposal_outcome(ctx);
    ASSERT_TRUE(outcome.has_value());
    ASSERT_FALSE(outcome->has_value());
    EXPECT_EQ(outcome->error(), make_error_condition(volume_error::INVALID_ENTRY));
}

// The proposal completes with its apply's outcome, not just "dispatched": an entry that parses but whose apply rejects
// it (a verdict past its own watermark) fails the proposal.
TEST_F(CraftRaftEntriesTest, OnCommitApplyRejectionFailsProposal) {
    auto header_buf = make_header(CraftEntryType::SyncRSCommitLSN);
    auto key_buf = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/3, /*client_token=*/0, /*empty_slots=*/{5});
    auto const ctx = make_test_proposal_ctx();

    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    auto const outcome = take_test_proposal_outcome(ctx);
    ASSERT_TRUE(outcome.has_value());
    ASSERT_FALSE(outcome->has_value());
    EXPECT_EQ(outcome->error(), make_error_condition(volume_error::INVALID_ENTRY));
    EXPECT_EQ(dev_->last_append_lsn(), -1); // nothing applied
}

// An apply that faults the partition (a verdict for a slot already in the index) fails the proposal too.
TEST_F(CraftRaftEntriesTest, OnCommitApplyFaultFailsProposal) {
    dev_->seed_lsns(5, {});
    dev_->seed_commit_lsn(5);
    journal_->slots[3] = JournalSlot{.lsn = 3, .all_zeros = false, .lba_off_bytes = 0, .len_bytes = 4};
    auto header_buf = make_header(CraftEntryType::SyncRSCommitLSN);
    auto key_buf = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3});
    auto const ctx = make_test_proposal_ctx();

    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    auto const outcome = take_test_proposal_outcome(ctx);
    ASSERT_TRUE(outcome.has_value());
    ASSERT_FALSE(outcome->has_value());
    EXPECT_EQ(outcome->error(), make_error_condition(volume_error::INTERNAL_ERROR));
    EXPECT_TRUE(dev_->is_faulted());
}

// An exception escaping an apply fails that entry's proposal and faults the partition (the apply stopped partway), and
// the queue keeps applying the entries after it.
TEST_F(CraftRaftEntriesTest, ApplyThatThrowsFailsProposalAndQueueContinues) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {});
    fetcher_.should_throw = true; // the Sync's catch-up of slots 3..5 throws
    auto sync_hdr = make_header(CraftEntryType::SyncRSCommitLSN);
    auto sync_key = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{});
    auto login_hdr = make_header(CraftEntryType::InternalLogin);
    auto login_key = make_internal_login_key(/*client_token=*/7, /*term=*/5);
    auto const sync_ctx = make_test_proposal_ctx();

    dev_->raft_listener().on_commit(1, as_blob(sync_hdr), as_blob(sync_key), {}, sync_ctx);
    dev_->raft_listener().on_commit(2, as_blob(login_hdr), as_blob(login_key), {}, {});

    auto const outcome = take_test_proposal_outcome(sync_ctx);
    ASSERT_TRUE(outcome.has_value());
    ASSERT_FALSE(outcome->has_value());
    EXPECT_EQ(outcome->error(), make_error_condition(volume_error::INTERNAL_ERROR));
    EXPECT_TRUE(dev_->is_faulted());
    EXPECT_EQ(dev_->term(), 5u); // not wedged: the login behind it applied
}

TEST_F(CraftRaftEntriesTest, OnErrorNotLeaderFailsProposalWithNotLeader) {
    auto const ctx = make_test_proposal_ctx();

    dev_->raft_listener().on_error(homestore::ReplServiceError::NOT_LEADER, {}, {}, ctx);

    auto const outcome = take_test_proposal_outcome(ctx);
    ASSERT_TRUE(outcome.has_value());
    ASSERT_FALSE(outcome->has_value());
    EXPECT_EQ(outcome->error(), craft::make_error_condition(craft::craft_error::NOT_LEADER));
}

TEST_F(CraftRaftEntriesTest, OnErrorOtherFailsProposalWithInternalError) {
    auto const ctx = make_test_proposal_ctx();

    dev_->raft_listener().on_error(homestore::ReplServiceError::TIMEOUT, {}, {}, ctx);

    auto const outcome = take_test_proposal_outcome(ctx);
    ASSERT_TRUE(outcome.has_value());
    ASSERT_FALSE(outcome->has_value());
    EXPECT_EQ(outcome->error(), make_error_condition(volume_error::INTERNAL_ERROR));
}

TEST_F(CraftRaftEntriesTest, OnRollbackFailsProposalWithNotLeader) {
    auto const ctx = make_test_proposal_ctx();

    dev_->raft_listener().on_rollback(1, {}, {}, ctx);

    auto const outcome = take_test_proposal_outcome(ctx);
    ASSERT_TRUE(outcome.has_value());
    ASSERT_FALSE(outcome->has_value());
    EXPECT_EQ(outcome->error(), craft::make_error_condition(craft::craft_error::NOT_LEADER));
}

// value_awaitable must complete exactly once: a later report for the same proposal is dropped.
TEST_F(CraftRaftEntriesTest, FirstProposalOutcomeWins) {
    auto header_buf = make_header(CraftEntryType::SyncRSCommitLSN);
    auto key_buf = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/3, /*client_token=*/0, /*empty_slots=*/{});
    auto const ctx = make_test_proposal_ctx();

    dev_->raft_listener().on_rollback(1, {}, {}, ctx);
    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    auto const outcome = take_test_proposal_outcome(ctx);
    ASSERT_TRUE(outcome.has_value());
    ASSERT_FALSE(outcome->has_value());
    EXPECT_EQ(outcome->error(), craft::make_error_condition(craft::craft_error::NOT_LEADER));
}

// The deadline sweep completes an overdue proposal with timed_out (its outcome is unknown, not rejected), and leaves
// one still within its deadline pending.
TEST_F(CraftRaftEntriesTest, DeadlineSweepExpiresOverdueProposal) {
    ProposalDeadlines deadlines;
    auto const t0 = ProposalDeadlines::clock::now();
    auto const ctx = make_test_proposal_ctx();
    deadlines.add(ctx, t0 + std::chrono::seconds{10});

    deadlines.sweep(t0 + std::chrono::seconds{9});
    EXPECT_FALSE(take_test_proposal_outcome(ctx).has_value()); // still pending
    deadlines.sweep(t0 + std::chrono::seconds{10});

    auto const outcome = take_test_proposal_outcome(ctx);
    ASSERT_TRUE(outcome.has_value());
    ASSERT_FALSE(outcome->has_value());
    EXPECT_EQ(outcome->error(), std::make_error_condition(std::errc::timed_out));
    EXPECT_EQ(deadlines.pending(), 0u);
}

// Overlapping proposals keep their own deadlines: one added later with an earlier deadline expires first, and none
// postpones another (as iomgr v13's shared one-shot timer would).
TEST_F(CraftRaftEntriesTest, OverlappingDeadlinesExpireIndependently) {
    ProposalDeadlines deadlines;
    auto const t0 = ProposalDeadlines::clock::now();
    auto const a = make_test_proposal_ctx();
    auto const b = make_test_proposal_ctx();
    auto const c = make_test_proposal_ctx();
    deadlines.add(a, t0 + std::chrono::seconds{10});
    deadlines.add(b, t0 + std::chrono::seconds{1});
    deadlines.add(c, t0 + std::chrono::seconds{20});

    deadlines.sweep(t0 + std::chrono::seconds{1});
    EXPECT_FALSE(take_test_proposal_outcome(a).has_value());
    EXPECT_TRUE(take_test_proposal_outcome(b).has_value());

    deadlines.sweep(t0 + std::chrono::seconds{10});
    EXPECT_TRUE(take_test_proposal_outcome(a).has_value());
    EXPECT_FALSE(take_test_proposal_outcome(c).has_value());
    EXPECT_EQ(deadlines.pending(), 1u);
}

// The entry commits after its deadline: it's still applied, but the proposer was already told timed_out.
TEST_F(CraftRaftEntriesTest, CommitAfterDeadlineIsAppliedButOutcomeStaysTimedOut) {
    auto header_buf = make_header(CraftEntryType::SyncRSCommitLSN);
    auto key_buf = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/3, /*client_token=*/0, /*empty_slots=*/{});
    ProposalDeadlines deadlines;
    auto const t0 = ProposalDeadlines::clock::now();
    auto const ctx = make_test_proposal_ctx();
    deadlines.add(ctx, t0);

    deadlines.sweep(t0);
    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    auto const outcome = take_test_proposal_outcome(ctx);
    ASSERT_TRUE(outcome.has_value());
    ASSERT_FALSE(outcome->has_value());
    EXPECT_EQ(outcome->error(), std::make_error_condition(std::errc::timed_out));
    EXPECT_EQ(dev_->last_append_lsn(), 3); // applied all the same
}

// Closed, as the proposer's stop() does it: hands back every proposal still tracked, with or without a deadline, and
// tracks no more.
TEST_F(CraftRaftEntriesTest, ClosedDeadlinesHandBackThePendingAndTrackNoMore) {
    ProposalDeadlines deadlines;
    auto const a = make_test_proposal_ctx();
    auto const b = make_test_proposal_ctx();
    EXPECT_TRUE(deadlines.add(a, ProposalDeadlines::clock::now() + std::chrono::seconds{10}));
    EXPECT_TRUE(deadlines.add(b, ProposalDeadlines::clock::time_point::max()));

    auto const pending = deadlines.close();
    EXPECT_EQ(pending, (std::vector< intrusive< homestore::repl_req_ctx > >{a, b}));
    EXPECT_EQ(deadlines.pending(), 0u);
    EXPECT_FALSE(deadlines.add(make_test_proposal_ctx(), ProposalDeadlines::clock::now()));
    EXPECT_EQ(deadlines.pending(), 0u);
}

// A proposal that commits before its deadline is released by the next sweep, not held until its deadline.
TEST_F(CraftRaftEntriesTest, SweepDropsCommittedProposalWithoutExpiringIt) {
    auto header_buf = make_header(CraftEntryType::SyncRSCommitLSN);
    auto key_buf = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/3, /*client_token=*/0, /*empty_slots=*/{});
    ProposalDeadlines deadlines;
    auto const t0 = ProposalDeadlines::clock::now();
    auto const ctx = make_test_proposal_ctx();
    deadlines.add(ctx, t0 + std::chrono::seconds{10});

    dev_->raft_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);
    deadlines.sweep(t0 + std::chrono::seconds{1}); // well before the deadline

    auto const outcome = take_test_proposal_outcome(ctx);
    ASSERT_TRUE(outcome.has_value());
    EXPECT_TRUE(outcome->has_value());
    EXPECT_EQ(deadlines.pending(), 0u);
}

// A ctx this backend didn't create (or none, as on a follower) is left alone.
TEST_F(CraftRaftEntriesTest, ForeignProposalCtxIgnored) {
    auto const foreign = intrusive< homestore::repl_req_ctx >{new homestore::repl_req_ctx()};

    dev_->raft_listener().on_error(homestore::ReplServiceError::TIMEOUT, {}, {}, foreign);
    dev_->raft_listener().on_rollback(1, {}, {}, foreign);

    EXPECT_FALSE(take_test_proposal_outcome(foreign).has_value());
}

// ── apply queue (SDSTOR-26655) ────────────────────────────────────────────────
//
// Committed entries apply one at a time, in log order, each fully before the next. Each test holds a Sync(5) apply
// mid-catch-up on the fetcher's gate (slots 3..5 are missing), then commits more entries behind it.

// An InternalLogin committed while a Sync is still applying waits for it, and the Sync's proposal completes only once
// its apply has finished.
TEST_F(CraftRaftEntriesTest, InternalLoginWaitsForSuspendedSyncApply) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {});
    fetcher_.hold_fetch = true;
    auto sync_hdr = make_header(CraftEntryType::SyncRSCommitLSN);
    auto sync_key = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{});
    auto login_hdr = make_header(CraftEntryType::InternalLogin);
    auto login_key = make_internal_login_key(/*client_token=*/7, /*term=*/5);
    auto const sync_ctx = make_test_proposal_ctx();

    dev_->raft_listener().on_commit(1, as_blob(sync_hdr), as_blob(sync_key), {}, sync_ctx);
    dev_->raft_listener().on_commit(2, as_blob(login_hdr), as_blob(login_key), {}, {});
    EXPECT_FALSE(take_test_proposal_outcome(sync_ctx).has_value()); // still applying
    EXPECT_EQ(dev_->term(), 0u);                                    // the login waits behind it

    fetcher_.fetch_gate.complete(true);

    auto const outcome = take_test_proposal_outcome(sync_ctx);
    ASSERT_TRUE(outcome.has_value());
    EXPECT_TRUE(outcome->has_value());
    EXPECT_EQ(dev_->term(), 5u);
}

// A second Sync committed during the first one's apply changes its state at once, but its I/O (the catch-up) starts
// only after the first apply finishes.
TEST_F(CraftRaftEntriesTest, SyncsApplyInLogOrder) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {});
    fetcher_.hold_fetch = true;
    auto hdr = make_header(CraftEntryType::SyncRSCommitLSN);
    auto key5 = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{});
    auto key8 = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/8, /*client_token=*/0, /*empty_slots=*/{});

    dev_->raft_listener().on_commit(1, as_blob(hdr), as_blob(key5), {}, {});
    dev_->raft_listener().on_commit(2, as_blob(hdr), as_blob(key8), {}, {});
    EXPECT_EQ(dev_->last_append_lsn(), 8);                                 // state: at commit
    EXPECT_EQ(fetcher_.last_requested, (std::vector< int64_t >{3, 4, 5})); // I/O: only the first catch-up so far

    fetcher_.fetch_gate.complete(true);

    EXPECT_EQ(fetcher_.last_requested, (std::vector< int64_t >{3, 4, 5, 6, 7, 8}));
}

// A Sync waiting in the queue already fences writes: its Empty verdict takes effect at commit, so a late write to the
// voided slot is rejected before it can be acked, not discarded once the queue reaches the verdict.
TEST_F(CraftRaftEntriesTest, QueuedSyncVerdictFencesWritesAtCommit) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {});
    fetcher_.hold_fetch = true;
    auto hdr = make_header(CraftEntryType::SyncRSCommitLSN);
    auto key3 = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/3, /*client_token=*/0, /*empty_slots=*/{});
    auto key5 = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{4});

    dev_->raft_listener().on_commit(1, as_blob(hdr), as_blob(key3), {}, {}); // held in catch-up
    dev_->raft_listener().on_commit(2, as_blob(hdr), as_blob(key5), {}, {}); // queued behind it
    EXPECT_TRUE(dev_->is_empty_slot(4));

    auto w = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{.term = 0, .commit_lsn = -1, .all_committed_lsn = -1}, /*dlsn=*/4,
                    /*addr=*/0, /*len=*/k_page_size, sisl::sg_list{})); // empty data -> all_zeros write
    ASSERT_FALSE(w.has_value());
    EXPECT_EQ(w.error(), make_error_condition(volume_error::EMPTY_SLOT));

    fetcher_.fetch_gate.complete(true);
}

// ...and stops serving the voided slot at commit: its overlay entry is retired then, not when its blocks are freed.
TEST_F(CraftRaftEntriesTest, QueuedSyncVerdictStopsServingVoidedSlotAtCommit) {
    dev_->set_peer_fetcher(&fetcher_);
    auto w = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{.term = 0, .commit_lsn = -1, .all_committed_lsn = -1}, /*dlsn=*/4,
                    /*addr=*/5 * k_page_size, /*len=*/k_page_size, sisl::sg_list{})); // all_zeros write at lba 5
    ASSERT_TRUE(w.has_value());
    ASSERT_EQ(dev_->overlay_lsn_for(5), 4);
    fetcher_.hold_fetch = true;
    auto hdr = make_header(CraftEntryType::SyncRSCommitLSN);
    auto key3 = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/3, /*client_token=*/0, /*empty_slots=*/{});
    auto key5 = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{4});

    ASSERT_TRUE(dev_->overlay_has_extent(4));

    dev_->raft_listener().on_commit(1, as_blob(hdr), as_blob(key3), {}, {}); // held catching up 0..3
    dev_->raft_listener().on_commit(2, as_blob(hdr), as_blob(key5), {}, {}); // voids the held slot 4
    EXPECT_EQ(dev_->overlay_lsn_for(5), -1);
    EXPECT_FALSE(dev_->overlay_has_extent(4)); // retired through its extent, not an overlay scan

    fetcher_.fetch_gate.complete(true);
}

// An lsn's extent lasts only while it owns an overlay entry: later writes take over its LBAs, and the extent goes
// with the last one, so the extents never outnumber the overlay.
TEST_F(CraftRaftEntriesTest, SupersededWriteDropsItsOverlayExtent) {
    auto const write_at = [this](int64_t dlsn, uint64_t lba, uint64_t nlbas) {
        return homeblocks::detail::sync_get(
            dev_->write(craft::client_hdr{.term = 0, .commit_lsn = -1, .all_committed_lsn = -1}, dlsn,
                        /*addr=*/lba * k_page_size, /*len=*/nlbas * k_page_size, sisl::sg_list{})); // all_zeros
    };
    ASSERT_TRUE(write_at(/*dlsn=*/4, /*lba=*/5, /*nlbas=*/2).has_value()); // lbas 5..6
    ASSERT_TRUE(write_at(/*dlsn=*/6, /*lba=*/5, /*nlbas=*/1).has_value());
    EXPECT_TRUE(dev_->overlay_has_extent(4)); // still owns lba 6

    ASSERT_TRUE(write_at(/*dlsn=*/7, /*lba=*/6, /*nlbas=*/1).has_value());

    EXPECT_FALSE(dev_->overlay_has_extent(4));
    EXPECT_TRUE(dev_->overlay_has_extent(6));
    EXPECT_TRUE(dev_->overlay_has_extent(7));
}

// A committed InternalLogin waiting in the queue already fences the old session: its writes are rejected from the
// login's commit, not once the queue reaches it. The new term is admitted only when the login applies.
TEST_F(CraftRaftEntriesTest, QueuedLoginFencesOldSessionWritesAtCommit) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {});
    fetcher_.hold_fetch = true;
    auto sync_hdr = make_header(CraftEntryType::SyncRSCommitLSN);
    auto sync_key = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{});
    auto login_hdr = make_header(CraftEntryType::InternalLogin);
    auto login_key = make_internal_login_key(/*client_token=*/7, /*term=*/5);
    auto const write_at = [this](uint64_t term, int64_t dlsn) {
        return homeblocks::detail::sync_get(
            dev_->write(craft::client_hdr{.term = term, .commit_lsn = -1, .all_committed_lsn = -1}, dlsn,
                        /*addr=*/0, /*len=*/k_page_size, sisl::sg_list{})); // empty data -> all_zeros write
    };
    auto const stale = craft::make_error_condition(craft::craft_error::STALE_TERM);

    dev_->raft_listener().on_commit(1, as_blob(sync_hdr), as_blob(sync_key), {}, {});   // held in catch-up
    dev_->raft_listener().on_commit(2, as_blob(login_hdr), as_blob(login_key), {}, {}); // queued behind it

    auto old_session = write_at(/*term=*/0, /*dlsn=*/6);
    ASSERT_FALSE(old_session.has_value());
    EXPECT_EQ(old_session.error(), stale);
    auto new_session = write_at(/*term=*/5, /*dlsn=*/6); // not admitted until the login applies
    ASSERT_FALSE(new_session.has_value());
    EXPECT_EQ(new_session.error(), stale);

    fetcher_.fetch_gate.complete(true);

    EXPECT_TRUE(write_at(/*term=*/5, /*dlsn=*/6).has_value());
}

// An old-session write already in flight when the login commits isn't acked either: its post-flight check sees the
// fence, though the login itself is still queued.
TEST_F(CraftRaftEntriesTest, QueuedLoginFencesInFlightOldSessionWrite) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {});
    fetcher_.hold_fetch = true;
    auto sync_hdr = make_header(CraftEntryType::SyncRSCommitLSN);
    auto sync_key = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{});
    auto login_hdr = make_header(CraftEntryType::InternalLogin);
    auto login_key = make_internal_login_key(/*client_token=*/7, /*term=*/5);
    dev_->raft_listener().on_commit(1, as_blob(sync_hdr), as_blob(sync_key), {}, {}); // held in catch-up
    journal_->on_write = [&](int64_t) {
        dev_->raft_listener().on_commit(2, as_blob(login_hdr), as_blob(login_key), {}, {}); // queued behind it
    };

    auto r = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{.term = 0, .commit_lsn = -1, .all_committed_lsn = -1}, /*dlsn=*/6,
                    /*addr=*/0, /*len=*/k_page_size, sisl::sg_list{})); // empty data -> all_zeros write

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_EQ(dev_->term(), 0u); // the login hasn't applied yet

    journal_->on_write = nullptr;
    fetcher_.fetch_gate.complete(true);
}

// A malformed entry fails its proposal at once, even while an apply is in progress, and doesn't block later entries.
TEST_F(CraftRaftEntriesTest, MalformedEntryFailsAtOnceWithoutStallingTheQueue) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {});
    fetcher_.hold_fetch = true;
    auto sync_hdr = make_header(CraftEntryType::SyncRSCommitLSN);
    auto sync_key = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{});
    std::vector< uint8_t > bad_hdr; // too small to hold an entry type
    auto login_hdr = make_header(CraftEntryType::InternalLogin);
    auto login_key = make_internal_login_key(/*client_token=*/7, /*term=*/5);
    auto const bad_ctx = make_test_proposal_ctx();

    dev_->raft_listener().on_commit(1, as_blob(sync_hdr), as_blob(sync_key), {}, {});
    dev_->raft_listener().on_commit(2, as_blob(bad_hdr), as_blob(sync_key), {}, bad_ctx);
    auto const bad = take_test_proposal_outcome(bad_ctx);
    ASSERT_TRUE(bad.has_value());
    ASSERT_FALSE(bad->has_value());
    EXPECT_EQ(bad->error(), make_error_condition(volume_error::INVALID_ENTRY));
    dev_->raft_listener().on_commit(3, as_blob(login_hdr), as_blob(login_key), {}, {});

    fetcher_.fetch_gate.complete(true);

    EXPECT_EQ(dev_->term(), 5u);
}

// stop_apply() waits out the apply in progress and fails the queued entry unapplied; an entry committed after it
// fails too.
TEST_F(CraftRaftEntriesTest, StopApplyWaitsOutInFlightApplyAndDropsTheRest) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(2, {});
    fetcher_.hold_fetch = true;
    auto sync_hdr = make_header(CraftEntryType::SyncRSCommitLSN);
    auto sync_key = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{});
    auto login_hdr = make_header(CraftEntryType::InternalLogin);
    auto login_key = make_internal_login_key(/*client_token=*/7, /*term=*/5);
    auto const sync_ctx = make_test_proposal_ctx();
    auto const login_ctx = make_test_proposal_ctx();
    auto const canceled = std::make_error_condition(std::errc::operation_canceled);

    dev_->raft_listener().on_commit(1, as_blob(sync_hdr), as_blob(sync_key), {}, sync_ctx);
    dev_->raft_listener().on_commit(2, as_blob(login_hdr), as_blob(login_key), {}, login_ctx);
    bool stopped = false;
    homeblocks::detail::detach([](std::shared_ptr< CraftReplDev > dev, bool& done) -> async_status {
        auto r = co_await dev->stop_apply();
        done = true;
        co_return r;
    }(dev_, stopped));

    EXPECT_FALSE(stopped); // the Sync is still applying
    auto const dropped = take_test_proposal_outcome(login_ctx);
    ASSERT_TRUE(dropped.has_value());
    ASSERT_FALSE(dropped->has_value());
    EXPECT_EQ(dropped->error(), canceled);

    fetcher_.fetch_gate.complete(true);

    EXPECT_TRUE(stopped);
    auto const applied = take_test_proposal_outcome(sync_ctx);
    ASSERT_TRUE(applied.has_value());
    EXPECT_TRUE(applied->has_value());
    EXPECT_EQ(dev_->term(), 0u); // the dropped login never applied

    auto const late_ctx = make_test_proposal_ctx();
    dev_->raft_listener().on_commit(3, as_blob(login_hdr), as_blob(login_key), {}, late_ctx);
    auto const late = take_test_proposal_outcome(late_ctx);
    ASSERT_TRUE(late.has_value());
    ASSERT_FALSE(late->has_value());
    EXPECT_EQ(late->error(), canceled);
    EXPECT_EQ(dev_->term(), 0u);

    // A Sync committed after the stop changes no state either: no watermark, no verdict.
    auto late_sync_key = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/8, /*client_token=*/0, /*empty_slots=*/{7});
    auto const late_sync_ctx = make_test_proposal_ctx();
    dev_->raft_listener().on_commit(4, as_blob(sync_hdr), as_blob(late_sync_key), {}, late_sync_ctx);
    auto const late_sync = take_test_proposal_outcome(late_sync_ctx);
    ASSERT_TRUE(late_sync.has_value());
    ASSERT_FALSE(late_sync->has_value());
    EXPECT_EQ(late_sync->error(), canceled);
    EXPECT_EQ(dev_->last_append_lsn(), 5);
    EXPECT_FALSE(dev_->is_empty_slot(7));
}

// ══ S8: durable partition state and restart recovery (SDSTOR-22745) ══════════

// One page at lba lsn, carrying data (so freeing it frees blocks) unless zeros.
JournalSlot page_slot(int64_t lsn, bool zeros = false) {
    return JournalSlot{.lsn = lsn,
                       .all_zeros = zeros,
                       .lba_off_bytes = static_cast< lba_t >(lsn) * k_page_size,
                       .len_bytes = k_page_size,
                       .csums = zeros ? std::vector< homestore::csum_t >{} : std::vector< homestore::csum_t >{1}};
}

// Zero slots go through the index's delete path, which reports the blocks it unmapped (see replace_on_delete).
std::map< int64_t, JournalSlot > page_slots(std::initializer_list< int64_t > lsns, bool zeros = false) {
    std::map< int64_t, JournalSlot > m;
    for (int64_t l : lsns)
        m[l] = page_slot(l, zeros);
    return m;
}

CraftDurableState sample_state() {
    return CraftDurableState{.term = 3,
                             .client_token = 77,
                             .last_applied_raft_idx = 9,
                             .durable_commit_lsn = 1,
                             .sync_watermark = 6,
                             .empty_lsns = {4}};
}

// An index that applies nothing and replaces block 7 at every LBA it unmaps or remaps.
status replace_on_write(lba_t, lba_t, std::unordered_map< lba_t, BlockInfo >& info) {
    for (auto& [_, b] : info)
        b.old_blkid = homestore::blk_id{7, 1, 0};
    return ok();
}
status replace_on_delete(lba_t, lba_t, std::vector< homestore::blk_id >& freed) {
    freed.push_back(homestore::blk_id{7, 1, 0});
    return ok();
}

// ── codec ──

TEST(CraftDurableStateCodec, RoundTrips) {
    for (auto const& s : {sample_state(), CraftDurableState{}}) {
        auto const bytes = encode_durable_state(s);
        auto const d = decode_durable_state(bytes.data(), bytes.size());
        ASSERT_TRUE(d.has_value());
        EXPECT_EQ(*d, s);
    }
}

TEST(CraftDurableStateCodec, RejectsMalformedImages) {
    auto s = sample_state();
    s.empty_lsns = {4, 9};
    auto const good = encode_durable_state(s);

    EXPECT_FALSE(decode_durable_state(nullptr, 0).has_value());
    EXPECT_FALSE(decode_durable_state(good.data(), 10).has_value());              // shorter than the header
    EXPECT_FALSE(decode_durable_state(good.data(), good.size() - 1).has_value()); // verdicts cut short
    auto longer = good;
    longer.push_back(0);
    EXPECT_FALSE(decode_durable_state(longer.data(), longer.size()).has_value()); // trailing bytes

    auto bad_magic = good;
    bad_magic[0] ^= 0xff;
    EXPECT_FALSE(decode_durable_state(bad_magic.data(), bad_magic.size()).has_value());
    auto bad_version = good;
    bad_version[8] ^= 0xff;
    EXPECT_FALSE(decode_durable_state(bad_version.data(), bad_version.size()).has_value());

    // Verdicts must be strictly ascending and not negative; at or below the frontier (1) is fine.
    size_t const first = good.size() - 2 * sizeof(int64_t);
    int64_t const nine = 9, four = 4, one = 1, negative = -1;
    auto unsorted = good;
    std::memcpy(unsorted.data() + first, &nine, sizeof(nine));
    std::memcpy(unsorted.data() + first + sizeof(int64_t), &four, sizeof(four));
    EXPECT_FALSE(decode_durable_state(unsorted.data(), unsorted.size()).has_value());
    auto below_zero = good;
    std::memcpy(below_zero.data() + first, &negative, sizeof(negative));
    EXPECT_FALSE(decode_durable_state(below_zero.data(), below_zero.size()).has_value());
    auto at_frontier = good;
    std::memcpy(at_frontier.data() + first, &one, sizeof(one));
    EXPECT_TRUE(decode_durable_state(at_frontier.data(), at_frontier.size()).has_value());
}

// ── write-ahead persistence ──

// Without a store nothing is persisted, and the applies work as before.
TEST_F(CraftRaftEntriesTest, WithoutAStoreNothingIsPersisted) {
    EXPECT_TRUE(homeblocks::detail::sync_get(persist()).has_value());
    commit(1, make_header(CraftEntryType::InternalLogin), make_internal_login_key(42, 5));
    EXPECT_EQ(dev_->term(), 5u);
}

// InternalLogin makes the new session durable, then admits it; the image then records the entry as applied.
TEST_F(CraftRaftEntriesTest, InternalLoginPersistsTheSessionBeforeAdmittingIt) {
    auto* store = with_store();
    store->hold = true;
    commit(1, make_header(CraftEntryType::InternalLogin), make_internal_login_key(42, 5));
    EXPECT_EQ(dev_->term(), 0u) << "admitted before the session was durable";

    store->gate.complete(true);
    EXPECT_EQ(dev_->term(), 5u);
    EXPECT_EQ(dev_->client_token(), 42u);
    ASSERT_TRUE(store->saved.has_value());
    EXPECT_EQ(store->saved->term, 5u);
    EXPECT_EQ(store->saved->client_token, 42u);
    EXPECT_EQ(store->saved->last_applied_raft_idx, 1);
}

// A session that can't be made durable isn't admitted here. Later entries still apply, and the next image that is
// written holds the committed login's session: a restart admits it.
TEST_F(CraftRaftEntriesTest, InternalLoginPersistFailureDoesNotAdmit) {
    auto* store = with_store();
    commit(1, make_header(CraftEntryType::InternalLogin), make_internal_login_key(42, 3));
    ASSERT_EQ(store->saved->last_applied_raft_idx, 1);

    store->fail_persist = true;
    commit(2, make_header(CraftEntryType::InternalLogin), make_internal_login_key(43, 5));
    EXPECT_EQ(dev_->term(), 3u) << "admitted although not durable";

    store->fail_persist = false;
    commit(3, make_header(CraftEntryType::SyncRSCommitLSN), make_sync_rs_commit_lsn_key(2, 0, {}));
    EXPECT_EQ(dev_->last_append_lsn(), 2);
    EXPECT_EQ(store->saved->sync_watermark, 2);
    EXPECT_EQ(store->saved->last_applied_raft_idx, 3);
    EXPECT_EQ(store->saved->term, 5u);
    EXPECT_EQ(store->saved->client_token, 43u);
}

// A Sync's verdicts and watermark are durable before any block a verdict voids is freed.
TEST_F(CraftRaftEntriesTest, SyncMakesItsVerdictsDurableBeforeFreeing) {
    auto* store = with_store();
    journal_->slots = page_slots({0, 1, 2, 3});
    dev_->seed_lsns(3, {});
    std::optional< CraftDurableState > at_free;
    journal_->on_free = [&] { at_free = store->saved; };

    ASSERT_TRUE(do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3}).has_value());
    EXPECT_EQ(journal_->free_data_calls, 1);
    ASSERT_TRUE(at_free.has_value()) << "freed before anything was durable";
    EXPECT_EQ(at_free->empty_lsns, (std::set< int64_t >{3}));
    EXPECT_EQ(at_free->sync_watermark, 5);
}

// If the verdicts can't be made durable nothing is freed: a restart replaying this Sync would free the same blocks
// again. The verdict itself holds.
TEST_F(CraftRaftEntriesTest, SyncLeaksVoidedBlocksIfItsVerdictsCantBeMadeDurable) {
    auto* store = with_store();
    store->fail_persist = true;
    journal_->slots = page_slots({0, 1, 2, 3});
    dev_->seed_lsns(3, {});

    ASSERT_TRUE(do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3}).has_value());
    EXPECT_EQ(journal_->free_data_calls, 0);
    EXPECT_TRUE(dev_->is_empty_slot(3));
}

// The same for a catch-up fill an Empty verdict voided while it was being journalled.
TEST_F(CraftRaftEntriesTest, CatchUpLeaksAVoidedFillIfTheVerdictCantBeMadeDurable) {
    auto* store = with_store();
    store->fail_persist = true;
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(5, {4});
    fetcher_.response = {page_slot(4)};
    journal_->on_write = [this](int64_t lsn) {
        if (lsn == 4) dev_->seed_empty({4});
    };

    ASSERT_TRUE(do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0).has_value());
    EXPECT_EQ(dev_->overlay_lsn_for(4), -1);
    EXPECT_EQ(journal_->free_data_calls, 0);
}

// Concurrent persist_state() calls share one writer: each returns once an image taken after its call is durable.
TEST_F(CraftRaftEntriesTest, ConcurrentPersistsShareOneWriter) {
    auto* store = with_store();
    store->hold = true;
    std::optional< status > first, second, third;
    auto run = [](CraftRaftEntriesTest* t, std::optional< status >& out) -> async_status {
        out = co_await t->persist();
        co_return ok();
    };
    homeblocks::detail::detach(run(this, first));  // the writer starts, held by the store
    homeblocks::detail::detach(run(this, second)); // waits for the next round
    homeblocks::detail::detach(run(this, third));  // joins that same round
    EXPECT_FALSE(first.has_value());
    EXPECT_EQ(store->persist_calls, 1);

    store->gate.complete(true);
    ASSERT_TRUE(first && second && third);
    EXPECT_TRUE(first->has_value() && second->has_value() && third->has_value());
    EXPECT_EQ(store->persist_calls, 2); // one round for `first`, one for `second` and `third`
}

// A failed write fails the callers it covered, not later ones.
TEST_F(CraftRaftEntriesTest, PersistFailureFailsItsCallers) {
    auto* store = with_store();
    store->fail_persist = true;
    EXPECT_FALSE(homeblocks::detail::sync_get(persist()).has_value());
    store->fail_persist = false;
    EXPECT_TRUE(homeblocks::detail::sync_get(persist()).has_value());
}

// ── durable commit frontier ──

// A checkpoint that flushed makes commit_lsn the durable frontier, and keeps the verdicts at or below it.
TEST_F(CraftRaftEntriesTest, FlushedCheckpointRecordsTheDurableCommit) {
    auto* store = with_store();
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(5);
    dev_->seed_lsns(10, {});

    ASSERT_TRUE(do_apply(/*rs_commit_lsn=*/12, /*client_token=*/0, /*empty_slots=*/{11, 12}).has_value());
    EXPECT_EQ(trigger_.call_count, 1);
    ASSERT_TRUE(store->saved.has_value());
    EXPECT_EQ(store->saved->durable_commit_lsn, 12);
    EXPECT_EQ(store->saved->empty_lsns, (std::set< int64_t >{11, 12}));
}

// stop() waits out a checkpoint trigger in flight: once its flush completes it still persists the frontier and frees
// blocks, which a destroy or a shutdown must not pull out from under it. None starts after stop().
TEST_F(CraftRaftEntriesTest, StopWaitsForTheCheckpointInFlight) {
    auto* store = with_store();
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(5);
    dev_->seed_lsns(10, {});
    trigger_.hold = true;
    ASSERT_TRUE(do_apply(/*rs_commit_lsn=*/12, /*client_token=*/0, /*empty_slots=*/{11, 12}).has_value());
    ASSERT_EQ(trigger_.call_count, 1);

    bool stopped = false;
    homeblocks::detail::detach([](CraftReplDev* dev, bool* stopped) -> async_status {
        (void)co_await dev->stop(/*keep_queued=*/true);
        *stopped = true;
        co_return ok();
    }(dev_.get(), &stopped));
    EXPECT_FALSE(stopped) << "the checkpoint is still in flight";
    trigger_.gate.complete(true);
    EXPECT_TRUE(stopped);
    ASSERT_TRUE(store->saved.has_value());
    EXPECT_EQ(store->saved->durable_commit_lsn, 12) << "it finished before stop() returned";

    dev_->seed_lsns(20, {});
    ASSERT_TRUE(do_apply(/*rs_commit_lsn=*/20, /*client_token=*/0).has_value());
    EXPECT_EQ(trigger_.call_count, 1) << "none starts after stop()";
}

// A checkpoint request that coalesced with a flush already running proves nothing about the slots applied since that
// flush started: the frontier stays.
TEST_F(CraftRaftEntriesTest, CoalescedCheckpointDoesNotRecordTheDurableCommit) {
    with_store();
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(5);
    trigger_.flushed = false;
    dev_->seed_lsns(10, {});

    ASSERT_TRUE(do_apply(/*rs_commit_lsn=*/10, /*client_token=*/0).has_value());
    EXPECT_EQ(trigger_.call_count, 1);
    EXPECT_EQ(dev_->durable_state().durable_commit_lsn, -1);
}

// The frontier never moves back, and a failed persist leaves it recorded in memory for the next one.
TEST_F(CraftRaftEntriesTest, DurableCommitNeverRegressesAndSurvivesAPersistFailure) {
    auto* store = with_store();
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(5);
    dev_->seed_lsns(20, {});
    ASSERT_TRUE(do_apply(10, 0).has_value());
    EXPECT_EQ(store->saved->durable_commit_lsn, 10);

    store->fail_persist = true;
    ASSERT_TRUE(do_apply(20, 0).has_value());
    EXPECT_EQ(dev_->durable_state().durable_commit_lsn, 20);
    EXPECT_EQ(store->saved->durable_commit_lsn, 10);
}

// ── superseded blocks ──

// With a state store, a block an index write supersedes is freed only once a checkpoint covering the replacing slot is
// durable: until then a restart could apply the older slot again, which still needs its block.
TEST_F(CraftRaftEntriesTest, SupersededBlocksWaitForTheDurableFrontier) {
    with_store();
    journal_->slots = page_slots({0, 1}, /*zeros=*/true);
    dev_->seed_lsns(1, {});
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(1);

    trigger_.flushed = false; // a coalesced checkpoint: the blocks keep waiting
    ASSERT_TRUE(homeblocks::detail::sync_get(dev_->commit_with(0, replace_on_write, replace_on_delete)).has_value());
    EXPECT_EQ(journal_->free_data_calls, 0);

    trigger_.flushed = true; // a flushed checkpoint at commit 1 covers both replacing slots
    ASSERT_TRUE(homeblocks::detail::sync_get(dev_->commit_with(1, replace_on_write, replace_on_delete)).has_value());
    EXPECT_EQ(dev_->durable_state().durable_commit_lsn, 1);
    EXPECT_EQ(journal_->free_data_calls, 2);
}

// Without a store nothing is applied again after a restart, so superseded blocks are freed at once, as before S8.
TEST_F(CraftRaftEntriesTest, SupersededBlocksAreFreedAtOnceWithoutAStore) {
    journal_->slots = page_slots({0}, /*zeros=*/true);
    dev_->seed_lsns(0, {});
    ASSERT_TRUE(homeblocks::detail::sync_get(dev_->commit_with(0, replace_on_write, replace_on_delete)).has_value());
    EXPECT_EQ(journal_->free_data_calls, 1);
}

// ── restart recovery ──

// A partition that never persisted anything restarts empty and healthy.
TEST_F(CraftRaftEntriesTest, RestartWithoutAnImageStartsEmpty) {
    restart(std::nullopt, page_slots({0, 1}));
    EXPECT_FALSE(dev_->is_faulted());
    EXPECT_FALSE(dev_->is_recovering());
    EXPECT_EQ(dev_->commit_lsn(), -1);
    EXPECT_EQ(dev_->last_append_lsn(), -1);
}

// The session comes back from the image; commit_lsn restarts at the durable frontier; last_append_lsn is the larger of
// the journal tail and the Sync watermark; a slot above the frontier the journal doesn't hold (and isn't Empty) is
// missing, and the ones it holds are readable again.
TEST_F(CraftRaftEntriesTest, RestartRestoresTheSessionAndTheJournalRange) {
    restart(sample_state(), page_slots({0, 1, 2, 3, 7}, /*zeros=*/true)); // a zero slot's overlay entry needs no blkid
    ASSERT_FALSE(dev_->is_faulted());
    EXPECT_FALSE(dev_->is_recovering());

    EXPECT_EQ(dev_->term(), 3u);
    EXPECT_EQ(dev_->client_token(), 77u);
    EXPECT_EQ(dev_->commit_lsn(), 1);
    EXPECT_EQ(dev_->last_append_lsn(), 7);
    EXPECT_TRUE(dev_->is_empty_slot(4));
    EXPECT_EQ(dev_->missing_count(), 2u);
    EXPECT_TRUE(dev_->is_missing(5));
    EXPECT_TRUE(dev_->is_missing(6));
    EXPECT_EQ(dev_->overlay_lsn_for(2), 2);
    EXPECT_EQ(dev_->overlay_lsn_for(7), 7);
    EXPECT_EQ(dev_->overlay_lsn_for(1), -1) << "at the frontier: in the index, not the overlay";
    EXPECT_EQ(dev_->durable_state(), sample_state());
}

// A restart commits the blocks of every live slot again, as the allocator's record of them may not have been persisted:
// the slots above the durable frontier the journal holds, not the zero ones, the Empty ones or those at the frontier.
TEST_F(CraftRaftEntriesTest, RestartCommitsTheBlocksOfLiveSlotsAgain) {
    auto slots = page_slots({0, 1, 2, 3, 4, 7});
    for (auto& [lsn, slot] : slots)
        slot.blkid = homestore::multi_blk_id{static_cast< homestore::blk_num_t >(100 + lsn), 1, 0};
    slots[3] = page_slot(3, /*zeros=*/true);
    restart(sample_state(), std::move(slots)); // frontier 1, 4 Empty, 5 and 6 missing
    ASSERT_FALSE(dev_->is_faulted());
    EXPECT_EQ(journal_->commits, (std::vector< std::pair< homestore::blk_num_t, bool > >{{102, true}, {107, true}}));
}

// Blocks a restart can't commit again could be handed out while a slot still references them: the partition faults.
TEST_F(CraftRaftEntriesTest, RestartFaultsWhenItCantCommitALiveSlotsBlocks) {
    auto* store = with_store();
    store->saved = sample_state();
    journal_->slots = page_slots({0, 1, 2});
    journal_->slots[2].blkid = homestore::multi_blk_id{102, 1, 0};
    journal_->fail_commit = true;
    dev_->trigger_on_restart();
    EXPECT_TRUE(dev_->is_faulted());
}

// The verdicts at or below the durable frontier survive a restart too: a late write to a voided slot is still refused,
// not acked as already written.
TEST_F(CraftRaftEntriesTest, RestartKeepsTheVerdictsAtOrBelowTheFrontier) {
    auto image = sample_state(); // frontier 1
    image.empty_lsns = {0, 4};
    restart(image, page_slots({1, 2, 3}));
    ASSERT_FALSE(dev_->is_faulted());
    EXPECT_TRUE(dev_->is_empty_slot(0));
    auto const w = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{.term = 3, .commit_lsn = -1, .all_committed_lsn = -1}, /*dlsn=*/0,
                    /*addr=*/0, /*len=*/k_page_size, sisl::sg_list{}));
    ASSERT_FALSE(w.has_value());
    EXPECT_EQ(w.error(), make_error_condition(volume_error::EMPTY_SLOT));
}

// The Sync watermark can be above the journal tail: the slots up to it were never received here.
TEST_F(CraftRaftEntriesTest, RestartRaisesLastAppendToTheSyncWatermark) {
    restart(sample_state(), page_slots({0, 1, 2}));
    EXPECT_EQ(dev_->last_append_lsn(), 6);
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_FALSE(dev_->is_missing(4)); // Empty
    EXPECT_TRUE(dev_->is_missing(5));
    EXPECT_TRUE(dev_->is_missing(6));
}

// The log replay after a restart delivers some entries the image already counts as applied: they are skipped.
TEST_F(CraftRaftEntriesTest, RestartSkipsEntriesAppliedBeforeIt) {
    restart(sample_state(), page_slots({0, 1, 2, 3}));
    commit(9, make_header(CraftEntryType::InternalLogin), make_internal_login_key(100, 8));
    EXPECT_EQ(dev_->term(), 3u) << "applied again";
    commit(10, make_header(CraftEntryType::InternalLogin), make_internal_login_key(100, 8));
    EXPECT_EQ(dev_->term(), 8u);
}

// An image that can't be read, or a journal tail that can't be read, faults the partition: restarting it empty would
// forget its session and verdicts.
TEST_F(CraftRaftEntriesTest, RestartFaultsOnAnUnreadableImage) {
    auto* store = with_store();
    store->saved = sample_state();
    store->fail_load = true;
    dev_->trigger_on_restart();
    EXPECT_TRUE(dev_->is_faulted());
    EXPECT_TRUE(store->faulted) << "the record says so, for the next restart";
}

// A faulted partition persists nothing more: its record keeps the last image, marked faulted, and a checkpoint's save
// succeeds without writing. Persisting what the partition holds now could make a later restart look healthy.
TEST_F(CraftRaftEntriesTest, AFaultedPartitionPersistsNothingMore) {
    auto* store = with_store();
    ASSERT_TRUE(homeblocks::detail::sync_get(persist()).has_value());
    auto const saved = store->saved;
    auto const calls = store->persist_calls;
    dev_->seed_lsns(5, {});
    dev_->seed_commit_lsn(5);
    ASSERT_FALSE(do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3}).has_value());
    ASSERT_TRUE(dev_->is_faulted());
    EXPECT_TRUE(store->faulted);

    EXPECT_FALSE(homeblocks::detail::sync_get(persist()).has_value());
    EXPECT_TRUE(homeblocks::detail::sync_get(dev_->save_state()).has_value());
    EXPECT_EQ(store->persist_calls, calls);
    EXPECT_EQ(store->saved, saved);
}

// A record marked faulted faults the next restart at once.
TEST_F(CraftRaftEntriesTest, RestartFaultsOnAFaultedRecord) {
    auto* store = with_store();
    store->saved = sample_state();
    store->faulted = true;
    dev_->trigger_on_restart();
    EXPECT_TRUE(dev_->is_faulted());
}

TEST_F(CraftRaftEntriesTest, RestartFaultsOnAnUnreadableJournalTail) {
    auto* store = with_store();
    store->saved = sample_state();
    journal_->fail_tail = true;
    dev_->trigger_on_restart();
    EXPECT_TRUE(dev_->is_faulted());
}

// A backend that can't report its tail (the interface default) can't be restored either.
TEST_F(CraftRaftEntriesTest, RestartFaultsWhenTheBackendHasNoTail) {
    struct NoTailJournal : MockCraftJournalBackend {
        async_result< int64_t > tail_lsn() override { return CraftJournalBackend::tail_lsn(); }
    };
    auto store = std::make_unique< MockCraftStateStore >();
    store->saved = sample_state();
    dev_ = CraftReplDev::create(volume_id_t{}, std::make_unique< NoTailJournal >(), k_page_size, nullptr,
                                std::move(store));
    dev_->trigger_on_restart();
    EXPECT_TRUE(dev_->is_faulted());
}

// A read error other than "not there" still faults the rebuild.
TEST_F(CraftRaftEntriesTest, RestartFaultsOnAJournalReadError) {
    struct BadReadJournal : MockCraftJournalBackend {
        async_result< JournalSlot > read_slot(int64_t) override {
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
    };
    auto journal = std::make_unique< BadReadJournal >();
    journal->slots = page_slots({0, 1, 2, 3});
    auto store = std::make_unique< MockCraftStateStore >();
    store->saved = sample_state();
    dev_ = CraftReplDev::create(volume_id_t{}, std::move(journal), k_page_size, nullptr, std::move(store));
    dev_->trigger_on_restart();
    EXPECT_TRUE(dev_->is_faulted());
}

// Applying a slot journalled before the restart again must not free the block it replaces in the index: the index may
// already hold a NEWER slot's block there (a checkpoint ran past the recorded frontier). A slot above the replay window
// frees as usual (here deferred: there is a store).
TEST_F(CraftRaftEntriesTest, RestartReplayDoesNotFreeReplacedBlocks) {
    auto image = sample_state();
    image.empty_lsns.clear();
    image.sync_watermark = -1;
    restart(image, page_slots({0, 1, 2, 3}, /*zeros=*/true));
    ASSERT_EQ(dev_->last_append_lsn(), 3);
    journal_->slots[4] = page_slot(4, /*zeros=*/true);
    dev_->seed_lsns(4, {});
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(1);

    auto const c = homeblocks::detail::sync_get(dev_->commit_with(4, replace_on_write, replace_on_delete));
    ASSERT_TRUE(c.has_value());
    EXPECT_EQ(*c, 4);
    EXPECT_EQ(journal_->free_data_calls, 1) << "only slot 4's superseded block, once the frontier covered it";
}

// End to end: what one device persisted restores the next.
TEST_F(CraftRaftEntriesTest, PersistedStateRestoresAfterARestart) {
    auto* store = with_store();
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(2);
    journal_->slots = page_slots({0, 1, 2});
    dev_->seed_lsns(2, {});
    commit(1, make_header(CraftEntryType::SyncRSCommitLSN), make_sync_rs_commit_lsn_key(2, 0, {}));
    commit(2, make_header(CraftEntryType::InternalLogin), make_internal_login_key(42, 5));
    commit(3, make_header(CraftEntryType::SyncRSCommitLSN), make_sync_rs_commit_lsn_key(4, 0, {4}));
    ASSERT_TRUE(store->saved.has_value());
    auto const image = *store->saved;
    auto const slots = journal_->slots;

    restart(image, slots);
    ASSERT_FALSE(dev_->is_faulted());
    EXPECT_EQ(dev_->term(), 5u);
    EXPECT_EQ(dev_->client_token(), 42u);
    EXPECT_EQ(dev_->commit_lsn(), 2);
    EXPECT_EQ(dev_->last_append_lsn(), 4);
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_TRUE(dev_->is_empty_slot(4));
    EXPECT_EQ(dev_->durable_state().last_applied_raft_idx, 3);
}

// ── entries committed but not applied (pending) ──

CraftPendingEntry pending_sync(int64_t raft_idx, int64_t rs_commit_lsn, std::vector< int64_t > to_free = {}) {
    return CraftPendingEntry{.raft_idx = raft_idx,
                             .type = CraftEntryType::SyncRSCommitLSN,
                             .rs_commit_lsn = rs_commit_lsn,
                             .to_free = std::move(to_free)};
}

CraftPendingEntry pending_login(int64_t raft_idx, uint64_t client_token, uint64_t term) {
    return CraftPendingEntry{
        .raft_idx = raft_idx, .type = CraftEntryType::InternalLogin, .client_token = client_token, .term = term};
}

TEST(CraftDurableStateCodec, RoundTripsPendingEntries) {
    auto s = sample_state();
    s.pending = {pending_sync(10, 7, {5, 6}), pending_login(11, 55, 6)};
    auto const bytes = encode_durable_state(s);
    auto const d = decode_durable_state(bytes.data(), bytes.size());
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(*d, s);
}

TEST(CraftDurableStateCodec, RejectsMalformedPendingEntries) {
    auto s = sample_state();
    s.pending = {pending_sync(10, 7, {5}), pending_login(11, 55, 6)};
    auto const good = encode_durable_state(s);
    size_t const record =
        37; // packed: raft_idx, type, rs_commit_lsn, client_token, term, n_free (the login frees none)
    size_t const second = good.size() - record;

    EXPECT_FALSE(decode_durable_state(good.data(), good.size() - 1).has_value()); // cut short
    auto unsorted = good;
    int64_t const ten = 10;
    std::memcpy(unsorted.data() + second, &ten, sizeof(ten)); // raft_idx 10 twice
    EXPECT_FALSE(decode_durable_state(unsorted.data(), unsorted.size()).has_value());
    auto create_volume = good;
    create_volume[second + sizeof(int64_t)] = static_cast< uint8_t >(CraftEntryType::CreateVolume); // not queued
    EXPECT_FALSE(decode_durable_state(create_volume.data(), create_volume.size()).has_value());
    auto overrun = good;
    uint32_t const one = 1;
    std::memcpy(overrun.data() + good.size() - sizeof(uint32_t), &one, sizeof(one)); // a free past the end
    EXPECT_FALSE(decode_durable_state(overrun.data(), overrun.size()).has_value());
}

// Every image holds the entries committed but not yet applied, in log order; once applied they leave it.
TEST_F(CraftRaftEntriesTest, ImageHoldsTheEntriesNotYetApplied) {
    auto* store = with_store();
    dev_->pause_apply();
    commit(5, make_header(CraftEntryType::SyncRSCommitLSN), make_sync_rs_commit_lsn_key(3, 0, {}));
    commit(6, make_header(CraftEntryType::InternalLogin), make_internal_login_key(42, 7));
    ASSERT_TRUE(homeblocks::detail::sync_get(dev_->save_state()).has_value());
    ASSERT_TRUE(store->saved.has_value());
    EXPECT_EQ(store->saved->pending, (std::vector< CraftPendingEntry >{pending_sync(5, 3), pending_login(6, 42, 7)}));
    EXPECT_TRUE(dev_->durable_state().pending.empty()) << "the in-memory image keeps none";

    dev_->resume_apply();
    EXPECT_EQ(dev_->term(), 7u);
    ASSERT_TRUE(homeblocks::detail::sync_get(dev_->save_state()).has_value());
    EXPECT_TRUE(store->saved->pending.empty());
    EXPECT_EQ(store->saved->last_applied_raft_idx, 6);
}

// The entry being applied is pending until its apply completes, without the slots it frees: its frees may have
// happened, and a restart leaks them rather than free twice.
TEST_F(CraftRaftEntriesTest, ImageHoldsTheEntryBeingApplied) {
    auto* store = with_store();
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(0, {});
    journal_->slots[0] = page_slot(0);
    fetcher_.hold_fetch = true; // the Sync's catch-up waits for the gate
    commit(5, make_header(CraftEntryType::SyncRSCommitLSN), make_sync_rs_commit_lsn_key(2, 0, {0}));
    ASSERT_TRUE(homeblocks::detail::sync_get(dev_->save_state()).has_value());
    EXPECT_EQ(store->saved->pending, (std::vector< CraftPendingEntry >{pending_sync(5, 2)}));

    fetcher_.fetch_gate.complete(true);
    EXPECT_TRUE(store->saved->pending.empty());
    EXPECT_EQ(store->saved->last_applied_raft_idx, 5);
    EXPECT_EQ(journal_->free_data_calls, 1) << "slot 0, voided";
}

// A Sync still queued keeps in the image the slots its verdicts voided: its apply frees them, after a restart too.
TEST_F(CraftRaftEntriesTest, ImageHoldsTheFreesOfAQueuedSync) {
    auto* store = with_store();
    dev_->seed_lsns(3, {});
    journal_->slots[2] = page_slot(2);
    dev_->pause_apply();
    commit(5, make_header(CraftEntryType::SyncRSCommitLSN), make_sync_rs_commit_lsn_key(3, 0, {2}));
    ASSERT_TRUE(homeblocks::detail::sync_get(dev_->save_state()).has_value());
    EXPECT_EQ(store->saved->pending, (std::vector< CraftPendingEntry >{pending_sync(5, 3, {2})}));
    EXPECT_EQ(journal_->free_data_calls, 0);

    dev_->resume_apply();
    EXPECT_EQ(journal_->free_data_calls, 1);
    EXPECT_TRUE(store->saved->pending.empty());
}

// A restart applies again the image's pending entries above its last applied index, in log order, and the log replay
// then skips them.
TEST_F(CraftRaftEntriesTest, RestartAppliesThePendingEntriesAgain) {
    auto image = sample_state(); // last applied raft idx 9, session term 3
    image.pending = {pending_login(8, 88, 9), pending_sync(10, 7), pending_login(11, 55, 6)};
    auto* store = restart(image, page_slots({0, 1, 2, 3, 7}, /*zeros=*/true));
    ASSERT_FALSE(dev_->is_faulted());
    EXPECT_EQ(dev_->term(), 6u) << "the login at raft 8 was applied before the restart";
    EXPECT_EQ(dev_->client_token(), 55u);
    EXPECT_EQ(store->saved->last_applied_raft_idx, 11);
    EXPECT_TRUE(store->saved->pending.empty());
    EXPECT_EQ(dev_->invariant_violations(), std::vector< std::string >{}); // e.g. the login fence raised again

    commit(11, make_header(CraftEntryType::InternalLogin), make_internal_login_key(99, 12));
    EXPECT_EQ(dev_->term(), 6u) << "the log replay applied a pending entry a second time";
    commit(12, make_header(CraftEntryType::InternalLogin), make_internal_login_key(77, 8));
    EXPECT_EQ(dev_->term(), 8u);
}

// A pending Sync applies again without its commit-time state change, which the image already holds: no verdict check
// against the restored frontier (its verdicts here are at or below it). One recorded as being applied frees nothing:
// its frees may have happened before.
TEST_F(CraftRaftEntriesTest, RestartAppliesAPendingSyncWithoutItsVerdictsOrFrees) {
    auto image = sample_state();
    image.durable_commit_lsn = 3; // the Sync voided slot 3 before the restart; the frontier has passed it
    image.empty_lsns.clear();
    image.sync_watermark = 3;
    image.pending = {pending_sync(10, 3)};
    restart(image, page_slots({0, 1, 2, 3}));
    EXPECT_FALSE(dev_->is_faulted());
    EXPECT_EQ(journal_->free_data_calls, 0);
    EXPECT_EQ(dev_->durable_state().last_applied_raft_idx, 10);
}

// A Sync still queued at the restart frees the slots its verdicts voided when it applies again; the recovery keeps
// their blocks allocated until then.
TEST_F(CraftRaftEntriesTest, RestartFreesTheVoidedSlotsOfAQueuedSync) {
    auto image = sample_state(); // frontier 1, watermark 6
    image.empty_lsns = {2, 4};
    image.pending = {pending_sync(10, 3, {2})};
    auto slots = page_slots({0, 1, 2, 3});
    slots[2].blkid = homestore::multi_blk_id{102, 1, 0};
    restart(image, std::move(slots));
    ASSERT_FALSE(dev_->is_faulted());
    EXPECT_EQ(journal_->commits, (std::vector< std::pair< homestore::blk_num_t, bool > >{{102, true}}))
        << "kept until freed";
    EXPECT_EQ(journal_->free_data_calls, 1);
    EXPECT_EQ(dev_->durable_state().last_applied_raft_idx, 10);
}

// recover() is the awaitable form of the restart path: client I/O waits for it.
TEST_F(CraftRaftEntriesTest, RecoverRestoresAndClearsTheGate) {
    auto* store = with_store();
    store->saved = sample_state();
    journal_->slots = page_slots({0, 1, 2, 3});
    auto const r = homeblocks::detail::sync_get(dev_->recover());
    ASSERT_TRUE(r.has_value());
    EXPECT_FALSE(dev_->is_recovering());
    EXPECT_EQ(dev_->term(), 3u);
}

// ── journal reclaim ──

// A keep_alive of the current session carrying the set-wide committed prefix.
auto keep_alive_with(CraftReplDev& dev, int64_t all_committed_lsn) {
    return homeblocks::detail::sync_get(dev.keep_alive(craft::client_hdr{dev.term(), -1, all_committed_lsn}));
}

// The journal is reclaimed up to the lower of the persisted durable commit frontier and the set-wide committed prefix
// the client reports.
TEST_F(CraftRaftEntriesTest, ReclaimFollowsTheLowerOfTheDurableCommitAndAllCommitted) {
    with_store();
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(1);
    journal_->slots = page_slots({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, /*zeros=*/true);
    dev_->seed_lsns(10, {});
    ASSERT_TRUE(do_apply(/*rs_commit_lsn=*/10, /*client_token=*/0).has_value());
    ASSERT_EQ(dev_->durable_state().durable_commit_lsn, 10);
    EXPECT_EQ(journal_->reclaim_calls, 0) << "no committed prefix from the client yet";

    ASSERT_TRUE(keep_alive_with(*dev_, 6).has_value());
    EXPECT_EQ(dev_->reclaimed_upto(), 6);
    EXPECT_EQ(journal_->slots.begin()->first, 7);

    ASSERT_TRUE(keep_alive_with(*dev_, 20).has_value());
    EXPECT_EQ(dev_->reclaimed_upto(), 10) << "past the durable commit frontier";
}

// Without a state store the journal is the only copy: never reclaimed.
TEST_F(CraftRaftEntriesTest, NoReclaimWithoutAStore) {
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(1);
    journal_->slots = page_slots({0, 1, 2}, /*zeros=*/true);
    dev_->seed_lsns(2, {});
    ASSERT_TRUE(do_apply(2, 0).has_value());
    ASSERT_TRUE(keep_alive_with(*dev_, 2).has_value());
    EXPECT_EQ(journal_->reclaim_calls, 0);
}

// The floor follows the frontier as persisted: one recorded in memory but not written yet doesn't count, until a later
// persist writes it -- which then reclaims.
TEST_F(CraftRaftEntriesTest, ReclaimWaitsForTheFrontierToBePersisted) {
    auto* store = with_store();
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(1);
    journal_->slots = page_slots({0, 1, 2, 3, 4, 5}, /*zeros=*/true);
    dev_->seed_lsns(5, {});
    store->fail_persist = true;
    ASSERT_TRUE(do_apply(5, 0).has_value());
    ASSERT_EQ(dev_->durable_state().durable_commit_lsn, 5);
    ASSERT_TRUE(keep_alive_with(*dev_, 4).has_value());
    EXPECT_EQ(journal_->reclaim_calls, 0);

    store->fail_persist = false;
    ASSERT_TRUE(homeblocks::detail::sync_get(dev_->save_state()).has_value());
    EXPECT_EQ(dev_->reclaimed_upto(), 4);
}

// ── stop (destroy, shutdown) ──

// A shutdown's stop waits out the apply in progress and leaves the queued entries unapplied, also those committed
// after it: HomeStore's last checkpoint saves them as pending, for the restart to apply.
TEST_F(CraftRaftEntriesTest, StopForAShutdownKeepsTheQueuedEntries) {
    auto* store = with_store();
    store->hold = true; // the first login's write-ahead persist waits at the gate
    commit(1, make_header(CraftEntryType::InternalLogin), make_internal_login_key(1, 1));
    commit(2, make_header(CraftEntryType::InternalLogin), make_internal_login_key(2, 2));
    bool stopped = false;
    homeblocks::detail::detach([](CraftReplDev& dev, bool& stopped) -> sisl::async::task< void > {
        (void)co_await dev.stop(true /* keep_queued */);
        stopped = true;
    }(*dev_, stopped));
    EXPECT_FALSE(stopped) << "the apply in progress wasn't waited out";
    store->gate.complete(true);
    EXPECT_TRUE(stopped);
    EXPECT_EQ(dev_->term(), 1u) << "a queued entry applied";

    commit(3, make_header(CraftEntryType::InternalLogin), make_internal_login_key(3, 3));
    EXPECT_EQ(dev_->term(), 1u) << "an entry committed after the stop applied";
    ASSERT_TRUE(homeblocks::detail::sync_get(persist()).has_value());
    std::vector< int64_t > pending;
    for (auto const& e : store->saved->pending)
        pending.push_back(e.raft_idx);
    EXPECT_EQ(pending, (std::vector< int64_t >{2, 3}));

    (void)homeblocks::detail::sync_get(dev_->stop_apply()); // empties the queue for the teardown's invariant check
}

// A stop waits out the journal reclaim in progress, and no reclaim runs after it.
TEST_F(CraftRaftEntriesTest, StopEndsTheJournalReclaim) {
    with_store();
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(1);
    journal_->slots = page_slots({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, /*zeros=*/true);
    dev_->seed_lsns(10, {});
    ASSERT_TRUE(do_apply(/*rs_commit_lsn=*/10, /*client_token=*/0).has_value());
    journal_->hold_reclaim = true;
    ASSERT_TRUE(keep_alive_with(*dev_, 6).has_value()); // its reclaim to 6 waits at the gate
    ASSERT_EQ(journal_->reclaim_calls, 1);

    bool stopped = false;
    homeblocks::detail::detach([](CraftReplDev& dev, bool& stopped) -> sisl::async::task< void > {
        (void)co_await dev.stop(false /* keep_queued */);
        stopped = true;
    }(*dev_, stopped));
    EXPECT_FALSE(stopped) << "the reclaim in progress wasn't waited out";
    journal_->reclaim_gate.complete(true);
    EXPECT_TRUE(stopped);
    EXPECT_EQ(dev_->reclaimed_upto(), 6);

    ASSERT_TRUE(keep_alive_with(*dev_, 10).has_value());
    EXPECT_EQ(journal_->reclaim_calls, 1) << "a reclaim ran after the stop";
}

// A backend that can't reclaim is asked once; a failed reclaim is tried again on the next trigger.
TEST_F(CraftRaftEntriesTest, ReclaimStopsWhenUnsupportedAndRetriesAfterAFailure) {
    with_store();
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(1);
    journal_->slots = page_slots({0, 1, 2, 3}, /*zeros=*/true);
    dev_->seed_lsns(3, {});
    ASSERT_TRUE(do_apply(3, 0).has_value());

    journal_->fail_reclaim = true;
    ASSERT_TRUE(keep_alive_with(*dev_, 1).has_value());
    EXPECT_EQ(dev_->reclaimed_upto(), -1);
    journal_->fail_reclaim = false;
    ASSERT_TRUE(keep_alive_with(*dev_, 1).has_value());
    EXPECT_EQ(dev_->reclaimed_upto(), 1);

    journal_->reclaim_unsupported = true;
    int const calls = journal_->reclaim_calls;
    ASSERT_TRUE(keep_alive_with(*dev_, 2).has_value());
    ASSERT_TRUE(keep_alive_with(*dev_, 3).has_value());
    EXPECT_EQ(journal_->reclaim_calls, calls + 1);
    EXPECT_EQ(dev_->reclaimed_upto(), 1);
}

// A peer's fetch of a reclaimed slot fails the whole batch: it was held here, and reporting it as not held could let a
// leader verdict a committed slot Empty. Slots above the reclaim point, and Empty verdicts, are still served.
TEST_F(CraftRaftEntriesTest, FetchOfAReclaimedSlotFailsTheBatch) {
    with_store();
    dev_->set_checkpoint_trigger(std::ref(trigger_));
    dev_->set_checkpoint_lsn_interval(1);
    journal_->slots = page_slots({0, 1, 2, 4, 5}, /*zeros=*/true);
    dev_->seed_lsns(5, {3});
    ASSERT_TRUE(do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0, /*empty_slots=*/{3}).has_value());
    ASSERT_TRUE(keep_alive_with(*dev_, 4).has_value());
    ASSERT_EQ(dev_->reclaimed_upto(), 4);

    EXPECT_FALSE(homeblocks::detail::sync_get(dev_->fetch_data({2, 5})).has_value());
    auto const above = homeblocks::detail::sync_get(dev_->fetch_data({5}));
    ASSERT_TRUE(above.has_value());
    EXPECT_EQ(above->size(), 1u);
    auto const empty = homeblocks::detail::sync_get(dev_->fetch_data({3}));
    ASSERT_TRUE(empty.has_value());
    ASSERT_EQ(empty->size(), 1u);
    EXPECT_TRUE(empty->front().is_empty);
}

// A restart reads the reclaim point back from the journal, and the floor's frontier from the durable state.
TEST_F(CraftRaftEntriesTest, RestartReadsTheReclaimPoint) {
    auto* store = with_store();
    store->saved = sample_state(); // durable commit frontier 1
    journal_->slots = page_slots({1, 2, 3}, /*zeros=*/true);
    journal_->reclaimed = 0;
    dev_->trigger_on_restart();
    ASSERT_FALSE(dev_->is_faulted());
    EXPECT_EQ(dev_->reclaimed_upto(), 0);

    ASSERT_TRUE(keep_alive_with(*dev_, 5).has_value());
    EXPECT_EQ(dev_->reclaimed_upto(), 1) << "the restored frontier caps the floor";
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
