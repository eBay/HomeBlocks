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
//   - a client_token mismatch gates the ENTIRE apply: no reconciliation, no catch-up, no watermark advance
//   - empty_slots are range-validated against rs_commit_lsn and reconciled into empty_lsns_/missing_lsns_
//   - commit_lsn/last_append_lsn advance directly when there's no gap to catch up on
//   - fetch_data is invoked with exactly the missing LSNs when behind, and its response is persisted
//   - a peer response naming an unrequested or duplicate LSN is rejected as a whole batch
//   - catch-up is best-effort: a failed fetch, a failed write_slot, or no peer_fetcher_ at all still lets
//     commit_lsn advance, leaving unresolved LSNs in missing_lsns_
//   - commit_lsn never decrements
//   - on_commit parses a real serialized SyncRSCommitLSN entry and dispatches correctly (and rejects
//     malformed header/key blobs without touching state)
//
// InternalLogin tests verify:
//   - on_commit dispatches to apply_internal_login, which sets client_token/term
//   - a wrong-size key (too short or too long) is rejected, state untouched
//   - a second InternalLogin replaces client_token outright but never regresses term
//   - once applied, write()'s term-fence check reflects the new term end-to-end
//   - the client_token InternalLogin establishes is what apply_sync_rs_commit_lsn's token check uses
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles craft_repl_dev.cpp
// directly (same pattern as test_craft_truncate.cpp).

#include <gtest/gtest.h>
#include <map>
#include <optional>
#include <sisl/logging/logging.h>

#include "craft/craft_repl_dev.hpp"
#include "coro_helpers.hpp"

SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)
SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)

namespace homeblocks {
namespace {

// ── journal mock ──────────────────────────────────────────────────────────────
//
// Backed by a std::map so tests can inspect exactly which LSNs got persisted during catch-up.
// fail_on_write optionally injects an I/O error for a specific LSN.

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots;
    std::optional< int64_t > fail_on_write;

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const&, lba_count_t) override {
        co_return homestore::multi_blk_id{};
    }

    async_status write_slot(int64_t lsn, uint64_t /* term */, lba_t lba, lba_count_t len,
                            homestore::multi_blk_id /* blkid */, bool all_zeros) override {
        if (fail_on_write && *fail_on_write == lsn)
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        slots[lsn] = JournalSlot{.lsn = lsn, .all_zeros = all_zeros, .lba_off_bytes = lba, .len_bytes = len};
        co_return ok();
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override {
        auto it = slots.find(lsn);
        if (it == slots.end())
            co_return std::unexpected(std::make_error_condition(std::errc::no_such_file_or_directory));
        co_return it->second;
    }

    async_status truncate_to(int64_t) override { co_return ok(); }

    async_status free_data(homestore::multi_blk_id) override { co_return ok(); }

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

    async_result< std::vector< JournalSlot > > fetch_data(const std::vector< int64_t >& lsns,
                                                          uint32_t timeout_ms) override {
        last_requested = lsns;
        last_timeout_ms = timeout_ms;
        if (should_fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return response;
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
        dev_ = CraftReplDev::create(volume_id_t{}, std::move(mock));
    }

    auto do_apply(int64_t rs_commit_lsn, uint64_t client_token, std::vector< int64_t > empty_slots = {}) {
        return homeblocks::detail::sync_get(
            dev_->apply_sync_rs_commit_lsn(rs_commit_lsn, client_token, std::move(empty_slots)));
    }

    MockCraftJournalBackend* journal_{nullptr};
    MockCraftPeerFetcher fetcher_;
    std::shared_ptr< CraftReplDev > dev_;
};

namespace {

// ── client_token gate ─────────────────────────────────────────────────────────

// state_.client_token defaults to 0; a non-matching token must veto the whole apply.
TEST_F(CraftRaftEntriesTest, TokenMismatchSkipsWholeApply) {
    dev_->seed_lsns(5, {3});
    auto r = do_apply(/*rs_commit_lsn=*/100, /*client_token=*/999);

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::WRONG_TOKEN));
    EXPECT_EQ(dev_->commit_lsn(), -1);
    EXPECT_EQ(dev_->last_append_lsn(), 5);
    EXPECT_EQ(dev_->missing_count(), 1u);
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
    EXPECT_EQ(dev_->commit_lsn(), 5);
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
// still refuse the batch: none of the response is applied (same outcome as a fetch failure), even the
// entries that individually look fine, and commit_lsn still advances (best-effort).
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
    EXPECT_EQ(dev_->commit_lsn(), 2);
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
    EXPECT_EQ(dev_->commit_lsn(), 1);
}

// fetch_data fails outright: commit_lsn still advances (best-effort); every spanned LSN remains missing.
TEST_F(CraftRaftEntriesTest, BehindFetchFailsStillAdvancesCommitLsn) {
    dev_->set_peer_fetcher(&fetcher_);
    dev_->seed_lsns(0, {});
    fetcher_.should_fail = true;

    auto r = do_apply(/*rs_commit_lsn=*/3, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 3);
    EXPECT_EQ(dev_->last_append_lsn(), 3);
    EXPECT_EQ(dev_->missing_count(), 3u);
}

// No peer_fetcher_ wired at all (S9 not wired yet): same best-effort outcome as a fetch failure.
TEST_F(CraftRaftEntriesTest, BehindNoPeerFetcherStillAdvancesCommitLsn) {
    dev_->seed_lsns(0, {});

    auto r = do_apply(/*rs_commit_lsn=*/2, /*client_token=*/0);

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 2);
    EXPECT_EQ(dev_->missing_count(), 2u);
}

// A fetched slot's write_slot fails: that LSN alone stays missing; the rest of catch-up still applies,
// and commit_lsn still advances.
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
    EXPECT_EQ(dev_->commit_lsn(), 2);
}

// ── on_commit dispatch ─────────────────────────────────────────────────────────

TEST_F(CraftRaftEntriesTest, OnCommitDispatchesSyncRSCommitLSN) {
    auto header_buf = make_header(CraftEntryType::SyncRSCommitLSN);
    auto key_buf = make_sync_rs_commit_lsn_key(/*rs_commit_lsn=*/7, /*client_token=*/0, /*empty_slots=*/{});
    cintrusive< homestore::repl_req_ctx > ctx{};

    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    EXPECT_EQ(dev_->commit_lsn(), 7);
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

// client_token has no ordering semantics (it's an opaque id, unlike term) -- it's a plain overwrite even
// when the accompanying term regresses and is guarded. Pins down the intentional asymmetry between the
// two fields so it doesn't read as an oversight to a future reader.
TEST_F(CraftRaftEntriesTest, InternalLoginClientTokenOverwrittenEvenWhenTermRegresses) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    cintrusive< homestore::repl_req_ctx > ctx{};

    auto key1 = make_internal_login_key(/*client_token=*/1, /*term=*/5);
    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key1), {}, ctx);
    auto key2 = make_internal_login_key(/*client_token=*/99, /*term=*/3); // lower term, different token
    dev_->test_listener().on_commit(2, as_blob(header_buf), as_blob(key2), {}, ctx);

    EXPECT_EQ(dev_->term(), 5u);          // guarded against regression
    EXPECT_EQ(dev_->client_token(), 99u); // overwritten regardless
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
                    /*addr=*/0, /*len=*/0, {}, /*all_zeros=*/true));

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
                    /*addr=*/0, /*len=*/0, {}, /*all_zeros=*/true));

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
}

// Cross-entry-type integration: apply_sync_rs_commit_lsn's client_token check reads the SAME state_
// InternalLogin writes. Untestable before this ticket (state_.client_token was permanently 0, matching
// every SyncRSCommitLSN test's default). Once InternalLogin establishes a new token, do_apply must use
// it -- the OLD default (0) is now itself a mismatch.
TEST_F(CraftRaftEntriesTest, SyncRSCommitLSNUsesTokenEstablishedByInternalLogin) {
    auto header_buf = make_header(CraftEntryType::InternalLogin);
    auto key_buf = make_internal_login_key(/*client_token=*/7, /*term=*/1);
    cintrusive< homestore::repl_req_ctx > ctx{};
    dev_->test_listener().on_commit(1, as_blob(header_buf), as_blob(key_buf), {}, ctx);

    auto stale = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/0); // the old default -- now stale
    ASSERT_FALSE(stale.has_value());
    EXPECT_EQ(stale.error(), make_error_condition(volume_error::WRONG_TOKEN));

    auto fresh = do_apply(/*rs_commit_lsn=*/5, /*client_token=*/7); // the token InternalLogin just set
    ASSERT_TRUE(fresh.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 5);
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
