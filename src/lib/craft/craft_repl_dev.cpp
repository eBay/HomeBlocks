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

#include "craft_repl_dev.hpp"
#include "../coro_helpers.hpp"

#include <coroutine>
#include <cstring>

#include <homestore/blkdata_service.hpp>    // data_service(), async_alloc_write, blk_alloc_hints
#include <homestore/logstore/log_store.hpp> // home_log_store, logstore_seq_num_t, log_write_comp_cb_t
#include <iomgr/iomgr.hpp>                  // iomanager singleton, reactor_regex
#include <sisl/async/value_awaitable.hpp>   // value_awaitable<T>: lock-free completion-before-suspend-safe bridge

#include <optional>
#include <unordered_set>
#include <vector>

namespace homeblocks {

// ─── Journal entry on-disk format ─────────────────────────────────────────────

namespace {
// fetch_data's contract is one entry per requested LSN (never one that wasn't asked for, never
// repeated). Returns the first response LSN that violates it (unrequested or duplicated), or nullopt if
// every entry matches exactly one requested LSN. Erasing from `pending` as we go catches duplicates for
// free: a repeated lsn finds nothing left to erase the second time.
std::optional< int64_t > validate_fetch_response(std::vector< int64_t > const& requested,
                                                  std::vector< JournalSlot > const& response) {
    std::unordered_set< int64_t > pending{requested.begin(), requested.end()};
    for (auto const& slot : response) {
        if (pending.erase(slot.lsn) == 0) return slot.lsn;
    }
    return std::nullopt;
}
} // namespace

// ─── HomeStore journal backend ────────────────────────────────────────────────
//
// Each log slot is: [CraftJournalEntry header][serialized multi_blk_id bytes].
// The payload (HS_DATA_LINKED) is written directly to the data service; only the
// block reference is stored here.

static constexpr uint32_t k_journal_magic = 0xC4AF5AFE; // "CRAFT SAFE" — corrupt or non-CRAFT slots fail this
static constexpr uint8_t k_journal_version = 1;

// On-disk format: magic first so recovery can rule out corruption or log slots not written by
// this code before reading anything else. version follows so the rest of the layout can evolve
// without breaking the magic check. lsn is stored redundantly for self-describing recovery and
// cross-checking against the log-store sequence number. lba and len are BYTES (byte-addressed
// API contract), not block units.
//
// No CRC field: HomeStore's log_group_header already checksums the entire record body on every
// append and verifies it on read/recovery replay (LogGroup::compute_crc, log_group.cpp; checked
// in log_stream.cpp and log_dev.cpp, hard failure on mismatch) -- a CRAFT-level CRC would be
// redundant. magic/version only rule out a garbage or non-CRAFT slot, not a bit flip; that's
// covered one layer down instead of here.
#pragma pack(push, 1)
struct CraftJournalEntry {
    uint32_t magic;
    uint8_t version;
    uint64_t term;
    int64_t lsn;
    lba_t lba;
    lba_count_t len;
    uint8_t all_zeros;
};
#pragma pack(pop)
static_assert(sizeof(CraftJournalEntry) == 34,
              "CraftJournalEntry is a persisted on-disk format -- "
              "a layout change here is a format migration, not a code change");

// ─── HomeStore journal backend ─────────────────────────────────────────────────
//
// Production backend: wraps a HomeStore home_log_store.

class HomeStoreCraftJournalBackend : public CraftJournalBackend {
public:
    explicit HomeStoreCraftJournalBackend(shared< homestore::home_log_store > logstore, uint64_t vol_ordinal) :
            logstore_{std::move(logstore)}, vol_ordinal_{vol_ordinal} {}

    // Allocate blocks via the HomeStore data service and write the payload (zero-copy).
    // application_hint routes the allocation to this volume's chunk set via VolumeChunkSelector.
    // Returns the allocated multi_blk_id on success.
    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const& data,
                                                             lba_count_t /* len */) override {
        homestore::blk_alloc_hints hints;
        hints.application_hint = vol_ordinal_;
        homestore::multi_blk_id blkid{};
        auto res = co_await homestore::data_service().async_alloc_write(data, hints, blkid);
        if (!res) {
            LOGE("async_alloc_write failed: {}", res.error().message());
            co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
        }
        co_return blkid;
    }

    // Serialize the journal entry (header + blkid) and write it to the log store.
    async_status write_slot(int64_t lsn, uint64_t term, lba_t lba, lba_count_t len, homestore::multi_blk_id blkid,
                            bool all_zeros) override {
        CraftJournalEntry hdr{
            k_journal_magic, k_journal_version, term, lsn, lba, len, static_cast< uint8_t >(all_zeros)};
        uint32_t blkid_sz = blkid.serialized_size();
        sisl::io_blob_safe blob{static_cast< uint32_t >(sizeof(CraftJournalEntry)) + blkid_sz};
        std::memcpy(blob.bytes(), &hdr, sizeof(CraftJournalEntry));
        sisl::blob blkid_blob = blkid.serialize(); // non-owning view — copy before blkid goes out of scope
        // Internal HomeStore-API contract, not client-reachable: serialize() must return exactly
        // serialized_size() bytes, or the memcpy below overreads blkid_blob's owned buffer.
        DEBUG_ASSERT_EQ(blkid_blob.size(), blkid_sz, "multi_blk_id::serialize() size mismatch");
        std::memcpy(blob.bytes() + sizeof(CraftJournalEntry), blkid_blob.cbytes(), blkid_sz);
        // Bridge write_async (callback) to co_await via value_awaitable<bool>.
        //   • Deadlock-safe: the callback posts va->complete() to an iomgr reactor via run_on_forget,
        //     decoupling coroutine resume from LogDev::m_flush_mtx. In production HomeBlocks always has
        //     the repl data service (homeblks_impl.cpp:310,317), so the callback fires synchronously
        //     inside flush() -> on_flush_completion(), which executes under flush_guard() (non-recursive
        //     std::unique_lock on m_flush_mtx, log_dev.hpp:718). LogDev::read(), rollback(), and
        //     truncate() all take flush_guard() unconditionally, so any journal op issued from the
        //     continuation would self-deadlock without this dispatch.
        //   • INLINE-safe: if write_async fires the callback before await_suspend returns (INLINE
        //     log-dev mode used by solo_repl_dev, append_async -> flush_if_necessary()), value_awaitable::
        //     await_suspend atomically detects k_done and returns false so the coroutine never suspends —
        //     no UB from frame destruction inside the callback.
        //   • blob lifetime: blob is a coroutine-frame local; the frame stays alive through co_await.
        //     write_async holds only a reference, which remains valid for the full I/O duration.
        //
        // KNOWN RISK (lost completion -> permanent suspension): write_async's callback isn't
        // guaranteed to fire, verified against homestore source (log_dev.cpp, log_store.cpp). Two
        // triggers, one failure mode:
        //   - shutdown: write_async returns -1 without invoking the callback when the log store or
        //     logdev is stopping (log_store.cpp:48,71; log_dev.cpp:293,301 -- append_async's own
        //     is_stopping() check). On success it returns LogDev's internal m_log_idx, which starts
        //     at 0 for a fresh logdev and is NOT the lsn/dLSN passed in -- only < 0 means stopping.
        //     Guarded below.
        //   - journal I/O error: the flush path returns on a sync_pwritev failure BEFORE calling
        //     on_flush_completion (log_dev.cpp:531-539) -- no return-value signal, NOT covered
        //     below. Needs a HomeStore fix propagating the error into the completion path itself
        //     (not a status arg on a callback that won't fire); interim: timeout the await.
        auto va = std::make_shared< sisl::async::value_awaitable< bool > >();
        auto write_ret = logstore_->write_async(
            static_cast< homestore::logstore_seq_num_t >(lsn), blob, nullptr,
            [va](homestore::logstore_seq_num_t, sisl::io_blob&, homestore::logdev_key, void*) mutable {
                iomanager.run_on_forget(iomgr::reactor_regex::least_busy_io,
                                        [va = std::move(va)]() mutable { va->complete(true); });
            });
        if (write_ret < 0) {
            LOGE("write_async rejected lsn={}: log store or logdev is stopping; callback will not fire", lsn);
            co_return std::unexpected(make_error_condition(std::errc::operation_not_supported));
        }
        co_await *va;
        co_return ok();
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override {
        LOGW("HomeStoreCraftJournalBackend::read_slot lsn={} not yet implemented", lsn);
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    // Drop all journal entries with seq_num > lsn; lsn becomes the new tail.
    // home_log_store::rollback(to_lsn) removes everything ABOVE to_lsn, which is exactly what we need.
    async_status truncate_to(int64_t lsn) override {
        if (!logstore_->rollback(static_cast< homestore::logstore_seq_num_t >(lsn)))
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return ok();
    }

    async_status free_data(homestore::multi_blk_id blkid) override {
        auto res = co_await homestore::data_service().async_free_blk(blkid);
        if (!res) {
            LOGE("async_free_blk failed: {}", res.error().message());
            co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
        }
        co_return ok();
    }

private:
    shared< homestore::home_log_store > logstore_;
    uint64_t vol_ordinal_;
};

unique< CraftJournalBackend > make_homestore_journal_backend(shared< homestore::home_log_store > logstore,
                                                             uint64_t vol_ordinal) {
    return std::make_unique< HomeStoreCraftJournalBackend >(std::move(logstore), vol_ordinal);
}

// ─── constructor ──────────────────────────────────────────────────────────────

CraftReplDev::CraftReplDev(volume_id_t vol_id, unique< CraftJournalBackend > journal) :
        vol_id_{vol_id}, journal_{std::move(journal)}, raft_listener_{this} {}

// ─── get_rs_commit_lsn ────────────────────────────────────────────
// Snapshot the in-memory partition state under missing_mu_ for consistency with
// write() which updates state_ under the same lock.

async_result< craft::lsn_pair > CraftReplDev::get_rs_commit_lsn(uint64_t /* term */, bool /* is_login */) {
    craft::lsn_pair pair{};
    {
        std::lock_guard lk{missing_mu_};
        pair = {state_.commit_lsn, state_.last_append_lsn};
    }
    co_return pair;
}

// ─── truncate (S4) ────────────────────────────────────────────────────────────
//
// Called only during the login sequence, while no writes are in-flight (the
// CRAFT write path is quiesced by login serialisation). Three atomic steps:
//   1. Journal rollback: drop all entries with dLSN > lsn (tail truncation).
//   2. Clamp last_append_lsn to lsn if it is higher.
//   3. Erase all missing-set entries above lsn.
// commit_lsn is not touched: the new rs_commit_lsn passed by the caller is the
// dLSN up to which RAFT consensus has RESOLVED entries. Entries above that are
// the ones being dropped. The missing set tracks gaps in [commit_lsn+1,
// last_append_lsn]; after truncation every entry > lsn is gone from both the
// journal and the missing set.

async_status CraftReplDev::truncate(int64_t lsn) {
    // Guard before touching the journal: truncating below commit_lsn would drop
    // committed entries and break the commit_lsn <= last_append_lsn invariant.
    {
        std::lock_guard lk{missing_mu_};
        DEBUG_ASSERT_GE(lsn, state_.commit_lsn, "truncate below committed prefix");
    }

    // Step 1: journal rollback — synchronous; fails fast on I/O error.
    if (auto r = co_await journal_->truncate_to(lsn); !r) co_return r;

    // Steps 2 + 3 under the same mutex write() uses.
    {
        std::lock_guard lk{missing_mu_};
        if (state_.last_append_lsn > lsn) state_.last_append_lsn = lsn;
        missing_lsns_.erase(missing_lsns_.upper_bound(lsn), missing_lsns_.end());
    }
    co_return ok();
}

#ifdef _PRERELEASE
void CraftReplDev::seed_lsns(int64_t last_append, std::initializer_list< int64_t > missing) {
    std::lock_guard lk{missing_mu_};
    state_.last_append_lsn = last_append;
    missing_lsns_.clear();
    missing_lsns_.insert(missing);
}

void CraftReplDev::seed_commit_lsn(int64_t commit) {
    std::lock_guard lk{missing_mu_};
    state_.commit_lsn = commit;
}

void CraftReplDev::seed_term(uint64_t term) {
    std::lock_guard lk{missing_mu_};
    state_.term = term;
}

void CraftReplDev::seed_empty(std::initializer_list< int64_t > empty) {
    std::lock_guard lk{missing_mu_};
    empty_lsns_.clear();
    empty_lsns_.insert(empty);
    // Empty verdict resolves a gap: an LSN that was already in missing_lsns_ must be removed so
    // it does not permanently stall commit advancement. apply_sync_rs_commit_lsn (S5) must do the same.
    for (int64_t lsn : empty)
        missing_lsns_.erase(lsn);
}
#endif

// ─── stubs (S1/S3/S5/S7 implement these) ─────────────────────────────────────

async_result< craft::LoginResult > CraftReplDev::login(uint64_t /* client_token */) {
    LOGW("CraftReplDev::login not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_status CraftReplDev::logout(craft::client_hdr /* hdr */) {
    LOGW("CraftReplDev::logout not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::lsn_pair > CraftReplDev::write(craft::client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                                    sisl::sg_list data, bool all_zeros) {
    if (dlsn < 0) {
        LOGW("write rejected: invalid dlsn={}", dlsn);
        co_return std::unexpected(make_error_condition(std::errc::invalid_argument));
    }
    // Reject rather than abort: this condition is reachable from the client wire (S9 CraftConnector)
    // so a RELEASE_ASSERT would let a malformed frame abort the entire replica process. all_zeros and
    // data.size disagreeing either way is malformed: all_zeros=false requires a payload (the write());
    // all_zeros=true requires none (WRITE_ZEROES/unmap names a range, it does not also carry data).
    if (all_zeros == (data.size > 0)) {
        LOGW("write rejected: all_zeros={} disagrees with data.size={} dlsn={} addr={} len={}", all_zeros, data.size,
             dlsn, addr, len);
        co_return std::unexpected(make_error_condition(std::errc::invalid_argument));
    }

    {
        std::lock_guard lock{missing_mu_};
        // state_.term is guarded by missing_mu_ like the rest of state_ -- read it under the same lock
        // used for the gap-marking below rather than unlocked, now that apply_internal_login (22887)
        // actually mutates it from the RAFT commit thread.
        if (hdr.term != state_.term) {
            LOGW("write rejected: stale term want={} got={} dlsn={}", state_.term, hdr.term, dlsn);
            co_return std::unexpected(make_error_condition(volume_error::STALE_TERM));
        }
        if (empty_lsns_.contains(dlsn)) {
            LOGW("write rejected: slot is permanently empty dlsn={}", dlsn);
            co_return std::unexpected(make_error_condition(volume_error::EMPTY_SLOT));
        }
        // Idempotent: slot already successfully written — return snapshot without a second write_slot call.
        if (dlsn <= state_.last_append_lsn && !missing_lsns_.contains(dlsn)) {
            LOGT("write idempotent: dlsn={} already written", dlsn);
            co_return craft::lsn_pair{state_.commit_lsn, state_.last_append_lsn};
        }
        static constexpr int64_t k_max_ooo_gap = 1'000'000;
        // Guard 1: prevent signed overflow in the gap subtraction below (dlsn near INT64_MAX).
        if (dlsn > INT64_MAX - k_max_ooo_gap) {
            LOGW("write rejected: dlsn={} exceeds safe LSN range", dlsn);
            co_return std::unexpected(make_error_condition(std::errc::invalid_argument));
        }
        // Guard 2: cap the gap to prevent unbounded per-write allocation in missing_lsns_.
        if (dlsn - state_.last_append_lsn > k_max_ooo_gap) {
            LOGW("write rejected: dlsn={} too far ahead of last_append_lsn={}", dlsn, state_.last_append_lsn);
            co_return std::unexpected(make_error_condition(std::errc::value_too_large));
        }
        // Guard 3: cap cumulative missing_lsns_ growth, independent of per-write distance -- Guard 2
        // only bounds one write's contribution, so repeated smaller-than-cap jumps (e.g. +1,000,000
        // each) still grow the set unboundedly. Scoped to dlsn > last_append_lsn + 1 (a real gap of
        // >= 1 entry), not just dlsn > last_append_lsn: a gap-fill (dlsn <= last_append_lsn) or a
        // zero-gap in-order write (dlsn == last_append_lsn + 1) must stay exempt, or the set could
        // never drain once full.
        static constexpr size_t k_max_missing_lsns = 2'000'000;
        if (dlsn > state_.last_append_lsn + 1 && missing_lsns_.size() >= k_max_missing_lsns) {
            LOGW("write rejected: missing_lsns_ at capacity ({}) dlsn={}", missing_lsns_.size(), dlsn);
            co_return std::unexpected(make_error_condition(std::errc::value_too_large));
        }
        // Empty-verdicted LSNs in the gap are already resolved — skip them to avoid re-stalling commit advancement.
        for (int64_t gap = state_.last_append_lsn + 1; gap < dlsn; ++gap) {
            if (!empty_lsns_.contains(gap)) missing_lsns_.insert(gap);
        }
        if ((dlsn > state_.last_append_lsn) || missing_lsns_.contains(dlsn)) missing_lsns_.insert(dlsn);
        // Advanced before write_slot runs, not rolled back on failure (see
        // WriteSlotFails_LsnRemainsInMissing). Intentional, and safe per
        // CRAFT-Design's recovery-watermark argument: login takes rs_commit_lsn =
        // max(quorum.last_append_lsn) because false-include is benign, false-exclude is
        // catastrophic. A failed local append is indistinguishable, to login, from a write
        // that never reached quorum -- Phase 1b finds no real holder and marks it Empty.
        // No data is lost; the only cost is an avoidable Empty verdict.
        state_.last_append_lsn = std::max(state_.last_append_lsn, dlsn);
    }

    // HS_DATA_LINKED: allocate blocks and write payload before journalling the block reference.
    // all_zeros=true skips this; the early-return above guarantees !all_zeros implies data.size > 0.
    homestore::multi_blk_id blkid{};
    bool blkid_allocated = false;
    if (!all_zeros) {
        auto alloc_res = co_await journal_->alloc_write_data(data, static_cast< lba_count_t >(len));
        if (!alloc_res) {
            LOGE("alloc_write_data failed dlsn={}: {}", dlsn, alloc_res.error().message());
            co_return std::unexpected(alloc_res.error());
        }
        blkid = *alloc_res;
        blkid_allocated = true;
    }
    // addr and len are BYTES (byte-addressed API): CraftJournalEntry stores them verbatim as bytes.
    // hdr.term is already verified against state_.term under missing_mu_ above; pass it so the
    // on-disk entry carries the session term for stale-tail detection on recovery. dlsn is stored
    // redundantly in CraftJournalEntry.lsn for self-describing recovery.
    auto res = co_await journal_->write_slot(dlsn, hdr.term, static_cast< lba_t >(addr),
                                             static_cast< lba_count_t >(len), blkid, all_zeros);
    if (!res) {
        LOGE("write_slot failed dlsn={} addr={} len={}: {}", dlsn, addr, len, res.error().message());
        if (blkid_allocated) {
            if (auto fr = co_await journal_->free_data(blkid); !fr)
                LOGE("free_data failed after write_slot failure dlsn={}: {}", dlsn, fr.error().message());
        }
        co_return std::unexpected(res.error());
    }

    // Post-flight term recheck. Per CRAFT-Design, InternalLogin simultaneously bumps the term
    // and truncates, with login quiescing all writes before truncating — so if that invariant holds,
    // this branch is dead code (the term cannot change while write_slot is in flight).
    //
    // If the race occurs despite the quiescence guarantee, do NOT call free_data: write_slot already
    // wrote a durable journal entry at dlsn that references blkid. There are only two orderings:
    //   truncate ran after write_slot  — truncate drops the entry; no free needed.
    //   truncate ran before write_slot — the entry survives above the new tail; freeing blkid here
    //                                    leaves it dangling (re-allocatable blocks, journal still
    //                                    references them). Actively harmful.
    // In both orderings free_data is either unnecessary or wrong. blkid is intentionally not freed.
    // CraftJournalEntry.term lets recovery skip the stale entry; the next login truncates it durably.
    bool stale_post_flight = false;
    craft::lsn_pair snapshot;
    {
        std::lock_guard lock{missing_mu_};
        if (hdr.term != state_.term) {
            LOGW("write discarded post-flight: term changed dlsn={}", dlsn);
            stale_post_flight = true;
        } else {
            missing_lsns_.erase(dlsn);
            snapshot = {state_.commit_lsn, state_.last_append_lsn};
        }
    }

    if (stale_post_flight) { co_return std::unexpected(make_error_condition(volume_error::STALE_TERM)); }

    LOGT("write ok dlsn={} addr={} len={} all_zeros={}", dlsn, addr, len, all_zeros);
    co_return snapshot;
}

async_result< craft::read_result > CraftReplDev::read(craft::client_hdr /* hdr */, int64_t /* read_lsn */,
                                                      uint64_t /* addr */, uint64_t /* len */,
                                                      sisl::sg_list /* dest */) {
    LOGW("CraftReplDev::read not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::lsn_pair > CraftReplDev::keep_alive(craft::client_hdr /* hdr */) {
    LOGW("CraftReplDev::keep_alive not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::resolution_result > CraftReplDev::request_resolution(craft::client_hdr /* hdr */,
                                                                          int64_t /* upto */) {
    LOGW("CraftReplDev::request_resolution not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_status CraftReplDev::append(int64_t /* sync_to */, uint64_t /* client_token */) {
    LOGW("CraftReplDev::append not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

// ─── fetch_data (S6) ──────────────────────────────────────────────────────────
//
// Four-way response per requested LSN (in request order; omitted slots absent):
//   is_empty=true  : slot positively verdicted Empty by a prior SyncRSCommitLSN (S5); Empty beats data
//   present+data   : slot in journal, all_zeros=false; data payload present
//   present+zero   : slot in journal, all_zeros=true; no data payload (WRITE_ZEROES / range-unmap)
//   omitted        : slot not locally held — in missing_lsns_ or above last_append_lsn
//
// The Empty verdict is leader-only (S5 pre-resolution); this responder only reports state.
// empty_lsns_ is checked first: a slot in both empty_lsns_ and the journal returns is_empty=true
// (Empty beats data, the reconciliation invariant from S5).
//
// The missing_mu_ lock is dropped before each co_await read_slot() call to avoid holding a mutex
// across a suspension point. Callers are serialised by the login sequence (no concurrent writes
// while fetch_data runs), so the snapshot taken under the lock is stable.
//
// A read_slot() I/O error aborts the batch immediately (fail-fast); the partial result is discarded.
//
// TODO: the loop below re-acquires missing_mu_ once per requested LSN. Since the snapshot is
// already documented as stable for the whole batch (no concurrent writes during fetch_data),
// classification for every LSN could be done under a single lock acquisition up front instead --
// same result, fewer lock/unlock round trips for large batches.

async_result< std::vector< JournalSlot > > CraftReplDev::fetch_data(std::vector< int64_t > lsns) {
    std::vector< JournalSlot > result;
    result.reserve(lsns.size());

    for (int64_t lsn : lsns) {
        enum class SlotKind { Empty, Present, Absent };
        SlotKind kind;
        {
            std::lock_guard lk{missing_mu_};
            if (empty_lsns_.contains(lsn)) {
                kind = SlotKind::Empty;
            } else if (lsn >= 0 && lsn <= state_.last_append_lsn && !missing_lsns_.contains(lsn)) {
                kind = SlotKind::Present;
            } else {
                kind = SlotKind::Absent;
            }
        }

        if (kind == SlotKind::Empty) {
            result.push_back(JournalSlot{.lsn = lsn, .is_empty = true});
        } else if (kind == SlotKind::Present) {
            auto slot_r = co_await journal_->read_slot(lsn);
            if (!slot_r) co_return std::unexpected(slot_r.error());
            slot_r->lsn = lsn;
            result.push_back(std::move(*slot_r));
        }
        // Absent: omit from result (not-present-here)
    }

    co_return result;
}

// ─── RAFT listener ────────────────────────────────────────────────────────────

void CraftReplDev::CraftRaftListener::on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                                std::vector< homestore::multi_blk_id > const& /* blkids */,
                                                cintrusive< homestore::repl_req_ctx >& /* ctx */) {
    if (header.size() < sizeof(CraftEntryHeader)) {
        LOGE("on_commit lsn={} header too small ({} bytes)", lsn, header.size());
        return;
    }
    const auto* entry_hdr = reinterpret_cast< const CraftEntryHeader* >(header.cbytes());

    switch (entry_hdr->type) {
    case CraftEntryType::SyncRSCommitLSN: {
        if (key.size() < sizeof(SyncRSCommitLSNPayload)) {
            LOGE("on_commit lsn={} SyncRSCommitLSN key too small ({} bytes)", lsn, key.size());
            return;
        }
        const auto* payload = reinterpret_cast< const SyncRSCommitLSNPayload* >(key.cbytes());
        auto empty_slots    = parse_empty_slots(key);
        if (!empty_slots) {
            LOGE("on_commit lsn={} SyncRSCommitLSN malformed empty_slots", lsn);
            return;
        }
        // apply_sync_rs_commit_lsn co_awaits peer fetch + journal writes; on_commit itself is a synchronous
        // HomeStore callback, so fire-and-forget it.
        //
        // FIXME: KNOWN GAP (not yet fixed): this coroutine captures only the raw `owner_` pointer, not anything
        // that keeps CraftReplDev alive. If the object is destroyed (e.g. volume removal) while this
        // coroutine is suspended inside fetch_from_peer()/write_slot(), it resumes into freed memory --
        // use-after-free. Two possible fixes:
        //   Check comments: https://github.com/sbinmalek/HomeBlocks/pull/2#discussion_r3761568811
        //
        // FIXME: KNOWN GAP (not yet fixed), distinct from the lifetime issue above: detaching here also
        // breaks strict RAFT apply ordering. on_commit returns to HomeStore as soon as this coroutine hits
        // its first co_await, so HomeStore can call on_commit for the NEXT committed entry -- a synchronous
        // InternalLogin, or another detached SyncRSCommitLSN -- before this one's effects are fully applied.
        // No individual field access races (missing_mu_ still guards every access), but replicas can end up
        // applying entries in different effective orders depending on async completion timing, which
        // violates the determinism RAFT relies on for replicas to converge. See the commit_lsn advance at
        // the tail of apply_sync_rs_commit_lsn and the client_token overwrite in apply_internal_login for
        // the two mutation points this exposes. Real fix: one per-device serialized apply queue that both
        // entry types funnel through, processing one entry's full effect (including all its co_awaits)
        // before starting the next -- not independent detached tasks.
        detail::detach(owner_->apply_sync_rs_commit_lsn(payload->rs_commit_lsn, payload->client_token,
                                                        std::move(*empty_slots)));
        break;
    }
    case CraftEntryType::InternalLogin: {
        // Fixed-size payload, no variable trailing data (unlike SyncRSCommitLSN) -- exact-size check.
        if (key.size() != sizeof(InternalLoginPayload)) {
            LOGE("on_commit lsn={} InternalLogin key wrong size ({} bytes)", lsn, key.size());
            return;
        }
        const auto* login_payload = reinterpret_cast< const InternalLoginPayload* >(key.cbytes());
        // Pure in-memory state transition (no co_await) -- called directly, not detached.
        owner_->apply_internal_login(login_payload->client_token, login_payload->term);
        break;
    }
    default:
        LOGE("on_commit lsn={} unrecognized CraftEntryType={}", lsn, static_cast< uint8_t >(entry_hdr->type));
        break;
    }
}

// ─── RAFT apply helpers (S5 implements) ──────────────────────────────────────
//
// apply_sync_rs_commit_lsn (22886): client_token is verified against the current session first -- a mismatch
// gates the ENTIRE apply (no reconciliation, no catch-up, no watermark advance), since a RAFT entry whose
// token doesn't match the live session shouldn't be trusted to describe it. empty_slots is range-checked
// against rs_commit_lsn next, for the same reason and with the same all-or-nothing gate: SyncRSCommitLSN
// verdicts are only ever defined for slots the leader pre-resolved up to rs_commit_lsn (S5), so a negative
// or out-of-range entry is a malformed/corrupt RAFT entry, not a legitimate verdict -- trusting it would
// permanently poison empty_lsns_ for a slot that hasn't even been reached yet. Once both checks pass, every
// other step is best-effort forward progress: empty_slots are reconciled and the newly-spanned range is
// marked missing, catch-up attempts to fill in what it can from a peer, and commit_lsn/last_append_lsn
// advance regardless of whether catch-up fully succeeded -- mirroring truncate()'s invariant that apply
// never reverts the watermark, only advances it. A peer's fetch_data response gets its own all-or-nothing
// check (validate_fetch_response): unlike the two checks above, this one can't gate the whole apply (gap
// marking and last_append_lsn already advanced by the time the response arrives), so a malformed response
// is instead treated exactly like a failed fetch -- none of it applied, everything requested stays missing.

async_status CraftReplDev::apply_sync_rs_commit_lsn(int64_t rs_commit_lsn, uint64_t client_token,
                                                    std::vector< int64_t > empty_slots) {
    // Validated before any state is touched -- same all-or-nothing gate as the token check below, since an
    // out-of-range verdict means the entry itself cannot be trusted, not that this one slot should be skipped.
    for (int64_t lsn : empty_slots) {
        if (lsn < 0 || lsn > rs_commit_lsn) {
            LOGE("apply_sync_rs_commit_lsn: empty_slots lsn={} out of range [0, {}] -- rejecting entire apply",
                lsn, rs_commit_lsn);
            co_return std::unexpected(make_error_condition(volume_error::INVALID_ENTRY));
        }
    }

    std::vector< int64_t > to_fetch;
    uint64_t term;
    {
        std::lock_guard lk{missing_mu_};
        if (client_token != state_.client_token) {
            LOGW("apply_sync_rs_commit_lsn: client_token mismatch want={} got={} rs_commit_lsn={} -- skipping apply",
                state_.client_token, client_token, rs_commit_lsn);
            co_return std::unexpected(make_error_condition(volume_error::WRONG_TOKEN));
        }
        term = state_.term;

        for (int64_t lsn : empty_slots) {
            empty_lsns_.insert(lsn);
            missing_lsns_.erase(lsn);
        }

        // Everything newly spanned by this advance that isn't Empty-verdicted is a gap until catch-up
        // (below) resolves it -- same idiom write() uses for gaps opened by an out-of-order dlsn.
        for (int64_t lsn = state_.last_append_lsn + 1; lsn <= rs_commit_lsn; ++lsn) {
            if (!empty_lsns_.contains(lsn)) missing_lsns_.insert(lsn);
        }
        state_.last_append_lsn = std::max(state_.last_append_lsn, rs_commit_lsn);

        for (int64_t lsn : missing_lsns_) {
            if (lsn <= rs_commit_lsn) to_fetch.push_back(lsn);
        }
    }

    if (!to_fetch.empty()) {
        if (peer_fetcher_ == nullptr) {
            LOGW("apply_sync_rs_commit_lsn: {} lsn(s) missing but no peer_fetcher_ wired -- leaving as missing",
                to_fetch.size());
        } else if (auto fetched = co_await peer_fetcher_->fetch_data(to_fetch); !fetched) {
            LOGE("apply_sync_rs_commit_lsn: fetch_data failed: {} -- leaving {} lsn(s) as missing",
                fetched.error().message(), to_fetch.size());
        } else if (auto bad_lsn = validate_fetch_response(to_fetch, *fetched); bad_lsn) {
            // fetch_data's contract is one entry per requested LSN (never one we didn't ask for, never
            // repeated) -- any deviation means the response itself can't be trusted, so none of it is
            // applied (same outcome as a fetch failure) rather than cherry-picking the entries that look
            // fine from a peer that has already proven unreliable.
            LOGE("apply_sync_rs_commit_lsn: peer response lsn={} not requested (or duplicated) -- rejecting "
                 "entire batch, leaving {} lsn(s) as missing",
                *bad_lsn, to_fetch.size());
        } else {
            for (auto& slot : *fetched) {
                if (slot.is_empty) {
                    std::lock_guard lk{missing_mu_};
                    empty_lsns_.insert(slot.lsn);
                    missing_lsns_.erase(slot.lsn);
                    continue;
                }
                // HS_DATA_LINKED, same as write(): allocate blocks and write the payload before
                // journalling the block reference. all_zeros slots carry no data and skip alloc.
                homestore::multi_blk_id blkid{};
                if (!slot.all_zeros) {
                    auto alloc_res = co_await journal_->alloc_write_data(slot.data, slot.len_bytes);
                    if (!alloc_res) {
                        LOGE("apply_sync_rs_commit_lsn: alloc_write_data failed lsn={}: {} -- leaving as missing",
                            slot.lsn, alloc_res.error().message());
                        continue;
                    }
                    blkid = *alloc_res;
                }
                auto res = co_await journal_->write_slot(slot.lsn, term, slot.lba_off_bytes, slot.len_bytes, blkid,
                                                         slot.all_zeros);
                if (!res) {
                    LOGE("apply_sync_rs_commit_lsn: write_slot failed lsn={}: {} -- leaving as missing", slot.lsn,
                        res.error().message());
                    continue;
                }
                std::lock_guard lk{missing_mu_};
                missing_lsns_.erase(slot.lsn);
            }
        }
    }

    // Unconditional: commit_lsn is a replica-set-wide watermark RAFT already agreed on, independent of
    // whether this replica's local catch-up succeeded.
    //
    // KNOWN GAP: this can land late. Because on_commit detaches this coroutine (see the FIXME there),
    // a later-committed entry (InternalLogin, or another SyncRSCommitLSN) may have already applied by
    // the time this advance actually runs, breaking strict RAFT apply ordering.
    {
        std::lock_guard lk{missing_mu_};
        state_.commit_lsn = std::max(state_.commit_lsn, rs_commit_lsn);
    }
    LOGT("apply_sync_rs_commit_lsn ok rs_commit_lsn={} client_token={}", rs_commit_lsn, client_token);
    co_return ok();
}

// ─── InternalLogin apply (S5 / SDSTOR-22887) ─────────────────────────────────
//
// Pure in-memory state transition -- no journal I/O, no peer fetch -- so this stays synchronous
// (unlike apply_sync_rs_commit_lsn) and on_commit calls it directly rather than via detail::detach().
// "Enforce single-writer exclusivity" needs no explicit rejection here: every other RPC's term-fence
// check (STALE_TERM on mismatch) already does that. Overwriting state_.term is what invalidates any
// existing session -- a caller still presenting the old term is fenced out on its very next call.

void CraftReplDev::apply_internal_login(uint64_t client_token, uint64_t term) {
    std::lock_guard lk{missing_mu_};
    state_.client_token = client_token; // opaque id, no ordering semantics -- plain overwrite
    // term is RAFT-ordered in practice (the leader always proposes strictly increasing terms), but
    // guard against regression the same way commit_lsn/last_append_lsn already do rather than trusting
    // log order blindly.
    state_.term = std::max(state_.term, term);
    LOGD("apply_internal_login client_token={} term={}", client_token, state_.term);
}

} // namespace homeblocks
