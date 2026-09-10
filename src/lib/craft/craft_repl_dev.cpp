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

#include <algorithm>
#include <coroutine>
#include <cstring>
#include <limits>

#include <homestore/blkdata_service.hpp>    // data_service(), async_alloc_write, blk_alloc_hints
#include <homestore/crc.hpp>                // crc16_t10dif -- same routine and seed volume.cpp uses
#include <homestore/logstore/log_store.hpp> // home_log_store, logstore_seq_num_t, log_write_comp_cb_t
#include <iomgr/iomgr.hpp>                  // iomanager singleton, reactor_regex
#include <iomgr/timer.hpp>                  // iomgr::schedule_recurring -- the watchdog's RAII timer
#include <sisl/async/value_awaitable.hpp>   // value_awaitable<T>: lock-free completion-before-suspend-safe bridge

#include <optional>
#include <unordered_set>
#include <vector>

#include <homestore/homestore.hpp>         // hs() -- index_table.hpp calls it, relying on this being included first
#include <homestore/index/index_table.hpp> // IndexTable -- prerequisite index_fixed_table.hpp relies on
                                           // being already visible (volume.hpp provides both of these,
                                           // in this order, before including index_fixed_table.hpp;
                                           // that header isn't self-contained)
#include "../volume/index_fixed_table.hpp" // VolumeIndexTable::write_to_index / delete_lba_range
#include "home_blks_config.hpp"            // HB_DYNAMIC_CONFIG -- watchdog timeout from settings

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
// Each log slot is: [CraftJournalEntry header][csum_t per LBA][serialized multi_blk_id bytes].
// The payload (HS_DATA_LINKED) is written directly to the data service; only the
// block reference is stored here. The checksum array is empty for all_zeros slots
// (no data, nothing to sum) and otherwise has exactly len/lba_size entries, computed
// on the write path while the data is still in memory (see CraftReplDev::write()).

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

// Same CRC16 seed volume.cpp's non-CRAFT write/read path already uses -- keep both paths on one
// checksum algorithm rather than inventing a second one for CRAFT slots.
static constexpr homestore::csum_t k_craft_crc16_seed = 0x8005;

// Upper bound on a single write()/read() request's byte length -- both are client-wire-reachable
// (S9 CraftConnector), so len is untrusted input. Three concrete failure modes this closes, all
// reachable from a single malformed/adversarial request without this bound:
//   - read_impl() would allocate O(nlbas) heap (a std::vector<Source>/<bool> sized to len/lba_size_)
//     with no cap -- a multi-terabyte len drives a multi-gigabyte allocation attempt (OOM) from one
//     request.
//   - write()'s len is narrowed to lba_count_t (uint32_t) before being journaled
//     (CraftJournalEntry::len is also uint32_t, an on-disk format constraint); an unbounded len could
//     silently truncate to a value whose own nlbas is 0, permanently stalling commit_impl() on that
//     slot forever (nlbas==0 there is a hard abort, not a skip).
//   - nlbas computed from an unbounded len can itself land on exactly 0 (len/lba_size_ truncating a
//     huge value down via uint32_t wraparound), which would underflow end_lba = start_lba + nlbas - 1
//     into a near-UINT64_MAX range fed straight to the index/BTree.
// Byte offset of the csum_t array within a slot's blob (right after the fixed header).
static constexpr uint32_t k_csum_array_offset = sizeof(CraftJournalEntry);

// Byte offset of the serialized multi_blk_id within a slot's blob, given how many csum_t entries
// precede it.
static uint32_t blkid_offset(uint32_t nlbas) {
    return k_csum_array_offset + nlbas * static_cast< uint32_t >(sizeof(homestore::csum_t));
}

// Total blob size for a slot: header + csum array + serialized blkid.
static uint32_t slot_blob_size(uint32_t nlbas, uint32_t blkid_sz) { return blkid_offset(nlbas) + blkid_sz; }

// ─── HomeStore journal backend ─────────────────────────────────────────────────
//
// Production backend: wraps a HomeStore home_log_store.

class HomeStoreCraftJournalBackend : public CraftJournalBackend {
public:
    explicit HomeStoreCraftJournalBackend(shared< homestore::home_log_store > logstore, uint64_t vol_ordinal,
                                          uint32_t lba_size) :
            logstore_{std::move(logstore)}, vol_ordinal_{vol_ordinal}, lba_size_{lba_size} {}

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

    // Serialize the journal entry (header + csum array + blkid) and write it to the log store.
    async_status write_slot(int64_t lsn, uint64_t term, lba_t lba, lba_count_t len, homestore::multi_blk_id blkid,
                            bool all_zeros, std::vector< homestore::csum_t > const& csums) override {
        CraftJournalEntry hdr{
            k_journal_magic, k_journal_version, term, lsn, lba, len, static_cast< uint8_t >(all_zeros)};
        uint32_t nlbas = static_cast< uint32_t >(csums.size());
        uint32_t blkid_sz = blkid.serialized_size();
        sisl::io_blob_safe blob{slot_blob_size(nlbas, blkid_sz)};
        std::memcpy(blob.bytes(), &hdr, sizeof(CraftJournalEntry));
        if (nlbas > 0) {
            std::memcpy(blob.bytes() + k_csum_array_offset, csums.data(), nlbas * sizeof(homestore::csum_t));
        }
        sisl::blob blkid_blob = blkid.serialize(); // non-owning view — copy before blkid goes out of scope
        // Internal HomeStore-API contract, not client-reachable: serialize() must return exactly
        // serialized_size() bytes, or the memcpy below overreads blkid_blob's owned buffer.
        DEBUG_ASSERT_EQ(blkid_blob.size(), blkid_sz, "multi_blk_id::serialize() size mismatch");
        std::memcpy(blob.bytes() + blkid_offset(nlbas), blkid_blob.cbytes(), blkid_sz);
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

    // Reads and parses a slot's blob back into a JournalSlot. read_sync is a blocking call (no async
    // log-read API exists in HomeStore today) -- acceptable here: callers are commit/apply (inherently
    // serial) and startup-only overlay rebuild, neither of which needs concurrency on this path.
    async_result< JournalSlot > read_slot(int64_t lsn) override {
        homestore::log_buffer buf;
        try {
            // Throws std::out_of_range for a truncated or never-appended seq_num -- not an I/O error.
            buf = logstore_->read_sync(static_cast< homestore::logstore_seq_num_t >(lsn));
        } catch (std::out_of_range const&) {
            co_return std::unexpected(std::make_error_condition(std::errc::result_out_of_range));
        }

        // Validate-before-trust: check the blob is large enough for each region before reading any
        // length field out of it, so a truncated/corrupt record fails cleanly instead of overreading.
        if (buf.size() < sizeof(CraftJournalEntry)) {
            LOGE("read_slot lsn={} blob too small for header: size={}", lsn, buf.size());
            co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
        }
        CraftJournalEntry hdr;
        std::memcpy(&hdr, buf.bytes(), sizeof(CraftJournalEntry));
        if (hdr.magic != k_journal_magic || hdr.version != k_journal_version) {
            LOGE("read_slot lsn={} bad magic/version: magic={:#x} version={}", lsn, hdr.magic, hdr.version);
            co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
        }

        JournalSlot slot;
        slot.lsn = hdr.lsn;
        // Corruption / misplaced-record guard: hdr.lsn is a redundant copy of the seq_num this record
        // was written under. A mismatch means a corrupt on-disk blob (or, in principle, a misrouted
        // log-store sequence number) -- either way, trusting the record's other fields (lba/len/blkid)
        // from here on would be unsafe.
        if (hdr.lsn != lsn) {
            LOGE("read_slot lsn={} hdr.lsn={} mismatch -- corrupt or misplaced record", lsn, hdr.lsn);
            co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
        }
        slot.all_zeros = hdr.all_zeros != 0;
        slot.lba_off_bytes = hdr.lba;
        slot.len_bytes = hdr.len;

        // A non-all_zeros slot's len must be a positive multiple of lba_size -- write() enforces this
        // at the client boundary, but a stale/legacy or corrupted on-disk record could still violate
        // it. Must be checked before nlbas is used to derive the csum-array and blkid offsets below:
        // an unvalidated len (e.g. 0, or not lba-aligned) would silently misalign both, deserializing
        // garbage rather than failing cleanly.
        if (!slot.all_zeros && (hdr.len == 0 || hdr.len % lba_size_ != 0)) {
            LOGE("read_slot lsn={} hdr.len={} not a positive multiple of lba_size={} -- malformed record", lsn, hdr.len,
                 lba_size_);
            co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
        }
        uint32_t nlbas = slot.all_zeros ? 0 : (hdr.len / lba_size_);
        uint32_t csum_bytes = nlbas * static_cast< uint32_t >(sizeof(homestore::csum_t));
        if (buf.size() < k_csum_array_offset + csum_bytes) {
            LOGE("read_slot lsn={} blob too small for csum array: size={} need={}", lsn, buf.size(),
                 k_csum_array_offset + csum_bytes);
            co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
        }
        if (nlbas > 0) {
            slot.csums.resize(nlbas);
            std::memcpy(slot.csums.data(), buf.bytes() + k_csum_array_offset, csum_bytes);
        }

        uint32_t blkid_off = blkid_offset(nlbas);
        if (buf.size() < blkid_off) {
            LOGE("read_slot lsn={} blob too small for blkid: size={} need>={}", lsn, buf.size(), blkid_off);
            co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
        }
        sisl::blob blkid_blob{buf.bytes() + blkid_off, buf.size() - blkid_off};
        slot.blkid.deserialize(blkid_blob, /* copy = */ true);

        co_return slot;
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

    // Reads the raw local entry back off the log store -- the exact bytes write_slot wrote,
    // header + serialized blkid -- and hands the blkid to free_data. Never goes through
    // read_slot/JournalSlot: that type is wire-shared with craft::JournalSlot for peer fetch_data
    // responses and deliberately carries no blkid (meaningless to a remote peer).
    async_status free_slot(int64_t lsn) override {
        homestore::log_buffer buf;
        try {
            buf = logstore_->read_sync(static_cast< homestore::logstore_seq_num_t >(lsn));
        } catch (std::exception const& e) {
            LOGE("free_slot: read_sync failed lsn={}: {}", lsn, e.what());
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        if (buf.size() < sizeof(CraftJournalEntry)) {
            LOGE("free_slot: entry truncated lsn={} size={}", lsn, buf.size());
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        CraftJournalEntry hdr{};
        std::memcpy(&hdr, buf.bytes(), sizeof(CraftJournalEntry));
        // Same validate-before-trust guards as read_slot(): a corrupt/misplaced/malformed record must
        // fail cleanly here too, not just when read via read_slot(). Trusting hdr.len (and deriving
        // nlbas/blkid_off from it) or hdr.all_zeros from an unvalidated header risks deserializing a
        // garbage blkid and freeing whatever blocks that garbage happens to decode to.
        if (hdr.magic != k_journal_magic || hdr.version != k_journal_version) {
            LOGE("free_slot lsn={} bad magic/version: magic={:#x} version={}", lsn, hdr.magic, hdr.version);
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        if (hdr.lsn != lsn) {
            LOGE("free_slot lsn={} hdr.lsn={} mismatch -- corrupt or misplaced record", lsn, hdr.lsn);
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        if (hdr.all_zeros) co_return ok();
        if (hdr.len == 0 || hdr.len % lba_size_ != 0) {
            LOGE("free_slot lsn={} hdr.len={} not a positive multiple of lba_size={} -- malformed record", lsn,
                 hdr.len, lba_size_);
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        }

        // hdr.len bytes of real data means a non-empty csum array precedes the blkid in this
        // slot's blob -- [header][csums][blkid], not [header][blkid] -- so the blkid offset must
        // skip past the csum array, not assume it starts right after the fixed header.
        uint32_t nlbas = hdr.len / lba_size_;
        uint32_t blkid_off = blkid_offset(nlbas);
        if (buf.size() < blkid_off) {
            LOGE("free_slot: entry truncated lsn={} size={} need>={}", lsn, buf.size(), blkid_off);
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        homestore::multi_blk_id blkid{};
        blkid.deserialize(sisl::blob{buf.bytes() + blkid_off, buf.size() - blkid_off}, true /* copy */);
        co_return co_await free_data(blkid);
    }

    // dest.size must already be set by the caller to blkid's byte length (matches async_alloc_write's
    // own contract -- data_service() sizes the I/O from the sg_list, not from a separate parameter).
    async_status read_data(homestore::multi_blk_id blkid, sisl::sg_list& dest) override {
        auto res = co_await homestore::data_service().async_read(blkid, dest, dest.size, nullptr);
        if (!res) {
            LOGE("async_read failed: {}", res.error().message());
            co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
        }
        co_return ok();
    }

private:
    shared< homestore::home_log_store > logstore_;
    uint64_t vol_ordinal_;
    uint32_t lba_size_;
};

unique< CraftJournalBackend > make_homestore_journal_backend(shared< homestore::home_log_store > logstore,
                                                             uint64_t vol_ordinal, uint32_t lba_size) {
    return std::make_unique< HomeStoreCraftJournalBackend >(std::move(logstore), vol_ordinal, lba_size);
}

// ─── constructor ──────────────────────────────────────────────────────────────

CraftReplDev::CraftReplDev(volume_id_t vol_id, unique< CraftJournalBackend > journal, uint32_t lba_size,
                           shared< VolumeIndexTable > indx_tbl) :
        vol_id_{vol_id},
        journal_{std::move(journal)},
        lba_size_{lba_size},
        indx_tbl_{std::move(indx_tbl)},
        raft_listener_{this},
        watchdog_timeout_ns_{HB_DYNAMIC_CONFIG(craft_watchdog_timeout_ms) * 1'000'000ULL} {}

CraftReplDev::~CraftReplDev() {
    // iomgr::timer_token::cancel(wait=true) is a no-op if the watchdog was never armed (watchdog disabled
    // via craft_watchdog_timeout_ms == 0, or no write()/keep_alive() ever succeeded). Otherwise it removes the
    // recurring timer's underlying IODevice from its reactor via iomanager.run_on_wait(), which blocks THIS thread
    // until that removal has actually run on the reactor thread that also runs on_watchdog_tick(). The precise
    // mechanism that makes this race-free (verified against iomgr source, not assumed): the epoll
    // reactor's listen() loop tracks removed-this-batch iodevs in m_removed_iodevs and skips any
    // already-queued event for one (reactor_epoll.cpp's listen()/remove_iodev_impl) -- so even a timer
    // event sitting in the SAME epoll batch as the removal is discarded rather than dispatched. Once
    // cancel() returns, on_watchdog_tick() is guaranteed to never fire again against this (about to be
    // destroyed) object. No generation counter, in-flight counter, or shutting-down flag needed: this
    // single call is iomgr's own safety guarantee, not a hand-rolled one. (If iomgr ever ships a non-
    // epoll reactor backend without an equivalent same-batch-removal mechanism, this guarantee would need
    // re-verifying against that backend specifically.)
    watchdog_token_.cancel(/* wait = */ true);
}

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
// CRAFT write path is quiesced by login serialisation). Four atomic steps:
//   1. Journal rollback: drop all entries with dLSN > lsn (tail truncation).
//   2. Clamp last_append_lsn to lsn if it is higher.
//   3. Erase all missing-set entries above lsn.
//   4. Prune any journal-tail overlay entry referencing a now-rolled-back dLSN (see the S3 overlay's
//      own doc comment on overlay_ in the header) -- otherwise a stale entry above lsn would keep
//      pointing at a write the journal no longer has.
// commit_lsn is not touched: the new rs_commit_lsn passed by the caller is the
// dLSN up to which RAFT consensus has RESOLVED entries. Entries above that are
// the ones being dropped. The missing set tracks gaps in [commit_lsn+1,
// last_append_lsn]; after truncation every entry > lsn is gone from both the
// journal and the missing set.

async_status CraftReplDev::truncate(int64_t lsn) {
    // Guard before touching the journal: truncating below commit_lsn would drop
    // committed entries and break the commit_lsn <= last_append_lsn invariant.
    int64_t last_append_snapshot;
    {
        std::lock_guard lk{missing_mu_};
        DEBUG_ASSERT_GE(lsn, state_.commit_lsn, "truncate below committed prefix");
        last_append_snapshot = state_.last_append_lsn;
    }

    // Step 0 (before rollback): every entry above lsn that's about to be dropped may reference a
    // real data block -- home_log_store::rollback (behind truncate_to below) only removes the
    // journal RECORDS; block lifecycle (HS_DATA_LINKED) is this class's job, not HomeStore's. Each
    // entry's blkid must be read BEFORE truncate_to() destroys the record -- there is no way to
    // recover it afterward. A read_slot() failure here just means this specific lsn was never
    // journaled locally (a genuine gap): nothing to free, not an error, and must never abort the
    // truncate itself over a slot that was already absent.
    std::vector< homestore::multi_blk_id > freed_blkids;
    for (int64_t l = lsn + 1; l <= last_append_snapshot; ++l) {
        auto slot_r = co_await journal_->read_slot(l);
        if (slot_r && !slot_r->all_zeros && slot_r->blkid.is_valid()) freed_blkids.push_back(slot_r->blkid);
    }

    // Step 1: journal rollback — synchronous; fails fast on I/O error.
    if (auto r = co_await journal_->truncate_to(lsn); !r) co_return r;

    // Step 1.5: free every block collected above, now that the rollback has succeeded. Non-fatal on
    // individual failure -- same as every other reclaim call site in this class -- since the journal
    // record is already gone from the log either way; a failed free merely leaks that one block
    // rather than aborting an otherwise-successful truncate.
    for (auto const& blkid : freed_blkids) {
        if (auto fr = co_await journal_->free_data(blkid); !fr)
            LOGE("truncate: free_data failed reclaiming blk={} (dropped above lsn={}): {}", blkid.to_string(), lsn,
                 fr.error().message());
    }

    // Steps 2 + 3 under the same mutex write() uses.
    {
        std::lock_guard lk{missing_mu_};
        if (state_.last_append_lsn > lsn) state_.last_append_lsn = lsn;
        missing_lsns_.erase(missing_lsns_.upper_bound(lsn), missing_lsns_.end());
    }

    // Step 4: prune any overlay entry referencing a now-rolled-back dLSN. Without this, a stale
    // entry above lsn would keep pointing at a write the journal no longer has -- a subsequent read
    // could serve data from a block that's since been freed or reallocated to something else
    // entirely under the new term.
    {
        std::lock_guard lk{overlay_mu_};
        for (auto it = overlay_.begin(); it != overlay_.end();) {
            if (it->second.lsn > lsn) {
                it = overlay_.erase(it);
            } else {
                ++it;
            }
        }
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

// ─── stubs (login/logout are separate scope, not part of S3's commit/read path) ──────────────────

async_result< craft::LoginResult > CraftReplDev::login(uint64_t /* client_token */) {
    LOGW("CraftReplDev::login not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_status CraftReplDev::logout(craft::client_hdr /* hdr */) {
    LOGW("CraftReplDev::logout not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::lsn_pair > CraftReplDev::write(craft::client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                                    sisl::sg_list data) {
    // Checked ahead of recovering_: a faulted restart recovery is permanent, not a "still starting up,
    // try again shortly" condition -- see recovery_faulted_'s doc comment.
    if (recovery_faulted_.load(std::memory_order_acquire)) {
        LOGE("write rejected: partition permanently faulted after failed restart recovery dlsn={}", dlsn);
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    if (recovering_.load(std::memory_order_acquire)) {
        LOGW("write rejected: overlay rebuild in progress after restart dlsn={}", dlsn);
        co_return std::unexpected(make_error_condition(volume_error::OFFLINE));
    }
    if (dlsn < 0) {
        LOGW("write rejected: invalid dlsn={}", dlsn);
        co_return std::unexpected(make_error_condition(std::errc::invalid_argument));
    }
    const bool all_zeros = (data.size == 0);
    // addr/len must be a positive, block-aligned byte range: nlbas = len / lba_size_ is used
    // throughout (CRC computation, overlay population, and commit()'s later index-range
    // application) as an LBA count/offset. len==0 (or a len that doesn't evenly divide lba_size_,
    // silently rounding nlbas down to 0) would let nlbas==0 reach commit_impl's
    // end_lba = start_lba + nlbas - 1, which underflows lba_t (unsigned) into a near-UINT64_MAX
    // range applied to the real index -- an effectively unbounded loop reachable straight from the
    // client wire.
    const uint64_t max_io_len = HB_DYNAMIC_CONFIG(craft_max_io_len_mb) * 1024ULL * 1024ULL;
    if (len == 0 || len % lba_size_ != 0 || addr % lba_size_ != 0 || len > max_io_len) {
        LOGW("write rejected: addr={} len={} not block-aligned (lba_size={}) or exceeds max {} dlsn={}", addr, len,
             lba_size_, max_io_len, dlsn);
        co_return std::unexpected(make_error_condition(std::errc::invalid_argument));
    }
    // data.size must match len exactly: the CRC loop below reads nlbas*lba_size_ == len bytes from
    // data.iovs[0]'s buffer, and a data.size that merely claims to match len without the backing
    // iovec actually being that large would over-read past the caller's buffer. Exactly one iovec is
    // required: the CRC loop and alloc_write_data both only ever look at iovs[0], so a second (or
    // later) iovec is always dead/unused today -- but a caller-supplied sg_list claiming exactly
    // `len` total bytes while carrying extra trailing iovecs is self-inconsistent input, and trusting
    // iovs.size()==1 here means the CRC/copy logic never needs to change if that assumption is ever
    // revisited. Rejecting it now is cheap, forward-looking hygiene, not a response to an exploitable
    // gap -- data.iovs[0].iov_len < len above already fully closes the "CRC only covers iovs[0]"
    // corruption scenario on its own (a multi-iovec write can only reach this point if iovs[0] alone
    // already covers the whole range).
    if (!all_zeros && (data.iovs.size() != 1 || data.size != len || data.iovs[0].iov_len < len)) {
        LOGW("write rejected: data.size={} does not match len={} or iovs.size()={} != 1 dlsn={}", data.size, len,
             data.iovs.size(), dlsn);
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
        // Advance the reclaim floor from ordinary IO -- same max-monotonic pattern as keep_alive().
        // Without this, all_committed_lsn only moves via keep_alive() messages, so quiet partitions
        // (write-only, no explicit keep_alive) never advance the reclaim floor at all.
        // hdr.all_committed_lsn is client-controlled wire input: -1 is the "unset" sentinel (skip),
        // any other negative value is malformed (ignore rather than corrupt this long-lived floor).
        // There is no other per-call bound to validate against here -- see keep_alive()'s doc comment.
        if (hdr.all_committed_lsn >= -1)
            state_.all_committed_lsn = std::max(state_.all_committed_lsn, hdr.all_committed_lsn);
        if (empty_lsns_.contains(dlsn)) {
            LOGW("write rejected: slot is permanently empty dlsn={}", dlsn);
            co_return std::unexpected(make_error_condition(volume_error::EMPTY_SLOT));
        }
        // Idempotent: slot already successfully written — return snapshot without a second write_slot call.
        if (dlsn <= state_.last_append_lsn && !missing_lsns_.contains(dlsn)) {
            LOGT("write idempotent: dlsn={} already written", dlsn);
            co_return craft::lsn_pair{state_.commit_lsn, state_.last_append_lsn};
        }
        // Reject a concurrent duplicate rather than racing it: two write() calls for the SAME dlsn
        // arriving concurrently (a malformed/retried request -- a well-behaved client never does
        // this for one dlsn) would otherwise both pass the idempotency check above (neither has
        // advanced last_append_lsn yet) and both proceed to alloc_write_data below, doubly
        // allocating real blocks for one dlsn -- only whichever write_slot call lands last would
        // ever be referenced by the journal, permanently leaking the other's blocks.
        if (in_flight_write_dlsns_.contains(dlsn)) {
            LOGW("write rejected: dlsn={} already has a write in flight", dlsn);
            co_return std::unexpected(make_error_condition(std::errc::operation_in_progress));
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
        in_flight_write_dlsns_.insert(dlsn);
    }
    // Erased on every exit from here on (success or failure) -- see the in_flight_write_dlsns_ check
    // above for why this exists. A coroutine's local objects are destroyed on any co_return exactly
    // like a normal function's, so this RAII guard is safe across every path below, including ones
    // that suspend (co_await) before returning.
    struct InFlightDlsnGuard {
        CraftReplDev* self;
        int64_t dlsn;
        ~InFlightDlsnGuard() {
            std::lock_guard lk{self->missing_mu_};
            self->in_flight_write_dlsns_.erase(dlsn);
        }
    } in_flight_guard{this, dlsn};

    // HS_DATA_LINKED: allocate blocks and write payload before journalling the block reference.
    // all_zeros=true skips this; the early-return above guarantees !all_zeros implies data.size > 0.
    homestore::multi_blk_id blkid{};
    bool blkid_allocated = false;
    std::vector< homestore::csum_t > csums; // one per LBA; stays empty for all_zeros (no data to sum)
    if (!all_zeros) {
        auto alloc_res = co_await journal_->alloc_write_data(data, static_cast< lba_count_t >(len));
        if (!alloc_res) {
            LOGE("alloc_write_data failed dlsn={}: {}", dlsn, alloc_res.error().message());
            co_return std::unexpected(alloc_res.error());
        }
        blkid = *alloc_res;
        blkid_allocated = true;

        // Per-LBA CRC16, computed here while the payload is still in memory -- same routine and
        // seed volume.cpp's non-CRAFT write path already uses (crc16_t10dif / init_crc_16). Walks a
        // single flat buffer (data's first iovec), matching that same path's assumption; every
        // sg_list CRAFT builds anywhere in this codebase today is a single iovec.
        uint32_t nlbas = static_cast< uint32_t >(len / lba_size_);
        csums.reserve(nlbas);
        auto const* buf = static_cast< uint8_t const* >(data.iovs[0].iov_base);
        for (uint32_t i = 0; i < nlbas; ++i) {
            csums.push_back(crc16_t10dif(k_craft_crc16_seed, buf + i * lba_size_, lba_size_));
        }
    }
    // addr and len are BYTES (byte-addressed API): CraftJournalEntry stores them verbatim as bytes.
    // hdr.term is already verified against state_.term under missing_mu_ above; pass it so the
    // on-disk entry carries the session term for stale-tail detection on recovery. dlsn is stored
    // redundantly in CraftJournalEntry.lsn for self-describing recovery.
    auto res = co_await journal_->write_slot(dlsn, hdr.term, static_cast< lba_t >(addr),
                                             static_cast< lba_count_t >(len), blkid, all_zeros, csums);
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
            // Populate the journal-tail overlay (makes this entry locally readable ahead of commit()
            // applying it) BEFORE erasing dlsn from missing_lsns_, and both under the same missing_mu_
            // critical section commit_impl's is_missing check also takes: otherwise a concurrent
            // commit_impl run (another write()'s or keep_alive()'s piggyback) could see dlsn already
            // erased, read/apply/retire it, and advance commit_lsn past it -- all before this call gets
            // around to inserting the overlay entry. That would leave a permanently-orphaned overlay
            // entry below commit_lsn: harmless (clamped out at read time) but never retired until the
            // same LBA happens to be overwritten by a later write. Doing both under one lock closes the
            // window entirely -- commit_impl either still sees dlsn as missing (stalls) or sees it
            // resolved with the overlay already populated, never the state in between.
            populate_overlay(dlsn, static_cast< lba_t >(addr) / lba_size_, static_cast< uint32_t >(len / lba_size_),
                             all_zeros, blkid, csums);
            missing_lsns_.erase(dlsn);
            snapshot = {state_.commit_lsn, state_.last_append_lsn};
        }
    }

    if (stale_post_flight) { co_return std::unexpected(make_error_condition(volume_error::STALE_TERM)); }

    // Best-effort piggyback: advance commit_lsn toward the client's own view of it. Every outcome --
    // a stall at a gap, or even a genuine commit() fault -- is ignored here; the write itself already
    // succeeded, and the next write/keep_alive retries the advance. keep_alive() is where a real
    // commit() error actually surfaces (advancing the frontier is its entire purpose).
    co_await commit(hdr.commit_lsn);

    touch_watchdog();

    // Re-snapshot after the piggyback commit() above: the pre-commit snapshot captured earlier
    // (before missing_lsns_.erase()) would otherwise report stale watermarks -- the ack should
    // reflect whatever progress commit() just made, not a view from before this call's own piggyback ran.
    {
        std::lock_guard lock{missing_mu_};
        snapshot = {state_.commit_lsn, state_.last_append_lsn};
    }

    LOGT("write ok dlsn={} addr={} len={} all_zeros={}", dlsn, addr, len, all_zeros);
    co_return snapshot;
}

// ─── commit() (internal; never a wire op) ────────────────────────────────────
//
// Advances commit_lsn toward upto_lsn by applying each committable slot to the index. At most one
// run is ever active at a time (commit_running_, guarded by missing_mu_ and reset via RAII on every
// exit path); a concurrent caller is a safe no-op -- the in-flight run covers the same ground, and
// every subsequent write()/keep_alive() retries the advance. Never holds a lock across the co_await
// read_slot() suspension point below (same rule fetch_data's doc comment already establishes).

async_result< int64_t > CraftReplDev::commit_impl(int64_t upto_lsn, write_index_fn_t const& write_fn,
                                                  delete_index_fn_t const& delete_fn) {
    int64_t commit_lsn, last_append_lsn;
    {
        std::lock_guard lk{missing_mu_};
        if (commit_running_) co_return state_.commit_lsn; // another run is already advancing
        commit_running_ = true;
        commit_lsn = state_.commit_lsn;
        last_append_lsn = state_.last_append_lsn;
    }
    // Guaranteed reset on every exit path (stall, success, or error): a local RAII object's destructor
    // runs when the coroutine frame unwinds, exactly like a plain function's locals on return.
    struct RunningGuard {
        CraftReplDev* self;
        ~RunningGuard() {
            std::lock_guard lk{self->missing_mu_};
            self->commit_running_ = false;
        }
    } guard{this};

    int64_t const target = std::min(upto_lsn, last_append_lsn);
    for (int64_t lsn = commit_lsn + 1; lsn <= target; ++lsn) {
        bool is_missing, is_empty;
        {
            std::lock_guard lk{missing_mu_};
            is_missing = missing_lsns_.contains(lsn);
            is_empty = empty_lsns_.contains(lsn);
        }
        if (is_missing) break; // stall at the first hole -- not an error

        // Retires the overlay entry for each LBA in [start_lba, end_lba] whose recorded lsn equals
        // retiring_lsn -- shared by the data-apply path below and the is_empty path further down (a
        // higher-dLSN overlay entry for the same LBA, from a later append not yet committed, must
        // survive either way -- see the original comment this was factored out of).
        auto retire_overlay_range = [this](lba_t start_lba, lba_t end_lba, int64_t retiring_lsn) {
            std::lock_guard lk{overlay_mu_};
            for (lba_t l = start_lba; l <= end_lba; ++l) {
                auto it = overlay_.find(l);
                if (it != overlay_.end() && it->second.lsn == retiring_lsn) overlay_.erase(it);
            }
        };

        if (!is_empty) {
            auto slot_r = co_await journal_->read_slot(lsn);
            if (!slot_r) co_return std::unexpected(slot_r.error());
            auto& slot = *slot_r;

            lba_t const start_lba = static_cast< lba_t >(slot.lba_off_bytes) / lba_size_;
            uint32_t const nlbas = static_cast< uint32_t >(slot.len_bytes / lba_size_);
            // Defense-in-depth: write() rejects len==0 at the client boundary, but a stale/legacy
            // on-disk record could still have one. nlbas==0 must never reach the end_lba
            // computation below -- start_lba + 0 - 1 underflows lba_t (unsigned) into a
            // near-UINT64_MAX range that would be applied to the real index.
            if (nlbas == 0) {
                LOGE("commit: slot lsn={} has len_bytes={} (nlbas=0) -- malformed record, aborting", lsn,
                     slot.len_bytes);
                co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
            }
            lba_t const end_lba = start_lba + nlbas - 1;

            if (slot.all_zeros) {
                std::vector< homestore::blk_id > freed;
                if (auto r = delete_fn(start_lba, end_lba, freed); !r) co_return std::unexpected(r.error());
                // Reclaim inline, same as write()'s own free_data call sites -- free errors are
                // logged but non-fatal (the block is merely leaked, not corrupted).
                for (auto const& blk : freed) {
                    if (auto fr = co_await journal_->free_data(homestore::multi_blk_id{blk}); !fr)
                        LOGE("free_data failed reclaiming blk={} lsn={}: {}", blk.to_string(), lsn,
                             fr.error().message());
                }
            } else {
                // Decompose the slot's blkid into per-LBA single-block BlockInfo entries -- same shape
                // as write()'s own overlay-population decomposition and volume.cpp's non-CRAFT path.
                std::unordered_map< lba_t, BlockInfo > blocks_info;
                auto pieces = slot.blkid.iterate();
                uint32_t csum_idx = 0;
                lba_t lba = start_lba;
                while (auto piece = pieces.next()) {
                    for (homestore::blk_count_t i = 0; i < piece->blk_count(); ++i, ++lba, ++csum_idx) {
                        // Defense-in-depth matching populate_overlay()'s identical guard: csums should
                        // have exactly nlbas entries (write() computes and stores them that way), but a
                        // corrupt on-disk blob could produce a shorter array -- an OOB read here would
                        // silently fabricate a checksum rather than signalling the corruption.
                        if (csum_idx >= slot.csums.size()) {
                            LOGE("commit: csums array shorter ({}) than blkid piece count (lba={} lsn={}) "
                                 "-- corrupt on-disk record, aborting commit for this lsn",
                                 slot.csums.size(), lba, lsn);
                            co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
                        }
                        homestore::blk_id single_bid{static_cast< homestore::blk_num_t >(piece->blk_num() + i), 1,
                                                     piece->chunk_num()};
                        blocks_info.emplace(lba, BlockInfo{single_bid, homestore::blk_id{}, slot.csums[csum_idx]});
                    }
                }
                if (auto r = write_fn(start_lba, end_lba, blocks_info); !r) co_return std::unexpected(r.error());
                // Reclaim any superseded old blkid inline, same as write()'s own free_data call sites.
                // old_blkid == new_blkid means this slot was already applied (crash-replay of an
                // idempotent commit before commit_lsn was durably persisted): the index already holds
                // this exact blkid, so freeing it here would free a block the index still references.
                for (auto const& [_, info] : blocks_info) {
                    if (!info.old_blkid.is_valid() || info.old_blkid == info.new_blkid) continue;
                    if (auto fr = co_await journal_->free_data(homestore::multi_blk_id{info.old_blkid}); !fr)
                        LOGE("free_data failed reclaiming blk={} lsn={}: {}", info.old_blkid.to_string(), lsn,
                             fr.error().message());
                }
            }

            retire_overlay_range(start_lba, end_lba, lsn);
        } else {
            // Empty-verdicted (S5 SyncRSCommitLSN): the index deliberately never applies this lsn.
            // But "Empty beats data" reconciliation (see request_resolution's doc comment in
            // home_blocks.hpp) means the verdict holds even if THIS replica itself already journaled
            // real data for it -- e.g. the leader's resolution round verdicted Empty without
            // successfully using this replica as a holder. If that happened, write()'s post-flight
            // already created a real overlay entry for it that would otherwise never get retired
            // (permanently stale, and -- worse -- still SERVED on reads, since the overlay wins over
            // the index within the horizon, directly contradicting the Empty verdict). read_slot()
            // here is purely to discover the LBA range to retire, not to apply anything to the index.
            // A read_slot() failure just means this replica never had this lsn locally either (the
            // ordinary case: this lsn was this replica's OWN gap, resolved as empty because no
            // replica anywhere held it) -- nothing to retire, not an error.
            if (auto slot_r = co_await journal_->read_slot(lsn); slot_r) {
                auto& slot = *slot_r;
                if (uint32_t const nlbas = static_cast< uint32_t >(slot.len_bytes / lba_size_); nlbas > 0) {
                    lba_t const start_lba = static_cast< lba_t >(slot.lba_off_bytes) / lba_size_;
                    retire_overlay_range(start_lba, start_lba + nlbas - 1, lsn);
                }
            }
        }

        std::lock_guard lk{missing_mu_};
        state_.commit_lsn = lsn;
    }

    std::lock_guard lk{missing_mu_};
    co_return state_.commit_lsn;
}

async_result< int64_t > CraftReplDev::commit(int64_t upto_lsn) {
    if (!indx_tbl_) {
        // No index configured (write-path-only tests): nothing to apply, no-op safely.
        std::lock_guard lk{missing_mu_};
        co_return state_.commit_lsn;
    }
    write_index_fn_t write_fn = [this](lba_t s, lba_t e, std::unordered_map< lba_t, BlockInfo >& info) {
        return indx_tbl_->write_to_index(s, e, info);
    };
    delete_index_fn_t delete_fn = [this](lba_t s, lba_t e, std::vector< homestore::blk_id >& freed) {
        return indx_tbl_->delete_lba_range(s, e, freed);
    };
    auto r = co_await commit_impl(upto_lsn, write_fn, delete_fn);
    co_return r;
}

#ifdef _PRERELEASE
async_result< int64_t > CraftReplDev::commit_with(int64_t upto_lsn, write_index_fn_t write_fn,
                                                  delete_index_fn_t delete_fn) {
    auto r = co_await commit_impl(upto_lsn, write_fn, delete_fn);
    co_return r;
}
#endif

// ─── overlay population (shared by write() and rebuild_overlay()) ───────────

void CraftReplDev::populate_overlay(int64_t dlsn, lba_t start_lba, uint32_t nlbas, bool all_zeros,
                                    homestore::multi_blk_id const& blkid,
                                    std::vector< homestore::csum_t > const& csums) {
    lba_t lba = start_lba;
    std::lock_guard lk{overlay_mu_};
    if (all_zeros) {
        for (uint32_t i = 0; i < nlbas; ++i, ++lba) {
            auto it = overlay_.find(lba);
            if (it == overlay_.end() || dlsn > it->second.lsn) {
                overlay_[lba] = OverlayEntry{.lsn = dlsn, .all_zeros = true};
            }
        }
    } else {
        // Decompose blkid's pieces (each a contiguous run of blocks) into single-block blk_ids,
        // one per LBA -- same shape as volume.cpp's non-CRAFT write path (volume.cpp:239-253),
        // generalized to multi_blk_id::iterate() since CRAFT's blkid may have more than one piece.
        auto pieces = blkid.iterate();
        uint32_t csum_idx = 0;
        while (auto piece = pieces.next()) {
            for (homestore::blk_count_t i = 0; i < piece->blk_count(); ++i, ++lba, ++csum_idx) {
                // Defense-in-depth: csums normally has exactly nlbas entries (write() computes it
                // that way, and rebuild_overlay()'s read_slot() call is expected to validate the
                // on-disk array length matches its own parsed nlbas) -- but rebuild_overlay()'s data
                // ultimately comes from a persisted blob, so a corrupted/truncated on-disk record
                // must not turn into an out-of-bounds read here.
                if (csum_idx >= csums.size()) {
                    LOGE("populate_overlay: csums array shorter ({}) than blkid piece count needs (lba={} "
                         "dlsn={}) -- skipping remaining LBAs for this entry",
                         csums.size(), lba, dlsn);
                    return;
                }
                auto it = overlay_.find(lba);
                if (it != overlay_.end() && dlsn <= it->second.lsn) continue;
                homestore::blk_id single_bid{static_cast< homestore::blk_num_t >(piece->blk_num() + i), 1,
                                             piece->chunk_num()};
                overlay_[lba] = OverlayEntry{.lsn = dlsn, .blkid = single_bid, .csum = csums[csum_idx]};
            }
        }
    }
}

// ─── read() ───────────────────────────────────────────────────────────────────
//
// Serves [addr, addr+len) as of read_lsn from the LBA index (committed, <= commit_lsn) merged with
// the journal-tail overlay (appended but not yet committed, horizon-clamped to (commit_lsn, read_lsn]
// so an overlay entry above read_lsn is held but never served -- the index's older, still-valid-as-of-
// read_lsn value is used instead). Never fetches from a peer.

async_result< craft::read_result > CraftReplDev::read(craft::client_hdr hdr, int64_t read_lsn, uint64_t addr,
                                                      uint64_t len, sisl::sg_list dest) {
    // Checked ahead of recovering_: a faulted restart recovery is permanent, not a "still starting up,
    // try again shortly" condition -- see recovery_faulted_'s doc comment.
    if (recovery_faulted_.load(std::memory_order_acquire)) {
        LOGE("read rejected: partition permanently faulted after failed restart recovery");
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    if (recovering_.load(std::memory_order_acquire)) {
        LOGW("read rejected: overlay rebuild in progress after restart");
        co_return std::unexpected(make_error_condition(volume_error::OFFLINE));
    }
    {
        std::lock_guard lock{missing_mu_};
        if (hdr.term != state_.term) {
            LOGW("read rejected: stale term want={} got={}", state_.term, hdr.term);
            co_return std::unexpected(make_error_condition(volume_error::STALE_TERM));
        }
        // Advance the reclaim floor from ordinary IO -- same max-monotonic pattern as keep_alive().
        // Reject malformed negative values -- see write()'s doc comment.
        if (hdr.all_committed_lsn >= -1)
            state_.all_committed_lsn = std::max(state_.all_committed_lsn, hdr.all_committed_lsn);
    }
    // Best-effort piggyback, same reasoning as write()'s: read()'s own success does not depend on
    // whether commit() advances further, so a genuine commit() fault here is swallowed rather than
    // failing an otherwise-servable read.
    co_await commit(hdr.commit_lsn);

    if (!indx_tbl_) {
        LOGW("read rejected: no index configured");
        co_return std::unexpected(make_error_condition(std::errc::not_supported));
    }
    read_index_fn_t read_fn = [this](lba_t s, lba_t e, index_kv_list_t& kvs) {
        return indx_tbl_->read_from_index(s, e, kvs);
    };
    auto r = co_await read_impl(read_lsn, addr, len, std::move(dest), read_fn);
    co_return r;
}

#ifdef _PRERELEASE
async_result< craft::read_result > CraftReplDev::read_with(int64_t read_lsn, uint64_t addr, uint64_t len,
                                                           sisl::sg_list dest, read_index_fn_t read_fn) {
    auto r = co_await read_impl(read_lsn, addr, len, std::move(dest), read_fn);
    co_return r;
}
#endif

// Core apply algorithm behind read(): merges committed index state with the journal-tail overlay
// (horizon-clamped), reads every data-carrying LBA, verifies its checksum, and collapses any
// all-zero-content LBA to a hole extent -- this scan runs ONLY here, never on the write path.
async_result< craft::read_result > CraftReplDev::read_impl(int64_t read_lsn, uint64_t addr, uint64_t len,
                                                           sisl::sg_list dest, read_index_fn_t const& read_fn) {
    // Same boundary validation as write(): nlbas=0 would underflow end_lba below (lba_t is unsigned).
    // The upper bound also prevents nlbas itself from landing anywhere near UINT32_MAX (which would
    // size sources/is_hole below to a multi-gigabyte allocation from one client-controlled len) --
    // read(), like write(), is reachable straight from the client wire, so len is untrusted input.
    const uint64_t max_io_len = HB_DYNAMIC_CONFIG(craft_max_io_len_mb) * 1024ULL * 1024ULL;
    if (len == 0 || len % lba_size_ != 0 || addr % lba_size_ != 0 || len > max_io_len) {
        LOGW("read rejected: addr={} len={} not block-aligned (lba_size={}) or exceeds max {}", addr, len, lba_size_,
             max_io_len);
        co_return std::unexpected(make_error_condition(std::errc::invalid_argument));
    }
    // Unlike commit_lsn/all_committed_lsn on the write path, read_lsn has no "-1 means unset"
    // convention -- it is always a horizon the caller must have a real value for. A negative value
    // here would silently make every overlay entry's `it->second.lsn <= read_lsn` clamp check false
    // (since no real lsn is negative), serving a read that looks committed-only rather than the
    // explicit rejection a malformed request deserves.
    if (read_lsn < 0) {
        LOGW("read rejected: negative read_lsn={}", read_lsn);
        co_return std::unexpected(make_error_condition(std::errc::invalid_argument));
    }
    lba_t const start_lba = static_cast< lba_t >(addr) / lba_size_;
    uint32_t const nlbas = static_cast< uint32_t >(len / lba_size_);
    // Defense-in-depth, mirroring commit_impl()'s identical guard: with len bounded above, nlbas can
    // only be 0 here if len==0 (already rejected), but this stays as a hard backstop against any
    // future change to the bound above reintroducing the underflow risk.
    if (nlbas == 0) {
        LOGE("read rejected: addr={} len={} produced nlbas=0 -- malformed request", addr, len);
        co_return std::unexpected(make_error_condition(std::errc::invalid_argument));
    }
    lba_t const end_lba = start_lba + nlbas - 1;

    int64_t commit_lsn_snapshot;
    {
        std::lock_guard lk{missing_mu_};
        commit_lsn_snapshot = state_.commit_lsn;
    }
    // read_lsn and hdr.commit_lsn arrive in the same frame with nothing coupling them: read()'s own
    // piggyback commit (or a concurrent write()/keep_alive()'s) can advance commit_lsn_snapshot past
    // read_lsn before this snapshot is even taken. Once that happens the pre-read_lsn version is gone
    // from the index -- apply is a blind overwrite past commit_lsn by design (see commit()'s doc
    // comment) -- so there is nothing left on this replica to serve read_lsn correctly. Reject rather
    // than silently returning the too-new index value (CRAFT-Design: "writes above H are ignored even
    // if the replica holds them").
    if (read_lsn < commit_lsn_snapshot) {
        LOGW("read rejected: read_lsn={} is below commit_lsn={} -- horizon already advanced, unanswerable", read_lsn,
             commit_lsn_snapshot);
        co_return std::unexpected(make_error_condition(volume_error::HORIZON_STALE));
    }

    // Committed state from the index.
    index_kv_list_t index_kvs;
    if (auto r = read_fn(start_lba, end_lba, index_kvs); !r) co_return std::unexpected(r.error());
    std::unordered_map< lba_t, VolumeIndexValue > index_map;
    index_map.reserve(index_kvs.size());
    for (auto const& [key, value] : index_kvs)
        index_map.emplace(key.lba(), value);

    // read_fn above takes no lock of its own (an index query, not a read of missing_mu_-guarded
    // state) -- a concurrent commit_impl run can apply new LSNs to the index for an LBA in
    // [start_lba, end_lba] while read_fn is executing, even though the pre-check above passed
    // against the OLD snapshot. Re-snapshot commit_lsn now and re-check against read_lsn: if it
    // moved past read_lsn during the query, index_kvs just fetched may already reflect state above
    // the client's horizon for some LBA in range -- reject rather than silently serve a stale-snapshot
    // read as if it were still valid. This also narrows commit_lsn_snapshot to the freshest known-safe
    // value for the overlay clamp just below (a strictly tighter, still-correct bound).
    {
        std::lock_guard lk{missing_mu_};
        commit_lsn_snapshot = state_.commit_lsn;
    }
    if (read_lsn < commit_lsn_snapshot) {
        LOGW("read rejected: commit_lsn advanced to {} past read_lsn={} while the index was being "
             "queried -- horizon already advanced, unanswerable",
             commit_lsn_snapshot, read_lsn);
        co_return std::unexpected(make_error_condition(volume_error::HORIZON_STALE));
    }

    // Per-LBA source: hole (default), or a single-block data reference (blkid + csum) from whichever
    // of overlay/index wins. Overlay wins over the index for the same LBA (it is strictly newer), but
    // ONLY within the horizon -- an overlay entry above read_lsn is held but never served here; the
    // index's value (committed as of commit_lsn <= read_lsn's caller-assumed frontier) is used instead.
    struct Source {
        bool hole{true};
        homestore::blk_id blkid{};
        homestore::csum_t csum{0};
    };
    std::vector< Source > sources(nlbas);
    {
        std::lock_guard lk{overlay_mu_};
        for (uint32_t i = 0; i < nlbas; ++i) {
            lba_t const lba = start_lba + i;
            auto it = overlay_.find(lba);
            if (it != overlay_.end() && it->second.lsn > commit_lsn_snapshot && it->second.lsn <= read_lsn) {
                if (!it->second.all_zeros) sources[i] = Source{false, it->second.blkid, it->second.csum};
                continue; // all_zeros overlay entry -> hole (Source's default)
            }
            if (auto idx_it = index_map.find(lba); idx_it != index_map.end())
                sources[i] = Source{false, idx_it->second.blkid(), idx_it->second.checksum()};
            // else: absent from both index and overlay -> hole (Source's default)
        }
    }

    // dest is a single flat iovec -- same assumption write()'s CRC computation already makes; every
    // sg_list CRAFT builds anywhere in this codebase is a single iovec.
    if (dest.iovs.empty()) {
        LOGW("read rejected: dest sg_list has no iovecs (size={})", dest.size);
        co_return std::unexpected(make_error_condition(std::errc::invalid_argument));
    }
    // dest.iovs[0].iov_len must be able to hold the full requested range -- an undersized buffer
    // would otherwise have the memset/read_data calls below write past its end.
    uint64_t const dest_capacity = static_cast< uint64_t >(nlbas) * lba_size_;
    if (dest.iovs[0].iov_len < dest_capacity) {
        LOGW("read rejected: dest iov_len={} smaller than required={} (nlbas={} lba_size={})", dest.iovs[0].iov_len,
             dest_capacity, nlbas, lba_size_);
        co_return std::unexpected(make_error_condition(std::errc::invalid_argument));
    }
    auto* dest_buf = static_cast< uint8_t* >(dest.iovs[0].iov_base);
    std::vector< bool > is_hole(nlbas, false); // value-initialized to false regardless; explicit for clarity

    for (uint32_t i = 0; i < nlbas;) {
        if (sources[i].hole) {
            is_hole[i] = true;
            std::memset(dest_buf + i * lba_size_, 0, lba_size_);
            ++i;
            continue;
        }
        // Extend the contiguous run: same blk_num/chunk-progression merge volume.cpp's non-CRAFT
        // read path uses (generate_blkids_to_read) -- batches one async_read per contiguous run
        // instead of one per LBA. Capped at blk_count_t's max (65535): craft_max_io_len_mb (up to
        // 128 MiB by default) / a small lba_size_ can produce a single contiguous run far larger
        // than blk_count_t (uint16_t) can hold, which would otherwise silently truncate in the
        // static_cast below. Splitting into an extra run here just costs one more read_data batch
        // in that rare case -- correctness is unaffected, nothing is lost or misread.
        uint32_t j = i + 1;
        while (j < nlbas && !sources[j].hole && sources[j].blkid.blk_num() == sources[j - 1].blkid.blk_num() + 1 &&
               sources[j].blkid.chunk_num() == sources[j - 1].blkid.chunk_num() &&
               (j - i) < std::numeric_limits< homestore::blk_count_t >::max()) {
            ++j;
        }
        uint32_t const run_nlbas = j - i;
        homestore::multi_blk_id const run_blkid{
            sources[i].blkid.blk_num(), static_cast< homestore::blk_count_t >(run_nlbas), sources[i].blkid.chunk_num()};
        sisl::sg_list run_sg;
        run_sg.size = run_nlbas * lba_size_;
        run_sg.iovs.push_back(iovec{dest_buf + i * lba_size_, run_sg.size});
        if (auto r = co_await journal_->read_data(run_blkid, run_sg); !r) co_return std::unexpected(r.error());

        for (uint32_t k = i; k < j; ++k) {
            uint8_t const* lba_buf = dest_buf + k * lba_size_;
            // CRC is checked FIRST, unconditionally -- a bit-flip that happens to zero out real
            // (non-zero-checksummed) data must surface as CRC_MISMATCH, not silently collapse to an
            // indistinguishable hole. Only once the content is verified intact does the all-zero
            // collapse below decide hole-vs-data.
            auto const computed = crc16_t10dif(k_craft_crc16_seed, lba_buf, lba_size_);
            if (computed != sources[k].csum) {
                LOGE("read: crc mismatch lba={} expected={} actual={}", start_lba + k, sources[k].csum, computed);
                co_return std::unexpected(make_error_condition(volume_error::CRC_MISMATCH));
            }
            // Read-time-only collapse: a data write whose payload happened to be all-zero bytes
            // reads back as a hole. This scan must never run on the write path.
            is_hole[k] = std::all_of(lba_buf, lba_buf + lba_size_, [](uint8_t b) { return b == 0; });
        }
        i = j;
    }

    // Merge adjacent same-type LBAs into extents, ascending by addr.
    std::vector< craft::io_extent > extents;
    for (uint32_t i = 0; i < nlbas;) {
        uint32_t j = i + 1;
        while (j < nlbas && is_hole[j] == is_hole[i])
            ++j;
        extents.push_back(
            craft::io_extent{(start_lba + i) * lba_size_, (j - i) * static_cast< uint64_t >(lba_size_), is_hole[i]});
        i = j;
    }

    craft::lsn_pair snapshot;
    {
        std::lock_guard lk{missing_mu_};
        snapshot = {state_.commit_lsn, state_.last_append_lsn};
    }
    co_return craft::read_result{std::move(extents), snapshot};
}

async_result< craft::lsn_pair > CraftReplDev::keep_alive(craft::client_hdr hdr) {
    // Checked ahead of recovering_: a faulted restart recovery is permanent, not a "still starting up,
    // try again shortly" condition -- see recovery_faulted_'s doc comment.
    if (recovery_faulted_.load(std::memory_order_acquire)) {
        LOGE("keep_alive rejected: partition permanently faulted after failed restart recovery");
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    if (recovering_.load(std::memory_order_acquire)) {
        LOGW("keep_alive rejected: overlay rebuild in progress after restart");
        co_return std::unexpected(make_error_condition(volume_error::OFFLINE));
    }
    {
        std::lock_guard lock{missing_mu_};
        if (hdr.term != state_.term) {
            LOGW("keep_alive rejected: stale term want={} got={}", state_.term, hdr.term);
            co_return std::unexpected(make_error_condition(volume_error::STALE_TERM));
        }
        // Max-monotonic: never let a stale/reordered message regress the floor S8's eventual journal
        // reclaim reads. The reclaim action itself is not implemented here -- see the doc comment.
        // hdr.all_committed_lsn is client-controlled wire input with no per-call bound to validate
        // against (it legitimately advances independently of this call's own commit_lsn/
        // last_append_lsn -- confirmed by existing tests). -1 is the "unset" sentinel (skip); any
        // OTHER negative value is unambiguously malformed and is ignored rather than corrupting this
        // long-lived floor. Does not catch an absurdly-large-but-positive value (no bound exists for
        // that here) -- S8's eventual reclaim implementation must validate against real journal state
        // before consuming this floor, not this layer.
        if (hdr.all_committed_lsn >= -1)
            state_.all_committed_lsn = std::max(state_.all_committed_lsn, hdr.all_committed_lsn);
    }

    touch_watchdog();

    // Unlike write()'s piggyback, advancing the frontier IS this call's entire purpose -- a genuine
    // commit() fault (not a stall, which is never an error) propagates to the caller instead of being
    // silently swallowed.
    if (auto r = co_await commit(hdr.commit_lsn); !r) co_return std::unexpected(r.error());

    std::lock_guard lock{missing_mu_};
    co_return craft::lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}

// ─── client-liveness watchdog (S7) ───────────────────────────────────────────

void CraftReplDev::touch_watchdog() {
    if (watchdog_timeout_ns_ == 0) return;
    {
        std::lock_guard lk{missing_mu_};
        // Not yet logged in -- no session to watch.
        // TODO: this only prevents ARMING before first login, doesn't DISARM on logout -- real
        // logout() must also call watchdog_token_.cancel() once it's implemented (currently a stub).
        if (state_.term == 0) return;
    }
    // Record activity unconditionally (cheap atomic store) before the arm-once check below, so even
    // the very first call -- which also arms the recurring timer -- leaves a fresh timestamp for that
    // timer's first tick to read.
    last_contact_ns_.store(std::chrono::steady_clock::now().time_since_epoch().count(), std::memory_order_relaxed);

    // Arm the recurring timer at most once, lazily, on the first successful write()/keep_alive() after
    // login (state_.term != 0, checked above) -- every call after that just updates last_contact_ns_
    // above; there is nothing to cancel-and-reschedule anymore (see on_watchdog_tick()'s doc comment
    // for why a RECURRING timer removes the need for that entirely). watchdog_token_ is only ever
    // touched under watchdog_arm_mu_ here, or during single-object-owner destruction -- never both at
    // once, by the same contract that already governs every other member (calling any method
    // concurrently with the destructor is a lifetime violation regardless of the watchdog).
    //
    // reactor_regex::all_worker, not all_user: a RECURRING timer's underlying IODevice must actually
    // attach to a reactor matching its scope (iomgr.cpp's schedule_global_timer dispatches all_worker
    // to m_global_worker_timer, all_user to a SEPARATE m_global_user_timer) -- unlike a one-shot
    // timer's reactor-agnostic heap entry. "User" reactors are optional (only exist if something
    // explicitly calls iomanager.create_reactor()); the default worker pool iomanager.start() always
    // creates is the only scope guaranteed to have at least one matching reactor to attach to.
    //
    // Ticks at HALF watchdog_timeout_ns_, not the full interval -- see on_watchdog_tick()'s doc comment
    // for why: ticking at the same cadence as the staleness threshold doubles worst-case detection
    // latency, which iomgr's own IOWatchDog avoids by keeping its tick interval and staleness threshold
    // as two independently configured values (drive.io_watchdog_timer_sec vs drive.io_timeout_limit_sec
    // in watchdog.cpp) -- this mirrors that separation with a single derived constant instead of a
    // second constructor parameter, since nothing here needs the two independently tunable.
    //
    // Also unlike this codebase's OTHER recurring timers (homeblks_impl.cpp's shutdown/vol-gc timers,
    // and HomeStore's CPManager/RaftReplService, which each first create a DEDICATED reactor via
    // iomanager.create_reactor() for their timer rather than sharing the worker pool): this timer is
    // per-CraftReplDev-instance (per partition), not a single process-wide background task, so it
    // deliberately does NOT set up a dedicated reactor per instance. Known, accepted cost of that choice
    // at high partition counts: each instance's recurring timerfd is registered on EVERY worker reactor
    // (iomgr_timer.cpp's schedule() for recurring=true calls add_io_device with reactor_regex scope,
    // which attaches to all matching reactors), so N active partitions across W worker reactors produce
    // N*(W-1) redundant timerfd-read wakeups per tick (only the reactor whose timerfd read returns a
    // nonzero count proceeds) and an O(N) iodev-registration cost whenever a new worker reactor starts
    // (IOInterface::on_reactor_start's loop over every registered iodev, generic_interface.cpp). This is
    // noise at the partition counts CRAFT runs at today; revisit (e.g. one shared dedicated reactor for
    // all CraftReplDev watchdogs process-wide) if that ever changes.
    std::lock_guard lk{watchdog_arm_mu_};
    if (watchdog_token_.active()) return;
    watchdog_token_ = iomgr::schedule_recurring(
        std::chrono::nanoseconds{watchdog_timeout_ns_ / 2}, iomgr::reactor_regex::all_worker,
        [this]() { on_watchdog_tick(); }, /* wait_to_schedule = */ true);
}

// Runs every watchdog_timeout_ns_/2 once armed (iomgr::timer_token's recurring timer, not a one-shot
// that reschedules itself) -- checks whether last_contact_ns_ is stale (elapsed >= the FULL
// watchdog_timeout_ns_, not the tick interval) and, if so, proposes a SyncRSCommitLSN entry via
// append(), fire-and-forget (detail::detach -- this runs in a plain timer-callback context, not a
// coroutine caller awaiting a result). If the client stays silent this fires on every tick once past
// the threshold, not just once, matching the old design's intent ("a permanently-silent client keeps
// getting append() attempts, not just one").
//
// A recurring timer (vs the old one-shot-that-reschedules-itself) needs none of the generation
// counter / in-flight counter / shutting-down flag the previous design required: iomgr's recurring
// timer is backed by a real timerfd IODevice (iomgr_timer.cpp's timer_epoll::cancel dispatches a
// recurring handle to remove_io_device, NOT the one-shot heap-erase path that caused a reproduced
// SEGFAULT here earlier), and IOInterface::remove_io_device(wait=true) posts the removal to the same
// reactor thread that runs this callback and blocks until it completes -- verified against iomgr's
// actual epoll reactor source (reactor_epoll.cpp's listen()/remove_iodev_impl and their shared
// m_removed_iodevs set, which discards any already-queued event for an iodev removed within the same
// epoll batch) rather than assumed from "a reactor only does one thing at a time" reasoning alone. That
// is exactly the mutual-exclusion guarantee the destructor's watchdog_token_.cancel(true) call relies
// on -- see its own doc comment for the same detail.
//
// Trade-off, honestly noted: checking staleness once per tick (rather than the old design's precise
// "fires exactly timeout_ns after the last reset") means worst-case detection latency is bounded by
// timeout_ns + one tick interval -- with ticks at timeout_ns/2, that is ~1.5x timeout_ns in the worst
// case (activity right after a tick, then silence), not the full 2x a same-cadence tick/threshold would
// give. Not perfectly precise (that would need re-arming a one-shot on every reset, reintroducing the
// exact hazard this redesign removed), but bounded and cheap to keep tight by adjusting the tick
// divisor, not the threshold itself.
void CraftReplDev::on_watchdog_tick() {
    auto const now_ns = std::chrono::steady_clock::now().time_since_epoch().count();
    auto const last_ns = last_contact_ns_.load(std::memory_order_relaxed);
    if (now_ns - last_ns < static_cast< int64_t >(watchdog_timeout_ns_)) return; // recent activity -- not stale yet

#ifdef _PRERELEASE
    ++watchdog_fire_count_;
#endif
    int64_t last_append_lsn;
    uint64_t client_token;
    {
        std::lock_guard lk{missing_mu_};
        last_append_lsn = state_.last_append_lsn;
        client_token = state_.client_token;
    }
    detail::detach(append(last_append_lsn, client_token));
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
// The missing_mu_ lock is held only for the up-front classification pass below, dropped before any
// co_await read_slot() call to avoid holding a mutex across a suspension point. Callers are
// serialised by the login sequence (no concurrent writes while fetch_data runs), so the snapshot
// taken under the lock is stable for the whole batch.
//
// A read_slot() I/O error aborts the batch immediately (fail-fast); the partial result is discarded.

async_result< std::vector< JournalSlot > > CraftReplDev::fetch_data(std::vector< int64_t > lsns) {
    enum class SlotKind { Empty, Present, Absent };

    std::vector< SlotKind > kinds;
    kinds.reserve(lsns.size());
    {
        std::lock_guard lk{missing_mu_};
        for (int64_t lsn : lsns) {
            if (empty_lsns_.contains(lsn)) {
                kinds.push_back(SlotKind::Empty);
            } else if (lsn >= 0 && lsn <= state_.last_append_lsn && !missing_lsns_.contains(lsn)) {
                kinds.push_back(SlotKind::Present);
            } else {
                kinds.push_back(SlotKind::Absent);
            }
        }
    }

    std::vector< JournalSlot > result;
    result.reserve(lsns.size());

    for (size_t i = 0; i < lsns.size(); ++i) {
        const int64_t lsn = lsns[i];
        switch (kinds[i]) {
        case SlotKind::Empty:
            result.push_back(JournalSlot{.lsn = lsn, .is_empty = true});
            break;
        case SlotKind::Present: {
            auto slot_r = co_await journal_->read_slot(lsn);
            if (!slot_r) co_return std::unexpected(slot_r.error());
            slot_r->lsn = lsn;
            result.push_back(std::move(*slot_r));
            break;
        }
        case SlotKind::Absent:
            break; // omit from result (not-present-here)
        }
    }

    co_return result;
}

// ─── overlay rebuild on restart (S3) ─────────────────────────────────────────
//
// A fresh CraftReplDev's overlay_ starts empty -- nothing else repopulates the appended-but-not-
// yet-committed entries after a restart. Walks (commit_lsn, last_append_lsn], skipping missing/
// Empty-verdicted lsns (nothing to read for those), and applies the exact same highest-dLSN-wins
// rule write()'s own post-flight update uses via the shared populate_overlay() helper. Missing lsns
// are skipped, not a stop condition (unlike commit_impl(), which must stall at the first gap since
// it advances a CONTIGUOUS prefix): a hole only means that ONE lsn's entry isn't locally present;
// later lsns in the same range can still hold real, out-of-order-appended entries that must not be
// skipped too.

async_status CraftReplDev::rebuild_overlay() {
    int64_t commit_lsn, last_append_lsn;
    {
        std::lock_guard lk{missing_mu_};
        commit_lsn = state_.commit_lsn;
        last_append_lsn = state_.last_append_lsn;
    }

    for (int64_t lsn = commit_lsn + 1; lsn <= last_append_lsn; ++lsn) {
        bool is_missing, is_empty;
        {
            std::lock_guard lk{missing_mu_};
            is_missing = missing_lsns_.contains(lsn);
            is_empty = empty_lsns_.contains(lsn);
        }
        if (is_missing || is_empty) continue;

        auto slot_r = co_await journal_->read_slot(lsn);
        if (!slot_r) co_return std::unexpected(slot_r.error());
        auto& slot = *slot_r;

        lba_t const start_lba = static_cast< lba_t >(slot.lba_off_bytes) / lba_size_;
        uint32_t const nlbas = static_cast< uint32_t >(slot.len_bytes / lba_size_);
        // Matching commit_impl()'s identical guard: nlbas==0 produces start_lba + 0 - 1 underflow
        // inside populate_overlay()'s any end_lba computation. A restart path should skip and continue
        // (rather than abort the entire rebuild) since the replayed journal is not being mutated.
        if (nlbas == 0) {
            LOGE("rebuild_overlay: slot lsn={} has len_bytes={} (nlbas=0) -- malformed record, skipping", lsn,
                 slot.len_bytes);
            continue;
        }
        populate_overlay(lsn, start_lba, nlbas, slot.all_zeros, slot.blkid, slot.csums);
    }

    co_return ok();
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
        auto empty_slots = parse_empty_slots(key);
        if (!empty_slots) {
            LOGE("on_commit lsn={} SyncRSCommitLSN malformed empty_slots", lsn);
            return;
        }
        // apply_sync_rs_commit_lsn co_awaits peer fetch + journal writes; on_commit itself is a synchronous
        // HomeStore callback, so fire-and-forget it.
        //
        // Lifetime: on_commit itself only touches the raw `owner_` pointer, which is safe since HomeStore
        // never calls on_commit on a dead device. The DETACHED coroutine this dispatches into is a separate
        // concern -- apply_sync_rs_commit_lsn opens with `auto self = shared_from_this()`, so the coroutine
        // frame holds a strong reference across every co_await, keeping CraftReplDev alive even if every
        // external owner (e.g. a volume-removal path) drops its shared_ptr mid-apply. Requires every
        // CraftReplDev to be owned via shared_ptr
        //
        // FIXME: KNOWN GAP (not yet fixed): detaching here also breaks strict RAFT apply ordering.
        // on_commit returns to HomeStore as soon as this coroutine hits its first co_await, so
        // HomeStore can call on_commit for the NEXT committed entry -- a synchronous InternalLogin, or
        // another detached SyncRSCommitLSN -- before this one's effects are fully applied.
        // No individual field access races (missing_mu_ still guards every access), but replicas can end up
        // applying entries in different effective orders depending on async completion timing, which
        // violates the determinism RAFT relies on for replicas to converge. See the commit_lsn advance at
        // the tail of apply_sync_rs_commit_lsn and the client_token overwrite in apply_internal_login for
        // the two mutation points this exposes. Real fix: one per-device serialized apply queue that both
        // entry types funnel through, processing one entry's full effect (including all its co_awaits)
        // before starting the next -- not independent detached tasks.
        detail::detach(
            owner_->apply_sync_rs_commit_lsn(payload->rs_commit_lsn, payload->client_token, std::move(*empty_slots)));
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

// Fired by HomeStore wherever this partition's repl_dev is recovered on restart. Fire-and-forget,
// same as on_watchdog_tick() -- this runs in a plain callback context, not a coroutine caller
// awaiting a result. Sets recovering_ BEFORE detaching the recovery coroutine so no write()/read()/
// keep_alive() that arrives after on_restart() fires but before rebuild_overlay() completes can ever
// observe a partially-rebuilt overlay (write()/read()/keep_alive() all reject with OFFLINE while
// recovering_ is set -- per SDSTOR-22905, the overlay must be fully rebuilt before the volume accepts
// new client I/O). run_recovery() clears the flag once rebuild_overlay() actually succeeds -- see its
// own doc comment for what happens on failure instead.
//
// Reachable more than once, in principle, if HomeStore's repl_dev_listener contract ever allows
// on_restart() to fire again on the same instance -- there is no code-level guard against that here;
// correctness for that case currently rests entirely on HomeStore's calling convention (observed today:
// fired at most once per instance per restart cycle), not on anything this class enforces itself.
//
// Currently a no-op in practice: CraftPartitionState itself (state_) has no superblock-recovery path
// yet, so commit_lsn/last_append_lsn are still at their default -1/-1 when this fires, making
// rebuild_overlay()'s walk range empty and run_recovery() clear recovering_ almost immediately. This
// gate is still the correct wiring for once real superblock recovery populates state_ before this
// fires.
void CraftReplDev::CraftRaftListener::on_restart() {
    owner_->recovering_.store(true, std::memory_order_release);
    detail::detach(owner_->run_recovery());
}

// Awaits rebuild_overlay() to completion. On success, clears recovering_ -- the volume resumes
// accepting client I/O against a now-complete overlay. On FAILURE (e.g. a corrupt journal record),
// does the opposite of what a naive "always clear, log and move on" version would: leaves recovering_
// set and additionally sets recovery_faulted_ (checked first, ahead of recovering_, by every client-
// facing entry point), so the partition permanently rejects I/O with INTERNAL_ERROR instead of quietly
// resuming against an overlay that stopped partway through -- entries at and above the failing LSN
// were never populated, so any subsequent read for an LBA only covered by one of those entries would
// otherwise silently fall through to stale, already-superseded index/committed state with no error at
// all. A stuck-rejecting partition is a visible, actionable failure; a partition silently serving stale
// data is not. There is deliberately no self-healing path out of this state here -- an operator
// decision (or a future re-sync-from-peer mechanism) is required.
async_status CraftReplDev::run_recovery() {
    auto r = co_await rebuild_overlay();
    if (!r) {
        LOGE("rebuild_overlay failed during restart recovery: {} -- partition permanently faulted, will "
             "not accept client I/O (see recovery_faulted_'s doc comment)",
             r.error().message());
        recovery_faulted_.store(true, std::memory_order_release);
        co_return r;
    }
    recovering_.store(false, std::memory_order_release);
    co_return r;
}

// ─── RAFT apply helpers (S5 implements) ──────────────────────────────────────
//
// apply_sync_rs_commit_lsn (22886): empty_slots is range-checked against rs_commit_lsn first -- the only
// all-or-nothing gate on this apply. SyncRSCommitLSN verdicts are only ever defined for slots the leader
// pre-resolved up to rs_commit_lsn (S5). client_token is NOT checked against the current session. Past the
// range check, every step is best-effort
// forward progress: empty_slots are reconciled and the newly-spanned range is marked missing, catch-up
// attempts to fill in what it can from a peer, and last_append_lsn advances regardless of whether catch-up
// fully succeeded -- mirroring truncate()'s invariant that apply never reverts the watermark, only advances
// it. commit_lsn is different: it's the local contiguous prefix (CRAFT-Design), so it only advances up to
// the first still-unresolved Missing slot, skipping over Empty ones, even though rs_commit_lsn itself is a
// watermark the whole replica set already agreed on. A peer's fetch_data response gets its own
// all-or-nothing check (validate_fetch_response): unlike the range check above, this one can't gate the
// whole apply (gap marking and last_append_lsn already advanced by the time the response arrives), so a
// malformed response is instead treated exactly like a failed fetch

async_status CraftReplDev::apply_sync_rs_commit_lsn(int64_t rs_commit_lsn, uint64_t client_token,
                                                    std::vector< int64_t > empty_slots) {
    // Lives in the coroutine frame across every co_await below -- see the lifetime comment at the
    // on_commit call site (detail::detach) for why this is required.
    auto self = shared_from_this();

    // Validated before any state is touched -- an out-of-range verdict means the entry itself cannot be
    // trusted, not that this one slot should be skipped, so it gates the entire apply.
    for (int64_t lsn : empty_slots) {
        if (lsn < 0 || lsn > rs_commit_lsn) {
            LOGE("apply_sync_rs_commit_lsn: empty_slots lsn={} out of range [0, {}] -- rejecting entire apply", lsn,
                 rs_commit_lsn);
            co_return std::unexpected(make_error_condition(volume_error::INVALID_ENTRY));
        }
    }

    std::vector< int64_t > to_free;
    std::vector< int64_t > to_fetch;
    uint64_t term;
    {
        std::lock_guard lk{missing_mu_};
        // client_token is NOT gated against state_.client_token here. Per the login sequence (CRAFT-Design),
        // SyncRSCommitLSN applies BEFORE InternalLogin (which sets state_.client_token), so an equality-fence
        // here would veto the very entry that carries login's own Empty verdicts,
        // and would also veto every post-restart watchdog SyncRSCommitLSN, since state_ is
        // in-memory-only and client_token resets to 0 across a restart. craft_client's reference
        // (MemCraftReplica::cold_apply_sync) discards the parameter outright for the same reason.
        // Exclusivity comes from RAFT's commit ordering plus the term fence every other IO already
        // checks (see apply_internal_login's header comment), not from an equality check here.
        term = state_.term;

        for (int64_t lsn : empty_slots) {
            if (missing_lsns_.erase(lsn)) { to_free.push_back(lsn); }
        }
        empty_lsns_.insert(empty_slots.begin(), empty_slots.end());

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

    if (!to_free.empty()) {
        for (int64_t lsn : to_free) {
            if (auto fr = co_await journal_->free_slot(lsn); !fr) {
                LOGE("apply_sync_rs_commit_lsn: free_slot failed lsn={}: {} -- blocks may leak", lsn,
                     fr.error().message());
            }
        }
    }

    if (!to_fetch.empty()) {
        if (peer_fetcher_ == nullptr) {
            LOGW("apply_sync_rs_commit_lsn: {} lsn(s) missing but no peer_fetcher_ wired -- leaving as missing",
                 to_fetch.size());
        } else if (auto fetched = co_await peer_fetcher_->fetch_data(to_fetch, peer_fetch_timeout_ms_); !fetched) {
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
                bool blkid_allocated = false;
                if (!slot.all_zeros) {
                    auto alloc_res = co_await journal_->alloc_write_data(slot.data, slot.len_bytes);
                    if (!alloc_res) {
                        LOGE("apply_sync_rs_commit_lsn: alloc_write_data failed lsn={}: {} -- leaving as missing",
                             slot.lsn, alloc_res.error().message());
                        continue;
                    }
                    blkid = *alloc_res;
                    blkid_allocated = true;
                }

                // FIXME: We need to address the case when blkid is not set. How would write_slot handle that?
                auto res = co_await journal_->write_slot(slot.lsn, term, slot.lba_off_bytes, slot.len_bytes, blkid,
                                                         slot.all_zeros, slot.csums);
                if (!res) {
                    LOGE("apply_sync_rs_commit_lsn: write_slot failed lsn={}: {} -- leaving as missing", slot.lsn,
                         res.error().message());
                    if (blkid_allocated) {
                        detail::detach([self, blkid, lsn = slot.lsn]() -> async_status {
                            if (auto fr = co_await self->journal_->free_data(blkid); !fr)
                                LOGE("apply_sync_rs_commit_lsn: free_data failed after write_slot failure lsn={}: {}",
                                     lsn, fr.error().message());
                            co_return ok();
                        }());
                    }
                    continue;
                }
                std::lock_guard lk{missing_mu_};
                missing_lsns_.erase(slot.lsn);
            }
        }
    }

    // commit_lsn (CRAFT-Design) is the LOCAL CONTIGUOUS prefix, distinct from rs_commit_lsn (the
    // replica-set-wide watermark RAFT already agreed on): it must skip over Empty slots but never
    // advance past an unresolved Missing one, even if catch-up above left holes below rs_commit_lsn.
    // Mirrors craft_client's reference MemCraftReplica::apply_up_to.
    //
    // KNOWN GAP: this can land late. Because on_commit detaches this coroutine (see the FIXME there),
    // a later-committed entry (InternalLogin, or another SyncRSCommitLSN) may have already applied by
    // the time this advance actually runs, breaking strict RAFT apply ordering.
    {
        std::lock_guard lk{missing_mu_};
        int64_t next = state_.commit_lsn + 1;
        while (next <= rs_commit_lsn && !missing_lsns_.contains(next)) {
            state_.commit_lsn = next; // resolved (present or Empty) -- Empty is skipped, not gated on
            ++next;
        }
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
