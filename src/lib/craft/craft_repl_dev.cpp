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
#include <homestore/checkpoint/cp_mgr.hpp>  // cp_mgr(), CPManager::trigger_cp_flush()
#include <homestore/logstore/log_store.hpp> // home_log_store, logstore_seq_num_t, log_write_comp_cb_t
#include <iomgr/iomgr.hpp>                  // iomanager singleton, reactor_regex
#include <iomgr/timer.hpp>                  // iomgr::schedule_recurring -- the watchdog's RAII timer
#include <sisl/async/value_awaitable.hpp>   // value_awaitable<T>: lock-free completion-before-suspend-safe bridge

#include <optional>
#include <unordered_map>
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

// Counts one round's quorum replies by sender: only voters other than this member count, each once. Fails closed if
// the voter list is empty or excludes this member. Replies a correct transport never sends are logged under `round`.
class VoterTally {
public:
    // drop: this member's own reply. use: evidence but not a vote (a learner's, an untagged one, one outside
    // `eligible`, or a repeat, which merges with the member's first reply since "absent" is only a snapshot).
    // count: a distinct voter.
    enum class Reply { drop, use, count };

    // eligible, if set, narrows the votes further: a voter outside it is evidence, not a vote (login counts only the
    // members its poll quiesced).
    VoterTally(char const* round, craft::peer_id_t self, std::vector< craft::peer_id_t > voters,
               std::optional< std::vector< craft::peer_id_t > > eligible = std::nullopt) :
            round_{round}, self_{self}, voters_{std::move(voters)}, eligible_{std::move(eligible)} {
        if (!is_voter(self_))
            LOGW("{}: this member ({}) isn't among the {} voter(s) -- failing closed", round_,
                 boost::uuids::to_string(self_), voters_.size());
    }

    Reply add(craft::peer_id_t const& member) {
        if (member == self_) {
            LOGW("{}: dropping a reply from this member itself", round_);
            return Reply::drop;
        }
        if (member.is_nil()) {
            LOGW("{}: a reply carries no member id -- used as evidence, not counted", round_);
            return Reply::use;
        }
        if (!seen_.insert(member).second) {
            LOGW("{}: repeat reply from {} -- merged, not counted", round_, boost::uuids::to_string(member));
            return Reply::use;
        }
        if (!is_voter(member)) return Reply::use; // a learner
        if (eligible_ && std::ranges::find(*eligible_, member) == eligible_->end()) {
            LOGI("{}: {} isn't eligible to vote here (the login poll didn't quiesce it) -- used as evidence only",
                 round_, boost::uuids::to_string(member));
            return Reply::use;
        }
        ++counted_;
        return Reply::count;
    }

    // This member plus the counted voters are a majority of the voters.
    bool has_quorum() const { return is_voter(self_) && 1 + counted_ >= quorum_size(); }
    size_t counted() const { return counted_; }
    size_t quorum_size() const { return voters_.size() / 2 + 1; }

private:
    bool is_voter(craft::peer_id_t const& m) const {
        return std::find(voters_.begin(), voters_.end(), m) != voters_.end();
    }

    char const* round_;
    craft::peer_id_t self_;
    std::vector< craft::peer_id_t > voters_;
    std::optional< std::vector< craft::peer_id_t > > eligible_;
    std::set< craft::peer_id_t > seen_;
    size_t counted_{0};
};

// Logs a proposal of `what` that didn't apply. A timeout's outcome is unknown, and losing leadership or the session
// (an InternalLogin that lost its term included) is part of normal operation; anything else is an error.
void log_unapplied_proposal(std::string const& what, std::error_condition const& e) {
    if (e == std::errc::timed_out) {
        LOGW("{} proposal timed out -- outcome unknown, it may still commit", what);
    } else if (e == craft::make_error_condition(craft::craft_error::NOT_LEADER) ||
               e == craft::make_error_condition(craft::craft_error::STALE_TERM)) {
        LOGW("{} not applied: {}", what, e.message());
    } else {
        LOGE("{} proposal failed: {}", what, e.message());
    }
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

    status commit_data(homestore::multi_blk_id const& blkid, bool recovering) override {
        if (!blkid.is_valid()) return ok();
        if (!recovering) return commit_blks(blkid, false);
        // Only the blocks the allocator lost with the restart, those no checkpoint persisted (written after the last
        // one before a crash): it has held the rest since the load, and once HomeStore has started it refuses to
        // persist them again. Nothing else allocates in this volume's chunks before its recovery is done. recommit: the
        // recovery may run after HomeStore's start, when commit_blk() no longer reserves in memory.
        auto pieces = blkid.iterate();
        while (auto const piece = pieces.next()) {
            if (homestore::data_service().is_blk_alloced(*piece)) continue;
            if (auto r = commit_blks(homestore::multi_blk_id{*piece}, true); !r) return r;
        }
        return ok();
    }

    static status commit_blks(homestore::multi_blk_id const& blkid, bool recommit) {
        if (auto r = homestore::data_service().commit_blk(blkid, recommit); !r) {
            LOGE("commit_blk {} failed: {}", blkid.to_string(), r.error().message());
            return std::unexpected(r.error());
        }
        return ok();
    }

    // Serialize the journal entry (header + csum array + blkid) and write it to the log store.
    async_status write_slot(int64_t lsn, uint64_t term, lba_t lba, lba_count_t len, homestore::multi_blk_id blkid,
                            bool all_zeros, std::vector< homestore::csum_t > const& csums) override {
        CraftJournalEntry hdr{
            k_journal_magic, k_journal_version, term, lsn, lba, len, static_cast< uint8_t >(all_zeros)};
        uint32_t nlbas = static_cast< uint32_t >(csums.size());
        uint32_t blkid_sz = blkid.serialized_size();
        auto const blob = std::make_shared< sisl::io_blob_safe >(slot_blob_size(nlbas, blkid_sz));
        std::memcpy(blob->bytes(), &hdr, sizeof(CraftJournalEntry));
        if (nlbas > 0) {
            std::memcpy(blob->bytes() + k_csum_array_offset, csums.data(), nlbas * sizeof(homestore::csum_t));
        }
        sisl::blob blkid_blob = blkid.serialize(); // non-owning view — copy before blkid goes out of scope
        // Internal HomeStore-API contract, not client-reachable: serialize() must return exactly
        // serialized_size() bytes, or the memcpy below overreads blkid_blob's owned buffer.
        DEBUG_ASSERT_EQ(blkid_blob.size(), blkid_sz, "multi_blk_id::serialize() size mismatch");
        std::memcpy(blob->bytes() + blkid_offset(nlbas), blkid_blob.cbytes(), blkid_sz);
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
        //   • blob lifetime: write_async keeps only a view of blob, until HomeStore flushes it -- which may be
        //     after this write timed out and returned (below). The completion callback owns blob, so it lives
        //     until then; a write HomeStore never completes leaks it, with the partition it faulted.
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
        //     on_flush_completion (log_dev.cpp:531-539) -- no return-value signal. Needs a HomeStore fix
        //     propagating the error into the completion path itself (SDSTOR-24993); until then the wait
        //     has a deadline (craft_journal_write_timeout_ms) and fails timed_out past it.
        auto w = std::make_shared< JournalWriteDeadlines::Wait >();
        auto write_ret = logstore_->write_async(
            static_cast< homestore::logstore_seq_num_t >(lsn), *blob, nullptr,
            [w, blob](homestore::logstore_seq_num_t, sisl::io_blob&, homestore::logdev_key, void*) mutable {
                iomanager.run_on_forget(iomgr::reactor_regex::least_busy_io,
                                        [w = std::move(w)]() mutable { w->complete(true); });
            });
        if (write_ret < 0) {
            LOGE("write_async rejected lsn={}: log store or logdev is stopping; callback will not fire", lsn);
            co_return std::unexpected(make_error_condition(std::errc::operation_not_supported));
        }
        // Never without one: a write HomeStore never completes would hold what awaits it, and a shutdown, forever.
        auto ms = HB_DYNAMIC_CONFIG(craft_journal_write_timeout_ms);
        if (ms == 0) ms = homeblkscfg::HomeBlksSettingsT{}.craft_journal_write_timeout_ms;
        deadlines_.add(w, JournalWriteDeadlines::clock::now() + std::chrono::milliseconds{ms});
        if (!co_await w->done) {
            LOGE("journal write lsn={} not completed within its deadline -- it may still land", lsn);
            co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
        }
        co_return ok();
    }

    void expire_overdue() override { deadlines_.sweep(JournalWriteDeadlines::clock::now()); }

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
        if (hdr.magic != k_journal_magic || hdr.version != k_journal_version) {
            LOGE("free_slot: corrupt or foreign entry lsn={} magic={:#x} version={} -- refusing to free", lsn,
                 hdr.magic, hdr.version);
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        if (hdr.lsn != lsn) {
            LOGE("free_slot: lsn mismatch requested={} stored={} -- refusing to free", lsn, hdr.lsn);
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        if (hdr.all_zeros) co_return ok();

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

    // The log store's highest seq_num not rolled back: -1 for a store never written to (a new store starts at seq_num
    // 0). A reopened store has its tail once its log replay is done, which is before the device is attached to it.
    async_result< int64_t > tail_lsn() override { co_return static_cast< int64_t >(logstore_->tail_lsn()); }

    // home_log_store::truncate() drops every entry <= upto at once (reading one fails from then on). The log device
    // releases their space at its next device truncation, which HomeStore's resource manager runs periodically; a
    // crash before that brings them back, and the restart reports the older reclaim point, so the device reclaims
    // again. Not past the last entry: truncating beyond the tail would move it (HomeStore's baseline-resync path).
    async_status reclaim_upto(int64_t upto) override {
        upto = std::min< int64_t >(upto, logstore_->tail_lsn());
        if (upto <= static_cast< int64_t >(logstore_->truncated_upto())) co_return ok();
        if (!logstore_->truncate(static_cast< homestore::logstore_seq_num_t >(upto)))
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return ok();
    }
    int64_t reclaimed_upto() const override { return static_cast< int64_t >(logstore_->truncated_upto()); }

private:
    shared< homestore::home_log_store > logstore_;
    uint64_t vol_ordinal_;
    uint32_t lba_size_;
    JournalWriteDeadlines deadlines_;
};

unique< CraftJournalBackend > make_homestore_journal_backend(shared< homestore::home_log_store > logstore,
                                                             uint64_t vol_ordinal, uint32_t lba_size) {
    return std::make_unique< HomeStoreCraftJournalBackend >(std::move(logstore), vol_ordinal, lba_size);
}

// Thin wrapper over homestore::cp_mgr(). One instance is shared by every volume's CraftReplDev.
checkpoint_trigger_fn_t make_homestore_checkpoint_trigger_fn() {
    return [](bool force) -> async_result< bool > {
        // None while HomeStore starts: its data service registers for checkpoints only after the log replay, and a
        // partition reattached once its journal replays may already apply slots by then. Such a checkpoint wouldn't
        // persist their blocks' allocations, yet the durable commit frontier it advanced would keep a restart from
        // committing them again. As if coalesced: a later trigger moves the frontier.
        if (homestore::hs()->is_initializing()) co_return false;
        // true: this call switched over a new checkpoint and its flush completed.
        if (co_await homestore::cp_mgr().trigger_cp_flush(force)) co_return true;
        // cp_mgr().trigger_cp_flush() returns false, synchronously, when a flush is already in
        // progress (cp_mgr.cpp: m_in_flush_phase). That's expected and harmless when force=false
        // (fire_checkpoint_trigger coalesces with any in-flight flush on purpose). Only a force=true
        // false return is a real failure worth surfacing.
        if (!force) co_return false;
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    };
}

// ─── HomeStore RAFT proposer ──────────────────────────────────────────────────

namespace {
// One in-flight proposal: owns the entry bytes until HomeStore is done with them, and carries the
// completion CraftRaftListener's on_commit / on_error fulfil. Same bridge as volume.hpp's repl_result_ctx.
struct craft_propose_ctx : public homestore::repl_req_ctx {
    std::vector< uint8_t > header;
    std::vector< uint8_t > key;
    sisl::async::value_awaitable< status > promise_;
    std::atomic< bool > completed_{false}; // value_awaitable must complete exactly once
};

// No-op unless ctx is one of ours -- followers and log replay see a null ctx. First outcome wins, should
// HomeStore ever report more than one (e.g. on_rollback then on_error).
void complete_proposal(cintrusive< homestore::repl_req_ctx >& ctx, status s) {
    auto* p = dynamic_cast< craft_propose_ctx* >(ctx.get());
    if (p == nullptr || p->completed_.exchange(true, std::memory_order_acq_rel)) return;
    p->promise_.complete(std::move(s));
}

// The proposal missed its deadline (craft_raft_propose_timeout_ms). The entry may still commit, so the caller learns
// "outcome unknown", not a rejection; a later commit is still applied, but its completion is dropped.
void expire_proposal(cintrusive< homestore::repl_req_ctx >& ctx) {
    complete_proposal(ctx, std::unexpected(std::make_error_condition(std::errc::timed_out)));
}

// True once ctx has an outcome, or if it isn't one of ours (nothing to expire).
bool proposal_completed(cintrusive< homestore::repl_req_ctx >& ctx) {
    auto* p = dynamic_cast< craft_propose_ctx* >(ctx.get());
    return p == nullptr || p->completed_.load(std::memory_order_acquire);
}
} // namespace

void JournalWriteDeadlines::add(shared< Wait > w, clock::time_point deadline) {
    std::lock_guard lk{mu_};
    while (!pending_.empty() && pending_.front().first->completed.load(std::memory_order_acquire))
        pending_.pop_front();
    pending_.emplace_back(std::move(w), deadline);
}

void JournalWriteDeadlines::sweep(clock::time_point now) {
    std::vector< shared< Wait > > expired;
    {
        std::lock_guard lk{mu_};
        while (!pending_.empty()) {
            auto const& [w, deadline] = pending_.front();
            if (!w->completed.load(std::memory_order_acquire)) {
                if (deadline > now) break; // the rest are due later still
                expired.push_back(w);
            }
            pending_.pop_front();
        }
    }
    // Outside the lock: failing a wait resumes its writer.
    for (auto const& w : expired)
        w->complete(false);
}

size_t JournalWriteDeadlines::pending() const {
    std::lock_guard lk{mu_};
    return pending_.size();
}

bool ProposalDeadlines::add(intrusive< homestore::repl_req_ctx > ctx, clock::time_point deadline) {
    std::lock_guard lk{mu_};
    if (closed_) return false;
    pending_.emplace_back(std::move(ctx), deadline);
    return true;
}

std::vector< intrusive< homestore::repl_req_ctx > > ProposalDeadlines::close() {
    std::vector< intrusive< homestore::repl_req_ctx > > all;
    std::lock_guard lk{mu_};
    closed_ = true;
    for (auto& entry : pending_)
        all.push_back(std::move(entry.first));
    pending_.clear();
    return all;
}

void ProposalDeadlines::sweep(clock::time_point now) {
    std::vector< intrusive< homestore::repl_req_ctx > > expired;
    {
        std::lock_guard lk{mu_};
        std::erase_if(pending_, [&](auto const& entry) {
            if (proposal_completed(entry.first)) return true;
            if (entry.second > now) return false;
            expired.push_back(entry.first);
            return true;
        });
    }
    // Outside the lock: completing resumes the proposer's caller, which may propose again.
    for (auto const& ctx : expired)
        expire_proposal(ctx);
}

size_t ProposalDeadlines::pending() const {
    std::lock_guard lk{mu_};
    return pending_.size();
}

namespace {
class HomeStoreCraftRaftProposer : public CraftRaftProposer {
public:
    HomeStoreCraftRaftProposer(shared< homestore::repl_dev > rd, craft::peer_id_t replica_id) :
            rd_{std::move(rd)}, my_id_{replica_id} {}

    // The group's stage before its RAFT server, which the reaper frees a while after the destroy commits.
    // ponytail: the check and the access aren't atomic; a guard needs HomeStore's accessors to count themselves in the
    // drain its teardown waits for (protected today).
    bool is_leader() const override { return !is_going_away(*rd_) && rd_->is_leader(); }
    craft::peer_id_t my_id() const override { return my_id_; }
    // get_replication_quorum() lists every config member, learners included; get_replication_status() (complete
    // only on the leader) says which of them can vote.
    std::vector< craft::peer_id_t > voters() const override {
        if (is_going_away(*rd_)) return {};
        auto ids = rd_->get_replication_quorum();
        for (auto const& p : rd_->get_replication_status())
            if (!p.can_vote) std::erase(ids, p.id_);
        return ids;
    }

    // Once the entry is in the log, only commit or rollback completes it (gc_repl_reqs() skips the proposer), so a
    // leader without quorum would wait out the outage: expire_overdue() completes it with timed_out instead, and the
    // caller then resumes inside it.
    async_status propose(sisl::blob header, sisl::blob key) override {
        auto ctx = intrusive< craft_propose_ctx >{new craft_propose_ctx()};
        ctx->header.assign(header.cbytes(), header.cbytes() + header.size());
        ctx->key.assign(key.cbytes(), key.cbytes() + key.size());
        // Tracked even without a deadline, for stop() to complete it.
        auto const ms = HB_DYNAMIC_CONFIG(craft_raft_propose_timeout_ms);
        auto const deadline = ms > 0 ? ProposalDeadlines::clock::now() + std::chrono::milliseconds{ms}
                                     : ProposalDeadlines::clock::time_point::max();
        if (!deadlines_.add(ctx, deadline))
            co_return std::unexpected(std::make_error_condition(std::errc::operation_canceled));
        rd_->async_alloc_write(sisl::blob{ctx->header.data(), static_cast< uint32_t >(ctx->header.size())},
                               sisl::blob{ctx->key.data(), static_cast< uint32_t >(ctx->key.size())},
                               sisl::sg_list{} /* key-only entry */, ctx);
        co_return co_await ctx->promise_;
    }

    void expire_overdue() override { deadlines_.sweep(ProposalDeadlines::clock::now()); }

    void stop() override {
        for (auto const& ctx : deadlines_.close())
            expire_proposal(ctx);
    }

private:
    shared< homestore::repl_dev > rd_;
    craft::peer_id_t my_id_;
    ProposalDeadlines deadlines_;
};
} // namespace

unique< CraftRaftProposer > make_homestore_raft_proposer(shared< homestore::repl_dev > rd,
                                                         craft::peer_id_t replica_id) {
    return std::make_unique< HomeStoreCraftRaftProposer >(std::move(rd), replica_id);
}

#ifdef _PRERELEASE
intrusive< homestore::repl_req_ctx > make_test_proposal_ctx() {
    return intrusive< craft_propose_ctx >{new craft_propose_ctx()};
}

std::optional< status > take_test_proposal_outcome(intrusive< homestore::repl_req_ctx > const& ctx) {
    auto* p = dynamic_cast< craft_propose_ctx* >(ctx.get());
    if (p == nullptr || !p->promise_.await_ready()) return std::nullopt;
    return p->promise_.await_resume();
}
#endif

// ─── constructor ──────────────────────────────────────────────────────────────

CraftReplDev::CraftReplDev(volume_id_t vol_id, unique< CraftJournalBackend > journal, uint32_t lba_size,
                           shared< VolumeIndexTable > indx_tbl, unique< CraftStateStore > state_store,
                           unique< CraftRaftProposer > raft_proposer) :
        vol_id_{vol_id},
        journal_{std::move(journal)},
        lba_size_{lba_size},
        indx_tbl_{std::move(indx_tbl)},
        raft_listener_{this},
        owned_raft_proposer_{std::move(raft_proposer)},
        raft_proposer_{owned_raft_proposer_.get()},
        sync_lsn_interval_{static_cast< int64_t >(HB_DYNAMIC_CONFIG(sync_rs_commit_lsn_interval))},
        watchdog_timeout_ns_{HB_DYNAMIC_CONFIG(craft_watchdog_timeout_ms) * 1'000'000ULL},
        state_store_{std::move(state_store)} {}

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
// Snapshot the in-memory partition state under state_mu_ for consistency with
// write() which updates state_ under the same lock.

async_result< RsCommitLsnReply > CraftReplDev::get_rs_commit_lsn(uint64_t term, bool is_login) {
    // Same as fetch_data(): a faulted member answers nothing, so it can't count toward anyone's quorum.
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("get_rs_commit_lsn rejected: partition permanently faulted");
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    RsCommitLsnReply reply;
    {
        std::lock_guard lk{state_mu_};
        // Fence, then read, under the one lock: a write past write()'s check has already moved last_append_lsn, and
        // every later one is turned away.
        if (is_login && !write_quiesced_locked(term)) {
            LOGI("login poll: quiescing writes at term <= {} (session term {})", term, state_.term);
            quiesce_term_ = term;
        }
        // The term counts a login committed but not yet applied: a leader that hasn't learned of that commit mustn't
        // pass a poll at the replaced session's term.
        reply = {.lsns = {state_.commit_lsn, state_.last_append_lsn}, .term = std::max(state_.term, login_fence_term_)};
    }
    co_return reply;
}

// ─── login poll (S7 / SDSTOR-22889) ───────────────────────────────────────────

async_result< CraftReplDev::LoginPoll > CraftReplDev::login_poll(uint64_t term) {
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("login_poll rejected: partition permanently faulted");
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    if (raft_proposer_ == nullptr || peer_fetcher_ == nullptr)
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    if (!raft_proposer_->is_leader())
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::NOT_LEADER));
    auto self = shared_from_this();

    // This member quiesces too: otherwise an old write acked by it and a member the poll misses could land above the
    // watermark, and the login's truncation would drop an acked write.
    auto own = co_await get_rs_commit_lsn(term, /*is_login=*/true);
    if (!own) co_return std::unexpected(own.error());
    // Already past this round's term here: the poll can only fail STALE_TERM, so don't wait for every member.
    if (own->term > term) {
        LOGW("login_poll: this member is already at term {}, past {} -- a newer login exists", own->term, term);
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
    }
    auto polled = co_await peer_fetcher_->poll_quorum_lsns(term, /*is_login=*/true, peer_fetch_timeout_ms_);
    if (!polled) {
        LOGE("login_poll: quorum poll failed: {}", polled.error().message());
        co_return std::unexpected(polled.error());
    }
    // Rechecked after the await, as any of these can change during it.
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("login_poll: partition faulted during the poll -- abandoning term={}", term);
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    if (!raft_proposer_->is_leader()) {
        LOGW("login_poll: lost leadership during the poll -- abandoning term={}", term);
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::NOT_LEADER));
    }
    LoginPoll result;
    {
        // A login committed meanwhile (say, an earlier attempt's InternalLogin whose proposal timed out) leaves this
        // round's term stale, though no member may have applied it yet.
        std::lock_guard lk{state_mu_};
        if (login_fence_term_ > term) {
            LOGW("login_poll: a login at term {} committed during the poll, past {}", login_fence_term_, term);
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
        }
        // Read after the poll, not before: a Sync committed meanwhile (an old-session round's, say) moves this
        // member's frontier, and the watermark mustn't fall below the committed prefix.
        result.rs_commit_lsn = state_.last_append_lsn;
    }
    uint64_t newest_term = own->term;
    // Only distinct voters other than this leader count toward quorum; every other reply still feeds the watermark
    // (VoterTally).
    VoterTally tally{"login_poll", raft_proposer_->my_id(), raft_proposer_->voters()};
    for (auto const& p : *polled) {
        if (tally.add(p.member) == VoterTally::Reply::drop) continue;
        newest_term = std::max(newest_term, p.reply.term);
        result.rs_commit_lsn = std::max(result.rs_commit_lsn, p.reply.lsns.last_append_lsn);
        if (!p.member.is_nil() && std::ranges::find(result.quiesced, p.member) == result.quiesced.end())
            result.quiesced.push_back(p.member);
    }
    if (newest_term > term) {
        LOGW("login_poll: a member is already at term {}, past {} -- a newer login exists", newest_term, term);
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
    }
    if (!tally.has_quorum()) {
        LOGW("login_poll: {} distinct voter(s) + self < quorum {} -- login fails", tally.counted(),
             tally.quorum_size());
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::NO_QUORUM));
    }
    LOGI("login_poll: term={} rs_commit_lsn={} from {} quiesced member(s)", term, result.rs_commit_lsn,
         result.quiesced.size());
    co_return result;
}

async_result< std::vector< int64_t > > CraftReplDev::login_resolve(int64_t rs_commit_lsn, uint64_t term,
                                                                   std::vector< craft::peer_id_t > quiesced) {
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("login_resolve rejected: partition permanently faulted");
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    auto pre = co_await pre_resolve_slots(rs_commit_lsn, term, std::move(quiesced));
    if (!pre) co_return std::unexpected(pre.error());
    // Rechecked after the await: a partition that faulted meanwhile can't vouch for these verdicts (a Sync that
    // faulted it never raised the settled floor, say).
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("login_resolve: partition faulted during pre-resolution -- abandoning rs_commit_lsn={}", rs_commit_lsn);
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    if (!pre->unresolved.empty()) {
        LOGW("login_resolve: {} slot(s) <= {} unresolved (first {}) -- login fails, nothing proposed",
             pre->unresolved.size(), rs_commit_lsn, *std::ranges::min_element(pre->unresolved));
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::NO_QUORUM));
    }
    co_return std::move(pre->empty_slots);
}

async_status CraftReplDev::login_sync(AsyncMutex::Lock const& lock, int64_t rs_commit_lsn, uint64_t client_token,
                                      std::vector< int64_t > empty_slots, uint64_t term) {
    DEBUG_ASSERT(static_cast< bool >(lock), "login_sync without holding watermark_mu_");
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("login_sync rejected: partition permanently faulted");
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    auto self = shared_from_this();
    // Proposed even if rs_commit_lsn is already covered: the entry carries the new client's token and the verdicts,
    // each member's apply re-runs its catch-up up to rs_commit_lsn, and the check below is then about this entry.
    auto const proposed = co_await propose_sync(lock, rs_commit_lsn, client_token, empty_slots, term);
    // Checked whatever the outcome: a fault during the proposal outranks its failure or timeout.
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("login_sync: partition faulted during SyncRSCommitLSN({})", rs_commit_lsn);
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    if (!proposed) co_return std::unexpected(proposed.error());

    // The entry has applied here (a proposal completes after its apply). An earlier login's timed-out InternalLogin
    // may have committed meanwhile, replacing the session without moving commit_lsn.
    std::lock_guard lk{state_mu_};
    if (!session_current_locked(term)) {
        LOGW("login_sync: term moved past the login's ({} -> {}) during SyncRSCommitLSN({})", term, state_.term,
             rs_commit_lsn);
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
    }
    // InternalLogin will truncate to rs_commit_lsn, so this member must be exactly there: past it (or past an applied
    // Sync's watermark it couldn't yet catch up to), an earlier round's timed-out Sync committed late and the watermark
    // is stale; short of it, a slot login_resolve() settled is somehow still missing. Either way the login fails and
    // the client retries.
    if (state_.commit_lsn > rs_commit_lsn || applied_sync_lsn_ > rs_commit_lsn) {
        LOGW("login_sync: commit_lsn {} / applied Sync {} past the login's watermark {} -- a late Sync; login fails",
             state_.commit_lsn, applied_sync_lsn_, rs_commit_lsn);
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::NO_QUORUM));
    }
    if (state_.commit_lsn < rs_commit_lsn) {
        LOGE("login_sync: commit_lsn {} stopped short of the login's watermark {} (first missing {}) -- login fails",
             state_.commit_lsn, rs_commit_lsn, missing_lsns_.empty() ? -1 : *missing_lsns_.begin());
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::NO_QUORUM));
    }
    co_return ok();
}

async_status CraftReplDev::login_install(AsyncMutex::Lock const& lock, int64_t rs_commit_lsn, uint64_t client_token,
                                         uint64_t term) {
    DEBUG_ASSERT(static_cast< bool >(lock), "login_install without holding watermark_mu_");
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("login_install rejected: partition permanently faulted");
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    auto self = shared_from_this();
    std::vector< uint8_t > key(sizeof(InternalLoginPayload));
    serialize_internal_login(key.data(), client_token, term + 1, rs_commit_lsn);

    // The proposal completes once the entry has applied here: ok means the new session is installed, and STALE_TERM
    // that an earlier InternalLogin for the same term applied first, so this one was ignored.
    auto const proposed = co_await propose_entry(lock, CraftEntryType::InternalLogin, key, term);
    // Checked whatever the outcome: a fault during the proposal outranks its failure or timeout.
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("login_install: partition faulted during InternalLogin(term={})", term + 1);
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    if (!proposed) {
        log_unapplied_proposal(fmt::format("InternalLogin(term={})", term + 1), proposed.error());
        co_return proposed;
    }
    // Installed, but a newer login (another leader's, after a leadership change) may have committed behind it while it
    // waited in the apply queue: that fenced the new session already.
    std::lock_guard lk{state_mu_};
    if (!session_current_locked(term + 1)) {
        LOGW("login_install: session {} was replaced as it was installed (login fence {})", term + 1,
             login_fence_term_);
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
    }
    co_return ok();
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
        std::lock_guard lk{state_mu_};
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
        std::lock_guard lk{state_mu_};
        if (state_.last_append_lsn > lsn) state_.last_append_lsn = lsn;
        missing_lsns_.erase(missing_lsns_.upper_bound(lsn), missing_lsns_.end());
        // The periodic cadence counts from the append frontier, so its baseline can't sit above the new one.
        last_periodic_sync_lsn_ = std::min(last_periodic_sync_lsn_, lsn);
    }

    // Step 4: prune any overlay entry referencing a now-rolled-back dLSN. Without this, a stale
    // entry above lsn would keep pointing at a write the journal no longer has -- a subsequent read
    // could serve data from a block that's since been freed or reallocated to something else
    // entirely under the new term.
    {
        std::lock_guard lk{overlay_mu_};
        for (auto it = overlay_.begin(); it != overlay_.end();) {
            if (it->second.lsn > lsn) {
                it = drop_overlay_entry_locked(it);
            } else {
                ++it;
            }
        }
    }
    co_return ok();
}

#ifdef _PRERELEASE
void CraftReplDev::seed_lsns(int64_t last_append, std::initializer_list< int64_t > missing) {
    std::lock_guard lk{state_mu_};
    state_.last_append_lsn = last_append;
    missing_lsns_.clear();
    missing_lsns_.insert(missing);
}

void CraftReplDev::seed_commit_lsn(int64_t commit) {
    std::lock_guard lk{state_mu_};
    state_.commit_lsn = commit;
}

void CraftReplDev::seed_term(uint64_t term) {
    std::lock_guard lk{state_mu_};
    state_.term = term;
    login_fence_term_ = std::max(login_fence_term_, term);
}

void CraftReplDev::seed_in_flight(int64_t lsn) {
    std::lock_guard lk{state_mu_};
    in_flight_write_dlsns_.insert(lsn);
}

void CraftReplDev::seed_empty(std::initializer_list< int64_t > empty) {
    std::lock_guard lk{state_mu_};
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
    // Checked ahead of recovering_: a faulted partition is permanent, not a "still starting up,
    // try again shortly" condition -- see faulted_'s doc comment.
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("write rejected: partition permanently faulted dlsn={}", dlsn);
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
        std::lock_guard lock{state_mu_};
        // state_.term is guarded by state_mu_ like the rest of state_ -- read it under the same lock
        // used for the gap-marking below rather than unlocked, now that apply_internal_login (22887)
        // actually mutates it from the RAFT commit thread.
        if (hdr.term != state_.term) {
            LOGW("write rejected: stale term want={} got={} dlsn={}", state_.term, hdr.term, dlsn);
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
        }
        if (hdr.term < login_fence_term_) { // a newer login has committed and waits to apply
            LOGW("write rejected: term={} fenced by a committed login (term {}) dlsn={}", hdr.term, login_fence_term_,
                 dlsn);
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
        }
        if (write_quiesced_locked(hdr.term)) { // a login is replacing this session
            LOGW("write rejected: term={} quiesced by a login poll (term {}) dlsn={}", hdr.term, *quiesce_term_, dlsn);
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
        }
        // Advance the reclaim floor from ordinary IO -- same max-monotonic pattern as keep_alive().
        // Without this, all_committed_lsn only moves via keep_alive() messages, so quiet partitions
        // (write-only, no explicit keep_alive) never advance the reclaim floor at all.
        // hdr.all_committed_lsn is client-controlled wire input: -1 is the "unset" sentinel (skip),
        // any other negative value is malformed (ignore rather than corrupt this long-lived floor).
        // There is no other per-call bound to validate against here -- see keep_alive()'s doc comment.
        if (hdr.all_committed_lsn >= -1)
            state_.all_committed_lsn = std::max(state_.all_committed_lsn, hdr.all_committed_lsn);
        // The session's resolved prefix caps background SyncRSCommitLSN rounds (-1 = not sent).
        if (hdr.commit_lsn >= 0) client_commit_lsn_ = std::max(client_commit_lsn_, hdr.commit_lsn);
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
        if (missing_at_capacity_locked(dlsn)) {
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
            std::lock_guard lk{self->state_mu_};
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
    // hdr.term is already verified against state_.term under state_mu_ above; pass it so the
    // on-disk entry carries the session term for stale-tail detection on recovery. dlsn is stored
    // redundantly in CraftJournalEntry.lsn for self-describing recovery.
    auto res = co_await journal_->write_slot(dlsn, hdr.term, static_cast< lba_t >(addr),
                                             static_cast< lba_count_t >(len), blkid, all_zeros, csums);
    if (!res) {
        LOGE("write_slot failed dlsn={} addr={} len={}: {}", dlsn, addr, len, res.error().message());
        // Timed out: it may still land, referencing blkid, so blkid stays; and the journal can't be trusted any more.
        if (res.error() == std::errc::timed_out) {
            fault();
            co_return std::unexpected(res.error());
        }
        if (blkid_allocated) {
            if (auto fr = co_await journal_->free_data(blkid); !fr)
                LOGE("free_data failed after write_slot failure dlsn={}: {}", dlsn, fr.error().message());
        }
        co_return std::unexpected(res.error());
    }
    // The slot references blkid durably now: from here on it is the allocator's to keep across restarts. If that can't
    // be recorded, a checkpoint passing the slot would leave it pointing at blocks the allocator may hand out again:
    // the partition faults, persisting nothing more, and the blocks the journal references stay.
    if (blkid_allocated) {
        if (auto r = journal_->commit_data(blkid, /* recovering = */ false); !r) {
            fault();
            co_return std::unexpected(r.error());
        }
    }

    // Post-flight recheck: while write_slot was in flight, a login may have committed or applied (the session moved
    // on), a login poll may have quiesced this term, or the slot may have been voted Empty.
    //
    // A write whose session moved on is discarded, but do NOT call free_data: write_slot already wrote a durable
    // journal entry at dlsn that references blkid. There are only two orderings:
    //   truncate ran after write_slot  — truncate drops the entry; no free needed.
    //   truncate ran before write_slot — the entry survives above the new tail; freeing blkid here
    //                                    leaves it dangling (re-allocatable blocks, journal still
    //                                    references them). Actively harmful.
    // In both orderings free_data is either unnecessary or wrong. blkid is intentionally not freed.
    // CraftJournalEntry.term lets recovery skip the stale entry; the next login truncates it durably.
    bool stale_post_flight = false;
    bool voided_post_flight = false;
    bool quiesced_post_flight = false;
    craft::lsn_pair snapshot;
    {
        std::lock_guard lock{state_mu_};
        if (!session_current_locked(hdr.term)) {
            LOGW("write discarded post-flight: term changed or fenced dlsn={}", dlsn);
            stale_post_flight = true;
        } else if (empty_lsns_.contains(dlsn)) {
            // Voted Empty while this write was in flight.
            LOGW("write discarded post-flight: slot verdicted Empty while in flight dlsn={}", dlsn);
            voided_post_flight = true;
        } else {
            missing_lsns_.erase(dlsn);
            // Locally readable ahead of commit() indexing it. Under state_mu_, so an Empty verdict either voids this
            // write above or retires this entry.
            populate_overlay(dlsn, static_cast< lba_t >(addr) / lba_size_, static_cast< uint32_t >(len / lba_size_),
                             all_zeros, blkid, csums);
            // Caught in flight by a login poll: the slot stays, as the poll counted it in last_append_lsn, but the
            // write isn't acked.
            quiesced_post_flight = write_quiesced_locked(hdr.term);
        }
    }

    if (stale_post_flight) { co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM)); }
    if (quiesced_post_flight) {
        LOGW("write kept but not acked: quiesced by a login poll while in flight dlsn={}", dlsn);
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
    }
    if (voided_post_flight) {
        // Apply didn't free this slot (it was still missing then), so free its blocks here: those in hand, as the
        // slot's journal record may be reclaimed by now (the verdict lets the commit step over it).
        if (blkid_allocated) co_await free_voided(dlsn, blkid);
        co_return std::unexpected(make_error_condition(volume_error::EMPTY_SLOT));
    }

    // Best-effort piggyback: advance commit_lsn toward the client's own view of it. Every outcome --
    // a stall at a gap, or even a genuine commit() fault -- is ignored here; the write itself already
    // succeeded, and the next write/keep_alive retries the advance. keep_alive() is where a real
    // commit() error actually surfaces (advancing the frontier is its entire purpose).
    co_await commit(hdr.commit_lsn);

    touch_watchdog();
    maybe_trigger_periodic_sync();

    // Snapshot after the piggyback commit() above, so the ack reflects whatever progress it just made. A login poll or
    // a login can land while commit() awaits; the write isn't acked then either, though its slot stays published.
    {
        std::lock_guard lock{state_mu_};
        if (!session_current_locked(hdr.term) || write_quiesced_locked(hdr.term)) {
            LOGW("write kept but not acked: session fenced or quiesced before the ack dlsn={}", dlsn);
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
        }
        snapshot = {state_.commit_lsn, state_.last_append_lsn};
    }

    LOGT("write ok dlsn={} addr={} len={} all_zeros={}", dlsn, addr, len, all_zeros);
    co_return snapshot;
}

// ─── proactive checkpoint trigger (SDSTOR-22888) ─────────────────────────────
//
// Shared by every commit_lsn-advance path (commit_impl() -- covering write()'s piggyback, keep_alive()
// and the SyncRSCommitLSN apply -- and finish_sync_rs_commit_lsn()'s index-less walk) so a checkpoint fires
// regardless of which path is driving commit_lsn forward. Split into a locked check and an unlocked
// fire because the fire awaits (via detail::detach) and must not run with state_mu_ held.

bool CraftReplDev::checkpoint_interval_crossed_locked(int64_t commit_lsn_snapshot) {
    // Interval reuses sync_rs_commit_lsn_interval (via checkpoint_lsn_interval_) rather than its own
    // knob -- ties checkpoint cadence to the periodic SyncRSCommitLSN cadence. last_checkpoint_lsn_ is
    // updated here, before the caller releases state_mu_, so two overlapping callers can't both read
    // the same stale last_checkpoint_lsn_ and both decide to fire.
    if (commit_lsn_snapshot - last_checkpoint_lsn_ < checkpoint_lsn_interval_) return false;
    last_checkpoint_lsn_ = commit_lsn_snapshot;
    return true;
}

void CraftReplDev::fire_checkpoint_trigger(int64_t commit_lsn_snapshot, bool force) {
    if (!checkpoint_trigger_) {
        LOGW("commit_lsn={} crossed checkpoint interval but no checkpoint_trigger_ wired -- skipping",
             commit_lsn_snapshot);
        return;
    }
    // Unforced (a commit crossing the interval): let this coalesce with any checkpoint already in flight rather
    // than forcing back-to-back flushes under high commit throughput (see checkpoint_trigger_fn_t's doc comment);
    // retry_checkpoint() forces. Detached (fire-and-forget): nothing here depends on the flush completing. A
    // failure is logged, not propagated, same posture as catch-up/fetch failures elsewhere in this class.
    {
        std::lock_guard lk{state_mu_};
        if (checkpoints_stopped_) return; // stop(): nothing here touches HomeStore anymore
        ++checkpoints_running_;
    }
    // Parameters, not captures: a coroutine lambda's captures live in the temporary closure, which is gone by
    // the time the coroutine resumes.
    detail::detach([](shared< CraftReplDev > self, int64_t commit_lsn_snapshot, bool force) -> async_status {
        struct done_t {
            CraftReplDev* dev;
            ~done_t() { dev->checkpoint_done(); }
        } const done{self.get()}; // on every way out
        auto cp = co_await self->checkpoint_trigger_(force);
        if (!cp) LOGE("checkpoint trigger failed at commit_lsn={}: {}", commit_lsn_snapshot, cp.error().message());
        if (cp && *cp) co_return co_await self->advance_durable_commit(commit_lsn_snapshot);
        // Failed, coalesced into a flush that was already running (it may predate some of the slots applied up to
        // commit_lsn_snapshot, so it proves nothing about them), or skipped while HomeStore started: the sweep tries
        // again (retry_checkpoint()), even if no more slots commit.
        {
            std::lock_guard lk{self->state_mu_};
            self->checkpoint_retry_lsn_ = std::max(self->checkpoint_retry_lsn_, commit_lsn_snapshot);
        }
        co_return ok();
    }(shared_from_this(), commit_lsn_snapshot, force));
}

// Not while a trigger runs, and not once a later one covered the frontier.
void CraftReplDev::retry_checkpoint() {
    int64_t lsn;
    {
        std::lock_guard lk{state_mu_};
        if (checkpoint_retry_lsn_ <= durable_.durable_commit_lsn) {
            checkpoint_retry_lsn_ = -1;
            return;
        }
        if (checkpoints_running_ > 0) return;
        lsn = std::exchange(checkpoint_retry_lsn_, -1);
    }
    fire_checkpoint_trigger(lsn, /* force = */ true);
}

void CraftReplDev::checkpoint_done() {
    shared< sisl::async::value_awaitable< bool > > idle;
    {
        std::lock_guard lk{state_mu_};
        if (--checkpoints_running_ == 0) idle = std::exchange(checkpoints_idle_, nullptr);
    }
    if (idle) idle->complete(true); // outside the lock: resumes stop()
}

// ─── durable commit frontier and block frees (S8) ────────────────────────────

async_status CraftReplDev::advance_durable_commit(int64_t lsn) {
    {
        std::lock_guard lk{state_mu_};
        if (lsn <= durable_.durable_commit_lsn) co_return ok();
        // The verdicts at or below it stay: a client that lost a resolution reply asks again, and a late write to a
        // voided slot must still be refused, not acked as already written.
        durable_.durable_commit_lsn = lsn;
    }
    if (auto r = co_await persist_state(); !r) {
        // Recorded in memory; the next persist writes it. The superseded blocks wait for a frontier that is durable.
        LOGE("persisting the durable commit_lsn={} failed: {}", lsn, r.error().message());
        co_return ok();
    }
    // A restart never applies a slot <= lsn again, so the blocks those slots superseded can go.
    std::vector< homestore::blk_id > releasable;
    {
        std::lock_guard lk{state_mu_};
        auto const keep = std::stable_partition(deferred_frees_.begin(), deferred_frees_.end(),
                                                [lsn](auto const& e) { return e.first > lsn; });
        for (auto it = keep; it != deferred_frees_.end(); ++it)
            releasable.push_back(it->second);
        deferred_frees_.erase(keep, deferred_frees_.end());
    }
    co_return co_await free_blocks(lsn, std::move(releasable));
}

// A block an index write replaces belongs to an OLDER slot. With a state store, freeing it at once is unsafe: until a
// checkpoint covering the replacing slot completes, a restart applies the older slot again from the journal and points
// the index at a block that may belong to someone else by then. So the free waits for the durable commit frontier
// (advance_durable_commit()). A crash before that leaks the waiting blocks, bounded by the window since the last
// durable checkpoint -- never corruption. Without a store nothing is restored after a restart, so they go at once.
async_status CraftReplDev::release_superseded(int64_t replacing_lsn, std::vector< homestore::blk_id > blocks) {
    if (blocks.empty()) co_return ok();
    if (state_store_) {
        std::lock_guard lk{state_mu_};
        for (auto const& b : blocks)
            deferred_frees_.emplace_back(replacing_lsn, b);
        co_return ok();
    }
    co_return co_await free_blocks(replacing_lsn, std::move(blocks));
}

async_status CraftReplDev::free_blocks(int64_t lsn, std::vector< homestore::blk_id > blocks) {
    for (auto const& blk : blocks) {
        if (auto fr = co_await journal_->free_data(homestore::multi_blk_id{blk}); !fr)
            LOGE("free_data failed reclaiming blk={} lsn={}: {}", blk.to_string(), lsn, fr.error().message());
    }
    co_return ok();
}

// Every block an Empty verdict voids is freed only once the verdict is durable, wherever the free happens (a Sync's
// apply, a voided write or fill). Otherwise a restart replaying the Sync with no memory of the verdict finds the slot's
// journal record and frees the same blocks a second time. If the verdict can't be made durable, the blocks leak.
async_status CraftReplDev::free_voided(int64_t lsn, homestore::multi_blk_id blkid) {
    if (auto r = co_await persist_state(); !r) {
        LOGE("verdict voiding lsn={} not durable: {} -- leaking its blocks rather than risk freeing them twice", lsn,
             r.error().message());
        co_return ok();
    }
    if (auto fr = co_await journal_->free_data(blkid); !fr)
        LOGE("freeing the blocks of voided lsn={} failed: {} -- they may leak", lsn, fr.error().message());
    co_return ok();
}

// ─── commit() (internal; never a wire op) ────────────────────────────────────
//
// Advances commit_lsn toward upto_lsn by applying each committable slot to the index. At most one
// run is ever active at a time (commit_running_, guarded by state_mu_ and reset via RAII on every
// exit path); a concurrent caller is a safe no-op -- the in-flight run covers the same ground, and
// every subsequent write()/keep_alive() retries the advance. Never holds a lock across the co_await
// read_slot() suspension point below (same rule fetch_data's doc comment already establishes).

async_result< int64_t > CraftReplDev::commit_impl(int64_t upto_lsn, write_index_fn_t write_fn,
                                                  delete_index_fn_t delete_fn) {
    int64_t commit_lsn, last_append_lsn, replay_upto;
    {
        std::lock_guard lk{state_mu_};
        if (commit_running_) co_return state_.commit_lsn; // another run is already advancing
        commit_running_ = true;
        commit_lsn = state_.commit_lsn;
        last_append_lsn = state_.last_append_lsn;
        replay_upto = replay_upto_;
    }
    // Tracks the highest lsn successfully applied so far, updated per-iteration below WITHOUT
    // state_mu_. Written to the shared state_.commit_lsn exactly once, in
    // RunningGuard's destructor, instead of per-iteration -- see the destructor's own comment for why
    // this trades a per-iteration lock for a real (accepted) staleness window on the read path.
    int64_t final_commit_lsn = commit_lsn;

    // Guaranteed reset on every exit path (stall, success, or error): a local RAII object's destructor
    // runs when the coroutine frame unwinds, exactly like a plain function's locals on return. Also
    // runs the checkpoint-interval check here rather than only on the happy-path tail: folding it into
    // this same critical section means every exit -- including an early error return mid-loop -- still
    // gets a chance to fire the checkpoint (a slot that keeps failing on retry no longer permanently
    // starves it), and merges what would otherwise be two separate state_mu_ acquisitions into one.
    struct RunningGuard {
        CraftReplDev* self;
        int64_t* final_commit_lsn_ptr;
        ~RunningGuard() {
            bool should_checkpoint;
            {
                std::lock_guard lk{self->state_mu_};
                self->state_.commit_lsn = *final_commit_lsn_ptr;
                self->indexing_lsn_ = *final_commit_lsn_ptr; // drop a slot that failed before reaching the index
                self->commit_running_ = false;
                should_checkpoint = self->checkpoint_interval_crossed_locked(*final_commit_lsn_ptr);
            }
            if (should_checkpoint) self->fire_checkpoint_trigger(*final_commit_lsn_ptr);
        }
    } guard{this, &final_commit_lsn};

    int64_t const target = std::min(upto_lsn, last_append_lsn);
    for (int64_t lsn = commit_lsn + 1; lsn <= target; ++lsn) {
        bool is_missing, is_empty;
        {
            std::lock_guard lk{state_mu_};
            is_missing = missing_lsns_.contains(lsn);
            is_empty = empty_lsns_.contains(lsn);
            // Same lock as apply's Empty guard: once set, a verdict for lsn faults instead of freeing.
            if (!is_missing && !is_empty) indexing_lsn_ = lsn;
        }
        if (is_missing) break; // stall at the first hole -- not an error
        // A slot journalled before the restart may be applied again over an index that already holds a newer slot's
        // block for the same LBA: never free what applying it replaces (see replay_upto_).
        bool const replaying = lsn <= replay_upto;

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
                // Reclaim what the delete unmapped (release_superseded() says when); a failed free merely leaks.
                if (replaying) freed.clear();
                co_await release_superseded(lsn, std::move(freed));
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
                // Reclaim any superseded old blkid (release_superseded() says when).
                // old_blkid == new_blkid means this slot was already applied (crash-replay of an
                // idempotent commit before commit_lsn was durably persisted): the index already holds
                // this exact blkid, so freeing it here would free a block the index still references.
                std::vector< homestore::blk_id > superseded;
                for (auto const& [_, info] : blocks_info) {
                    if (replaying || !info.old_blkid.is_valid() || info.old_blkid == info.new_blkid) continue;
                    superseded.push_back(info.old_blkid);
                }
                co_await release_superseded(lsn, std::move(superseded));
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

        final_commit_lsn = lsn; // no lock -- see final_commit_lsn's own doc comment above
    }

    co_return final_commit_lsn;
}

async_result< int64_t > CraftReplDev::commit(int64_t upto_lsn) {
#ifdef _PRERELEASE
    if (test_write_fn_) co_return co_await commit_impl(upto_lsn, test_write_fn_, test_delete_fn_);
#endif
    if (!indx_tbl_) {
        // No index configured (write-path-only tests): nothing to apply, no-op safely.
        std::lock_guard lk{state_mu_};
        co_return state_.commit_lsn;
    }
    write_index_fn_t write_fn = [this](lba_t s, lba_t e, std::unordered_map< lba_t, BlockInfo >& info) {
        return indx_tbl_->write_to_index(s, e, info);
    };
    delete_index_fn_t delete_fn = [this](lba_t s, lba_t e, std::vector< homestore::blk_id >& freed) {
        return indx_tbl_->delete_lba_range(s, e, freed);
    };
    auto r = co_await commit_impl(upto_lsn, std::move(write_fn), std::move(delete_fn));
    co_return r;
}

#ifdef _PRERELEASE
async_result< int64_t > CraftReplDev::commit_with(int64_t upto_lsn, write_index_fn_t write_fn,
                                                  delete_index_fn_t delete_fn) {
    auto r = co_await commit_impl(upto_lsn, std::move(write_fn), std::move(delete_fn));
    co_return r;
}
#endif

// ─── overlay population (shared by write() and rebuild_overlay()) ───────────

void CraftReplDev::retire_overlay_range(lba_t start_lba, lba_t end_lba, int64_t retiring_lsn) {
    std::lock_guard lk{overlay_mu_};
    for (lba_t l = start_lba; l <= end_lba; ++l) {
        auto it = overlay_.find(l);
        if (it != overlay_.end() && it->second.lsn == retiring_lsn) drop_overlay_entry_locked(it);
    }
}

void CraftReplDev::retire_overlay_slot(int64_t lsn) {
    std::lock_guard lk{overlay_mu_};
    auto const extent = overlay_extent_.find(lsn);
    if (extent == overlay_extent_.end()) return; // owns no overlay entry
    auto const start_lba = extent->second.start_lba;
    auto const nlbas = extent->second.nlbas;
    for (uint32_t i = 0; i < nlbas; ++i) {
        auto it = overlay_.find(start_lba + i);
        if (it != overlay_.end() && it->second.lsn == lsn) drop_overlay_entry_locked(it); // the last drops the extent
    }
}

void CraftReplDev::set_overlay_entry_locked(lba_t lba, OverlayEntry entry, lba_t start_lba, uint32_t nlbas) {
    if (auto it = overlay_.find(lba); it != overlay_.end()) drop_overlay_entry_locked(it);
    ++overlay_extent_.try_emplace(entry.lsn, OverlayExtent{.start_lba = start_lba, .nlbas = nlbas}).first->second.live;
    overlay_.emplace(lba, entry);
}

auto CraftReplDev::drop_overlay_entry_locked(std::unordered_map< lba_t, OverlayEntry >::iterator it)
    -> std::unordered_map< lba_t, OverlayEntry >::iterator {
    if (auto extent = overlay_extent_.find(it->second.lsn);
        extent != overlay_extent_.end() && --extent->second.live == 0)
        overlay_extent_.erase(extent);
    return overlay_.erase(it);
}

CraftReplDev::SlotClaim CraftReplDev::peek_slot_write_locked(int64_t lsn) const {
    if (in_flight_write_dlsns_.contains(lsn)) return SlotClaim::in_flight;
    if (empty_lsns_.contains(lsn) || (lsn <= state_.last_append_lsn && !missing_lsns_.contains(lsn)))
        return SlotClaim::held;
    return SlotClaim::claimed;
}

CraftReplDev::SlotClaim CraftReplDev::claim_slot_write_locked(int64_t lsn) {
    auto const claim = peek_slot_write_locked(lsn);
    if (claim == SlotClaim::claimed) in_flight_write_dlsns_.insert(lsn);
    return claim;
}

bool CraftReplDev::missing_at_capacity_locked(int64_t lsn) const {
    return lsn > state_.last_append_lsn + 1 && missing_lsns_.size() >= max_missing_lsns_;
}

void CraftReplDev::populate_overlay(int64_t dlsn, lba_t start_lba, uint32_t nlbas, bool all_zeros,
                                    homestore::multi_blk_id const& blkid,
                                    std::vector< homestore::csum_t > const& csums) {
    lba_t lba = start_lba;
    std::lock_guard lk{overlay_mu_};
    if (all_zeros) {
        for (uint32_t i = 0; i < nlbas; ++i, ++lba) {
            auto it = overlay_.find(lba);
            if (it == overlay_.end() || dlsn > it->second.lsn) {
                set_overlay_entry_locked(lba, OverlayEntry{.lsn = dlsn, .all_zeros = true}, start_lba, nlbas);
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
                set_overlay_entry_locked(lba, OverlayEntry{.lsn = dlsn, .blkid = single_bid, .csum = csums[csum_idx]},
                                         start_lba, nlbas);
            }
        }
    }
}

void CraftReplDev::populate_overlay_from_slot(int64_t lsn, JournalSlot const& slot,
                                              homestore::multi_blk_id const& blkid) {
    // Skipped rather than published: populate_overlay() walks blkid, not nlbas, and commit_impl() rejects
    // nlbas == 0, so nothing would ever retire the entry.
    uint32_t const nlbas = static_cast< uint32_t >(slot.len_bytes / lba_size_);
    if (nlbas == 0) {
        LOGE("slot lsn={} has len_bytes={} (nlbas=0) -- malformed record, no overlay entry", lsn, slot.len_bytes);
        return;
    }
    populate_overlay(lsn, static_cast< lba_t >(slot.lba_off_bytes) / lba_size_, nlbas, slot.all_zeros, blkid,
                     slot.csums);
}

// ─── read() ───────────────────────────────────────────────────────────────────
//
// Serves [addr, addr+len) as of read_lsn from the LBA index (committed, <= commit_lsn) merged with
// the journal-tail overlay (appended but not yet committed, horizon-clamped to (commit_lsn, read_lsn]
// so an overlay entry above read_lsn is held but never served -- the index's older, still-valid-as-of-
// read_lsn value is used instead). Never fetches from a peer.

async_result< craft::read_result > CraftReplDev::read(craft::client_hdr hdr, int64_t read_lsn, uint64_t addr,
                                                      uint64_t len, sisl::sg_list dest) {
    // Checked ahead of recovering_: a faulted partition is permanent, not a "still starting up,
    // try again shortly" condition -- see faulted_'s doc comment.
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("read rejected: partition permanently faulted");
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    if (recovering_.load(std::memory_order_acquire)) {
        LOGW("read rejected: overlay rebuild in progress after restart");
        co_return std::unexpected(make_error_condition(volume_error::OFFLINE));
    }
    {
        std::lock_guard lock{state_mu_};
        if (hdr.term != state_.term) {
            LOGW("read rejected: stale term want={} got={}", state_.term, hdr.term);
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
        }
        // Advance the reclaim floor from ordinary IO -- same max-monotonic pattern as keep_alive().
        // Reject malformed negative values -- see write()'s doc comment.
        if (hdr.all_committed_lsn >= -1)
            state_.all_committed_lsn = std::max(state_.all_committed_lsn, hdr.all_committed_lsn);
        // The session's resolved prefix caps background SyncRSCommitLSN rounds (-1 = not sent).
        if (hdr.commit_lsn >= 0) client_commit_lsn_ = std::max(client_commit_lsn_, hdr.commit_lsn);
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
    int64_t index_horizon;
    {
        std::lock_guard lk{state_mu_};
        commit_lsn_snapshot = state_.commit_lsn;
        // After a restart the index may also hold slots above commit_lsn, up to replay_upto_: a checkpoint flushes the
        // index whenever it runs, while commit_lsn comes back from the durable commit frontier.
        index_horizon = std::max(commit_lsn_snapshot, replay_upto_);
    }
    // read_lsn and hdr.commit_lsn arrive in the same frame with nothing coupling them: read()'s own
    // piggyback commit (or a concurrent write()/keep_alive()'s) can advance commit_lsn_snapshot past
    // read_lsn before this snapshot is even taken. Once that happens the pre-read_lsn version is gone
    // from the index -- apply is a blind overwrite past commit_lsn by design (see commit()'s doc
    // comment) -- so there is nothing left on this replica to serve read_lsn correctly. Reject rather
    // than silently returning the too-new index value (CRAFT-Design: "writes above H are ignored even
    // if the replica holds them").
    if (read_lsn < index_horizon) {
        LOGW("read rejected: read_lsn={} is below what the index may hold ({}, commit_lsn={}) -- horizon already "
             "advanced, unanswerable",
             read_lsn, index_horizon, commit_lsn_snapshot);
        co_return std::unexpected(make_error_condition(volume_error::HORIZON_STALE));
    }

    // Committed state from the index.
    index_kv_list_t index_kvs;
    if (auto r = read_fn(start_lba, end_lba, index_kvs); !r) co_return std::unexpected(r.error());
    std::unordered_map< lba_t, VolumeIndexValue > index_map;
    index_map.reserve(index_kvs.size());
    for (auto const& [key, value] : index_kvs)
        index_map.emplace(key.lba(), value);

    // read_fn above takes no lock of its own (an index query, not a read of state_mu_-guarded
    // state) -- a concurrent commit_impl run can apply new LSNs to the index for an LBA in
    // [start_lba, end_lba] while read_fn is executing, even though the pre-check above passed
    // against the OLD snapshot. Re-snapshot commit_lsn now and re-check against read_lsn: if it
    // moved past read_lsn during the query, index_kvs just fetched may already reflect state above
    // the client's horizon for some LBA in range -- reject rather than silently serve a stale-snapshot
    // read as if it were still valid. This also narrows commit_lsn_snapshot to the freshest known-safe
    // value for the overlay clamp just below (a strictly tighter, still-correct bound).
    {
        std::lock_guard lk{state_mu_};
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
        std::lock_guard lk{state_mu_};
        snapshot = {state_.commit_lsn, state_.last_append_lsn};
    }
    co_return craft::read_result{std::move(extents), snapshot};
}

async_result< craft::lsn_pair > CraftReplDev::keep_alive(craft::client_hdr hdr) {
    // Checked ahead of recovering_: a faulted partition is permanent, not a "still starting up,
    // try again shortly" condition -- see faulted_'s doc comment.
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("keep_alive rejected: partition permanently faulted");
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    if (recovering_.load(std::memory_order_acquire)) {
        LOGW("keep_alive rejected: overlay rebuild in progress after restart");
        co_return std::unexpected(make_error_condition(volume_error::OFFLINE));
    }
    {
        std::lock_guard lock{state_mu_};
        if (hdr.term != state_.term) {
            LOGW("keep_alive rejected: stale term want={} got={}", state_.term, hdr.term);
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
        }
        // Max-monotonic: never let a stale/reordered message regress the floor journal reclaim reads.
        // hdr.all_committed_lsn is client-controlled wire input with no per-call bound to validate
        // against (it legitimately advances independently of this call's own commit_lsn/
        // last_append_lsn -- confirmed by existing tests). -1 is the "unset" sentinel (skip); any
        // OTHER negative value is unambiguously malformed and is ignored rather than corrupting this
        // long-lived floor. Does not catch an absurdly-large-but-positive value (no bound exists for
        // that here): the reclaim floor bounds it by the persisted durable commit frontier
        // (reclaim_floor_locked()).
        if (hdr.all_committed_lsn >= -1)
            state_.all_committed_lsn = std::max(state_.all_committed_lsn, hdr.all_committed_lsn);
        // The session's resolved prefix caps background SyncRSCommitLSN rounds (-1 = not sent).
        if (hdr.commit_lsn >= 0) client_commit_lsn_ = std::max(client_commit_lsn_, hdr.commit_lsn);
    }

    touch_watchdog();
    request_reclaim(); // all_committed_lsn may have raised the reclaim floor

    // Unlike write()'s piggyback, advancing the frontier IS this call's entire purpose -- a genuine
    // commit() fault (not a stall, which is never an error) propagates to the caller instead of being
    // silently swallowed.
    if (auto r = co_await commit(hdr.commit_lsn); !r) co_return std::unexpected(r.error());

    std::lock_guard lock{state_mu_};
    co_return craft::lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}

// ─── client-liveness watchdog (S7) ───────────────────────────────────────────

void CraftReplDev::touch_watchdog() {
    if (watchdog_timeout_ns_ == 0) return;
    {
        std::lock_guard lk{state_mu_};
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
// watchdog_timeout_ns_, not the tick interval) and, if so and this member leads, detaches
// watchdog_sync() (GetRSCommitLSN poll, then append()) -- this runs in a plain timer-callback context,
// not a coroutine caller awaiting a result. Every stale tick tries again; watchdog_sync() skips once
// the quorum's max last_append has already been proposed, so a silent client costs one RAFT entry.
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
    // A faulted leader can't run a round (append() refuses), so don't poll the quorum for one.
    if (faulted_.load(std::memory_order_acquire)) return;
    // Only the leader proposes; followers' ticks just detect staleness.
    if (raft_proposer_ == nullptr || !raft_proposer_->is_leader()) return;
    if (watermark_mu_.locked() || watchdog_round_in_flight_.load(std::memory_order_acquire)) return;
    // A tick can race the destructor's cancel(wait=true): the refcount may already be zero, where
    // shared_from_this() would throw. Held across detach so watchdog_sync() can take its own reference.
    auto self = weak_from_this().lock();
    if (!self) return;
    detail::detach(watchdog_sync());
}

// Client gone quiet: sync to what any responder holds, capped at the client's confirmed prefix
// (client_commit_lsn_). A delayed write can still land after the poll, so the round must not reach
// anything the client hasn't resolved.
async_status CraftReplDev::watchdog_sync() {
    auto self = shared_from_this();
    // One round at a time, poll included: a poll slower than the tick interval must not let later ticks
    // pile up more rounds, each holding `self` and so delaying teardown.
    bool expected = false;
    if (!watchdog_round_in_flight_.compare_exchange_strong(expected, true)) co_return ok();
    struct RoundGuard {
        CraftReplDev* dev;
        ~RoundGuard() { dev->watchdog_round_in_flight_.store(false, std::memory_order_release); }
    } round_guard{this};

    if (peer_fetcher_ == nullptr || raft_proposer_ == nullptr)
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));

    uint64_t term, client_token;
    int64_t target;
    {
        std::lock_guard lk{state_mu_};
        term = state_.term;
        client_token = state_.client_token;
        target = state_.last_append_lsn;
        // The target can't pass client_commit_lsn_, which only moves when the client talks to this leader. If that's
        // already proposed, the poll can't confirm anything new, so skip it rather than poll every idle tick.
        if (client_commit_lsn_ <= last_proposed_sync_lsn_) co_return ok();
    }

    auto polled = co_await peer_fetcher_->poll_quorum_lsns(term, /* is_login = */ false, peer_fetch_timeout_ms_);
    if (!polled) {
        LOGW("watchdog_sync: quorum poll failed: {}", polled.error().message());
        co_return std::unexpected(polled.error());
    }
    // Only distinct voters other than this leader count toward quorum; every other reply still feeds the
    // target (VoterTally).
    VoterTally tally{"watchdog_sync", raft_proposer_->my_id(), raft_proposer_->voters()};
    for (auto const& p : *polled) {
        if (tally.add(p.member) == VoterTally::Reply::drop) continue;
        target = std::max(target, p.reply.lsns.last_append_lsn);
    }
    if (!tally.has_quorum()) {
        LOGW("watchdog_sync: {} distinct voter(s) + self < quorum {} -- not syncing", tally.counted(),
             tally.quorum_size());
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::NO_QUORUM));
    }

    {
        std::lock_guard lk{state_mu_};
        target = std::min(target, client_commit_lsn_);
        // Also covers target == -1 (no client commit yet): last_proposed_sync_lsn_ starts at -1
        if (target <= last_proposed_sync_lsn_) co_return ok(); // nothing newly confirmed since the last sync
    }

    if (auto r = co_await append(target, client_token, term); !r) {
        LOGW("watchdog_sync: append({}) failed: {}", target, r.error().message());
        co_return std::unexpected(r.error());
    }
    co_return ok();
}

void CraftReplDev::maybe_trigger_periodic_sync() {
    if (raft_proposer_ == nullptr || sync_lsn_interval_ <= 0) return;
    if (watermark_mu_.locked()) return;
    if (!raft_proposer_->is_leader()) return;

    int64_t sync_to;
    uint64_t client_token, term;
    {
        std::lock_guard lk{state_mu_};
        // Cadence: every sync_lsn_interval_ appended LSNs since the last attempt (not the last proposal,
        // which can stall below an unresolved hole and would then retrigger on every write).
        if (state_.last_append_lsn - last_periodic_sync_lsn_ < sync_lsn_interval_) return;
        last_periodic_sync_lsn_ = state_.last_append_lsn;
        // Only what the client has confirmed: nothing at or below its commit_lsn is still in flight, so the
        // round can't vote a write the client may yet count as acknowledged Empty.
        sync_to = std::min(state_.last_append_lsn, client_commit_lsn_);
        if (sync_to <= last_proposed_sync_lsn_) return; // nothing newly confirmed
        client_token = state_.client_token;
        term = state_.term; // the session client_commit_lsn_ belongs to
    }
    // TODO: detach() runs the round inline until its first suspension, so the write() that triggered it acks only
    // after the round's checks, candidate scan and RAFT submission. Hand it to a worker reactor instead (needs a
    // test seam: test_craft_pre_resolution doesn't start iomgr).
    detail::detach(append(sync_to, client_token, term));
}

async_result< craft::resolution_result > CraftReplDev::request_resolution(craft::client_hdr hdr, int64_t upto) {
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("request_resolution rejected: partition permanently faulted");
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    if (recovering_.load(std::memory_order_acquire)) {
        LOGW("request_resolution rejected: overlay rebuild in progress after restart");
        co_return std::unexpected(make_error_condition(volume_error::OFFLINE));
    }
    if (raft_proposer_ == nullptr) co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    // NOT_LEADER before STALE_TERM, as in craft_client's reference: the client broadcasts to every member.
    if (!raft_proposer_->is_leader())
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::NOT_LEADER));
    uint64_t client_token;
    {
        std::lock_guard lk{state_mu_};
        if (!session_current_locked(hdr.term)) {
            LOGW("request_resolution rejected: stale or fenced term want={} got={}", state_.term, hdr.term);
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
        }
        client_token = state_.client_token;
    }
    if (upto < 0) co_return std::unexpected(std::make_error_condition(std::errc::invalid_argument));

    // hdr.term, not a fresh read: if InternalLogin applies before append() checks, this old-session request
    // must fail STALE_TERM rather than run inside the new session. Waits its turn behind a running round: failing
    // would leave the client's failed slot unresolved until its next failed write.
    co_return co_await append(upto, client_token, hdr.term, /*wait_turn=*/true);
}

// ─── append: propose SyncRSCommitLSN (S5 / SDSTOR-22908) ─────────────────────

async_result< craft::resolution_result > CraftReplDev::append(int64_t sync_to, uint64_t client_token, uint64_t term,
                                                              bool wait_turn) {
    // Checked on entry, and again by a Resolve once it has waited for watermark_mu_: the round it waited out may have
    // changed any of these.
    auto const refused = [this, sync_to, term]() -> std::optional< std::error_condition > {
        // Covers every trigger: a faulted leader's state is missing a committed entry, so it can't plan a round.
        if (faulted_.load(std::memory_order_acquire)) {
            LOGE("append rejected: partition permanently faulted sync_to={}", sync_to);
            return make_error_condition(volume_error::INTERNAL_ERROR);
        }
        if (raft_proposer_ == nullptr) return std::make_error_condition(std::errc::not_supported);
        if (!raft_proposer_->is_leader()) return craft::make_error_condition(craft::craft_error::NOT_LEADER);
        if (sync_to < 0) return std::make_error_condition(std::errc::invalid_argument);
        // Checked, not adopted: a caller's session that InternalLogin has since replaced must not run here.
        // pre_resolve_slots() and propose_sync() re-verify it after every await.
        std::lock_guard lk{state_mu_};
        if (!session_current_locked(term)) {
            LOGW("append: term moved past the caller's ({} -> {}) -- abandoning sync_to={}", term, state_.term,
                 sync_to);
            return craft::make_error_condition(craft::craft_error::STALE_TERM);
        }
        return std::nullopt;
    };
    if (auto const e = refused()) co_return std::unexpected(*e);

    // Triggers detach this; keep the device alive across the awaits below.
    auto self = shared_from_this();

    std::optional< AsyncMutex::Lock > round;
    if (wait_turn) {
        round = co_await watermark_mu_.lock();
        if (auto const e = refused()) co_return std::unexpected(*e);
    } else {
        round = watermark_mu_.try_lock();
        if (!round) {
            LOGD("append: another round or a login holds the watermark -- skipping sync_to={}", sync_to);
            co_return std::unexpected(std::make_error_condition(std::errc::operation_in_progress));
        }
    }

    auto pre = co_await pre_resolve_slots(sync_to, term);
    if (!pre) co_return std::unexpected(pre.error());

    // Never propose past an unresolved slot.
    int64_t target = sync_to;
    if (!pre->unresolved.empty())
        target = std::min(target, *std::min_element(pre->unresolved.begin(), pre->unresolved.end()) - 1);
    std::vector< int64_t > verdicts;
    for (int64_t lsn : pre->empty_slots)
        if (lsn <= target) verdicts.push_back(lsn);

    // Already covered by a committed entry, nothing new to say -- skip the RAFT round.
    bool skip;
    {
        std::lock_guard lk{state_mu_};
        skip = target <= last_proposed_sync_lsn_ && verdicts.empty();
    }

    if (!skip) {
        if (auto r = co_await propose_sync(*round, target, client_token, verdicts, term); !r)
            co_return std::unexpected(r.error());
    }

    // Every Empty verdict <= target, earlier rounds' included, so a client that lost a reply still learns them.
    // TODO: make empty_lsns_ a std::set (like missing_lsns_) so this is begin()..upper_bound(target), already
    // sorted and unique, instead of a full scan plus sort/unique.
    craft::resolution_result result{.resolved_upto = target, .empty_slots = {}};
    {
        std::lock_guard lk{state_mu_};
        // If InternalLogin replaced the caller's session during the awaits above, the reply is gone (the entry
        // committed regardless, and propose_sync() moved the watermark).
        if (!session_current_locked(term)) {
            LOGW("append: term moved past the caller's ({} -> {}) after resolving to {}", term, state_.term, target);
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
        }
        for (int64_t lsn : empty_lsns_)
            if (lsn <= target) result.empty_slots.push_back(lsn);
    }
    result.empty_slots.insert(result.empty_slots.end(), verdicts.begin(), verdicts.end());
    std::sort(result.empty_slots.begin(), result.empty_slots.end());
    result.empty_slots.erase(std::unique(result.empty_slots.begin(), result.empty_slots.end()),
                             result.empty_slots.end());
    LOGD("append: SyncRSCommitLSN resolved_upto={} (asked {}) verdicts={}", target, sync_to, verdicts.size());
    co_return result;
}

async_status CraftReplDev::propose_sync(AsyncMutex::Lock const& lock, int64_t rs_commit_lsn, uint64_t client_token,
                                        std::vector< int64_t > const& empty_slots, uint64_t term) {
    std::vector< uint8_t > key(sync_rs_commit_lsn_key_size(empty_slots.size()));
    serialize_sync_rs_commit_lsn(key.data(), rs_commit_lsn, client_token, term, empty_slots);

    if (auto r = co_await propose_entry(lock, CraftEntryType::SyncRSCommitLSN, key, term); !r) {
        log_unapplied_proposal(fmt::format("SyncRSCommitLSN({})", rs_commit_lsn), r.error());
        co_return std::unexpected(r.error());
    }
    // The entry committed whichever session is current now, so the watermark moves either way.
    std::lock_guard lk{state_mu_};
    last_proposed_sync_lsn_ = std::max(last_proposed_sync_lsn_, rs_commit_lsn);
    co_return ok();
}

async_status CraftReplDev::propose_entry([[maybe_unused]] AsyncMutex::Lock const& lock, CraftEntryType type,
                                         std::vector< uint8_t >& key, uint64_t term) {
    DEBUG_ASSERT(static_cast< bool >(lock), "proposing without holding watermark_mu_");
    // The caller awaited (pre-resolution at least): re-check right before proposing.
    if (!raft_proposer_->is_leader())
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::NOT_LEADER));
    {
        std::lock_guard lk{state_mu_};
        if (!session_current_locked(term))
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
    }

    std::vector< uint8_t > header(sizeof(CraftEntryHeader));
    reinterpret_cast< CraftEntryHeader* >(header.data())->type = type;
    co_return co_await raft_proposer_->propose(sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                                               sisl::blob{key.data(), static_cast< uint32_t >(key.size())});
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
// The state_mu_ lock is held only for the up-front classification pass below, dropped before any
// co_await read_slot() call to avoid holding a mutex across a suspension point. Callers are
// serialised by the login sequence (no concurrent writes while fetch_data runs), so the snapshot
// taken under the lock is stable for the whole batch.
//
// A read_slot() I/O error aborts the batch immediately (fail-fast); the partial result is discarded. So does a slot
// the journal reclaim has dropped (see the classification below).

async_result< std::vector< JournalSlot > > CraftReplDev::fetch_data(std::vector< int64_t > lsns) {
    // A faulted partition's slots no longer match the replica set's, so they can't be evidence for anyone's
    // resolution. Failing makes it a non-responder, which only makes other members' rounds more cautious.
    if (faulted_.load(std::memory_order_acquire)) {
        LOGE("fetch_data rejected: partition permanently faulted");
        co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
    }
    enum class SlotKind { Empty, Present, Absent };

    std::vector< SlotKind > kinds;
    kinds.reserve(lsns.size());
    {
        std::lock_guard lk{state_mu_};
        for (int64_t lsn : lsns) {
            if (empty_lsns_.contains(lsn)) {
                kinds.push_back(SlotKind::Empty);
            } else if (lsn >= 0 && lsn <= reclaimed_upto_) {
                // Held here, and reclaimed from the journal since: the batch fails. Omitting the slot would read as
                // "not held here", which a leader can count toward verdicting a committed slot Empty.
                LOGW("fetch_data rejected: lsn={} is reclaimed (journal reclaimed up to {})", lsn, reclaimed_upto_);
                co_return std::unexpected(std::make_error_condition(std::errc::result_out_of_range));
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
// yet-committed entries after a restart. It also commits each live slot's blocks to the allocator again
// (CraftJournalBackend::commit_data()). Walks (commit_lsn, last_append_lsn], skipping missing/
// Empty-verdicted lsns (nothing to read for those), and applies the exact same highest-dLSN-wins
// rule write()'s own post-flight update uses via the shared populate_overlay() helper. Missing lsns
// are skipped, not a stop condition (unlike commit_impl(), which must stall at the first gap since
// it advances a CONTIGUOUS prefix): a hole only means that ONE lsn's entry isn't locally present;
// later lsns in the same range can still hold real, out-of-order-appended entries that must not be
// skipped too.

async_status CraftReplDev::rebuild_overlay() {
    int64_t commit_lsn, last_append_lsn;
    {
        std::lock_guard lk{state_mu_};
        commit_lsn = state_.commit_lsn;
        last_append_lsn = state_.last_append_lsn;
    }

    for (int64_t lsn = commit_lsn + 1; lsn <= last_append_lsn; ++lsn) {
        bool is_missing, is_empty;
        {
            std::lock_guard lk{state_mu_};
            is_missing = missing_lsns_.contains(lsn);
            is_empty = empty_lsns_.contains(lsn);
        }
        if (is_missing || is_empty) continue;

        auto slot_r = co_await journal_->read_slot(lsn);
        if (!slot_r) {
            // Never journalled here (the restored range reaches up to the Sync watermark): missing, catch-up fills it.
            // Any other read error is a fault.
            auto const e = slot_r.error();
            if (e != std::errc::result_out_of_range && e != std::errc::no_such_file_or_directory)
                co_return std::unexpected(e);
            std::lock_guard lk{state_mu_};
            missing_lsns_.insert(lsn);
            continue;
        }
        // A live slot's blocks: the allocator's record of them may not have been persisted before the restart, and
        // nothing holds them in memory after it. Committed again before anything here can allocate.
        if (!slot_r->all_zeros) {
            if (auto r = journal_->commit_data(slot_r->blkid, /* recovering = */ true); !r) co_return r;
        }
        // A malformed record is skipped, not fatal: the replayed journal isn't being mutated.
        populate_overlay_from_slot(lsn, *slot_r, slot_r->blkid);
    }

    co_return ok();
}

// ─── RAFT listener ────────────────────────────────────────────────────────────

// Validates the entry and queues it for the apply queue, which applies it in log order and then completes this
// member's own proposal. An entry that doesn't parse fails the proposal here and never enters the queue.
void CraftReplDev::CraftRaftListener::on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                                std::vector< homestore::multi_blk_id > const& /* blkids */,
                                                cintrusive< homestore::repl_req_ctx >& ctx) {
    auto const invalid = [&ctx] {
        complete_proposal(ctx, std::unexpected(make_error_condition(volume_error::INVALID_ENTRY)));
    };
    if (header.size() < sizeof(CraftEntryHeader)) {
        LOGE("on_commit lsn={} header too small ({} bytes)", lsn, header.size());
        return invalid();
    }
    PendingApply entry{
        .lsn = lsn, .type = reinterpret_cast< const CraftEntryHeader* >(header.cbytes())->type, .ctx = ctx};
    std::vector< int64_t > empty_slots;

    switch (entry.type) {
    case CraftEntryType::SyncRSCommitLSN: {
        auto sync = parse_sync_rs_commit_lsn(key);
        if (!sync) {
            LOGE("on_commit lsn={} SyncRSCommitLSN key malformed ({} bytes)", lsn, key.size());
            return invalid();
        }
        entry.rs_commit_lsn = sync->rs_commit_lsn;
        entry.client_token = sync->client_token;
        entry.term = sync->term;
        empty_slots = std::move(sync->empty_slots);
        break;
    }
    case CraftEntryType::InternalLogin: {
        auto const login = parse_internal_login(key);
        if (!login) {
            LOGE("on_commit lsn={} InternalLogin key malformed ({} bytes)", lsn, key.size());
            return invalid();
        }
        entry.client_token = login->client_token;
        entry.term = login->term;
        entry.rs_commit_lsn = login->rs_commit_lsn;
        break;
    }
    default:
        LOGE("on_commit lsn={} unrecognized CraftEntryType={}", lsn, static_cast< uint8_t >(entry.type));
        return invalid();
    }
    owner_->enqueue_apply(std::move(entry), empty_slots);
}

// ─── RAFT apply queue ─────────────────────────────────────────────────────────

void CraftReplDev::enqueue_apply(PendingApply entry, std::vector< int64_t > const& empty_slots) {
    std::optional< std::error_condition > rejected;
    bool start = false;
    std::optional< int64_t > applied_before_restart;
    {
        std::lock_guard lk{apply_mu_};
        if (apply_stopped_) {
            rejected = std::make_error_condition(std::errc::operation_canceled);
        } else if (faulted_.load(std::memory_order_acquire)) {
            // A faulted partition applies nothing more (no catch-up, no journal or index writes): its record and its
            // journal stay as the fault left them, for whoever recovers it by hand. Entries held while a recovery that
            // failed ran end here too.
            rejected = make_error_condition(volume_error::INTERNAL_ERROR);
        } else if (entry.lsn <= restored_raft_idx_) {
            // Applied before the restart, and its effects restored from the image: applying it again could e.g. free
            // the blocks of a slot it voided a second time.
            applied_before_restart = restored_raft_idx_;
        } else {
            // Fences take effect at commit; only I/O waits for the queue. Both session rules below are decided here,
            // in log order, so every replica decides them the same way.
            if (entry.type == CraftEntryType::SyncRSCommitLSN) {
                bool replaced;
                {
                    std::lock_guard sl{state_mu_};
                    replaced = entry.term < login_fence_term_;
                }
                if (replaced) {
                    // Proposed in a session that a committed InternalLogin has since replaced (another leader's round
                    // after a leadership change, or one behind a timed-out login): the new session starts at that
                    // login's watermark, so this entry mustn't settle slots past it.
                    LOGW("SyncRSCommitLSN({}) lsn={}: session {} was replaced by a committed login -- rejected",
                         entry.rs_commit_lsn, entry.lsn, entry.term);
                    rejected = craft::make_error_condition(craft::craft_error::STALE_TERM);
                } else if (auto to_free = install_sync_rs_commit_lsn(entry.rs_commit_lsn, empty_slots); to_free) {
                    // Its verdicts fence write() and its watermark fences pre_resolve_slots() from now on.
                    entry.to_free = std::move(*to_free);
                } else {
                    rejected = to_free.error();
                }
            } else if (entry.type == CraftEntryType::InternalLogin) {
                std::lock_guard sl{state_mu_};
                // A login that isn't newer than one committed ahead of it is ignored when it applies (STALE_TERM), so
                // only a newer one is checked here.
                if (entry.term > login_fence_term_ && entry.rs_commit_lsn < applied_sync_lsn_) {
                    // A Sync committed ahead of it moved the watermark past this login's, which only another leader
                    // can do (a leadership change mid-login): the session would start below committed slots. It
                    // doesn't fence the old session it no longer replaces.
                    LOGW("InternalLogin(term={}) lsn={}: its watermark {} is below a committed Sync's {} -- rejected",
                         entry.term, entry.lsn, entry.rs_commit_lsn, applied_sync_lsn_);
                    rejected = craft::make_error_condition(craft::craft_error::NO_QUORUM);
                } else {
                    // The old session can't write while the login waits for its turn; the login applies in order.
                    login_fence_term_ = std::max(login_fence_term_, entry.term);
                }
            }
            if (!rejected) {
                apply_queue_.push_back(std::move(entry));
                // else the running drainer, or resume_apply(), picks it up
                start = !apply_paused_ && !std::exchange(apply_running_, true);
            }
        }
    }
    if (rejected) {
        LOGW("apply lsn={} not applied: {}", entry.lsn, rejected->message());
        complete_proposal(entry.ctx, std::unexpected(*rejected));
        return;
    }
    if (applied_before_restart) {
        LOGD("apply lsn={} skipped: applied before the restart (up to raft lsn={})", entry.lsn,
             *applied_before_restart);
        complete_proposal(entry.ctx, ok());
        return;
    }
    // Starts inline: with nothing awaited, the entry has applied by the time on_commit() returns.
    if (start) detail::detach(drain_apply());
}

// Pops the next entry to apply, or marks the queue idle (waking stop_apply()) and returns nullopt.
std::optional< CraftReplDev::PendingApply > CraftReplDev::next_apply() {
    bool notify;
    decltype(apply_queue_) dropped;
    {
        std::lock_guard lk{apply_mu_};
        // Queued before a fault: dropped, as enqueue_apply() refuses the ones that commit after it.
        if (faulted_.load(std::memory_order_acquire)) dropped.swap(apply_queue_);
        if (!apply_queue_.empty() && !apply_stopped_ && !apply_paused_) {
            auto entry = std::move(apply_queue_.front());
            apply_queue_.pop_front();
            applying_ = entry.as_pending();
            applying_->to_free.clear(); // its frees may happen before its apply is recorded: a restart leaks them
            return entry;
        }
        applying_.reset();
        apply_running_ = false;
        notify = std::exchange(apply_idle_wanted_, false);
    }
    for (auto const& e : dropped) {
        LOGW("apply lsn={} not applied: the partition is faulted", e.lsn);
        complete_proposal(e.ctx, std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR)));
    }
    if (notify) apply_idle_.complete(true); // outside the lock: resumes stop_apply()'s caller
    return std::nullopt;
}

// Applies queued entries one at a time until the queue is empty, completing each entry's proposal with its outcome.
async_status CraftReplDev::drain_apply() {
    auto self = shared_from_this(); // holds the device across every apply's co_awaits
    while (auto entry = next_apply()) {
        // An escaping exception would end the drainer with apply_running_ still set: the queue would never drain
        // again, and stop_apply() would never return.
        status outcome;
        try {
            outcome = co_await apply_one(*entry);
        } catch (std::exception const& e) {
            // The apply stopped partway, so its state can't be trusted: fault the partition, as a failed recovery does.
            LOGE("apply lsn={} threw: {} -- faulting the partition", entry->lsn, e.what());
            fault();
            outcome = std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
        } catch (...) {
            LOGE("apply lsn={} threw an unknown exception -- faulting the partition", entry->lsn);
            fault();
            outcome = std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
        }
        bool const applied = static_cast< bool >(outcome);
        if (!applied) LOGW("apply lsn={} failed: {}", entry->lsn, outcome.error().message());
        complete_proposal(entry->ctx, std::move(outcome));

        // Then the image records the entry as applied -- after the durable effects its apply wrote ahead -- so the log
        // replay after a restart skips it. A failed one too: what its apply wrote ahead is in the image either way (a
        // login's session, admitted after a restart), and applying it again would change nothing. If this persist
        // fails, the entry is applied again after a restart (from the image's pending entries or the log replay).
        {
            // In one step for every image taken: the entry is either pending or recorded as applied.
            std::lock_guard al{apply_mu_};
            std::lock_guard lk{state_mu_};
            durable_.last_applied_raft_idx = std::max(durable_.last_applied_raft_idx, entry->lsn);
            applying_.reset();
        }
        if (auto r = co_await persist_state(); !r)
            LOGE("persisting the last applied raft lsn={} failed: {}", entry->lsn, r.error().message());
    }
    co_return ok();
}

async_status CraftReplDev::apply_one(PendingApply const& entry) {
    switch (entry.type) {
    case CraftEntryType::SyncRSCommitLSN: // its state change was made at commit
        co_return co_await finish_sync_rs_commit_lsn(entry.rs_commit_lsn, entry.client_token, entry.to_free);
    case CraftEntryType::InternalLogin:
        // An ignored login fails STALE_TERM, which tells its proposer that it lost the term.
        co_return co_await apply_internal_login(entry.client_token, entry.term);
    default: // on_commit() queues only the types above
        co_return std::unexpected(make_error_condition(volume_error::INVALID_ENTRY));
    }
}

async_status CraftReplDev::stop_apply() {
    std::deque< PendingApply > dropped;
    bool wait;
    {
        std::lock_guard lk{apply_mu_};
        apply_stopped_ = true;
        dropped.swap(apply_queue_);
        wait = apply_running_;
        if (wait) apply_idle_wanted_ = true;
    }
    for (auto& entry : dropped)
        complete_proposal(entry.ctx, std::unexpected(std::make_error_condition(std::errc::operation_canceled)));
    if (!dropped.empty()) LOGI("apply queue stopped: {} committed entr(ies) not applied", dropped.size());
    if (wait) co_await apply_idle_;
    co_return ok();
}

async_status CraftReplDev::stop(bool keep_queued) {
    if (keep_queued) {
        bool wait;
        {
            std::lock_guard lk{apply_mu_};
            apply_paused_ = true; // the drainer ends after the apply in progress; what commits now stays queued
            wait = apply_running_;
            if (wait) apply_idle_wanted_ = true;
        }
        if (wait) co_await apply_idle_;
    } else {
        (void)co_await stop_apply();
    }
    shared< sisl::async::value_awaitable< bool > > reclaimed;
    {
        std::lock_guard lk{state_mu_};
        reclaim_stopped_ = true;
        if (reclaim_running_) reclaimed = reclaim_idle_ = std::make_shared< sisl::async::value_awaitable< bool > >();
    }
    if (reclaimed) co_await *reclaimed;
    shared< sisl::async::value_awaitable< bool > > checkpointed;
    {
        std::lock_guard lk{state_mu_};
        checkpoints_stopped_ = true;
        if (checkpoints_running_ > 0)
            checkpointed = checkpoints_idle_ = std::make_shared< sisl::async::value_awaitable< bool > >();
    }
    if (checkpointed) co_await *checkpointed;
    watchdog_token_.cancel(/* wait = */ true);
    if (raft_proposer_) raft_proposer_->stop(); // the pending proposals of its rounds end
    co_return ok();
}

#ifdef _PRERELEASE
void CraftReplDev::pause_apply() {
    std::lock_guard lk{apply_mu_};
    apply_paused_ = true; // an apply in progress finishes; the drainer then stops
}

void CraftReplDev::resume_apply() {
    bool start;
    {
        std::lock_guard lk{apply_mu_};
        apply_paused_ = false;
        start = !apply_queue_.empty() && !std::exchange(apply_running_, true);
    }
    if (start) detail::detach(drain_apply());
}

std::vector< std::string > CraftReplDev::invariant_violations() {
    std::vector< std::string > found;
    {
        std::lock_guard lk{apply_mu_};
        if (apply_running_ || !apply_queue_.empty())
            found.push_back(fmt::format("apply queue not idle: {} queued, drainer {}", apply_queue_.size(),
                                        apply_running_ ? "running" : "not running"));
        if (!apply_running_ && applying_)
            found.push_back(fmt::format("raft lsn {} still marked as being applied", applying_->raft_idx));
    }
    std::lock_guard sl{state_mu_};
    if (login_fence_term_ < state_.term)
        found.push_back(fmt::format("login fence term {} below the session term {}", login_fence_term_, state_.term));
    if (durable_.durable_commit_lsn > state_.commit_lsn)
        found.push_back(fmt::format("durable commit frontier {} above commit_lsn {}", durable_.durable_commit_lsn,
                                    state_.commit_lsn));
    for (int64_t lsn : durable_.empty_lsns)
        if (!empty_lsns_.contains(lsn)) found.push_back(fmt::format("durable verdict {} is not in effect", lsn));
    for (int64_t lsn : empty_lsns_)
        if (missing_lsns_.contains(lsn)) found.push_back(fmt::format("lsn {} is both missing and Empty", lsn));

    std::lock_guard ol{overlay_mu_};
    std::unordered_map< int64_t, uint32_t > owned; // overlay entries per lsn
    for (auto const& [lba, entry] : overlay_) {
        ++owned[entry.lsn];
        auto const extent = overlay_extent_.find(entry.lsn);
        if (extent == overlay_extent_.end()) {
            found.push_back(fmt::format("overlay lba {} (lsn {}) has no extent", lba, entry.lsn));
        } else if (lba < extent->second.start_lba || lba >= extent->second.start_lba + extent->second.nlbas) {
            found.push_back(fmt::format("overlay lba {} (lsn {}) is outside its extent [{}, +{})", lba, entry.lsn,
                                        extent->second.start_lba, extent->second.nlbas));
        }
    }
    for (auto const& [lsn, extent] : overlay_extent_) {
        auto const it = owned.find(lsn);
        uint32_t const live = it == owned.end() ? 0 : it->second;
        if (live == 0) {
            found.push_back(fmt::format("extent of lsn {} outlived its overlay entries", lsn));
        } else if (extent.live != live) {
            found.push_back(
                fmt::format("extent of lsn {} counts {} entries, the overlay has {}", lsn, extent.live, live));
        }
    }
    return found;
}
#endif

void CraftReplDev::CraftRaftListener::on_error(homestore::ReplServiceError err, sisl::blob const& /* header */,
                                               sisl::blob const& /* key */,
                                               cintrusive< homestore::repl_req_ctx >& ctx) {
    LOGW("proposal failed to replicate: {}", enum_name(err));
    complete_proposal(ctx,
                      std::unexpected(err == homestore::ReplServiceError::NOT_LEADER
                                          ? craft::make_error_condition(craft::craft_error::NOT_LEADER)
                                          : make_error_condition(volume_error::INTERNAL_ERROR)));
}

void CraftReplDev::CraftRaftListener::on_rollback(int64_t lsn, sisl::blob const& /* header */,
                                                  sisl::blob const& /* key */,
                                                  cintrusive< homestore::repl_req_ctx >& ctx) {
    LOGW("on_rollback lsn={}", lsn);
    complete_proposal(ctx, std::unexpected(craft::make_error_condition(craft::craft_error::NOT_LEADER)));
}

// Fired by HomeStore wherever this partition's repl_dev is recovered on restart. Fire-and-forget,
// same as on_watchdog_tick() -- this runs in a plain callback context, not a coroutine caller
// awaiting a result. Sets recovering_ BEFORE detaching the recovery coroutine so no write()/read()/
// keep_alive() that arrives after on_restart() fires but before rebuild_overlay() completes can ever
// observe a partially-rebuilt overlay (write()/read()/keep_alive() all reject with OFFLINE while
// recovering_ is set -- per SDSTOR-22905, the overlay must be fully rebuilt before the volume accepts
// new client I/O). recover() clears the flag once rebuild_overlay() actually succeeds -- see its
// own doc comment for what happens on failure instead.
//
// Reachable more than once, in principle, if HomeStore's repl_dev_listener contract ever allows
// on_restart() to fire again on the same instance -- there is no code-level guard against that here;
// correctness for that case currently rests entirely on HomeStore's calling convention (observed today:
// fired at most once per instance per restart cycle), not on anything this class enforces itself.
//
// With a state store, recover() first restores state_ from the durable image and the journal, then rebuilds the
// overlay over (commit_lsn, last_append_lsn]. Without one, state_ stays at its defaults and the walk is empty.
void CraftReplDev::CraftRaftListener::on_restart() {
    owner_->recovering_.store(true, std::memory_order_release);
    detail::detach(owner_->recover());
}

// Awaits restore_state(), then rebuild_overlay(), to completion, then queues again the entries the image held as
// pending. On success, clears recovering_ -- the volume resumes
// accepting client I/O against a now-complete overlay. On FAILURE (e.g. a corrupt journal record),
// does the opposite of what a naive "always clear, log and move on" version would: leaves recovering_
// set and additionally sets faulted_ (checked first, ahead of recovering_, by every client-
// facing entry point), so the partition permanently rejects I/O with INTERNAL_ERROR instead of quietly
// resuming against an overlay that stopped partway through -- entries at and above the failing LSN
// were never populated, so any subsequent read for an LBA only covered by one of those entries would
// otherwise silently fall through to stale, already-superseded index/committed state with no error at
// all. A stuck-rejecting partition is a visible, actionable failure; a partition silently serving stale
// data is not. There is deliberately no self-healing path out of this state here -- an operator
// decision (or a future re-sync-from-peer mechanism) is required.
async_status CraftReplDev::recover() {
    recovering_.store(true, std::memory_order_release);
    result< std::vector< CraftPendingEntry > > pending;
    status r;
    // A recovery that throws (a journal read HomeStore fails with an exception) fails like any other: escaping, the
    // exception would leave the volume's reattach, and with it init and every checkpoint, waiting forever.
    try {
        // An image or journal tail that can't be read faults the partition just as a failed rebuild does: restarting
        // it empty would forget its session and verdicts.
        pending = co_await restore_state();
        if (!pending) {
            r = std::unexpected(pending.error());
        } else {
            r = co_await rebuild_overlay();
        }
        // A Sync still queued frees, when it applies, the slots its verdicts voided: their blocks stay the
        // allocator's until then (rebuild_overlay() skips Empty slots).
        if (r) {
            for (auto const& p : *pending) {
                for (int64_t lsn : p.to_free) {
                    auto const slot = co_await journal_->read_slot(lsn);
                    if (!slot) {
                        // Never journalled here: nothing held. Any other read error fails the recovery, as in
                        // rebuild_overlay(): skipped, the slot's blocks would stay free, and the Sync's free, reading
                        // the slot again, would free them after the allocator may have handed them out.
                        auto const e = slot.error();
                        if (e == std::errc::result_out_of_range || e == std::errc::no_such_file_or_directory) continue;
                        r = std::unexpected(e);
                        break;
                    }
                    if (slot->all_zeros) continue; // nothing held: nothing to free either
                    if (r = journal_->commit_data(slot->blkid, /* recovering = */ true); !r) break;
                }
                if (!r) break;
            }
        }
    } catch (std::exception const& e) {
        LOGE("restart recovery threw: {}", e.what());
        r = std::unexpected(std::make_error_condition(std::errc::io_error));
    } catch (...) {
        LOGE("restart recovery threw an unknown exception");
        r = std::unexpected(std::make_error_condition(std::errc::io_error));
    }
    if (!r) {
        LOGE("restart recovery failed: {} -- partition permanently faulted, will "
             "not accept client I/O (see faulted_'s doc comment)",
             r.error().message());
        fault();
        co_return r;
    }
    requeue_pending(std::move(*pending));
    recovering_.store(false, std::memory_order_release);
    co_return r;
}

// An entry queued again here applies without its commit-time state change: the image already holds a Sync's verdicts
// and its watermark (restore_state()), the journal walk rebuilt the rest, and making the change again could find a
// verdict the restored frontier has passed. A Sync still queued at the restart frees the slots its verdicts voided, as
// it would have (recover() kept their blocks allocated); the one being applied comes without them, as its frees may
// have happened: freeing twice is worse than leaking. An InternalLogin raises the login fence again, as at commit.
void CraftReplDev::requeue_pending(std::vector< CraftPendingEntry > pending) {
    bool start = false;
    {
        std::lock_guard lk{apply_mu_};
        for (auto const& p : pending) {
            if (p.raft_idx <= restored_raft_idx_) continue; // applied before the restart
            {
                std::lock_guard sl{state_mu_};
                if (p.type == CraftEntryType::InternalLogin) login_fence_term_ = std::max(login_fence_term_, p.term);
                awaiting_free_.insert(p.to_free.begin(), p.to_free.end());
            }
            apply_queue_.push_back(PendingApply{.lsn = p.raft_idx,
                                                .type = p.type,
                                                .rs_commit_lsn = p.rs_commit_lsn,
                                                .client_token = p.client_token,
                                                .to_free = p.to_free,
                                                .term = p.term});
            restored_raft_idx_ = p.raft_idx; // the log replay delivers it too
        }
        start = !apply_queue_.empty() && !apply_paused_ && !std::exchange(apply_running_, true);
    }
    if (start) detail::detach(drain_apply());
}

// ─── durable partition state (S8 / SDSTOR-22745) ─────────────────────────────

namespace {
constexpr uint64_t k_durable_magic = 0x31425354'46415243ull; // "CRAFTSB1" little-endian
constexpr uint32_t k_durable_version = 2;
#pragma pack(push, 1)
struct DurableHeader {
    uint64_t magic;
    uint32_t version;
    uint32_t n_empty;
    uint32_t n_pending;
    uint64_t term;
    uint64_t client_token;
    int64_t last_applied_raft_idx;
    int64_t durable_commit_lsn;
    int64_t sync_watermark;
};
struct PendingRecord { // followed by n_free int64_t: CraftPendingEntry::to_free
    int64_t raft_idx;
    uint8_t type;
    int64_t rs_commit_lsn;
    uint64_t client_token;
    uint64_t term;
    uint32_t n_free;
};
#pragma pack(pop)
} // namespace

std::vector< uint8_t > encode_durable_state(CraftDurableState const& s) {
    // Empty verdicts are rare, and the pending entries are the apply queue's: neither gets near 2^32.
    RELEASE_ASSERT(s.empty_lsns.size() <= std::numeric_limits< uint32_t >::max() &&
                       s.pending.size() <= std::numeric_limits< uint32_t >::max(),
                   "{} Empty verdicts or {} pending entries overflow the image", s.empty_lsns.size(), s.pending.size());
    size_t frees = 0;
    for (auto const& p : s.pending)
        frees += p.to_free.size();
    DurableHeader const h{k_durable_magic,
                          k_durable_version,
                          static_cast< uint32_t >(s.empty_lsns.size()),
                          static_cast< uint32_t >(s.pending.size()),
                          s.term,
                          s.client_token,
                          s.last_applied_raft_idx,
                          s.durable_commit_lsn,
                          s.sync_watermark};
    std::vector< uint8_t > out(sizeof(h) + s.empty_lsns.size() * sizeof(int64_t) +
                               s.pending.size() * sizeof(PendingRecord) + frees * sizeof(int64_t));
    std::memcpy(out.data(), &h, sizeof(h));
    size_t off = sizeof(h);
    for (int64_t lsn : s.empty_lsns) {
        std::memcpy(out.data() + off, &lsn, sizeof(lsn));
        off += sizeof(lsn);
    }
    for (auto const& p : s.pending) {
        PendingRecord const r{.raft_idx = p.raft_idx,
                              .type = static_cast< uint8_t >(p.type),
                              .rs_commit_lsn = p.rs_commit_lsn,
                              .client_token = p.client_token,
                              .term = p.term,
                              .n_free = static_cast< uint32_t >(p.to_free.size())};
        std::memcpy(out.data() + off, &r, sizeof(r));
        off += sizeof(r);
        for (int64_t lsn : p.to_free) {
            std::memcpy(out.data() + off, &lsn, sizeof(lsn));
            off += sizeof(lsn);
        }
    }
    return out;
}

std::optional< CraftDurableState > decode_durable_state(uint8_t const* buf, size_t len) {
    if (buf == nullptr || len < sizeof(DurableHeader)) return std::nullopt;
    DurableHeader h;
    std::memcpy(&h, buf, sizeof(h));
    if (h.magic != k_durable_magic || h.version != k_durable_version) return std::nullopt;
    // The pending records are of variable size: checked one by one below, and that they end the image exactly.
    if ((len - sizeof(h)) / sizeof(int64_t) < h.n_empty) return std::nullopt;
    CraftDurableState s{.term = h.term,
                        .client_token = h.client_token,
                        .last_applied_raft_idx = h.last_applied_raft_idx,
                        .durable_commit_lsn = h.durable_commit_lsn,
                        .sync_watermark = h.sync_watermark};
    int64_t prev = -1;
    for (uint32_t i = 0; i < h.n_empty; ++i) {
        int64_t lsn;
        std::memcpy(&lsn, buf + sizeof(h) + i * sizeof(int64_t), sizeof(lsn));
        if (lsn <= prev) return std::nullopt; // strictly ascending, and not negative
        s.empty_lsns.insert(s.empty_lsns.end(), lsn);
        prev = lsn;
    }
    size_t off = sizeof(h) + static_cast< size_t >(h.n_empty) * sizeof(int64_t);
    int64_t prev_idx = -1;
    for (uint32_t i = 0; i < h.n_pending; ++i) {
        PendingRecord r;
        if (len - off < sizeof(r)) return std::nullopt;
        std::memcpy(&r, buf + off, sizeof(r));
        off += sizeof(r);
        auto const type = static_cast< CraftEntryType >(r.type);
        if (r.raft_idx <= prev_idx) return std::nullopt; // strictly ascending
        if (type != CraftEntryType::SyncRSCommitLSN && type != CraftEntryType::InternalLogin) return std::nullopt;
        if ((len - off) / sizeof(int64_t) < r.n_free) return std::nullopt;
        CraftPendingEntry p{.raft_idx = r.raft_idx,
                            .type = type,
                            .rs_commit_lsn = r.rs_commit_lsn,
                            .client_token = r.client_token,
                            .term = r.term};
        p.to_free.resize(r.n_free);
        std::memcpy(p.to_free.data(), buf + off, r.n_free * sizeof(int64_t));
        off += r.n_free * sizeof(int64_t);
        s.pending.push_back(std::move(p));
        prev_idx = r.raft_idx;
    }
    if (off != len) return std::nullopt;
    return s;
}

namespace {
// The record is this partition's, in this format, and holds the image its header announces.
bool valid_state_record(volume_id_t const& vol_id, homestore::superblk< craft_state_sb_t >& sb) {
    if (sb.is_empty()) return false;
    auto const buf = sb.raw_buf(); // may be longer than the record: rounded up to the meta blk alignment
    auto const* const hdr = sb.get();
    return buf->size() >= sizeof(craft_state_sb_t) && hdr->magic == craft_state_sb_t::k_magic &&
        hdr->version == craft_state_sb_t::k_version && hdr->vol_id == vol_id &&
        hdr->image_len <= buf->size() - sizeof(craft_state_sb_t);
}

// The image goes in the partition's own meta blk record, after a craft_state_sb_t header. The mutex orders a persist
// against destroy(), which the volume's destroy calls while a persist round may still be running.
class HomeStoreCraftStateStore : public CraftStateStore {
public:
    HomeStoreCraftStateStore(volume_id_t vol_id, homestore::superblk< craft_state_sb_t > sb) :
            vol_id_{vol_id}, sb_{std::move(sb)} {
        // The image a boot found in the record counts as written: the first checkpoint after a restart doesn't
        // rewrite every partition's record.
        if (valid_state_record(vol_id_, sb_) && !sb_.get()->faulted) {
            auto const* const image = sb_.raw_buf()->cbytes() + sizeof(craft_state_sb_t);
            written_.assign(image, image + sb_.get()->image_len);
        }
    }

    result< std::optional< CraftDurableState > > load() override {
        std::lock_guard lk{mu_};
        if (sb_.is_empty()) return std::nullopt;
        auto const buf = sb_.raw_buf(); // may be longer than the record: rounded up to the meta blk alignment
        auto const* const hdr = sb_.get();
        std::optional< CraftDurableState > d;
        if (valid_state_record(vol_id_, sb_)) {
            if (hdr->faulted) return std::unexpected(std::make_error_condition(std::errc::state_not_recoverable));
            d = decode_durable_state(buf->cbytes() + sizeof(craft_state_sb_t), hdr->image_len);
        }
        if (!d) return std::unexpected(std::make_error_condition(std::errc::illegal_byte_sequence));
        return d;
    }

    async_status persist(CraftDurableState const& s) override {
        auto image = encode_durable_state(s);
        std::lock_guard lk{mu_};
        if (destroyed_) co_return std::unexpected(std::make_error_condition(std::errc::operation_canceled));
        if (faulted_) co_return std::unexpected(std::make_error_condition(std::errc::state_not_recoverable));
        // Every checkpoint saves every partition, most of them unchanged since: the record already holds this image.
        if (image == written_) co_return ok();
        // A new buffer each time, sized to this image; the record keeps its meta blk.
        sb_.create(static_cast< uint32_t >(sizeof(craft_state_sb_t) + image.size()));
        sb_->vol_id = vol_id_;
        sb_->image_len = static_cast< uint32_t >(image.size());
        std::memcpy(sb_.raw_buf()->bytes() + sizeof(craft_state_sb_t), image.data(), image.size());
        sb_.write();
        written_ = std::move(image);
        co_return ok();
    }

    void destroy() override {
        std::lock_guard lk{mu_};
        destroyed_ = true;
        sb_.destroy();
    }

    void mark_faulted() override {
        std::lock_guard lk{mu_};
        if (destroyed_ || faulted_) return;
        faulted_ = true;
        mark_craft_state_faulted(vol_id_, sb_);
    }

private:
    std::mutex mu_;
    volume_id_t const vol_id_;
    homestore::superblk< craft_state_sb_t > sb_;
    std::vector< uint8_t > written_; // the image this store last wrote to the record; none yet: empty
    bool destroyed_{false};
    bool faulted_{false};
};
} // namespace

unique< CraftStateStore > make_homestore_state_store(volume_id_t vol_id, homestore::superblk< craft_state_sb_t > sb) {
    return std::make_unique< HomeStoreCraftStateStore >(vol_id, std::move(sb));
}

// Keeps the image when the record has one, for whoever looks at it later.
void mark_craft_state_faulted(volume_id_t vol_id, homestore::superblk< craft_state_sb_t >& sb) {
    if (!valid_state_record(vol_id, sb)) {
        sb.create(static_cast< uint32_t >(sizeof(craft_state_sb_t)));
        sb->vol_id = vol_id;
    }
    sb->faulted = 1;
    sb.write();
}

void CraftReplDev::fault() {
    if (faulted_.exchange(true, std::memory_order_acq_rel)) return;
    if (state_store_) state_store_->mark_faulted();
}

async_status CraftReplDev::save_state() {
    if (!faulted_.load(std::memory_order_acquire)) {
        auto r = co_await persist_state();
        if (!faulted_.load(std::memory_order_acquire)) co_return r;
    }
    // Faulted before, during or right after the save (its own write may have won the race with the mark): only once
    // the mark is durable, whoever makes it, as the checkpoint may cover entries this record lacks.
    if (state_store_) state_store_->mark_faulted();
    co_return ok();
}

async_status CraftReplDev::persist_state() {
    if (!state_store_) co_return ok();
    shared< sisl::async::shared_awaitable< bool > > round;
    bool start;
    {
        std::lock_guard lk{state_mu_};
        if (!persist_next_) persist_next_ = std::make_shared< sisl::async::shared_awaitable< bool > >();
        round = persist_next_;
        start = !std::exchange(persist_running_, true);
    }
    if (start) detail::detach(run_persist());
    if (co_await *round) co_return ok();
    co_return std::unexpected(std::make_error_condition(std::errc::io_error));
}

// The one writer behind persist_state(). Each round writes the image as it is when the round is taken, which holds
// every update its callers made before joining it; a caller joining meanwhile waits for the next round.
async_status CraftReplDev::run_persist() {
    auto self = shared_from_this();
    for (;;) {
        shared< sisl::async::shared_awaitable< bool > > round;
        CraftDurableState image;
        {
            // The entries not applied yet go with the image, taken together: apply_mu_ first, as enqueue_apply() takes
            // the two.
            std::lock_guard al{apply_mu_};
            std::lock_guard lk{state_mu_};
            round = std::exchange(persist_next_, nullptr);
            if (!round) {
                persist_running_ = false;
                co_return ok();
            }
            image = durable_;
            if (applying_) image.pending.push_back(*applying_);
            for (auto const& e : apply_queue_)
                image.pending.push_back(e.as_pending());
        }
        // An escaping exception would end the writer with persist_running_ still set: no persist would complete again.
        status r;
        try {
            // A faulted partition persists nothing more: its record stays as fault() marked it.
            if (faulted_.load(std::memory_order_acquire))
                r = std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
            else
                r = co_await state_store_->persist(image);
        } catch (std::exception const& e) {
            LOGE("persisting the durable partition state threw: {}", e.what());
            r = std::unexpected(std::make_error_condition(std::errc::io_error));
        } catch (...) {
            LOGE("persisting the durable partition state threw an unknown exception");
            r = std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        if (!r) LOGE("persisting the durable partition state failed: {}", r.error().message());
        // Whichever round makes a durable commit frontier durable -- the checkpoint's own, or a later one after that
        // failed -- is what the journal reclaim follows.
        bool frontier_persisted = false;
        if (r) {
            std::lock_guard lk{state_mu_};
            if (image.durable_commit_lsn > persisted_durable_commit_) {
                persisted_durable_commit_ = image.durable_commit_lsn;
                frontier_persisted = true;
            }
        }
        round->complete(static_cast< bool >(r)); // never under state_mu_: resumes the round's callers
        if (frontier_persisted) request_reclaim();
    }
}

async_result< std::vector< CraftPendingEntry > > CraftReplDev::restore_state() {
    if (!state_store_) co_return std::vector< CraftPendingEntry >{};
    result< std::optional< CraftDurableState > > loaded;
    try {
        loaded = state_store_->load();
    } catch (std::exception const& e) {
        LOGE("restore_state: loading the durable partition state threw: {}", e.what());
        loaded = std::unexpected(std::make_error_condition(std::errc::io_error));
    }
    if (!loaded) {
        LOGE("restore_state: durable partition state unreadable: {}", loaded.error().message());
        co_return std::unexpected(loaded.error());
    }
    // Never persisted: a new partition. Its journal may still hold writes acked before its first persist (a write at
    // term 0 needs no login): it starts from the default image, and its journal is read as any other's.
    CraftDurableState const d = loaded->value_or(CraftDurableState{});

    auto tail = co_await journal_->tail_lsn();
    if (!tail) {
        LOGE("restore_state: journal tail unavailable: {}", tail.error().message());
        co_return std::unexpected(tail.error());
    }
    {
        std::lock_guard lk{state_mu_};
        durable_ = d;
        durable_.pending.clear(); // returned instead, and queued again by recover()
        state_.term = d.term;
        state_.client_token = d.client_token;
        login_fence_term_ = std::max(login_fence_term_, d.term);
        state_.commit_lsn = d.durable_commit_lsn;
        indexing_lsn_ = d.durable_commit_lsn;
        state_.last_append_lsn = std::max({*tail, d.sync_watermark, d.durable_commit_lsn});
        applied_sync_lsn_ = std::max(applied_sync_lsn_, d.sync_watermark);
        empty_lsns_ = std::unordered_set< int64_t >(d.empty_lsns.begin(), d.empty_lsns.end());
        missing_lsns_.clear(); // rebuild_overlay() marks every slot above commit_lsn that the journal doesn't hold
        replay_upto_ = std::max(*tail, d.durable_commit_lsn);
        last_checkpoint_lsn_ = d.durable_commit_lsn;
        reclaimed_upto_ = journal_->reclaimed_upto();
        persisted_durable_commit_ = d.durable_commit_lsn;
    }
    {
        std::lock_guard lk{apply_mu_};
        restored_raft_idx_ = d.last_applied_raft_idx;
    }
    LOGI("restore_state: term={} commit_lsn={} journal tail={} sync_watermark={} empties={} last_applied_raft_idx={} "
         "pending={}",
         d.term, d.durable_commit_lsn, *tail, d.sync_watermark, d.empty_lsns.size(), d.last_applied_raft_idx,
         d.pending.size());
    co_return d.pending;
}

// ─── journal reclaim (S8) ─────────────────────────────────────────────────────

// Without a state store nothing is persisted, so the persisted frontier stays -1 and nothing is ever reclaimed: the
// journal is then the only copy.
int64_t CraftReplDev::reclaim_floor_locked() const {
    auto const floor = std::min(persisted_durable_commit_, state_.all_committed_lsn);
    return awaiting_free_.empty() ? floor : std::min(floor, *awaiting_free_.begin() - 1);
}

void CraftReplDev::request_reclaim() {
    {
        std::lock_guard lk{state_mu_};
        if (reclaim_running_ || reclaim_unsupported_ || reclaim_stopped_ || reclaim_floor_locked() <= reclaimed_upto_)
            return;
        reclaim_running_ = true;
    }
    detail::detach(run_reclaim());
}

// The one runner: reclaims to the current floor, and again while the floor keeps rising meanwhile.
async_status CraftReplDev::run_reclaim() {
    auto self = shared_from_this();
    shared< sisl::async::value_awaitable< bool > > idle; // a stop() waiting for this runner to end
    status ended = ok();
    for (;;) {
        int64_t floor;
        {
            std::lock_guard lk{state_mu_};
            floor = reclaim_floor_locked();
            if (reclaim_stopped_ || floor <= reclaimed_upto_) {
                reclaim_running_ = false;
                idle = std::exchange(reclaim_idle_, nullptr);
                break;
            }
        }
        // An escaping exception would end the runner with reclaim_running_ still set: no reclaim would run again.
        status r;
        try {
            r = co_await journal_->reclaim_upto(floor);
        } catch (std::exception const& e) {
            LOGE("journal reclaim to {} threw: {}", floor, e.what());
            r = std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        std::lock_guard lk{state_mu_};
        if (!r) {
            if (r.error() == std::errc::operation_not_supported) {
                reclaim_unsupported_ = true;
            } else {
                LOGE("journal reclaim to {} failed: {} -- tried again on the next trigger", floor, r.error().message());
            }
            reclaim_running_ = false;
            idle = std::exchange(reclaim_idle_, nullptr);
            ended = r;
            break;
        }
        reclaimed_upto_ = floor;
        LOGD("journal reclaimed to {}", floor);
    }
    if (idle) idle->complete(true); // outside the lock: resumes stop()
    co_return ended;
}

// ─── Leader pre-resolution (S5 / SDSTOR-22907) ────────────────────────────────
//
// candidates is the union of missing_lsns_ ∩ [0, upto] and (last_append_lsn, upto] minus empty_lsns_.
//
// A slot written here is recorded the way write() records one: erased from missing_lsns_, last_append_lsn
// advanced, and any gap below it marked missing. So applying the entry doesn't mark it missing again, and
// fetch_data serves it (C2a). Each write claims its slot first, so write() and apply catch-up never journal it
// too; a slot with a write already in flight is left unresolved (H1). A slot at or below an applied
// SyncRSCommitLSN is never voted Empty: that entry already settled it as present (C2b).

async_result< CraftReplDev::PreResolution >
CraftReplDev::pre_resolve_slots(int64_t upto, uint64_t term,
                                std::optional< std::vector< craft::peer_id_t > > counted_lackers) {
    auto self = shared_from_this();
    // At login no usable reply means no quorum, not an unwired fetcher.
    auto const no_evidence = counted_lackers ? craft::make_error_condition(craft::craft_error::NO_QUORUM)
                                             : std::make_error_condition(std::errc::not_supported);
    std::vector< int64_t > candidates;
    {
        std::lock_guard lk{state_mu_};
        // Same guard shape as write()'s own k_max_ooo_gap check: upto will eventually
        // carry client/RAFT-controlled wire input (SDSTOR-22908), so it must be bounded against
        // last_append_lsn before it drives candidates' size or either loop below.
        static constexpr int64_t k_max_pre_resolve_gap = 1'000'000;
        if (upto > INT64_MAX - k_max_pre_resolve_gap) {
            LOGW("pre_resolve_slots rejected: upto={} exceeds safe LSN range", upto);
            co_return std::unexpected(make_error_condition(std::errc::invalid_argument));
        }
        if (upto - state_.last_append_lsn > k_max_pre_resolve_gap) {
            LOGW("pre_resolve_slots rejected: upto={} too far ahead of last_append_lsn={}", upto,
                 state_.last_append_lsn);
            co_return std::unexpected(make_error_condition(std::errc::value_too_large));
        }
        if (!session_current_locked(term)) {
            LOGW("pre_resolve_slots: term moved past the caller's ({} -> {}) -- abandoning upto={}", term, state_.term,
                 upto);
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
        }
        candidates.reserve(missing_lsns_.size() +
                           static_cast< size_t >(std::max< int64_t >(0, upto - state_.last_append_lsn)));
        // missing_lsns_ is ordered, so once an entry exceeds upto every remaining entry does too --
        // stop instead of scanning the rest of the set.
        for (int64_t lsn : missing_lsns_) {
            if (lsn > upto) break;
            candidates.push_back(lsn);
        }
        for (int64_t lsn = state_.last_append_lsn + 1; lsn <= upto; ++lsn) {
            if (!empty_lsns_.contains(lsn)) candidates.push_back(lsn);
        }
    }
    if (candidates.empty()) co_return PreResolution{};

    if (peer_fetcher_ == nullptr || raft_proposer_ == nullptr) {
        LOGW("pre_resolve_slots: {} slot(s) <= {} unresolved but peer_fetcher_/raft_proposer_ not wired -- "
             "refusing to resolve",
             candidates.size(), upto);
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }
    if (!raft_proposer_->is_leader()) {
        LOGW("pre_resolve_slots: not leader -- refusing to resolve upto={}", upto);
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::NOT_LEADER));
    }

    auto responses = co_await peer_fetcher_->fetch_from_quorum(candidates, peer_fetch_timeout_ms_);
    if (!responses) {
        LOGE("pre_resolve_slots: fetch_from_quorum failed: {}", responses.error().message());
        co_return std::unexpected(responses.error());
    }

    // Checked again: leadership or the session may have changed during the fetch -- bail before any
    // local write rather than journal under a stale term.
    if (!raft_proposer_->is_leader()) {
        LOGW("pre_resolve_slots: lost leadership during quorum fetch -- abandoning upto={}", upto);
        co_return std::unexpected(craft::make_error_condition(craft::craft_error::NOT_LEADER));
    }
    // At or below settled_floor, an applied (or this leader's committed) SyncRSCommitLSN already settled every
    // slot, so one this leader still lacks there was settled present and never gets a fresh Empty (C2b).
    int64_t settled_floor;
    {
        std::lock_guard lk{state_mu_};
        if (!session_current_locked(term)) {
            LOGW("pre_resolve_slots: term changed during quorum fetch ({} -> {}) -- abandoning upto={}", term,
                 state_.term, upto);
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
        }
        settled_floor = std::max(applied_sync_lsn_, last_proposed_sync_lsn_);
    }
    if (responses->empty()) {
        LOGE("pre_resolve_slots: fetch_from_quorum returned zero responding members for {} candidate(s)",
             candidates.size());
        co_return std::unexpected(no_evidence);
    }

    // Empty beats data even across members, regardless of response order: seeing Empty for an lsn purges
    // any data an earlier-processed member already reported for it (data-then-empty order), and the guard
    // on the emplace below blocks a later member's stale data from being (re-)inserted once an earlier
    // member has already verdicted the same lsn Empty (empty-then-data order).
    std::unordered_map< int64_t, JournalSlot* > data_by_lsn;
    data_by_lsn.reserve(candidates.size());
    std::unordered_set< int64_t > empty_by_quorum;
    empty_by_quorum.reserve(candidates.size());
    // Only distinct voters other than this leader, and at login only quiesced ones, count toward quorum-lacks evidence;
    // any other reply still contributes its slots (VoterTally).
    VoterTally tally{"pre_resolve_slots", raft_proposer_->my_id(), raft_proposer_->voters(),
                     std::move(counted_lackers)};
    int valid_member_count = 0; // usable replies, counted or not
    for (auto& reply : *responses) {
        if (auto bad_lsn = validate_fetch_response(candidates, reply.slots); bad_lsn) {
            // Reject only this member's response, not the whole quorum fetch -- a broadcast should
            // tolerate one misbehaving member without losing legitimate evidence from the rest.
            LOGW("pre_resolve_slots: a quorum member's response named lsn={} not requested (or duplicated) -- "
                 "discarding that member's entire reply",
                 *bad_lsn);
            continue;
        }
        if (tally.add(reply.member) == VoterTally::Reply::drop) continue;
        ++valid_member_count;
        for (auto& slot : reply.slots) {
            if (slot.is_empty) {
                empty_by_quorum.insert(slot.lsn);
                data_by_lsn.erase(slot.lsn);
                continue;
            }
            if (!empty_by_quorum.contains(slot.lsn) && !data_by_lsn.contains(slot.lsn))
                data_by_lsn.emplace(slot.lsn, &slot);
        }
    }
    if (valid_member_count == 0) {
        // No usable reply (every one malformed or this member's own). Fail closed rather than
        // falling through to mint an Empty verdict for every candidate with nothing behind it.
        LOGE("pre_resolve_slots: none of {} reply(ies) is usable -- no trustworthy evidence for {} candidate(s)",
             responses->size(), candidates.size());
        co_return std::unexpected(no_evidence);
    }

    // Every candidate is a slot this leader lacks, so it counts itself among the members lacking it.
    bool const quorum_lacks = tally.has_quorum();
    if (!quorum_lacks)
        LOGW("pre_resolve_slots: {} distinct voter(s) + self < quorum {} -- no fresh Empty verdicts", tally.counted(),
             tally.quorum_size());

    auto const term_changed = [this, term] {
        std::lock_guard lk{state_mu_};
        return !session_current_locked(term);
    };

    PreResolution result;
    size_t capped = 0; // slots left unresolved by the missing_lsns_ cap
    for (int64_t lsn : candidates) {
        // A responder's Empty was already committed, so it wins over anything local.
        if (empty_by_quorum.contains(lsn)) {
            result.empty_slots.push_back(lsn);
            continue;
        }
        auto it = data_by_lsn.find(lsn);
        if (it == data_by_lsn.end()) {
            // Checked now, not before the fetch: a write() in flight owns the slot, and one that finished means
            // this leader holds it rather than lacking it.
            SlotClaim local;
            {
                std::lock_guard lk{state_mu_};
                local = peek_slot_write_locked(lsn);
            }
            if (local == SlotClaim::in_flight) {
                result.unresolved.push_back(lsn);
            } else if (local == SlotClaim::claimed) {
                // No responding member reported data or Empty: Empty only on quorum-lacks evidence, and only
                // above what an applied entry already settled.
                (quorum_lacks && lsn > settled_floor ? result.empty_slots : result.unresolved).push_back(lsn);
            }
            continue; // held: this leader has it now
        }
        JournalSlot* slot = it->second;
        SlotClaim claim{};
        bool at_capacity;
        {
            std::lock_guard lk{state_mu_};
            // A failed write above continues without a term check; claiming here would block the new session's
            // own write to this slot.
            if (!session_current_locked(term)) {
                LOGW("pre_resolve_slots: term changed mid-resolution -- abandoning at lsn={}", lsn);
                co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
            }
            // Same cap write() enforces: journaling this slot would open a gap past it.
            at_capacity = missing_at_capacity_locked(lsn);
            if (!at_capacity) claim = claim_slot_write_locked(lsn);
        }
        if (at_capacity) {
            result.unresolved.push_back(lsn);
            ++capped;
            continue;
        }
        if (claim == SlotClaim::in_flight) result.unresolved.push_back(lsn); // a write() in flight owns the slot
        if (claim != SlotClaim::claimed) continue;                           // held: it landed meanwhile
        SlotWriteClaim const release{this, lsn};
        // Same alloc -> write_slot -> blkid-guarded cleanup-on-failure pattern as
        // apply_sync_rs_commit_lsn's own catch-up loop -- see its comments for the HS_DATA_LINKED
        // rationale this mirrors.
        homestore::multi_blk_id blkid{};
        bool blkid_allocated = false;
        if (!slot->all_zeros) {
            auto alloc_res = co_await journal_->alloc_write_data(slot->data, slot->len_bytes);
            if (!alloc_res) {
                LOGE("pre_resolve_slots: alloc_write_data failed lsn={}: {} -- leaving as unresolved", slot->lsn,
                     alloc_res.error().message());
                result.unresolved.push_back(slot->lsn);
                continue;
            }
            blkid = *alloc_res;
            blkid_allocated = true;
        }
        // Each slot's IO is another await: a new session must never see this one's slots journalled, nor
        // missing_lsns_ touched. Nothing references blkid yet, so it's safe to free here.
        if (term_changed()) {
            if (blkid_allocated) {
                if (auto fr = co_await journal_->free_data(blkid); !fr)
                    LOGE("pre_resolve_slots: free_data failed lsn={}: {}", slot->lsn, fr.error().message());
            }
            LOGW("pre_resolve_slots: term changed mid-resolution -- abandoning at lsn={}", slot->lsn);
            co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
        }
        auto res = co_await journal_->write_slot(slot->lsn, term, slot->lba_off_bytes, slot->len_bytes, blkid,
                                                 slot->all_zeros, slot->csums);
        if (!res) {
            LOGE("pre_resolve_slots: write_slot failed lsn={}: {} -- leaving as unresolved", slot->lsn,
                 res.error().message());
            if (res.error() == std::errc::timed_out) { // as in write()
                fault();
                co_return std::unexpected(res.error());
            }
            if (blkid_allocated) {
                if (auto fr = co_await journal_->free_data(blkid); !fr)
                    LOGE("pre_resolve_slots: free_data failed after write_slot failure lsn={}: {}", slot->lsn,
                         fr.error().message());
            }
            result.unresolved.push_back(slot->lsn);
            continue;
        }
        if (blkid_allocated) { // as in write()
            if (auto r = journal_->commit_data(blkid, /* recovering = */ false); !r) {
                fault();
                co_return std::unexpected(r.error());
            }
        }
        bool voided = false;
        {
            // Term checked in the same critical section as the erase. If it changed during write_slot, the
            // entry stays journalled (blkid not freed -- the journal references it, same as write()'s
            // post-flight) under its stale term for the next login to truncate; missing_lsns_ is untouched.
            std::lock_guard lk{state_mu_};
            if (!session_current_locked(term)) {
                LOGW("pre_resolve_slots: term changed during write_slot -- abandoning at lsn={}", slot->lsn);
                co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
            }
            // An Empty verdict applied during write_slot found the slot still missing, so it freed nothing and had
            // no entry to retire: the fill is void, as in write()'s post-flight.
            if (empty_lsns_.contains(slot->lsn)) {
                voided = true;
            } else {
                missing_lsns_.erase(slot->lsn);
                // Record a slot filled above the frontier the way write() does (C2a).
                for (int64_t gap = state_.last_append_lsn + 1; gap < slot->lsn; ++gap)
                    if (!empty_lsns_.contains(gap)) missing_lsns_.insert(gap);
                state_.last_append_lsn = std::max(state_.last_append_lsn, slot->lsn);
                // Readable like a write()'s slot until commit indexes it. A later Empty verdict finds the slot
                // present and retires this entry.
                populate_overlay_from_slot(slot->lsn, *slot, blkid);
            }
        }
        // Freed while the claim is still held, so no other writer can journal the slot in between.
        if (voided && blkid_allocated) co_await free_voided(slot->lsn, blkid);
    }
    if (capped > 0)
        LOGW("pre_resolve_slots: missing_lsns_ at capacity -- left {} slot(s) above the frontier unresolved", capped);
    co_return result;
}

// ─── RAFT apply helpers (S5 implements) ──────────────────────────────────────
//
// A SyncRSCommitLSN applies in two halves: install_sync_rs_commit_lsn() changes its state at commit (on_commit), and
// finish_sync_rs_commit_lsn() does its I/O when the apply queue reaches it.
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

result< std::vector< int64_t > > CraftReplDev::install_sync_rs_commit_lsn(int64_t rs_commit_lsn,
                                                                          std::vector< int64_t > const& empty_slots) {
    // Validated before any state is touched -- an out-of-range verdict means the entry itself cannot be
    // trusted, not that this one slot should be skipped, so it gates the entire apply.
    for (int64_t lsn : empty_slots) {
        if (lsn < 0 || lsn > rs_commit_lsn) {
            LOGE("apply_sync_rs_commit_lsn: empty_slots lsn={} out of range [0, {}] -- rejecting entire apply", lsn,
                 rs_commit_lsn);
            return std::unexpected(make_error_condition(volume_error::INVALID_ENTRY));
        }
    }

    std::unordered_set< int64_t > to_free;
    std::lock_guard lk{state_mu_};
    // The entry's client_token is NOT gated against state_.client_token. Per the login sequence (CRAFT-Design),
    // SyncRSCommitLSN applies BEFORE InternalLogin (which sets state_.client_token), so an equality-fence
    // here would veto the very entry that carries login's own Empty verdicts,
    // and would also veto every post-restart watchdog SyncRSCommitLSN, since state_ is
    // in-memory-only and client_token resets to 0 across a restart. craft_client's reference
    // (MemCraftReplica::cold_apply_sync) discards the parameter outright for the same reason.
    // Exclusivity comes from RAFT's commit ordering plus the term fence every other IO already
    // checks (see apply_internal_login's header comment), not from an equality check here.

    // A verdict can't void a slot this replica already applied to its index, or one a running commit_impl() has
    // started indexing: freeing it would leave the index pointing at free blocks.
    for (int64_t lsn : empty_slots) {
        if (lsn <= std::max(state_.commit_lsn, indexing_lsn_) && !empty_lsns_.contains(lsn)) {
            LOGE("apply_sync_rs_commit_lsn: Empty verdict for lsn={} at or below commit_lsn={} (indexing_lsn={}) "
                 "-- faulting the partition rather than freeing indexed blocks",
                 lsn, state_.commit_lsn, indexing_lsn_);
            fault();
            return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
        }
    }
    applied_sync_lsn_ = std::max(applied_sync_lsn_, rs_commit_lsn);

    for (int64_t lsn : empty_slots) {
        bool const was_missing = missing_lsns_.erase(lsn) > 0;
        if (!was_missing && lsn <= state_.last_append_lsn && !empty_lsns_.contains(lsn)) { to_free.insert(lsn); }
    }
    empty_lsns_.insert(empty_slots.begin(), empty_slots.end());

    // Everything newly spanned by this advance that isn't Empty-verdicted is a gap until catch-up
    // resolves it -- same idiom write() uses for gaps opened by an out-of-order dlsn.
    for (int64_t lsn = state_.last_append_lsn + 1; lsn <= rs_commit_lsn; ++lsn) {
        if (!empty_lsns_.contains(lsn)) missing_lsns_.insert(lsn);
    }
    state_.last_append_lsn = std::max(state_.last_append_lsn, rs_commit_lsn);

    // The image takes the verdicts and the watermark now, with the state change: every block a verdict voids, here or
    // by a write or fill it beats, is freed only once an image holding the verdict is durable (free_voided()). It keeps
    // them as long as empty_lsns_ does. Without a store nothing is persisted.
    // ponytail: every verdict for the partition's lifetime, in memory and in the image; prune below the client's
    // acknowledged commit if Empty verdicts ever pile up.
    if (state_store_) durable_.empty_lsns.insert(empty_slots.begin(), empty_slots.end());
    durable_.sync_watermark = std::max(durable_.sync_watermark, rs_commit_lsn);

    // The voided slots stop being readable now, not once their blocks are freed: commit never retires their overlay
    // entries, since its advance steps over Empty slots.
    for (int64_t lsn : to_free)
        retire_overlay_slot(lsn);
    awaiting_free_.insert(to_free.begin(), to_free.end());
    return std::vector< int64_t >(to_free.begin(), to_free.end());
}

async_status CraftReplDev::finish_sync_rs_commit_lsn(int64_t rs_commit_lsn, uint64_t client_token,
                                                     std::vector< int64_t > to_free) {
    // Lives in the coroutine frame across every co_await below, so the device outlives the apply even if every
    // external owner drops it mid-apply (drain_apply() holds one too). Requires a shared_ptr-owned CraftReplDev.
    auto self = shared_from_this();

    // The verdicts are durable before any of their blocks is freed (free_voided() says why): one persist for them all.
    if (!to_free.empty()) {
        if (auto r = co_await persist_state(); !r) {
            LOGE("apply_sync_rs_commit_lsn: verdicts not durable: {} -- leaking {} voided slot(s) rather than risk "
                 "freeing them twice",
                 r.error().message(), to_free.size());
        } else {
            for (int64_t lsn : to_free) {
                if (auto fr = co_await journal_->free_slot(lsn); !fr) {
                    LOGE("apply_sync_rs_commit_lsn: free_slot failed lsn={}: {} -- blocks may leak", lsn,
                         fr.error().message());
                }
            }
        }
        {
            std::lock_guard lk{state_mu_};
            for (int64_t lsn : to_free) {
                if (auto it = awaiting_free_.find(lsn); it != awaiting_free_.end()) awaiting_free_.erase(it);
            }
        }
        request_reclaim(); // the reclaim may pass them now
    }

    std::vector< int64_t > to_fetch;
    uint64_t term;
    {
        std::lock_guard lk{state_mu_};
        term = state_.term; // stamps the catch-up writes
        for (int64_t lsn : missing_lsns_) {
            if (lsn > rs_commit_lsn) break; // missing_lsns_ is ordered
            to_fetch.push_back(lsn);
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
                    std::lock_guard lk{state_mu_};
                    empty_lsns_.insert(slot.lsn);
                    missing_lsns_.erase(slot.lsn);
                    continue;
                }
                // Leave a slot with a write in flight to that write, and skip one that landed meanwhile (H1).
                SlotClaim claim;
                {
                    std::lock_guard lk{state_mu_};
                    claim = claim_slot_write_locked(slot.lsn);
                }
                if (claim != SlotClaim::claimed) continue;
                SlotWriteClaim const release{this, slot.lsn};
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

                // FIXME: write_slot has no all_zeros branch -- it serializes whatever blkid it's given
                // relying on multi_blk_id's own serialize()/serialized_size() to degrade safely for a
                // default instance. That's an implicit, undocumented dependency on HomeStore's current
                // behavior -- see SDSTOR-25613.
                auto res = co_await journal_->write_slot(slot.lsn, term, slot.lba_off_bytes, slot.len_bytes, blkid,
                                                         slot.all_zeros, slot.csums);
                if (!res) {
                    LOGE("apply_sync_rs_commit_lsn: write_slot failed lsn={}: {} -- leaving as missing", slot.lsn,
                         res.error().message());
                    if (res.error() == std::errc::timed_out) { // as in write()
                        fault();
                        co_return std::unexpected(res.error());
                    }
                    // Awaited, not detached: once the apply ends, stop_apply() may let the volume release the chunks.
                    if (blkid_allocated) {
                        if (auto fr = co_await journal_->free_data(blkid); !fr)
                            LOGE("apply_sync_rs_commit_lsn: free_data failed after write_slot failure lsn={}: {}",
                                 slot.lsn, fr.error().message());
                    }
                    continue;
                }
                if (blkid_allocated) { // as in write()
                    if (auto r = journal_->commit_data(blkid, /* recovering = */ false); !r) {
                        fault();
                        co_return std::unexpected(r.error());
                    }
                }
                bool voided;
                {
                    std::lock_guard lk{state_mu_};
                    // Same as pre_resolve_slots' fills: an Empty verdict applied during write_slot leaves this fill
                    // void; otherwise it's readable until commit indexes it.
                    voided = empty_lsns_.contains(slot.lsn);
                    if (!voided) {
                        missing_lsns_.erase(slot.lsn);
                        populate_overlay_from_slot(slot.lsn, slot, blkid);
                    }
                }
                // Freed while the claim is still held, as in pre_resolve_slots.
                if (voided && blkid_allocated) co_await free_voided(slot.lsn, blkid);
            }
        }
    }

    // commit_lsn (CRAFT-Design) is the LOCAL CONTIGUOUS prefix, distinct from rs_commit_lsn (the
    // replica-set-wide watermark RAFT already agreed on): it must skip over Empty slots but never
    // advance past an unresolved Missing one, even if catch-up above left holes below rs_commit_lsn.
    // Mirrors craft_client's reference MemCraftReplica::apply_up_to.
    //
    // With an index, the advance goes through commit(), which applies each slot it passes to the index (same rule:
    // skip Empty, stop at Missing) and retires its overlay entry. Moving commit_lsn without that left those slots
    // unapplied: commit() never revisits a slot below commit_lsn, and a read serves overlay entries only above it, so
    // a read of such an LBA returned the older index value. A commit() already running is left to finish; until the
    // next write, keep_alive or Sync carries the advance further, reads still find the slots in the overlay.
    bool has_index = indx_tbl_ != nullptr;
#ifdef _PRERELEASE
    has_index = has_index || static_cast< bool >(test_write_fn_);
#endif
    if (has_index) {
        if (auto r = co_await commit(rs_commit_lsn); !r) {
            LOGE("apply_sync_rs_commit_lsn: commit({}) failed: {}", rs_commit_lsn, r.error().message());
        } else {
            LOGT("apply_sync_rs_commit_lsn ok rs_commit_lsn={} client_token={}", rs_commit_lsn, client_token);
        }
        co_return ok();
    }
    // No index (the write-path-only unit tests): nothing to apply, the frontier alone moves.
    int64_t commit_lsn_snapshot;
    bool should_checkpoint;
    {
        std::lock_guard lk{state_mu_};
        int64_t next = state_.commit_lsn + 1;
        while (next <= rs_commit_lsn && !missing_lsns_.contains(next)) {
            state_.commit_lsn = next; // resolved (present or Empty) -- Empty is skipped, not gated on
            ++next;
        }
        commit_lsn_snapshot = state_.commit_lsn;
        // SDSTOR-22888: nudge HomeStore to checkpoint proactively rather than waiting on its own
        // timer, so the journal-reclaim / RAFT-log-compaction floor (docs/craft/subtasks.md's S8)
        // doesn't lag arbitrarily far behind commit_lsn. Also invoked from commit_impl() so that
        // write()'s piggyback and keep_alive() (which don't go through this function) advance the
        // same checkpoint cadence -- see checkpoint_interval_crossed_locked()'s doc comment.
        should_checkpoint = checkpoint_interval_crossed_locked(commit_lsn_snapshot);
    }
    if (should_checkpoint) fire_checkpoint_trigger(commit_lsn_snapshot);
    LOGT("apply_sync_rs_commit_lsn ok rs_commit_lsn={} client_token={}", rs_commit_lsn, client_token);
    co_return ok();
}

async_status CraftReplDev::apply_sync_rs_commit_lsn(int64_t rs_commit_lsn, uint64_t client_token,
                                                    std::vector< int64_t > empty_slots) {
    auto to_free = install_sync_rs_commit_lsn(rs_commit_lsn, empty_slots);
    if (!to_free) co_return std::unexpected(to_free.error());
    co_return co_await finish_sync_rs_commit_lsn(rs_commit_lsn, client_token, std::move(*to_free));
}

// ─── InternalLogin apply (S5 / SDSTOR-22887) ─────────────────────────────────
//
// No journal I/O, no peer fetch; the apply queue runs it in log order like any other entry.
// "Enforce single-writer exclusivity" needs no explicit rejection here: every other RPC's term-fence
// check (STALE_TERM on mismatch) already does that. Overwriting state_.term is what invalidates any
// existing session -- a caller still presenting the old term is fenced out on its very next call.
//
// An entry whose term isn't newer than the session's is ignored outright, token included: a replay, or a second login
// that proposed the same term after the first one took it. Entries apply in log order, so every replica ignores the
// same ones.
//
// A newer session is made durable before it is admitted: once a write of it is acked, a restart must come back with
// its term, or the old session's writes would be admitted again. If persisting fails the session isn't admitted (the
// login fence raised at commit keeps the old one out), and the partition faults: deciding later logins against a
// session the others moved past, it would admit one at this term that every other replica ignores.

async_status CraftReplDev::apply_internal_login(uint64_t client_token, uint64_t term) {
    bool newer;
    {
        std::lock_guard lk{state_mu_};
        newer = term > state_.term;
        if (newer) {
            durable_.term = std::max(durable_.term, term);
            durable_.client_token = client_token;
        } else {
            LOGI("apply_internal_login: term {} isn't newer than the session's {} -- ignored (client_token={})", term,
                 state_.term, client_token);
        }
    }
    if (!newer) co_return std::unexpected(craft::make_error_condition(craft::craft_error::STALE_TERM));
    if (auto r = co_await persist_state(); !r) {
        LOGE("apply_internal_login: persisting session term={} failed: {} -- new session NOT admitted, partition "
             "faulted",
             term, r.error().message());
        fault();
        co_return r;
    }
    std::lock_guard lk{state_mu_};
    // Still newer: the session changes only here, one entry at a time, and at restore before the queue runs.
    state_.client_token = client_token;
    state_.term = term;
    client_commit_lsn_ = -1; // a new session starts with no confirmed prefix of its own
    LOGD("apply_internal_login client_token={} term={}", client_token, term);
    co_return ok();
}

} // namespace homeblocks
