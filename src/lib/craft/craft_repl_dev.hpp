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
#pragma once

#include "../hb_internal.hpp"
#include "craft_raft_entries.hpp"
#include "../volume/index_fixed_kv.hpp" // BlockInfo -- the shape commit()'s index write callback uses
#include <homestore/replication/repl_dev.hpp>
#include <iomgr/timer.hpp> // iomgr::timer_token -- RAII recurring timer used by the watchdog

#include <atomic>
#include <chrono>
#include <functional>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace homestore {
class home_log_store;
}

namespace homeblocks {

class VolumeIndexTable;

// ─── CRAFT vocabulary vs. this backend's own state ───────────────────────────
//
// The client-facing vocab (craft::client_hdr, craft::lsn_pair, craft::LoginResult, craft::read_result,
// craft::resolution_result, craft::io_extent, craft::craft_error) comes from the craft_client package's
// <craft/types.hpp> -- HomeStore-free, pulled in via hb_internal.hpp. That is the ONLY thing HomeBlocks takes from
// craft_client: the vocabulary the wire is defined in. HomeBlocks is the CRAFT *backend* -- the far side of the wire --
// so it implements no craft_replica and never constructs a CRAFT client (there is no make_client anywhere in this
// repo).
//
// CraftPartitionState and JournalSlot below are this backend's OWN state, in its own BLOCK units (lba_t /
// lba_count_t): neither crosses the client wire. The reference model in craft_client keeps its own copies for
// exactly the same reason -- two independent replica implementations, one shared wire.

// Per-partition CRAFT state. Authoritative in memory; recovered from the journal + superblock on restart.
struct CraftPartitionState {
    int64_t commit_lsn{-1};        // contiguous committed prefix (== Synced)
    int64_t last_append_lsn{-1};   // highest appended dLSN (may be uncommitted)
    uint64_t client_token{0};      // token from the last successful InternalLogin
    uint64_t term{0};              // current session term
    int64_t all_committed_lsn{-1}; // client-computed set-wide min commit_lsn, piggybacked on keep_alive/write;
                                   // floors journal reclaim (S8: truncate below min(this, checkpointed apply
                                   // frontier)) -- S3 only captures it, the reclaim action itself is S8's job.
                                   // TODO(S8): this is client-controlled wire input with only a narrow
                                   // malformed-negative check applied where it's captured (write()/read()/
                                   // keep_alive()) -- there is NO per-call bound possible there: a lagging
                                   // replica legitimately sees this arrive ahead of its own last_append_lsn/
                                   // commit_lsn (that gap is the signal it needs to catch up via S6 peer
                                   // exchange), so clamping against local state would break that. Before using
                                   // this as a reclaim floor, S8 must validate it against real journal/
                                   // checkpoint state (e.g. never reclaim past what's actually durable/
                                   // replicated here), since a buggy/malicious client could otherwise poison
                                   // it with an absurdly large value with no local signal to catch it.
};

// One journal slot returned by fetch_data() (server-to-server resync; never crosses the CLIENT wire). Four-way:
// data (is_empty=false, all_zeros=false), zero write (all_zeros=true, no data), Empty (is_empty=true), or
// omitted from the response (not-present-here).
struct JournalSlot {
    int64_t lsn{-1};
    bool is_empty{false};
    bool all_zeros{false};
    lba_t lba_off_bytes{0};
    lba_count_t len_bytes{0};
    sisl::sg_list data{};
    homestore::multi_blk_id blkid{};          // block reference (empty for all_zeros slots)
    std::vector< homestore::csum_t > csums{}; // one per LBA in range; empty for all_zeros
};

// ─── journal backend abstraction ─────────────────────────────────────────────
//
// Injected into CraftReplDev so unit tests can supply a mock without touching
// HomeStore. Production code passes HomeStoreCraftJournalBackend (defined in
// craft_repl_dev.cpp).

class CraftJournalBackend {
public:
    // Allocate blocks and write the data payload. Called BEFORE write_slot for non-zero writes.
    // all_zeros=true and empty data bypass this; write_slot receives an empty multi_blk_id.
    virtual async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const& data, lba_count_t len) = 0;
    // term is the session term captured from state_.term at write() time — stored in
    // CraftJournalEntry so recovery can skip stale-tail entries written under a deposed leader.
    // csums is one crc16 per LBA in [lba, lba+len) (byte len / this device's lba_size), computed by
    // the caller while the data is still in memory; empty for all_zeros (no data, nothing to sum).
    virtual async_status write_slot(int64_t lsn, uint64_t term, lba_t lba, lba_count_t len,
                                    homestore::multi_blk_id blkid, bool all_zeros,
                                    std::vector< homestore::csum_t > const& csums) = 0;
    virtual async_result< JournalSlot > read_slot(int64_t lsn) = 0;
    // Drop all entries with seq_num > lsn; lsn becomes the new journal tail.
    virtual async_status truncate_to(int64_t lsn) = 0;
    // Release blocks previously allocated by alloc_write_data. Called when write_slot fails or
    // when the write is discarded post-flight (stale term). Free errors are logged but non-fatal.
    virtual async_status free_data(homestore::multi_blk_id blkid) = 0;
    // TODO: Need to revisit this if this func can be avoided
    // Reads the already-committed local entry at lsn and, if it isn't all_zeros, frees the blkid
    // it references via free_data. Local-only by design: unlike read_slot/JournalSlot (the
    // wire-shared type used to answer a peer's fetch_data), a blkid has no meaning off this
    // replica, so this never needs to leave the local backend. Used by apply_sync_rs_commit_lsn's
    // to_free path to reclaim blocks under an entry a later SyncRSCommitLSN verdicts Empty.
    virtual async_status free_slot(int64_t lsn) = 0;
    // Read the data payload referenced by blkid into dest (dest.size already set by the caller to
    // blkid's byte length). Used by CraftReplDev::read() to fetch the bytes an index/overlay entry
    // only stores a block reference for. Mockable so read()'s tests stay light (no real HomeStore).
    virtual async_status read_data(homestore::multi_blk_id blkid, sisl::sg_list& dest) = 0;
    virtual ~CraftJournalBackend() = default;
};

// Factory that wraps a HomeStore log store. Used by volume.cpp when creating a CRAFT-mode volume.
// vol_ordinal must match vol_info_->ordinal so async_alloc_write routes to this volume's chunks.
// lba_size is the volume's per-block byte size -- read_slot() needs it to derive nlbas from a
// slot's on-disk byte length. Tests inject MockCraftJournalBackend directly.
unique< CraftJournalBackend > make_homestore_journal_backend(shared< homestore::home_log_store > logstore,
                                                             uint64_t vol_ordinal, uint32_t lba_size);

// ─── CraftPeerFetcher ─────────────────────────────────────────────────────────
//
// Abstraction over the server-to-server peer plane (mirrors craft::peer::craft_peer in
// craft_client 1:1, so a wire-backed implementation can forward each call straight into
// craft_client's peer codec (peer_codec.cpp) with no translation). Injected into CraftReplDev
// so unit tests can stub peer communication without a live network. Production wires
// CraftConnector (S9). Default (null) leaves catch-up/resolution stubbed.

class CraftPeerFetcher {
public:
    virtual async_result< craft::lsn_pair > get_rs_commit_lsn(uint64_t term, bool is_login) = 0;
    // `timeout_ms` is the deadline this call must complete within (CraftReplDev passes
    // peer_fetch_timeout_ms_, set from home_blks_config.fbs's peer_fetch_timeout_ms). A real transport
    // (S9) must treat a missed deadline as a hard failure, same as an unreachable peer -- this interface
    // only carries the contract; there's nothing to enforce yet since today's only implementations are
    // direct function calls (production is unwired, tests call synchronously).
    virtual async_result< std::vector< JournalSlot > > fetch_data(const std::vector< int64_t >& lsns,
                                                                  uint32_t timeout_ms) = 0;
    virtual ~CraftPeerFetcher() = default;
};

// ─── CraftReplDev ─────────────────────────────────────────────────────────────
//
// One instance per CRAFT-mode volume. Implements the full CRAFT data plane
// (write, read, login, truncate, ...) on top of a HomeStore log store and
// index. Non-CRAFT volumes are unaffected.

class CraftReplDev : public std::enable_shared_from_this< CraftReplDev > {
#ifdef _PRERELEASE
    // Lets test_craft_raft_entries.cpp call apply_sync_rs_commit_lsn (private) directly, so it can assert
    // on the exact result rather than only on-commit's discarded fire-and-forget outcome.
    friend class CraftRaftEntriesTest;
#endif

private:
    // Index write/delete operation shapes commit_impl() (and its test seam, commit_with()) are
    // parameterized over -- declared here, ahead of use, since a member function's declared parameter
    // types (unlike default-argument expressions) are not part of the class's deferred "complete-
    // class" lookup context and so must already be visible at the point of each declaration below.
    using write_index_fn_t = std::function< status(lba_t, lba_t, std::unordered_map< lba_t, BlockInfo >&) >;
    using delete_index_fn_t = std::function< status(lba_t, lba_t, std::vector< homestore::blk_id >&) >;
    // Same shape as VolumeIndexTable::read_from_index -- read_impl() (and its test seam, read_with())
    // are parameterized over it for the same reason commit_impl()/commit_with() are.
    using index_kv_list_t = std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >;
    using read_index_fn_t = std::function< status(lba_t, lba_t, index_kv_list_t&) >;

    // Private -- see create() below. shared_from_this() (used by apply_sync_rs_commit_lsn's detached
    // coroutine) requires the object to already be owned by a shared_ptr, so construction is gated behind
    // create() rather than exposed directly. lba_size is the volume's per-block byte size, fixed for the
    // volume's lifetime -- used to derive the per-write checksum-array length (len bytes / lba_size) on
    // the write path. indx_tbl is the volume's own index table, applied to by commit(); nullptr in tests
    // that only exercise the write path (commit()/overlay logic must no-op safely in that case).
    // The watchdog timeout is read from HB_DYNAMIC_CONFIG(craft_watchdog_timeout_ms); 0 disables it.
    // Tests override the config key in SetUp and restore it in TearDown.
    explicit CraftReplDev(volume_id_t vol_id, unique< CraftJournalBackend > journal, uint32_t lba_size,
                          shared< VolumeIndexTable > indx_tbl);

public:
    static shared< CraftReplDev > create(volume_id_t vol_id, unique< CraftJournalBackend > journal, uint32_t lba_size,
                                         shared< VolumeIndexTable > indx_tbl) {
        return shared< CraftReplDev >(new CraftReplDev(vol_id, std::move(journal), lba_size, std::move(indx_tbl)));
    }
    // Cancels the watchdog's recurring timer (iomgr::timer_token::cancel(wait=true)), blocking until
    // any in-flight tick has finished, so on_watchdog_tick() (which captures `this`) can never fire
    // against a destroyed object. No-op if the watchdog was never armed.
    ~CraftReplDev();

    // ── client-facing ──────────────────────────────────────────────────────
    //
    // These are 1:1 with the public CRAFT free functions over a volume_handle (home_blocks.hpp), which are in turn
    // 1:1 with the wire ops -- so a CRAFT server (CraftConnector) decodes a request and forwards it here with
    // nothing to translate. Hence their shape: BYTE-addressed (addr/len are absolute byte offsets, block-aligned to
    // the volume's lba_size; the byte<->block conversion is confined INSIDE this class, which owns the index), and
    // every op carries the wire's craft::client_hdr -- {term, commit_lsn, all_committed_lsn}. The term FENCES the
    // op (craft_error::STALE_TERM on mismatch); commit_lsn is the piggybacked commit that advances the frontier.
    // There is no standalone commit verb on the wire: every IO is its carrier.

    // Full login sequence (leader-only): the RAFT leader assigns the session TERM and returns it here -- a server
    // forwards LOGIN, it does not mint terms. Serialized: at most one in-flight login per partition. A follower
    // returns craft::LoginResult{term=0, leader_hint} (a redirect, NOT an error) so the client can retry.
    async_result< craft::LoginResult > login(uint64_t client_token);

    // Explicit end of session. Term-fenced; the leader propagates InternalLogout so subsequent IO at the old term
    // is rejected STALE_TERM. craft_error::NOT_LEADER on a follower.
    async_status logout(craft::client_hdr hdr);

    // Append data at the client-assigned dLSN. Zero-copy; does NOT apply to the LBA index (hdr.commit_lsn drives
    // that). Pass empty `data` for a WRITE_ZEROES/unmap over [addr, addr+len); pass non-empty `data` of exactly
    // `len` bytes for a data write. The write kind is derived from data.empty(). The ack returns the achieved
    // {commit_lsn, last_append_lsn} snapshotted with the append -- every CRAFT IO response piggybacks the
    // watermarks (the wire's write_rsp), so any round-trip refreshes the client's model of this member.
    async_result< craft::lsn_pair > write(craft::client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                          sisl::sg_list data);

    // read_lsn is the horizon H: serve the latest version <= H for [addr, addr+len), from the LBA index if applied
    // or from the journal-tail overlay if only Appended (no index write on the read path). Never fetches from a
    // peer. Fills the caller-owned `dest` in place (data -> bytes, holes -> zeros) and returns craft::read_result:
    // the sparse layout (which byte sub-ranges were data vs holes) plus the piggybacked watermarks.
    async_result< craft::read_result > read(craft::client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len,
                                            sisl::sg_list dest);

    // Advance the frontier toward hdr.commit_lsn + reset the client-liveness watchdog -- which is WHY it is
    // term-fenced: a deposed client must not be able to keep its session alive. hdr.all_committed_lsn is the
    // client-computed set-wide min commit_lsn; captured into state_ (max-monotonic, never regresses on a
    // stale/reordered message) so a floor of min(all_committed_lsn, checkpointed apply frontier) is available
    // for S8's journal reclaim -- the reclaim ACTION itself is S8's job, not implemented here. Returns the
    // achieved watermarks.
    async_result< craft::lsn_pair > keep_alive(craft::client_hdr hdr);

    // The client-requested resolution round (the wire's RESOLVE; the design's
    // client-request SyncRSCommitLSN trigger). LEADER-only: resolve every
    // unresolved slot <= upto -- fetch it from a holder, or verdict it Empty on
    // quorum-lacks evidence -- using the SAME pre-resolution machinery the
    // SyncRSCommitLSN proposer (S5) needs, then propose the entry carrying the
    // verdicts. The client broadcasts this to every member (it cannot know who
    // leads mid-session): a follower returns craft_error::NOT_LEADER (or, once
    // peer channels exist, may forward to its leader). Returns the Empty
    // verdicts <= upto; a late write into an Empty-verdicted slot is REJECTED
    // (reconciliation: Empty beats data). Term-fenced.
    async_result< craft::resolution_result > request_resolution(craft::client_hdr hdr, int64_t upto);

    // ── internal / peer API (server-to-server; NEVER reachable over the client wire) ──

    // Callee side of the GetRSCommitLSN broadcast -- matches craft::craft_peer::get_rs_commit_lsn's
    // shape (craft_client's include/craft/peer.hpp) so a future wire-decoded request has somewhere
    // to pass {term, is_login}. is_login=true is meant to quiesce prior-session writes before
    // reporting last_append (the fencing barrier); watchdog/periodic polls pass is_login=false.
    // Neither term-fencing nor quiesce is implemented yet -- both parameters are accepted but
    // unused until S9 needs them (matches craft_client's own reference implementation today).
    async_result< craft::lsn_pair > get_rs_commit_lsn(uint64_t term, bool is_login);

    // Drop all journal entries with dLSN > lsn; clear missing-set entries above lsn; clamp last_append_lsn;
    // prune any overlay entry whose recorded lsn > lsn (it referenced a now-rolled-back write -- leaving it
    // would let a later read serve stale data from a write that no longer exists in the journal). Called
    // only during login (quiesced -- no concurrent writes). commit_lsn is NOT changed.
    async_status truncate(int64_t lsn);

    // Propose a SyncRSCommitLSN RAFT entry (called by watchdog or leader during login).
    async_status append(int64_t sync_to, uint64_t client_token);

    // Return raw journal data for the requested LSNs. Empty slots return
    // JournalSlot{.is_empty=true} rather than an error.
    async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns);

    // Reconstructs the journal-tail overlay from the journal after a restart (a fresh instance's
    // overlay_ starts empty -- nothing else repopulates it). Walks (commit_lsn, last_append_lsn],
    // skipping missing/Empty-verdicted lsns, and applies the same highest-dLSN-wins rule write()'s
    // own post-flight overlay population uses. Called (via run_recovery(), which also gates client I/O
    // for the duration -- see recovering_) from CraftRaftListener::on_restart(); currently a no-op in
    // practice since CraftPartitionState itself has no superblock-recovery path yet (state_ stays at
    // its default -1/-1 until that separate piece of work lands) -- this is still the correct,
    // already-usable wiring for once it does.
    async_status rebuild_overlay();

    // ── observability ─────────────────────────────────────────────────────

    size_t missing_count() const {
        std::lock_guard lk{missing_mu_};
        return missing_lsns_.size();
    }

    bool is_missing(int64_t lsn) const {
        std::lock_guard lk{missing_mu_};
        return missing_lsns_.contains(lsn);
    }

    bool is_empty_slot(int64_t lsn) const {
        std::lock_guard lk{missing_mu_};
        return empty_lsns_.contains(lsn);
    }

    int64_t last_append_lsn() const {
        std::lock_guard lk{missing_mu_};
        return state_.last_append_lsn;
    }
    int64_t commit_lsn() const {
        std::lock_guard lk{missing_mu_};
        return state_.commit_lsn;
    }
    uint64_t client_token() const {
        std::lock_guard lk{missing_mu_};
        return state_.client_token;
    }
    uint64_t term() const {
        std::lock_guard lk{missing_mu_};
        return state_.term;
    }
    // The last all_committed_lsn captured from a client's keep_alive/write -- floors journal reclaim
    // (S8's job, not read anywhere yet in this class); exposed so S8's eventual reclaim logic has
    // something to read.
    int64_t all_committed_lsn() const {
        std::lock_guard lk{missing_mu_};
        return state_.all_committed_lsn;
    }

    // Wires the server-to-server peer channel used by apply_sync_rs_commit_lsn catch-up.
    // Called by CraftConnector (S9) after construction; tests inject a mock.
    void set_peer_fetcher(CraftPeerFetcher* f) { peer_fetcher_ = f; }

    // Overrides the deadline passed to fetch_from_peer (default mirrors home_blks_config.fbs).
    // Production sets this from HB_DYNAMIC_CONFIG(peer_fetch_timeout_ms) after construction (S8/S9).
    void set_peer_fetch_timeout_ms(uint32_t ms) { peer_fetch_timeout_ms_ = ms; }

#ifdef _PRERELEASE
    // Seeds partition watermarks and the missing set directly, bypassing write().
    // Only compiled when _PRERELEASE is defined; never present in production binaries.
    void seed_lsns(int64_t last_append, std::initializer_list< int64_t > missing = {});
    // Seeds commit_lsn independently of seed_lsns (which only touches last_append + missing).
    void seed_commit_lsn(int64_t commit);
    // Seeds the Empty-verdict set and removes those LSNs from missing_lsns_ (resolving any gap they
    // represented). Replaces any prior seeded empties. apply_sync_rs_commit_lsn (S5) must do the same.
    void seed_empty(std::initializer_list< int64_t > empty);
    // Seeds the session term so tests can exercise write() with a non-zero term without a full login.
    void seed_term(uint64_t term);
    // Exposes the RAFT listener so tests can drive on_commit() directly -- raft_listener_ has no other
    // accessor (production wiring into HomeStore's repl_dev happens elsewhere).
    homestore::repl_dev_listener& test_listener() { return raft_listener_; }

    // Test seam for commit(): runs the same apply-one-slot algorithm, but the index write/delete
    // operations are injected rather than routed through indx_tbl_, so tests can exercise commit()'s
    // logic (in-order apply, stall at a gap, Empty-slot skip, overlay retirement) against a fake
    // index instead of a real VolumeIndexTable.
    async_result< int64_t > commit_with(int64_t upto_lsn, write_index_fn_t write_fn, delete_index_fn_t delete_fn);

    // Test seam for read(): runs the same read algorithm, but the index read operation is injected
    // rather than routed through indx_tbl_, so tests can exercise read()'s logic (index/overlay merge,
    // horizon clamp, checksum verification, read-time all-zero collapse) against a fake index instead
    // of a real VolumeIndexTable.
    async_result< craft::read_result > read_with(int64_t read_lsn, uint64_t addr, uint64_t len, sisl::sg_list dest,
                                                 read_index_fn_t read_fn);

    // Test-only observability for the watchdog: append() is still a stub with no other observable
    // side effect, so this is how a test confirms on_watchdog_tick() actually fired.
    int watchdog_fire_count() const { return watchdog_fire_count_; }

    // Returns the recorded lsn for lba's overlay entry, or -1 if no overlay entry exists for lba.
    // Test-only observability for overlay retirement / highest-dLSN-wins correctness.
    int64_t overlay_lsn_for(lba_t lba) const {
        std::lock_guard lk{overlay_mu_};
        auto it = overlay_.find(lba);
        return it == overlay_.end() ? -1 : it->second.lsn;
    }

    // Test seam that exercises the EXACT production restart path (raft_listener_ is otherwise private
    // and only ever invoked by HomeStore itself), so a test can verify write()/read()/keep_alive() all
    // reject with OFFLINE while recovering_ is set, and succeed again once rebuild_overlay() completes.
    void trigger_on_restart() { raft_listener_.on_restart(); }

    // Test-only observability for the SDSTOR-22905 restart-recovery gate (see recovering_'s doc
    // comment): true from the moment on_restart() fires until run_recovery()'s rebuild_overlay() call
    // completes successfully (see is_recovery_faulted() for the failure outcome instead).
    bool is_recovering() const { return recovering_.load(std::memory_order_acquire); }

    // Test-only observability for recovery_faulted_: true if a restart's rebuild_overlay() ever failed,
    // permanently. Never transitions back to false.
    bool is_recovery_faulted() const { return recovery_faulted_.load(std::memory_order_acquire); }
#endif

private:
    // ── RAFT listener ──────────────────────────────────────────────────────
    //
    // Handles the two CRAFT RAFT entry types (SyncRSCommitLSN, InternalLogin).
    // All other HomeStore callbacks are no-ops for this backend.

    class CraftRaftListener : public homestore::repl_dev_listener {
    public:
        explicit CraftRaftListener(CraftReplDev* owner) : owner_{owner} {}

        // Dispatches on entry type; the real work is in the two apply_* helpers below.
        void on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                       std::vector< homestore::multi_blk_id > const& blkids,
                       cintrusive< homestore::repl_req_ctx >& ctx) override;

        // ── no-ops ────────────────────────────────────────────────────────
        bool on_pre_commit(int64_t, const sisl::blob&, const sisl::blob&,
                           cintrusive< homestore::repl_req_ctx >&) override {
            return true;
        }
        void on_error(homestore::ReplServiceError, const sisl::blob&, const sisl::blob&,
                      cintrusive< homestore::repl_req_ctx >&) override {}
        homestore::result< homestore::blk_alloc_hints >
        get_blk_alloc_hints(sisl::blob const&, uint32_t, cintrusive< homestore::repl_req_ctx >&) override {
            return homestore::blk_alloc_hints{};
        }
        void on_destroy(const homestore::group_id_t&) override {}
        void on_start_replace_member(const std::string&, const homestore::replica_member_info&,
                                     const homestore::replica_member_info&, homestore::trace_id_t) override {}
        void on_complete_replace_member(const std::string&, const homestore::replica_member_info&,
                                        const homestore::replica_member_info&, homestore::trace_id_t) override {}
        void on_clean_replace_member_task(const std::string&, const homestore::replica_member_info&,
                                          const homestore::replica_member_info&, homestore::trace_id_t) override {}
        void on_remove_member(const homestore::replica_id_t&, homestore::trace_id_t) override {}
        void on_rollback(int64_t, const sisl::blob&, const sisl::blob&,
                         cintrusive< homestore::repl_req_ctx >&) override {}
        // Fires rebuild_overlay() fire-and-forget (defined out-of-line in the .cpp -- needs
        // coro_helpers.hpp's detail::detach, which this header does not include).
        void on_restart() override;
        homestore::async_status create_snapshot(std::shared_ptr< homestore::snapshot_context >) override {
            co_return homestore::ok();
        }
        bool apply_snapshot(std::shared_ptr< homestore::snapshot_context >) override { return true; }
        std::shared_ptr< homestore::snapshot_context > last_snapshot() override { return nullptr; }
        int read_snapshot_obj(std::shared_ptr< homestore::snapshot_context >,
                              std::shared_ptr< homestore::snapshot_obj >) override {
            return 0;
        }
        void write_snapshot_obj(std::shared_ptr< homestore::snapshot_context >,
                                std::shared_ptr< homestore::snapshot_obj >) override {}
        void free_user_snp_ctx(void*&) override {}
        void on_no_space_left(homestore::repl_lsn_t, sisl::blob const&) override {}
        void notify_committed_lsn(int64_t) override {}
        void on_config_rollback(int64_t) override {}

    private:
        // Back-pointer to the owning CraftReplDev -- raft_listener_ is a value member of CraftReplDev
        // (see its declaration below), so this can never dangle
        CraftReplDev* owner_;
    };

    // Called from CraftRaftListener::on_commit after deserialising the entry type. Detached (fire-and-forget)
    // from on_commit since that HomeStore callback is synchronous but catch-up here needs to co_await peer
    // fetch + journal writes.
    async_status apply_sync_rs_commit_lsn(int64_t rs_commit_lsn, uint64_t client_token,
                                          std::vector< int64_t > empty_slots);
    void apply_internal_login(uint64_t client_token, uint64_t term);

    // Advance commit_lsn toward upto_lsn by applying each committable slot to the index (internal
    // helper; never a wire op -- there is no standalone commit verb on the CRAFT wire). Stalls
    // (returns without reaching upto_lsn) at the first gap in missing_lsns_ -- not an error, just the
    // achieved commit_lsn. At most one run is ever active at a time (commit_running_); a concurrent
    // caller is a safe no-op since the in-flight run covers the same ground and every subsequent
    // write()/keep_alive() retries the advance. Callers: write()'s post-flight piggyback (best-effort,
    // every outcome including real errors is ignored) and keep_alive() (propagates real errors, since
    // advancing the frontier is its entire purpose -- a stall is still not an error there either).
    async_result< int64_t > commit(int64_t upto_lsn);

    // Core apply-one-slot algorithm behind commit(), parameterized by the index write/delete
    // operations (write_index_fn_t / delete_index_fn_t, declared at the top of this class) so tests
    // can exercise it against a fake index instead of a real VolumeIndexTable. commit() binds these
    // to indx_tbl_'s real methods; commit_with() (test-only) binds test doubles.
    async_result< int64_t > commit_impl(int64_t upto_lsn, write_index_fn_t const& write_fn,
                                        delete_index_fn_t const& delete_fn);

    // Core algorithm behind read(), parameterized by the index read operation (read_index_fn_t,
    // declared at the top of this class) so tests can exercise it against a fake index instead of a
    // real VolumeIndexTable. read() binds this to indx_tbl_'s real read_from_index; read_with()
    // (test-only) binds a test double. Merges the index's committed state with the journal-tail
    // overlay (horizon-clamped to (commit_lsn, read_lsn]), reads every data-carrying LBA via
    // journal_->read_data(), verifies each LBA's checksum, and collapses any all-zero-content LBA to
    // a hole extent at read time (never at write time).
    async_result< craft::read_result > read_impl(int64_t read_lsn, uint64_t addr, uint64_t len, sisl::sg_list dest,
                                                 read_index_fn_t const& read_fn);

    // Core overlay-population rule shared by write()'s post-flight update and rebuild_overlay():
    // highest-dLSN-wins per LBA -- an entry only replaces whatever's already at that LBA if dlsn is
    // strictly greater than the recorded one. all_zeros populates the all_zeros marker (no blkid);
    // otherwise blkid is decomposed via multi_blk_id::iterate() into one single-block OverlayEntry
    // per LBA, paired with its csums[] entry.
    void populate_overlay(int64_t dlsn, lba_t start_lba, uint32_t nlbas, bool all_zeros,
                          homestore::multi_blk_id const& blkid, std::vector< homestore::csum_t > const& csums);

    // Records write()/keep_alive() activity and, on the first call after login, arms the watchdog's
    // recurring iomgr timer (iomgr::timer_token) that periodically checks for staleness -- see
    // on_watchdog_tick()'s doc comment for the full design and why a RECURRING timer (as opposed to
    // the old one-shot-that-reschedules-itself pattern) needs no generation counter, in-flight
    // counter, or shutting-down flag. No-ops if the watchdog is disabled
    // (craft_watchdog_timeout_ms == 0 at construction) or before the first successful login (state_.term == 0).
    void touch_watchdog();

    // Runs on every tick of the watchdog's recurring timer (once armed by touch_watchdog()): if
    // last_contact_ns_ is older than watchdog_timeout_ns_, proposes a SyncRSCommitLSN entry via
    // append(), fire-and-forget (detail::detach -- this runs in a plain timer-callback context, not a
    // coroutine caller awaiting a result). See the .cpp for the full safety argument (iomgr's
    // recurring-timer cancellation path, not the one-shot heap-erase path that caused a reproduced
    // SEGFAULT under the previous design).
    void on_watchdog_tick();

    // Awaits rebuild_overlay() to completion. On SUCCESS, clears recovering_ so the volume resumes
    // accepting client I/O against a now-complete overlay. On FAILURE, does NOT clear recovering_ --
    // instead sets recovery_faulted_ (checked ahead of recovering_ by every client-facing entry point),
    // permanently rejecting I/O rather than resuming against a silently incomplete overlay. This is the
    // coroutine CraftRaftListener::on_restart() detaches, rather than detaching rebuild_overlay()
    // directly, specifically so this outcome-dependent branching has somewhere to live.
    async_status run_recovery();

    volume_id_t vol_id_;
    unique< CraftJournalBackend > journal_;
    uint32_t lba_size_;
    shared< VolumeIndexTable > indx_tbl_;
    CraftPartitionState state_;
    // TODO: Can this be replaced with boost::icl::interval_set? Particularly helpful when a write
    // comes in with a huge gap -- gap-fill loops (write(), apply_sync_rs_commit_lsn()) currently
    // insert one LSN at a time under missing_mu_, which is O(gap width) instead of O(log ranges).
    std::set< int64_t > missing_lsns_;         // gaps between commit_lsn and last_append_lsn
    std::unordered_set< int64_t > empty_lsns_; // slots positively verdicted Empty by a prior SyncRSCommitLSN (S5)
    bool commit_running_{false}; // at most one commit_impl() run active at a time -- see commit()'s doc comment
    // dlsns currently between "claimed as non-idempotent" and "write_slot has completed" in write() --
    // see write()'s doc comment at the in_flight_write_dlsns_.contains() check for why this exists.
    std::set< int64_t > in_flight_write_dlsns_;
    mutable std::mutex
        missing_mu_; // guards state_, missing_lsns_, empty_lsns_, commit_running_, in_flight_write_dlsns_

    // One highest-dLSN-unapplied entry per LBA in (commit_lsn, last_append_lsn]: makes an appended-
    // but-not-yet-committed write locally readable ahead of commit() applying it to the index.
    //
    // Size is bounded by the number of DISTINCT LBAs written since commit_lsn (highest-dLSN-wins
    // collapses repeats to one entry each), not by write/dLSN count -- so its worst case is the
    // volume's entire LBA space, reached only if commit() never advances (a permanently missing
    // journal hole, or a client that stops calling keep_alive()/write() entirely). There is
    // deliberately no eviction/cap here: capping would mean either serving stale index data for an
    // evicted LBA that is genuinely only correct in the overlay, or rejecting new writes outright --
    // both are new backpressure semantics outside S3's scope. In practice this is kept in check by
    // the watchdog forcing periodic commit() progress via SyncRSCommitLSN proposals (S5) whenever a
    // client goes quiet, and by the client's own commit_lsn piggyback on every write/keep_alive.
    struct OverlayEntry {
        int64_t lsn{-1};
        homestore::blk_id blkid{};
        homestore::csum_t csum{0};
        bool all_zeros{false};
    };
    std::unordered_map< lba_t, OverlayEntry > overlay_;
    mutable std::mutex overlay_mu_; // lock order: missing_mu_ before overlay_mu_

    bool login_in_progress_{false};
    std::mutex login_mu_;
    CraftRaftListener raft_listener_;
    CraftPeerFetcher* peer_fetcher_{nullptr};  // null until S9 wires CraftConnector
    uint32_t peer_fetch_timeout_ms_{5000};     // deadline for fetch_data; overridden via set_peer_fetch_timeout_ms()
    std::atomic< uint64_t > write_counter_{0}; // incremented per write(); triggers periodic SyncRSCommitLSN append

    // Set by CraftRaftListener::on_restart() before detaching run_recovery(), cleared by run_recovery()
    // ONLY on a successful rebuild_overlay() (see recovery_faulted_ below for the failure path) --
    // write()/read()/keep_alive() all reject with volume_error::OFFLINE while this is set, per
    // SDSTOR-22905: the overlay must be fully rebuilt before the volume accepts new client I/O.
    //
    // Two caveats, both accepted rather than engineered around (see below for why):
    //   - This is a load-and-go gate, not a lock held for a call's full duration: a write()/read()/
    //     keep_alive() that loads recovering_==false can still be suspended (at a co_await point) and
    //     resume concurrently with a THEN-started rebuild_overlay() populating overlay_. Safe from data
    //     races (overlay_mu_ still serializes actual access) but not from serving a value rebuild_overlay()
    //     was about to correct. This is bounded entirely by HomeStore's restart contract: on_restart()
    //     fires before any client can be connected, so in production no live write()/read()/keep_alive()
    //     call can be in flight when it fires. Held to be an acceptable, explicitly documented limitation
    //     rather than adding suspension-point-granularity re-checks for a window that cannot occur under
    //     that contract; revisit if HomeStore's on_restart() timing guarantee ever changes.
    //   - login() (still a stub -- see the "stubs" section) is NOT gated by this flag either. Once login()
    //     is real, a concurrent login()->truncate() during recovery could destroy journal entries
    //     rebuild_overlay() is still walking. Whoever implements real login() must also gate it here.
    //
    // A plain atomic bool suffices for the single-writer part: on_restart()/run_recovery() are the only
    // writers, and HomeStore's OWN calling contract (not a runtime property of this class) is what
    // ensures they run as one complete cycle before any next one -- see run_recovery()'s comment.
    std::atomic< bool > recovering_{false};
    // Set (never cleared) by run_recovery() if rebuild_overlay() fails -- a failed rebuild leaves
    // overlay_ silently INCOMPLETE (some LSNs applied, then it bailed), so resuming client I/O against
    // it (as recovering_ alone clearing would do) could serve stale pre-write data with no error at
    // all. write()/read()/keep_alive() check this FIRST, ahead of recovering_, and reject with
    // volume_error::INTERNAL_ERROR permanently -- an honest, fail-closed fault rather than fail-open
    // data corruption. There is deliberately no self-recovery path: an operator (or a future re-sync-
    // from-peer mechanism) must intervene.
    std::atomic< bool > recovery_faulted_{false};

    uint64_t watchdog_timeout_ns_{0}; // ns; 0 = disabled; set from craft_watchdog_timeout_ms config at construction
    // Updated (relaxed store) on every touch_watchdog() call; read (relaxed load) by on_watchdog_tick()
    // to decide staleness. A plain atomic timestamp -- steady_clock::now().time_since_epoch().count()
    // -- rather than std::atomic<steady_clock::time_point>, since int64_t is unambiguously lock-free
    // and trivially comparable, with no reliance on time_point's own atomicity properties.
    std::atomic< int64_t > last_contact_ns_{0};
    // Guards the arm-once transition of watchdog_token_ only (touch_watchdog() takes this every call,
    // but only ever WRITES watchdog_token_ the first time, under the double-checked active() test).
    // watchdog_token_ is otherwise touched only by the destructor -- never concurrently with
    // touch_watchdog(), by the same lifetime contract that already governs every other member
    // (calling any method concurrently with the destructor is a caller lifetime violation regardless
    // of the watchdog).
    std::mutex watchdog_arm_mu_;
    // RAII recurring timer: armed once by touch_watchdog(), cancelled (wait=true) by the destructor.
    // Replaces the old watchdog_hdl_/watchdog_mu_/watchdog_generation_/watchdog_in_flight_/
    // watchdog_shutting_down_ machinery entirely -- see touch_watchdog()/on_watchdog_tick()'s doc
    // comments and the .cpp for why a RECURRING iomgr timer needs none of that.
    iomgr::timer_token watchdog_token_;
#ifdef _PRERELEASE
    std::atomic< int > watchdog_fire_count_{0}; // test-only; never present in production binaries
#endif
};

} // namespace homeblocks
