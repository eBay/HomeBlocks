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
#include "async_mutex.hpp"
#include "craft_raft_entries.hpp"
#include "craft_snapshot_reader.hpp"
#include "../volume/index_fixed_kv.hpp" // BlockInfo -- the shape commit()'s index write callback uses
#include <homestore/replication/repl_dev.hpp>
#include <iomgr/timer.hpp>                 // iomgr::timer_token -- RAII recurring timer used by the watchdog
#include <sisl/async/shared_awaitable.hpp> // persist_state()'s callers wait for the round that covers them
#include <sisl/async/value_awaitable.hpp>  // stop_apply() waits for the apply queue to go idle

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <initializer_list>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
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

// Per-partition CRAFT state. Authoritative in memory; restored at restart from the durable image (CraftDurableState)
// and the journal.
struct CraftPartitionState {
    int64_t commit_lsn{-1};        // contiguous committed prefix (== Synced)
    int64_t last_append_lsn{-1};   // highest appended dLSN (may be uncommitted)
    uint64_t client_token{0};      // token from the last successful InternalLogin
    uint64_t term{0};              // current session term
    int64_t all_committed_lsn{-1}; // client-computed set-wide min commit_lsn, piggybacked on keep_alive/write;
                                   // floors journal reclaim (see reclaim_floor_locked()). Client-controlled wire
                                   // input with only a narrow malformed-negative check where it's captured
                                   // (write()/read()/keep_alive()) -- there is NO per-call bound possible there: a
                                   // lagging replica legitimately sees this arrive ahead of its own
                                   // last_append_lsn/commit_lsn (that gap is the signal it needs to catch up via
                                   // S6 peer exchange). The reclaim floor bounds it by this member's persisted
                                   // durable commit frontier instead.
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

// One replica-set member's fetch_data-shaped reply to a pre-resolution broadcast (S5 / SDSTOR-22907).
// Reuses JournalSlot's existing four-way per-LSN contract (is_empty / present+data / present+zero /
// omitted-not-held-here) as-is. `member` is the responder, tagged by the transport (the wire carries no
// id); only distinct voters other than this member count toward a quorum.
struct QuorumSlotResponse {
    craft::peer_id_t member{};
    std::vector< JournalSlot > slots{};
};

// A member's answer to GetRSCommitLSN: its watermarks and its session term, counting a login it has committed but not
// yet applied.
struct RsCommitLsnReply {
    craft::lsn_pair lsns{};
    uint64_t term{0};
};

// One member's reply to a GetRSCommitLSN broadcast (poll_quorum_lsns), tagged the same way.
struct QuorumLsnResponse {
    craft::peer_id_t member{};
    RsCommitLsnReply reply{};
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
    // Highest lsn written and not rolled back, -1 for an empty journal. Restart recovery restores last_append_lsn from
    // it. A backend that can't answer reports not_supported, which faults a recovery that needs it.
    virtual async_result< int64_t > tail_lsn() {
        co_return std::unexpected(std::make_error_condition(std::errc::operation_not_supported));
    }
    // Journal reclaim: drops every entry with lsn <= upto (not past the journal's last entry). Their data blocks are
    // NOT freed: the index references them by then. Default: not_supported, and the device stops asking.
    virtual async_status reclaim_upto(int64_t /* upto */) {
        co_return std::unexpected(std::make_error_condition(std::errc::operation_not_supported));
    }
    // Highest lsn reclaimed so far, -1 for none. Read at restart.
    virtual int64_t reclaimed_upto() const { return -1; }
    // Commits the blocks a durable slot references: HomeStore's allocator persists an allocation only once committed,
    // so without it a restart frees them. recovering: a restart commits again those of a live slot that no checkpoint
    // persisted before it, the allocator holding no others, and reserves them in memory too.
    virtual status commit_data(homestore::multi_blk_id const& /* blkid */, bool /* recovering */) { return ok(); }
    // Fails the writes in flight past their deadline (see JournalWriteDeadlines). HomeBlocks calls it every second.
    virtual void expire_overdue() {}
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
//
// Two distinct call shapes, for two distinct purposes: fetch_data is single-peer, used by
// apply_sync_rs_commit_lsn's best-effort, non-gating catch-up; fetch_from_quorum is a broadcast,
// used by pre_resolve_slots (S5 / SDSTOR-22907), the LEADER-only step that must gate a
// not-yet-proposed SyncRSCommitLSN entry.

class CraftPeerFetcher {
public:
    virtual async_result< RsCommitLsnReply > get_rs_commit_lsn(uint64_t term, bool is_login) = 0;
    // `timeout_ms` is the deadline this call must complete within (CraftReplDev passes
    // peer_fetch_timeout_ms_, set from home_blks_config.fbs's peer_fetch_timeout_ms). A real transport
    // (S9) must treat a missed deadline as a hard failure, same as an unreachable peer -- this interface
    // only carries the contract; there's nothing to enforce yet since today's only implementations are
    // direct function calls (production is unwired, tests call synchronously).
    virtual async_result< std::vector< JournalSlot > > fetch_data(const std::vector< int64_t >& lsns,
                                                                  uint32_t timeout_ms) = 0;
    // Broadcasts fetch_data(lsns) to every OTHER responding replica-set member; a non-responding member
    // is simply absent from the result. Each reply carries its member. Whether the responders amount to a
    // quorum is the caller's judgement, made against CraftRaftProposer::voters() -- this interface only
    // transports.
    virtual async_result< std::vector< QuorumSlotResponse > > fetch_from_quorum(std::vector< int64_t > lsns,
                                                                                uint32_t timeout_ms) = 0;
    // Broadcasts GetRSCommitLSN(term, is_login) to every OTHER replica-set member and returns each
    // responder's {commit_lsn, last_append_lsn} and session term, tagged with its member; a non-responding member is
    // absent from the result. A login poll (is_login) waits for every member up to timeout_ms, not just the first
    // majority, since login's watermark is the max over every reply; a watchdog poll may return once a majority has
    // replied. Used by the watchdog trigger (is_login=false, never quiesces) and by login_poll() (is_login=true).
    virtual async_result< std::vector< QuorumLsnResponse > > poll_quorum_lsns(uint64_t term, bool is_login,
                                                                              uint32_t timeout_ms) = 0;
    virtual ~CraftPeerFetcher() = default;
};

// ─── CraftRaftProposer ────────────────────────────────────────────────────────
//
// Abstraction over this partition's RAFT group (a HomeStore repl_dev in production): leadership,
// membership, and proposing the CRAFT control-plane entries. Injected into CraftReplDev so unit tests
// can run the leader-side paths (append(), pre_resolve_slots()) without a live RAFT group. A CRAFT volume's
// device gets make_homestore_raft_proposer()'s, over the volume's repl_dev. Default (null) leaves every
// leader-only path failing closed.

class CraftRaftProposer {
public:
    virtual bool is_leader() const = 0;
    // This member's id.
    virtual craft::peer_id_t my_id() const = 0;
    // Voting members, this one included; learners excluded. The denominator of every quorum decision here.
    // Only the leader's view is complete, and quorum decisions are leader-only.
    virtual std::vector< craft::peer_id_t > voters() const = 0;
    // Proposes a key-only RAFT entry (no data payload). Completes once the entry has committed and been
    // dispatched to this member's CraftRaftListener::on_commit, or with an error if replication fails.
    virtual async_status propose(sisl::blob header, sisl::blob key) = 0;
    // Completes the proposals past their deadline (craft_raft_propose_timeout_ms) with timed_out: their entries may
    // still commit. HomeBlocks calls it every second for each partition's device, on one of its own timer's reactors.
    virtual void expire_overdue() {}
    // Stops for good: the proposals still pending complete with timed_out (they may still commit), later ones fail
    // with operation_canceled.
    virtual void stop() {}
    virtual ~CraftRaftProposer() = default;
};

// Wraps a HomeStore repl_dev whose listener is this partition's CraftRaftListener -- that listener is
// what completes each proposal (on_commit / on_error), so the two must belong to the same repl_dev.
// replica_id is this member's id: HomeBlocks' our_uuid(), which HomeStore gets from the repl_application.
unique< CraftRaftProposer > make_homestore_raft_proposer(shared< homestore::repl_dev > rd, craft::peer_id_t replica_id);

// The deadlines (craft_journal_write_timeout_ms) of a journal's writes in flight. HomeStore never completes a journal
// write whose device write failed (SDSTOR-24993): it would wait forever, and with it what awaits it (an apply, and so
// the device's stop()). Swept by the backend's expire_overdue(), which fails the overdue ones.
class JournalWriteDeadlines {
public:
    using clock = std::chrono::steady_clock;
    // One write's completion: true once written, false once timed out, whichever comes first.
    struct Wait {
        sisl::async::value_awaitable< bool > done;
        std::atomic< bool > completed{false};
        void complete(bool written) {
            if (!completed.exchange(true, std::memory_order_acq_rel)) done.complete(written);
        }
    };

    void add(shared< Wait > w, clock::time_point deadline);
    // Fails every wait whose deadline is at or before `now`, and forgets the completed ones.
    void sweep(clock::time_point now);
    size_t pending() const;

private:
    mutable std::mutex mu_;
    std::vector< std::pair< shared< Wait >, clock::time_point > > pending_;
};

// The deadlines (craft_raft_propose_timeout_ms) of a proposer's in-flight proposals, swept by its expire_overdue(). Not
// an iomgr one-shot per proposal: iomgr v13's one-shots share one timerfd that each new one re-arms to its own delay
// and nothing re-arms after a fire, so concurrent deadlines postpone or drop each other.
class ProposalDeadlines {
public:
    using clock = std::chrono::steady_clock;

    // False once closed: the proposal isn't tracked.
    bool add(intrusive< homestore::repl_req_ctx > ctx, clock::time_point deadline);
    // Completes every pending proposal whose deadline is at or before `now` with timed_out (its outcome is unknown:
    // the entry may still commit), and forgets the ones that already completed.
    void sweep(clock::time_point now);
    // Tracks nothing more, and hands back every proposal still tracked.
    std::vector< intrusive< homestore::repl_req_ctx > > close();
    size_t pending() const;

private:
    mutable std::mutex mu_;
    std::vector< std::pair< intrusive< homestore::repl_req_ctx >, clock::time_point > > pending_;
    bool closed_{false};
};

#ifdef _PRERELEASE
// Test seam for proposal completion: the same ctx make_homestore_raft_proposer() hands HomeStore, and its
// outcome once CraftRaftListener has completed it (nullopt while pending). Single-shot: takes the outcome.
intrusive< homestore::repl_req_ctx > make_test_proposal_ctx();
std::optional< status > take_test_proposal_outcome(intrusive< homestore::repl_req_ctx > const& ctx);
#endif

// ─── checkpoint_trigger_fn_t ──────────────────────────────────────────────────
//
// A callable trigger_cp_flush(bool force) -- abstraction over HomeStore's checkpoint manager
// (homestore::cp_mgr().trigger_cp_flush()). Injected into CraftReplDev so unit tests can exercise
// the trigger without touching HomeStore. Production code passes make_homestore_checkpoint_trigger_fn()'s result.
// Default (empty) leaves the trigger stubbed -- same posture as CraftPeerFetcher.
//
// The result is true only when a checkpoint that started during the call has completed: everything applied to the
// index before the call is then durable. false means the request coalesced with a flush already running (force=false),
// which may not cover the index as of the call. The durable commit frontier moves only on true.
using checkpoint_trigger_fn_t = std::function< async_result< bool >(bool force) >;

// Factory that wraps homestore::cp_mgr() (there is exactly one CPManager per HomeStore instance,
// unlike make_homestore_journal_backend which is per-volume). Each CraftReplDev stores its own copy
// of the returned callable by value (set_checkpoint_trigger) -- callers don't need to keep the
// factory's result alive themselves. Tests inject a plain callable directly.
checkpoint_trigger_fn_t make_homestore_checkpoint_trigger_fn();

// ─── Durable partition state (S8 / SDSTOR-22745) ─────────────────────────────
//
// What a partition needs after a restart that neither the journal nor the RAFT log gives back. Written AHEAD of the
// change it protects: CraftReplDev updates its in-memory image and persists it before the change takes effect (a
// session is admitted, a voided slot's blocks are freed), so a crash in between restarts no less safe than before.
//
// Not here: last_append_lsn comes back from the journal tail (raised to sync_watermark), and the missing set from a
// walk of the journal above durable_commit_lsn. commit_lsn restarts at durable_commit_lsn, the frontier a completed
// checkpoint covers; the slots above it are applied to the index again.

// A RAFT entry committed but not yet applied when its image was taken: what applying it again needs.
struct CraftPendingEntry {
    int64_t raft_idx{-1};
    CraftEntryType type{};
    int64_t rs_commit_lsn{-1}; // SyncRSCommitLSN
    uint64_t client_token{0};
    uint64_t term{0}; // InternalLogin
    // SyncRSCommitLSN still queued: the slots held here that its verdicts voided, which its apply frees. Empty for the
    // entry being applied: its frees may have happened.
    std::vector< int64_t > to_free{};

    bool operator==(CraftPendingEntry const&) const = default;
};

// A RetainFrom pin (resync.md 4.8): the journal keeps every slot above w while it holds. Named by its snapshot
// transfer's identity.
struct CraftRetainPin {
    int64_t raft_idx{-1};
    uint32_t digest{0};
    int64_t w{-1};
    bool operator==(CraftRetainPin const&) const = default;
};

struct CraftDurableState {
    uint64_t term{0};                  // session term of the last InternalLogin applied
    uint64_t client_token{0};          // and its client token
    int64_t last_applied_raft_idx{-1}; // RAFT entries at or below were applied: a replay of one is skipped
    int64_t durable_commit_lsn{-1};    // commit frontier covered by a completed checkpoint
    int64_t sync_watermark{-1};        // highest SyncRSCommitLSN rs_commit_lsn applied; last_append_lsn >= this
    std::set< int64_t > empty_lsns{};  // every Empty verdict, as empty_lsns_ (a late write or a resolution retry asks)
    // Set by a snapshot install (resync.md 4.9). journal_floor: the journal holds nothing at or below it (the install
    // dropped it), whatever the log store reports. read_gate: reads are refused until commit_lsn reaches it.
    int64_t journal_floor{-1};
    int64_t read_gate{-1};
    // The RetainFrom pins held (version 4). A restart replays the log above the checkpoint, which pins again what it
    // still holds.
    std::vector< CraftRetainPin > retain_pins{};
    // The RAFT entries committed but not yet applied when the image was taken, in log order. The RAFT log replay after
    // a restart starts above the last checkpoint, which may cover them: a restart applies those above
    // last_applied_raft_idx again from here. Taken by every persist; the in-memory image leaves it empty.
    std::vector< CraftPendingEntry > pending{};

    bool operator==(CraftDurableState const&) const = default;
};

// On-disk image of CraftDurableState: a fixed header, the Empty verdicts as int64s, then the pending entries. decode
// returns nullopt for anything malformed (bad magic or version, wrong length, verdicts not strictly ascending or at or
// below durable_commit_lsn, pending entries not strictly ascending or of a type the queue doesn't apply): an image that
// doesn't decode faults the partition, it never restarts it empty.
std::vector< uint8_t > encode_durable_state(CraftDurableState const& s);
std::optional< CraftDurableState > decode_durable_state(uint8_t const* buf, size_t len);

// Where a partition's CraftDurableState lives, one per partition (production: the partition's own meta blk; tests: in
// memory). persist() returns once the image is durable; CraftReplDev never has two persist() calls outstanding.
class CraftStateStore {
public:
    // The last image persisted, nullopt if none ever was, an error if what is there can't be used.
    virtual result< std::optional< CraftDurableState > > load() = 0;
    virtual async_status persist(CraftDurableState const& s) = 0;
    // Removes the state for good (the partition is being destroyed). A persist() after it fails.
    virtual void destroy() = 0;
    // Marks the state faulted for good: persist() fails from then on, and load() too, after a restart as well. The
    // partition can't be trusted any more, and coming back from its last image would forget what it applied since.
    // Idempotent; a call returns once the mark is durable, also when another call made it.
    virtual void mark_faulted() = 0;
    virtual ~CraftStateStore() = default;
};

// The meta blk record holding one partition's durable state: this header, then the encoded CraftDurableState
// (encode_durable_state()), image_len bytes. One per partition, removed with it.
#pragma pack(push, 1)
struct craft_state_sb_t {
    static constexpr uint64_t k_magic = 0x54415453'54465243ull; // "CRFTSTAT" little-endian
    static constexpr uint32_t k_version = 1;

    uint64_t magic{k_magic};
    uint32_t version{k_version};
    volume_id_t vol_id{}; // the partition (volume) it belongs to
    uint32_t image_len{0};
    uint8_t faulted{0}; // CraftStateStore::mark_faulted(): the image (if any) is kept, but no longer loads
};
#pragma pack(pop)
inline const std::string craft_state_meta_name{"CraftState"};

// A CraftStateStore in the partition's own meta blk record (craft_state_sb_t). `sb` is the record loaded at boot, or an
// empty one named craft_state_meta_name for a partition that has none yet: its first persist creates it.
unique< CraftStateStore > make_homestore_state_store(volume_id_t vol_id, homestore::superblk< craft_state_sb_t > sb);
// Marks a partition's state record faulted as CraftStateStore::mark_faulted() does, keeping its image if it has one, or
// writes a header alone: for a partition whose device never came, so has no store.
void mark_craft_state_faulted(volume_id_t vol_id, homestore::superblk< craft_state_sb_t >& sb);

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
    // Lets test_craft_pre_resolution.cpp call pre_resolve_slots (private) directly -- same reasoning.
    friend class CraftPreResolutionTest;
    // Lets test_craft_login.cpp call the login steps (private) one at a time.
    friend class CraftLoginTest;
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
    // state_store holds the partition's durable state (CraftDurableState). Given at construction, so it is in place
    // before anything is applied or recovered; without one nothing is persisted and a restart restores nothing (the
    // light unit tests).
    // raft_proposer proposes the partition's entries through its RAFT group; the device owns it. Without one nothing
    // is proposed: append() and request_resolution() refuse with not_supported.
    explicit CraftReplDev(volume_id_t vol_id, unique< CraftJournalBackend > journal, uint32_t lba_size,
                          shared< VolumeIndexTable > indx_tbl, unique< CraftStateStore > state_store,
                          unique< CraftRaftProposer > raft_proposer);

public:
    static shared< CraftReplDev > create(volume_id_t vol_id, unique< CraftJournalBackend > journal, uint32_t lba_size,
                                         shared< VolumeIndexTable > indx_tbl,
                                         unique< CraftStateStore > state_store = nullptr,
                                         unique< CraftRaftProposer > raft_proposer = nullptr) {
        return shared< CraftReplDev >(new CraftReplDev(vol_id, std::move(journal), lba_size, std::move(indx_tbl),
                                                       std::move(state_store), std::move(raft_proposer)));
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
    // stale/reordered message), it can raise the journal reclaim floor, so the reclaim runs again. Returns the
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

    // Callee side of the GetRSCommitLSN broadcast: this member's watermarks and its session term (a login committed but
    // not yet applied counts). A login poll (is_login) first quiesces writes whose term is at or below `term` (the
    // poll's, not this member's, which may lag behind a login it hasn't applied): they get STALE_TERM from then on, so
    // the reported last_append_lsn counts every write this member can still ack. The fence only rises. Watchdog polls
    // pass is_login=false and never quiesce.
    async_result< RsCommitLsnReply > get_rs_commit_lsn(uint64_t term, bool is_login);

    // Drop all journal entries with dLSN > lsn; clear missing-set entries above lsn; clamp last_append_lsn and
    // the periodic-sync baseline; prune any overlay entry whose recorded lsn > lsn (it referenced a now-rolled-back
    // write -- leaving it would let a later read serve stale data from a write that no longer exists in the
    // journal). Called only during login (quiesced -- no concurrent writes). commit_lsn is NOT changed.
    // FIXME(S4/S7): before dropping entries here, force a completed checkpoint --
    // co_await checkpoint_trigger_(true). Once entries above/below lsn are gone, the journal is
    // no longer a durable record of them; if HomeStore's checkpoint has only been requested and not yet
    // completed, a crash in between loses that data. HomeStore's own IndexTable::destroy() hits the
    // identical problem and force-flushes before removing its superblock for exactly this reason.
    async_status truncate(int64_t lsn);

    // LEADER-only. Proposes a SyncRSCommitLSN RAFT entry and waits for it to commit. The proposer behind the
    // periodic, watchdog and client-requested triggers (login proposes through login_sync()). Runs
    // pre_resolve_slots(sync_to) first and never proposes
    // past a slot it could not resolve -- the proposed watermark is clamped to just below the first one,
    // and that achieved watermark is what comes back as resolved_upto, alongside every Empty verdict
    // <= it (earlier rounds' included). It holds watermark_mu_ throughout: if that's held, it fails
    // operation_in_progress, or with wait_turn (a client's Resolve) queues for it.
    // term is the session term the caller validated; the round runs only within that session and fails
    // with STALE_TERM if InternalLogin has moved state_.term past it.
    async_result< craft::resolution_result > append(int64_t sync_to, uint64_t client_token, uint64_t term,
                                                    bool wait_turn = false);

    // Return raw journal data for the requested LSNs. Empty slots return
    // JournalSlot{.is_empty=true} rather than an error.
    async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns);

    // Reconstructs the journal-tail overlay from the journal after a restart (a fresh instance's
    // overlay_ starts empty -- nothing else repopulates it). Walks (commit_lsn, last_append_lsn],
    // skipping missing/Empty-verdicted lsns, and applies the same highest-dLSN-wins rule write()'s
    // own post-flight overlay population uses; a slot the journal doesn't hold becomes missing (catch-up fills it).
    // Called (via recover(), which also gates client I/O for the duration -- see recovering_) from
    // CraftRaftListener::on_restart(), after restore_state() set the range up.
    async_status rebuild_overlay();

    // ── observability ─────────────────────────────────────────────────────

    size_t missing_count() const {
        std::lock_guard lk{state_mu_};
        return missing_lsns_.size();
    }

    bool is_missing(int64_t lsn) const {
        std::lock_guard lk{state_mu_};
        return missing_lsns_.contains(lsn);
    }

    bool is_empty_slot(int64_t lsn) const {
        std::lock_guard lk{state_mu_};
        return empty_lsns_.contains(lsn);
    }

    int64_t last_append_lsn() const {
        std::lock_guard lk{state_mu_};
        return state_.last_append_lsn;
    }
    int64_t commit_lsn() const {
        std::lock_guard lk{state_mu_};
        return state_.commit_lsn;
    }
    // The durable commit frontier as last persisted (see advance_durable_commit()).
    int64_t persisted_durable_commit() const {
        std::lock_guard lk{state_mu_};
        return persisted_durable_commit_;
    }
    uint64_t client_token() const {
        std::lock_guard lk{state_mu_};
        return state_.client_token;
    }
    uint64_t term() const {
        std::lock_guard lk{state_mu_};
        return state_.term;
    }
    // The last all_committed_lsn captured from a client's keep_alive/write -- floors journal reclaim.
    int64_t all_committed_lsn() const {
        std::lock_guard lk{state_mu_};
        return state_.all_committed_lsn;
    }
    // True once the partition is permanently faulted (see faulted_): never false again.
    bool is_faulted() const { return faulted_.load(std::memory_order_acquire); }

    // Wires the server-to-server peer channel used by apply_sync_rs_commit_lsn catch-up.
    // Called by CraftConnector (S9) after construction; tests inject a mock.
    void set_peer_fetcher(CraftPeerFetcher* f) { peer_fetcher_ = f; }

    // Test seam: a proposer the device doesn't own (a mock the test keeps), in place of create()'s.
    void set_raft_proposer(CraftRaftProposer* p) { raft_proposer_ = p; }
    // HomeBlocks' timer, every second: the proposals (CraftRaftProposer::expire_overdue()) and the journal writes
    // (CraftJournalBackend::expire_overdue()) past their deadline fail.
    void expire_overdue() {
        if (raft_proposer_) raft_proposer_->expire_overdue();
        journal_->expire_overdue();
    }
    // Ends the proposals in flight (timed_out: they may still commit) and fails every later one, as stop() does. A
    // shutdown calls it before it waits for the requests in flight: one waiting on a proposal without a deadline
    // (craft_raft_propose_timeout_ms = 0) and without a quorum would hold it forever.
    void stop_proposing() {
        if (raft_proposer_) raft_proposer_->stop();
    }

    // Overrides the deadline passed to fetch_from_peer (default mirrors home_blks_config.fbs).
    // Production sets this from HB_DYNAMIC_CONFIG(peer_fetch_timeout_ms) after construction (S8/S9).
    void set_peer_fetch_timeout_ms(uint32_t ms) { peer_fetch_timeout_ms_ = ms; }

    // Wires the HomeStore checkpoint trigger used by apply_sync_rs_commit_lsn's periodic checkpoint
    // (SDSTOR-22888). One checkpoint_trigger_fn_t is shared by every volume's CraftReplDev; tests
    // inject a mock.
    void set_checkpoint_trigger(checkpoint_trigger_fn_t fn) { checkpoint_trigger_ = std::move(fn); }

    // Overrides the commit_lsn delta between checkpoint triggers (default matches
    // sync_rs_commit_lsn_interval's own default of 128, tying checkpoint cadence to the periodic
    // SyncRSCommitLSN cadence). Production sets this from HB_DYNAMIC_CONFIG(sync_rs_commit_lsn_interval)
    // after construction, same pattern as set_peer_fetch_timeout_ms.
    void set_checkpoint_lsn_interval(int64_t n) { checkpoint_lsn_interval_ = n; }

    // Handles this partition's CRAFT RAFT entries. HomeStore does not hold it: the group's listener (created before
    // this device exists) forwards to it.
    homestore::repl_dev_listener& raft_listener() { return raft_listener_; }

    // Stops applying committed RAFT entries: waits out the one being applied, and fails the queued ones and any that
    // commit later with operation_canceled, unapplied.
    async_status stop_apply();

    // Stops everything the device runs on its own, before what it works on goes away: the applies (waiting out the one
    // in progress), the journal reclaim (waiting out a running one), the checkpoint triggers (waiting out those in
    // flight), the watchdog and the proposer (CraftRaftProposer::stop()). keep_queued, for a shutdown: the queued
    // entries, and any that commit later, stay queued unapplied, for HomeStore's last checkpoint to save with the
    // partition's state. Otherwise, for a destroy, they fail as in stop_apply(). Called once.
    async_status stop(bool keep_queued);

    // Restart recovery: restores the durable state (restore_state()), rebuilds the overlay, then queues again the
    // entries the image holds as committed but not applied. Client I/O is rejected (OFFLINE) until it completes; a
    // failure faults the partition for good. No RAFT entry may reach the device before it completes: the volume
    // publishes the device only once recovered, its listener holding the entries meanwhile.
    async_status recover();

    // Persists the durable image, with the entries committed but not yet applied. HomeBlocks' checkpoint step calls it
    // for every CRAFT partition before HomeStore records the checkpoint: the RAFT log replay after a restart starts
    // above that checkpoint, so an entry it covers that isn't applied yet survives only here. A no-op without a store.
    // A partition faulted before or during the save saves nothing, and succeeds once its record is marked faulted
    // (see fault()): a checkpoint fails, which stops the process, only for a save that failed otherwise.
    async_status save_state();

    // The partition is being destroyed: removes its durable state from the store for good. Persists after it fail.
    void destroy_state() {
        if (state_store_) state_store_->destroy();
    }

    // ── snapshot (docs/craft/resync.md, section 4.9) ──

    // The control tuple as of the RAFT entry nuraft is snapshotting, L. nuraft calls create_snapshot() right after
    // committing L, before L + 1. A SyncRSCommitLSN's verdicts and watermark take effect at commit, so S and the Empty
    // verdicts are exactly L's; an InternalLogin applies in queue order, so the session is taken from the last login
    // still queued or being applied, else from the applied state. W is commit_lsn: commit() advances it only once the
    // slots up to it are in the index. Doesn't wait for anything (nuraft's commit thread is blocked meanwhile).
    craft_snapshot::control_tuple capture_snapshot_tuple();

    // A snapshot reader's view of this partition: its LBA index and its blocks, read synchronously (nuraft's snapshot
    // thread, never a reactor). The source holds the device. total_lbas: the volume's size in LBAs.
    static unique< craft_snapshot::snapshot_source > make_snapshot_source(shared< CraftReplDev > dev,
                                                                          uint64_t total_lbas);

    // The journal is reclaimed up to here: a snapshot whose W is below it can't be completed from this member's
    // journal (resync.md 4.9, "A stored context can go stale").
    int64_t journal_reclaimed_upto() const {
        std::lock_guard lk{state_mu_};
        return reclaimed_upto_;
    }
    uint32_t lba_size() const { return lba_size_; }

    // ── RetainFrom (resync.md 4.8) ──
    // Pins a transfer's W on this member's journal: nothing at or above it is reclaimed until unpin_retain(), or until
    // the pin is older than craft_retain_timeout_s or holds more than craft_retain_max_slots slots (then it is dropped
    // here, logged, and that transfer may fail: journal space is never traded for it). Idempotent.
    void pin_retain(int64_t raft_idx, uint32_t digest, int64_t w);
    void unpin_retain(int64_t raft_idx, uint32_t digest);
    std::vector< CraftRetainPin > retain_pins() const;
    // The leader's side of the round (resync.md 4.8): propose RetainFrom / ReleaseRetain through the group.
    async_status propose_retain(int64_t raft_idx, uint32_t digest, int64_t w);
    async_status propose_release_retain(int64_t raft_idx, uint32_t digest);
    // Validation, after the pin is on every source: false if a member that answers a poll no longer holds slot w + 1
    // (it reclaimed past w before it had the pin), or if this member's journal did. Without a peer fetcher (the peer
    // plane, S9, not wired) only this member's journal is checked.
    async_result< bool > sources_hold_above(int64_t w, uint32_t timeout_ms);
    // The members' watermarks, as a non-fencing poll reports them; empty without a peer fetcher.
    async_result< std::vector< QuorumLsnResponse > > poll_members(uint32_t timeout_ms);
    // ── snapshot install, the receiver's side (resync.md 4.9) ──
    //
    // Called by the group's listener from nuraft's snapshot thread, so they block (sync_get) and never run on a
    // reactor.

    // The exclusive install barrier (requirement 5): admits no new op (write, read, keep_alive, fetch, resolution,
    // poll: OFFLINE), pauses the apply queue, then waits out every op, commit, apply, reclaim and checkpoint already
    // running. Then persists the barrier (durable read_gate = k_install_in_progress), so a restart comes back behind
    // it. Returns P, the last_append_lsn after the drain; timed_out (the barrier lowered again) if the drain doesn't
    // finish in time. Raising it again while it is up (a transfer restarted) only returns the current last_append_lsn.
    result< int64_t > begin_snapshot_install(std::chrono::milliseconds timeout);
    // Installs one region batch: the volume over [start, end) becomes exactly the batch (requirement 2). Verifies the
    // data against its checksums, deletes the range's index entries (their blocks are freed by
    // release_snapshot_frees(), after the region's checkpoint), writes the data to new blocks, commits them and
    // indexes them.
    status install_snapshot_batch(craft_snapshot::region_batch const& b);
    // Frees the blocks the batches installed since the last call replaced. Called after a checkpoint made the new index
    // durable, so a restart can never find the old index pointing at a reused block.
    void release_snapshot_frees();
    // Completes the install (requirement 7). Drops the whole journal, rebases the watermarks on the snapshot's (or, for
    // a control-only install, keeps this member's certified prefix), merges the control state, flushes a checkpoint,
    // persists the state with the read gate G (max(W_hi, P), or P for a control-only install), and lifts the barrier.
    status complete_snapshot_install(craft_snapshot::snapshot_meta const& meta, int64_t w_hi, int64_t p,
                                     bool control_only);
    // Whether the install barrier is up (also after a restart that found one in progress).
    bool snapshot_installing() const { return installing_.load(std::memory_order_acquire); }
    // The read gate: reads are NOT_ELIGIBLE while commit_lsn is below it.
    int64_t read_gate() const {
        std::lock_guard lk{state_mu_};
        return read_gate_;
    }
    // The durable read_gate while an install is in progress.
    static constexpr int64_t k_install_in_progress = std::numeric_limits< int64_t >::max();

    // What CraftReplDevSnapshotSource reads (see make_snapshot_source()).
    int64_t snapshot_last_append_lsn() const;
    std::optional< std::vector< craft_snapshot::index_entry > > snapshot_read_index(lba_t start, lba_t end);
    bool snapshot_read_blocks(homestore::blk_id const& blocks, uint8_t* buf);

#ifdef _PRERELEASE
    // Seeds partition watermarks and the missing set directly, bypassing write().
    // Only compiled when _PRERELEASE is defined; never present in production binaries.
    void seed_lsns(int64_t last_append, std::initializer_list< int64_t > missing = {});
    // Seeds commit_lsn independently of seed_lsns (which only touches last_append + missing).
    void seed_commit_lsn(int64_t commit);
    // The journal's record of slot lsn.
    async_result< JournalSlot > journal_slot_for_test(int64_t lsn) { return journal_->read_slot(lsn); }
    // Seeds the Empty-verdict set and removes those LSNs from missing_lsns_ (resolving any gap they
    // represented). Replaces any prior seeded empties. apply_sync_rs_commit_lsn (S5) must do the same.
    void seed_empty(std::initializer_list< int64_t > empty);
    // Seeds the session term so tests can exercise write() with a non-zero term without a full login. Raises the login
    // fence with it, as a committed and applied login would.
    void seed_term(uint64_t term);
    // Marks lsn as having a journal write in flight, as write() does while its write is outstanding.
    void seed_in_flight(int64_t lsn);

    // Test seam for commit(): runs the same apply-one-slot algorithm, but the index write/delete
    // operations are injected rather than routed through indx_tbl_, so tests can exercise commit()'s
    // logic (in-order apply, stall at a gap, Empty-slot skip, overlay retirement) against a fake
    // index instead of a real VolumeIndexTable.
    async_result< int64_t > commit_with(int64_t upto_lsn, write_index_fn_t write_fn, delete_index_fn_t delete_fn);
    // Test seam for commit() itself: from now on commit(), and so each of its callers (the SyncRSCommitLSN apply
    // included), applies slots through these instead of indx_tbl_.
    void set_index_fns_for_test(write_index_fn_t write_fn, delete_index_fn_t delete_fn) {
        test_write_fn_ = std::move(write_fn);
        test_delete_fn_ = std::move(delete_fn);
    }

    // Test seam for read(): runs the same read algorithm, but the index read operation is injected
    // rather than routed through indx_tbl_, so tests can exercise read()'s logic (index/overlay merge,
    // horizon clamp, checksum verification, read-time all-zero collapse) against a fake index instead
    // of a real VolumeIndexTable.
    async_result< craft::read_result > read_with(int64_t read_lsn, uint64_t addr, uint64_t len, sisl::sg_list dest,
                                                 read_index_fn_t read_fn);

    // Test-only observability for the watchdog: counts ticks that found the session stale, on any member
    // (counted before the leader check, so it's independent of whether a CraftRaftProposer is wired).
    int watchdog_fire_count() const { return watchdog_fire_count_; }

    // Cancels the watchdog and waits for any in-progress tick to finish (the same cancel the destructor
    // does). A tick holds its own reference to the device, so tests call this before dropping theirs.
    void stop_watchdog() { watchdog_token_.cancel(/* wait = */ true); }

    // Overrides the periodic SyncRSCommitLSN cadence read from sync_rs_commit_lsn_interval at construction.
    void set_sync_lsn_interval(int64_t n) { sync_lsn_interval_ = n; }

    // Overrides the missing_lsns_ capacity, so tests can reach it without millions of entries.
    void set_max_missing_lsns(size_t n) { max_missing_lsns_ = n; }

    // The watermark mutex, so a test that isn't a friend can hold it the way a login does.
    AsyncMutex& watermark_mutex() { return watermark_mu_; }

    // Highest watermark a committed append() proposed, or -1.
    int64_t last_proposed_sync_lsn() const {
        std::lock_guard lk{state_mu_};
        return last_proposed_sync_lsn_;
    }

    // Highest commit_lsn the current session has piggybacked, or -1.
    int64_t client_commit_lsn() const {
        std::lock_guard lk{state_mu_};
        return client_commit_lsn_;
    }

    bool overlay_has_extent(int64_t lsn) const {
        std::lock_guard lk{overlay_mu_};
        return overlay_extent_.contains(lsn);
    }

    // Holds the apply queue: committed entries still make their commit-time effects, but none applies until
    // resume_apply(). Lets a test observe an entry between its commit and its apply.
    void pause_apply();
    void resume_apply();

    // Checks the invariants that hold whenever no operation or apply is running; returns one line per violation.
    // Every CRAFT test fixture checks it in TearDown().
    std::vector< std::string > invariant_violations();

    // The durable image as last handed to the state store, or about to be.
    CraftDurableState durable_state() const {
        std::lock_guard lk{state_mu_};
        return durable_;
    }

    // The journal is reclaimed up to here (entries at or below it are gone).
    int64_t reclaimed_upto() const {
        std::lock_guard lk{state_mu_};
        return reclaimed_upto_;
    }

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
    // comment): true from the moment recovery starts until recover() completes successfully (see
    // is_faulted() for the failure outcome instead).
    bool is_recovering() const { return recovering_.load(std::memory_order_acquire); }

    // Faults the partition, as a failure would.
    void fault_for_test() { fault(); }
#endif

private:
    // ── RAFT listener ──────────────────────────────────────────────────────
    //
    // Handles the two CRAFT RAFT entry types (SyncRSCommitLSN, InternalLogin).
    // All other HomeStore callbacks are no-ops for this backend.

    class CraftRaftListener : public homestore::repl_dev_listener {
    public:
        explicit CraftRaftListener(CraftReplDev* owner) : owner_{owner} {}

        // Validates the entry and queues it on the device's apply queue (see PendingApply).
        void on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                       std::vector< homestore::multi_blk_id > const& blkids,
                       cintrusive< homestore::repl_req_ctx >& ctx) override;

        // Fails this member's own pending proposal (see make_homestore_raft_proposer()).
        void on_error(homestore::ReplServiceError err, const sisl::blob& header, const sisl::blob& key,
                      cintrusive< homestore::repl_req_ctx >& ctx) override;

        // ── no-ops ────────────────────────────────────────────────────────
        bool on_pre_commit(int64_t, const sisl::blob&, const sisl::blob&,
                           cintrusive< homestore::repl_req_ctx >&) override {
            return true;
        }
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
        void on_rollback(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                         cintrusive< homestore::repl_req_ctx >& ctx) override;
        // Starts restart recovery (recover()) fire-and-forget (defined out-of-line in the .cpp -- needs
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

    // LEADER-only (S5 / SDSTOR-22907). Resolves every slot <= upto not yet confirmed present or Empty by
    // THIS leader: a known gap in missing_lsns_, or a slot beyond this leader's own last_append_lsn that
    // upto reaches past (never even opened as a gap -- e.g. a client Resolve naming a dLSN this leader
    // never itself received, or a login rs_commit_lsn that is the QUORUM's max, not this replica's own).
    // For each: fetch from the quorum; any responding member reporting is_empty=true wins that slot; a
    // slot no responding member reports data or Empty for is quorum-lacks-evidence, minted fresh as Empty;
    // a slot some responding member has real data for is written into this leader's own journal so it can
    // also correctly serve fetch_data() to others.
    //
    // A fresh Empty verdict needs quorum-lacks evidence: this leader plus enough distinct other voters to make
    // a majority of CraftRaftProposer::voters() (see VoterTally). An Empty inherited from a responder's prior
    // verdict needs no count.
    //
    // Returns the slots <= upto now verdicted Empty (freshly minted or inherited), and the ones left
    // unresolved: too few responders to mint Empty, or a local journal write that failed after the
    // fetch. append() never proposes past the first unresolved slot.
    //
    // Fails closed (no partial resolution) if peer_fetcher_ / raft_proposer_ is unset or the broadcast
    // itself fails outright -- unlike apply_sync_rs_commit_lsn's own peer_fetcher_==nullptr case (a
    // replica's own best-effort, non-gating catch-up), this method exists to gate a RAFT proposal, so an
    // inability to consult the quorum at all must surface as an error, never be silently swallowed.
    // Leadership and term are re-checked once the fetch returns, before any local write: NOT_LEADER /
    // STALE_TERM with nothing mutated if either changed during the await.
    struct PreResolution {
        std::vector< int64_t > empty_slots;
        std::vector< int64_t > unresolved;
    };
    // term: the session the round belongs to (STALE_TERM if state_.term differs, at entry or later).
    // counted_lackers, if set, are the only members whose lack counts toward a fresh Empty; every responder still
    // supplies data and committed Empty verdicts. Login sets it to the members its poll quiesced: one it didn't could
    // still ack an old-session write to a slot after reporting it lacks it. Set, a fetch with no usable reply fails
    // NO_QUORUM rather than not_supported.
    async_result< PreResolution >
    pre_resolve_slots(int64_t upto, uint64_t term,
                      std::optional< std::vector< craft::peer_id_t > > counted_lackers = std::nullopt);

    // Login step 1, leader-only: quiesces this member's writes at or below `term` (the latest committed session
    // term), then polls every other member with the same fence and waits for all of them up to the poll timeout.
    // Fails STALE_TERM if any member is already past `term`, and NO_QUORUM unless this member and the counted voters
    // are a majority.
    struct LoginPoll {
        int64_t rs_commit_lsn{-1}; // max last_append_lsn over this member and every responder
        // The members that replied, so are quiesced (not this one), learners included: quorum coverage counts voters
        // (VoterTally), not quiesced.size().
        std::vector< craft::peer_id_t > quiesced{};
    };
    async_result< LoginPoll > login_poll(uint64_t term);

    // Login step 2, leader-only: resolves every slot this member lacks up to rs_commit_lsn, holes below its own
    // last_append_lsn included, counting only the quiesced members' lacks. All or nothing: the watermark is fixed by
    // the poll, so any slot left unresolved fails NO_QUORUM before anything is proposed; a partition that is or becomes
    // faulted fails INTERNAL_ERROR. Returns the Empty verdicts the login's SyncRSCommitLSN carries.
    async_result< std::vector< int64_t > > login_resolve(int64_t rs_commit_lsn, uint64_t term,
                                                         std::vector< craft::peer_id_t > quiesced);

    // Login step 3, leader-only, under watermark_mu_ (`lock` is the proof): proposes SyncRSCommitLSN{rs_commit_lsn,
    // the new client's token, the step-2 verdicts} and waits for the leader to apply it, then checks the leader is
    // exactly at rs_commit_lsn. Always proposes, even if rs_commit_lsn is already covered. Fails NO_QUORUM if
    // commit_lsn isn't rs_commit_lsn afterwards (a late Sync past it, or a slot still missing), STALE_TERM if a login
    // replaced the session meanwhile, and INTERNAL_ERROR if the partition is or becomes faulted.
    async_status login_sync(AsyncMutex::Lock const& lock, int64_t rs_commit_lsn, uint64_t client_token,
                            std::vector< int64_t > empty_slots, uint64_t term);

    // Proposes SyncRSCommitLSN{rs_commit_lsn, client_token, empty_slots} and waits for it to commit and apply,
    // re-checking leadership and the session first; raises last_proposed_sync_lsn_ once it has. The caller holds
    // watermark_mu_ (`lock`): append() and login_sync().
    async_status propose_sync(AsyncMutex::Lock const& lock, int64_t rs_commit_lsn, uint64_t client_token,
                              std::vector< int64_t > const& empty_slots, uint64_t term);

    // Watchdog trigger body: polls the quorum's LSNs and append()s up to min(max(last_append),
    // client_commit_lsn_) if that's past what was last proposed. Leader-only; detached from
    // on_watchdog_tick().
    async_status watchdog_sync();

    // Periodic trigger: every sync_lsn_interval_ appended LSNs, detaches
    // append(min(last_append_lsn, client_commit_lsn_)) if that's past what was last proposed.
    // Leader-only; called from write()'s post-flight.
    void maybe_trigger_periodic_sync();

    // ── RAFT apply queue ──
    // Committed entries apply one at a time, in log order, each fully (catch-up included) before the next starts.
    // on_commit() validates and queues; a SyncRSCommitLSN's state change (its verdicts and watermarks) is made there
    // too, also in log order, so its fences take effect at commit rather than when the queue reaches it. The drainer
    // starts inline in on_commit() and runs until the queue empties. The queue is unbounded: on a follower whose
    // catch-up keeps timing out, later entries pile up behind it.
    struct PendingApply {
        int64_t lsn{-1};
        CraftEntryType type{};
        int64_t rs_commit_lsn{-1}; // SyncRSCommitLSN
        uint64_t client_token{0};
        std::vector< int64_t > to_free{};           // SyncRSCommitLSN: held slots its verdicts voided
        uint64_t term{0};                           // InternalLogin
        intrusive< homestore::repl_req_ctx > ctx{}; // this member's own proposal, if any; completed once applied

        // As the durable image holds it (CraftDurableState::pending).
        CraftPendingEntry as_pending() const {
            return CraftPendingEntry{.raft_idx = lsn,
                                     .type = type,
                                     .rs_commit_lsn = rs_commit_lsn,
                                     .client_token = client_token,
                                     .term = term,
                                     .to_free = to_free};
        }
    };
    // Admits a committed entry: unless the queue is stopped, makes its commit-time effects (a Sync's state change from
    // empty_slots, an InternalLogin's write fence) and queues the rest. Under apply_mu_, so once stop_apply() has run,
    // nothing changes.
    void enqueue_apply(PendingApply entry, std::vector< int64_t > const& empty_slots = {});
    std::optional< PendingApply > next_apply();
    async_status drain_apply();
    async_status apply_one(PendingApply const& entry);

    std::mutex apply_mu_;
    std::deque< PendingApply > apply_queue_; // guarded by apply_mu_
    bool apply_running_{false};              // guarded by apply_mu_: a drainer is active
    bool apply_stopped_{false};              // guarded by apply_mu_
    bool apply_paused_{false};               // guarded by apply_mu_; set by stop(keep_queued) and pause_apply()
    bool apply_idle_wanted_{false};          // guarded by apply_mu_: stop_apply() is waiting on apply_idle_
    sisl::async::value_awaitable< bool > apply_idle_;
    // RAFT entries at or below this were applied before the restart, or are queued again from the restored image: the
    // log replay delivers some of them again, and they are skipped. Guarded by apply_mu_; -1 = nothing restored.
    int64_t restored_raft_idx_{-1};
    // The entry being applied, until the image records it as applied: an image taken meanwhile holds it as pending.
    // Guarded by apply_mu_.
    std::optional< CraftPendingEntry > applying_;

    // A SyncRSCommitLSN's state change, made at commit: the range and index checks, its Empty verdicts (their slots
    // stop being readable now), the newly spanned missing range, last_append_lsn and applied_sync_lsn_. Returns the
    // held slots its verdicts voided, whose blocks finish_sync_rs_commit_lsn() frees.
    result< std::vector< int64_t > > install_sync_rs_commit_lsn(int64_t rs_commit_lsn,
                                                                std::vector< int64_t > const& empty_slots);
    // Its I/O, run by the apply queue: frees the voided slots' blocks, catches up the slots still missing at or below
    // rs_commit_lsn from a peer, and advances commit_lsn through commit(), which applies each slot it passes to the
    // index.
    async_status finish_sync_rs_commit_lsn(int64_t rs_commit_lsn, uint64_t client_token,
                                           std::vector< int64_t > to_free);
    // Both halves back to back, for tests that apply a Sync without the queue.
    async_status apply_sync_rs_commit_lsn(int64_t rs_commit_lsn, uint64_t client_token,
                                          std::vector< int64_t > empty_slots);
    // InternalLogin's apply, run by the apply queue: the new session is made durable, then admitted.
    async_status apply_internal_login(uint64_t client_token, uint64_t term);

    // Advance commit_lsn toward upto_lsn by applying each committable slot to the index (internal
    // helper; never a wire op -- there is no standalone commit verb on the CRAFT wire). Stalls
    // (returns without reaching upto_lsn) at the first gap in missing_lsns_ -- not an error, just the
    // achieved commit_lsn. At most one run is ever active at a time (commit_running_); a concurrent
    // caller is a safe no-op since the in-flight run covers the same ground and every subsequent
    // write()/keep_alive() retries the advance. Callers: write()'s post-flight piggyback (best-effort,
    // every outcome including real errors is ignored), keep_alive() (propagates real errors, since
    // advancing the frontier is its entire purpose -- a stall is still not an error there either), and
    // the SyncRSCommitLSN apply (finish_sync_rs_commit_lsn; logs an error).
    async_result< int64_t > commit(int64_t upto_lsn);

    // Core apply-one-slot algorithm behind commit(), parameterized by the index write/delete
    // operations (write_index_fn_t / delete_index_fn_t, declared at the top of this class) so tests
    // can exercise it against a fake index instead of a real VolumeIndexTable. commit() binds these
    // to indx_tbl_'s real methods; commit_with() (test-only) binds test doubles.
    async_result< int64_t > commit_impl(int64_t upto_lsn, write_index_fn_t write_fn, delete_index_fn_t delete_fn);

    // Must be called with state_mu_ held. Returns true if commit_lsn_snapshot has crossed
    // checkpoint_lsn_interval_ since last_checkpoint_lsn_ -- and if so, updates last_checkpoint_lsn_ to
    // commit_lsn_snapshot before returning, so an overlapping caller under the same lock observes the
    // new value rather than racing on a stale one. Split from fire_checkpoint_trigger() because that
    // awaits (via detail::detach) and must not run with the lock held.
    bool checkpoint_interval_crossed_locked(int64_t commit_lsn_snapshot);

    // Fires checkpoint_trigger_ detached (fire-and-forget), force=false so it coalesces with any flush
    // already in flight. Call only when checkpoint_interval_crossed_locked() just returned true for the
    // same commit_lsn_snapshot. Safe without state_mu_ held. Shared by every commit_lsn-advance path:
    // commit_impl() (covers write()'s piggyback, keep_alive() and the SyncRSCommitLSN apply) and
    // finish_sync_rs_commit_lsn()'s walk-forward loop, which runs only without an index.
    void fire_checkpoint_trigger(int64_t commit_lsn_snapshot);

    // ── durable state (S8) ──
    // Makes the in-memory image (durable_) durable. Callers update durable_ under state_mu_, await this, then make the
    // change it protects (write-ahead). Concurrent calls share one writer: each returns once an image taken after its
    // call has been persisted, and fails if that write failed. A no-op without a state store.
    async_status persist_state();
    async_status run_persist();
    // A checkpoint that started after every slot <= lsn was applied to the index has completed: records lsn as the
    // durable commit frontier, then frees the blocks those slots superseded.
    async_status advance_durable_commit(int64_t lsn);
    // Restart recovery, before rebuild_overlay(): loads the image and restores state_ from it and the journal tail.
    // Returns the image's pending entries, for requeue_pending() once the overlay is rebuilt.
    async_result< std::vector< CraftPendingEntry > > restore_state();
    // Frees blocks an index write superseded: at once without a state store, otherwise once the durable commit frontier
    // reaches replacing_lsn (until then a restart re-applies older slots, which may still need them).
    async_status release_superseded(int64_t replacing_lsn, std::vector< homestore::blk_id > blocks);
    async_status free_blocks(int64_t lsn, std::vector< homestore::blk_id > blocks);
    // Frees the blocks of a write or fill an Empty verdict voided at lsn, once the verdict is durable; leaks them if it
    // can't be made durable (see the .cpp).
    async_status free_voided(int64_t lsn, homestore::multi_blk_id blkid);

    // ── journal reclaim (S8) ──
    // The journal is reclaimed up to floor = min(durable commit frontier as persisted, all_committed_lsn), and below
    // every slot a committed Sync still has to free:
    //   - below the persisted frontier the index is durable, so a restart no longer applies those slots again;
    //   - below all_committed_lsn every member has committed, so no member's catch-up needs them. It is the client's
    //     word: taking the min with our own durable frontier keeps a wrong value from reclaiming anything a restart
    //     here still needs. One that is too large can leave a lagging member unable to catch up (liveness, not
    //     safety: a fetch of a reclaimed slot fails, it never reads as not held);
    //   - a Sync's verdicts let the commit, and so both of the above, step over the slots they void long before its
    //     apply frees their blocks, which it reads from the slots' journal records (awaiting_free_).
    // Recomputed when a durable frontier is persisted and on each keep_alive; one runner at a time. Never without a
    // state store: the journal is then the only copy.
    int64_t reclaim_floor_locked() const;
    void request_reclaim();
    async_status run_reclaim();

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

    // populate_overlay() for one journaled slot. blkid is this replica's own block reference for it (a fetched slot's
    // blkid is the peer's). A slot shorter than a page is a malformed record: logged, no entry.
    void populate_overlay_from_slot(int64_t lsn, JournalSlot const& slot, homestore::multi_blk_id const& blkid);

    // Retires the overlay entry for each LBA in [start_lba, end_lba] whose recorded lsn equals retiring_lsn.
    // A higher-dLSN entry for the same LBA (a later append not yet committed) survives. Used when commit applies
    // a slot or skips an Empty one, and when apply frees an Empty slot's blocks.
    void retire_overlay_range(lba_t start_lba, lba_t end_lba, int64_t retiring_lsn);
    // Retires every overlay entry lsn still holds, over the range it was published over (no overlay scan, no I/O).
    void retire_overlay_slot(int64_t lsn);

    // Claims lsn for one journal write, so pre-resolution, apply catch-up and write() never journal the same slot
    // (H1). in_flight: another write is outstanding; held: the slot is already journaled or Empty. Caller holds
    // state_mu_; a claim is released by a SlotWriteClaim.
    enum class SlotClaim { claimed, in_flight, held };
    SlotClaim claim_slot_write_locked(int64_t lsn);
    // What claim_slot_write_locked() would return, without claiming (claimed: free to claim). Caller holds state_mu_.
    SlotClaim peek_slot_write_locked(int64_t lsn) const;
    // True if journaling lsn would open a new gap while missing_lsns_ is at capacity. Filling a gap, or appending
    // right after the frontier, is always allowed so the set can drain. Caller holds state_mu_.
    bool missing_at_capacity_locked(int64_t lsn) const;
    // True if term is the current session and no newer login has committed. Caller holds state_mu_.
    bool session_current_locked(uint64_t term) const { return term == state_.term && term >= login_fence_term_; }
    // True if a login poll has quiesced writes at term. Caller holds state_mu_.
    bool write_quiesced_locked(uint64_t term) const { return quiesce_term_ && term <= *quiesce_term_; }
    struct SlotWriteClaim {
        CraftReplDev* dev;
        int64_t lsn;
        ~SlotWriteClaim() {
            std::lock_guard lk{dev->state_mu_};
            dev->in_flight_write_dlsns_.erase(lsn);
        }
    };

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

    // Queues again, in log order, the entries an image held as committed but not applied (those above its
    // last_applied_raft_idx); the log replay skips them from then on. See the .cpp for what applying them again leaves
    // out.
    void requeue_pending(std::vector< CraftPendingEntry > pending);

    volume_id_t vol_id_;
    unique< CraftJournalBackend > journal_;
    uint32_t lba_size_;
    shared< VolumeIndexTable > indx_tbl_;
    CraftPartitionState state_;
    // TODO: Can this be replaced with boost::icl::interval_set? Particularly helpful when a write
    // comes in with a huge gap -- gap-fill loops (write(), apply_sync_rs_commit_lsn()) currently
    // insert one LSN at a time under state_mu_, which is O(gap width) instead of O(log ranges).
    std::set< int64_t > missing_lsns_;         // gaps between commit_lsn and last_append_lsn
    size_t max_missing_lsns_{2'000'000};       // see missing_at_capacity_locked(); overridden only by tests
    std::unordered_set< int64_t > empty_lsns_; // slots positively verdicted Empty by a prior SyncRSCommitLSN (S5)
    bool commit_running_{false}; // at most one commit_impl() run active at a time -- see commit()'s doc comment
    // Highest slot commit_impl() has started indexing, so apply's Empty guard sees it before commit_lsn moves. Reset
    // to commit_lsn when the run ends. Guarded by state_mu_.
    int64_t indexing_lsn_{-1};
    // dlsns with a journal write outstanding, from write(), pre-resolution or apply catch-up -- see
    // write()'s doc comment at the in_flight_write_dlsns_.contains() check for why this exists.
    std::set< int64_t > in_flight_write_dlsns_;
    mutable std::mutex state_mu_; // guards state_, missing_lsns_, empty_lsns_, commit_running_,
                                  // in_flight_write_dlsns_, last_checkpoint_lsn_, client_commit_lsn_, and the
                                  // durable-state members (durable_ and its bookkeeping)

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
    // The LBA range each lsn was published over, kept while the lsn still owns an overlay entry, so
    // retire_overlay_slot() visits only that range. Bounded by the overlay itself. Guarded by overlay_mu_.
    struct OverlayExtent {
        lba_t start_lba{0};
        uint32_t nlbas{0};
        uint32_t live{0}; // overlay entries the lsn still owns; the extent goes when this reaches 0
    };
    std::unordered_map< int64_t, OverlayExtent > overlay_extent_;
    mutable std::mutex overlay_mu_; // lock order: state_mu_ before overlay_mu_

    // Every overlay_ insert and erase goes through these two, which keep overlay_extent_'s counts. Caller holds
    // overlay_mu_. The first publishes entry at lba (an lsn published over [start_lba, start_lba + nlbas)); the second
    // erases it and returns the next iterator.
    void set_overlay_entry_locked(lba_t lba, OverlayEntry entry, lba_t start_lba, uint32_t nlbas);
    std::unordered_map< lba_t, OverlayEntry >::iterator
    drop_overlay_entry_locked(std::unordered_map< lba_t, OverlayEntry >::iterator it);

    bool login_in_progress_{false};
    std::mutex login_mu_;
    CraftRaftListener raft_listener_;
    // TODO(S9): a raw pointer the device doesn't own, yet background work (pre-resolution, the watchdog, apply
    // catch-up) uses it after awaits while holding only the device. Whoever wires it must make it outlive every
    // round, e.g. by giving the device ownership, as for the proposer.
    CraftPeerFetcher* peer_fetcher_{nullptr}; // null until S9 wires CraftConnector
    uint32_t peer_fetch_timeout_ms_{5000};    // deadline for fetch_data; overridden via set_peer_fetch_timeout_ms()
    // The proposer every round uses: the one create() was given, which the device owns, or a test's
    // (set_raft_proposer). Null: nothing is proposed.
    unique< CraftRaftProposer > owned_raft_proposer_;
    CraftRaftProposer* raft_proposer_{nullptr};
    // Held by whoever may propose a SyncRSCommitLSN, i.e. move the replica-set watermark: one append() round, or a
    // login from its poll through its InternalLogin. Background rounds try_lock() and skip; login and Resolve lock()
    // and wait their turn.
    AsyncMutex watermark_mu_;
    std::atomic< bool > watchdog_round_in_flight_{false}; // single-flight gate for a whole watchdog_sync() round
    int64_t last_proposed_sync_lsn_{-1}; // highest SyncRSCommitLSN this leader committed: append()'s or a login's
                                         // (guarded by state_mu_)
    int64_t applied_sync_lsn_{-1};       // highest SyncRSCommitLSN watermark applied here (guarded by state_mu_)
    int64_t last_periodic_sync_lsn_{-1}; // last_append_lsn at the last periodic trigger (guarded by state_mu_)
    // Highest commit_lsn the current session has piggybacked on write/read/keep_alive: the client's
    // resolved prefix, so nothing at or below it is still in flight.
    // Reset when InternalLogin starts a new session. Guarded by state_mu_.
    int64_t client_commit_lsn_{-1};
    // The term of the latest committed InternalLogin, raised at commit ahead of its apply: writes and leader rounds at
    // a lower term are rejected (STALE_TERM), so the old session can't act while the login waits in the apply queue.
    // Guarded by state_mu_.
    uint64_t login_fence_term_{0};
    // Writes at or below this term are quiesced by a login poll (STALE_TERM); unset until the first poll. Only rises.
    // Fences writes only: reads, keep_alive and leader rounds go on. In memory only: a member that restarts before the
    // login finishes comes back unquiesced, a case left undefined. Guarded by state_mu_.
    std::optional< uint64_t > quiesce_term_;
    int64_t sync_lsn_interval_{128}; // periodic trigger cadence; from sync_rs_commit_lsn_interval

    // Set by CraftRaftListener::on_restart() before detaching recover(), cleared by recover()
    // ONLY on a successful rebuild_overlay() (see faulted_ below for the failure path) --
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
    // A plain atomic bool suffices for the single-writer part: on_restart()/recover() are the only
    // writers, and HomeStore's OWN calling contract (not a runtime property of this class) is what
    // ensures they run as one complete cycle before any next one -- see recover()'s comment.
    std::atomic< bool > recovering_{false};
    // Set (never cleared) when this partition can no longer serve correct data:
    //   - recover(), if restore_state() can't read the durable image or the journal tail: restarting empty would
    //     forget the session and the verdicts;
    //   - recover(), if rebuild_overlay() fails: overlay_ is silently INCOMPLETE (some LSNs applied,
    //     then it bailed), so resuming client I/O could serve stale pre-write data with no error at all;
    //   - apply_sync_rs_commit_lsn(), if an Empty verdict names a slot already applied to the index, or one
    //     commit_impl() is indexing: freeing it would leave the index pointing at free blocks.
    // write()/read()/keep_alive()/request_resolution() check this FIRST, ahead of recovering_, and reject
    // with volume_error::INTERNAL_ERROR permanently -- an honest, fail-closed fault rather than fail-open
    // data corruption. fetch_data()/get_rs_commit_lsn() reject too, so peers treat it as down, and append()
    // and the watchdog stop proposing. There is deliberately no self-recovery path: an operator (or a future
    // re-sync-from-peer mechanism) must intervene. Set through fault(), which also marks the state record, so a
    // restart doesn't bring the partition back from an image that lacks what it applied since; and from then on
    // nothing is persisted.
    std::atomic< bool > faulted_{false};
    void fault();

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
    write_index_fn_t test_write_fn_;            // set_index_fns_for_test(); test-only
    delete_index_fn_t test_delete_fn_;
#endif
    checkpoint_trigger_fn_t checkpoint_trigger_{}; // empty until production wiring; unit tests inject a mock
    int64_t checkpoint_lsn_interval_{128};         // commit_lsn delta between checkpoint triggers; see
                                                   // set_checkpoint_lsn_interval()
    int64_t last_checkpoint_lsn_{-1}; // commit_lsn as of the last triggered checkpoint (guarded by state_mu_);
                                      // restore_state() seeds it with the restored commit_lsn

    // ── durable state (S8) ──
    unique< CraftStateStore > state_store_; // null: nothing persisted, nothing restored
    CraftDurableState durable_;             // the image persist_state() writes (guarded by state_mu_)
    // persist_state()'s next round: the callers that joined it share the image the writer takes when it starts it, and
    // the round completes with whether that image was written. Null until a caller joins. Guarded by state_mu_.
    shared< sisl::async::shared_awaitable< bool > > persist_next_;
    bool persist_running_{false}; // a writer (run_persist) is active (guarded by state_mu_)
    // Slots at or below this were journalled before the restart and may already be in the durable index: a checkpoint
    // can run between our triggers, so the index can be ahead of durable_commit_lsn. Applying one again must not free
    // the block it replaces, which may belong to a NEWER slot the index already holds (the replay applies it next).
    // Leaking those blocks is bounded by the replay window; freeing them would corrupt. Guarded by state_mu_.
    int64_t replay_upto_{-1};
    // Blocks an index write superseded, each with the slot that replaced it, until the durable commit frontier reaches
    // that slot. Lost on a crash: a bounded leak. Guarded by state_mu_.
    std::vector< std::pair< int64_t, homestore::blk_id > > deferred_frees_;

    // ── snapshot install ──
    // Ops in progress, counted by admit_op(); installing_ refuses new ones. Both seq_cst: an op either sees the
    // barrier, or the barrier's drain sees the op.
    std::atomic< int64_t > active_ops_{0};
    std::atomic< bool > installing_{false};
    int64_t read_gate_{-1};                           // guarded by state_mu_
    std::vector< homestore::blk_id > snapshot_frees_; // guarded by state_mu_
    class OpAdmission {
    public:
        explicit OpAdmission(CraftReplDev* dev) : dev_{dev} {}
        OpAdmission(OpAdmission const&) = delete;
        OpAdmission& operator=(OpAdmission const&) = delete;
        ~OpAdmission() {
            if (dev_) dev_->active_ops_.fetch_sub(1);
        }
        explicit operator bool() const { return dev_ != nullptr; }

    private:
        CraftReplDev* dev_;
    };
    OpAdmission admit_op() {
        active_ops_.fetch_add(1);
        if (installing_.load()) {
            active_ops_.fetch_sub(1);
            return OpAdmission{nullptr};
        }
        return OpAdmission{this};
    }
    // Nothing runs that could change the journal, the index or the state: the install barrier's drain waits for it.
    bool snapshot_drained();
    // Starts the apply drainer if entries wait and nothing pauses or stops it.
    void resume_apply_if_queued();

    // RetainFrom pins and when each was taken here, by transfer identity (guarded by state_mu_).
    struct RetainPinState {
        int64_t w{-1};
        std::chrono::steady_clock::time_point since{};
    };
    std::map< std::pair< int64_t, uint32_t >, RetainPinState > retain_pins_;
    // Drops the pins past their age or slot cap; refreshes durable_.retain_pins. Under state_mu_.
    void prune_retain_pins_locked();
    void sync_durable_pins_locked();

    // ── journal reclaim (S8), all guarded by state_mu_ ──
    int64_t reclaimed_upto_{-1};           // journal entries at or below this are gone
    int64_t persisted_durable_commit_{-1}; // durable_commit_lsn as last persisted: the floor's input
    bool reclaim_running_{false};          // a run_reclaim() is active
    bool reclaim_unsupported_{false};      // the backend said not_supported: stop asking
    bool reclaim_stopped_{false};          // stop(): no reclaim runs anymore
    // The slots committed Syncs void and free when they apply (from the commit, or the re-queue at a restart, until the
    // frees are done): the reclaim stays below them.
    std::multiset< int64_t > awaiting_free_;
    // fire_checkpoint_trigger()'s detached rounds still running: after its flush, one persists the frontier and frees
    // blocks, so stop() waits for them. Guarded by state_mu_.
    int checkpoints_running_{0};
    bool checkpoints_stopped_{false}; // stop(): none starts anymore
    shared< sisl::async::value_awaitable< bool > > checkpoints_idle_;
    void checkpoint_done();
    // Completed when the running reclaim ends, for a stop() waiting for it.
    shared< sisl::async::value_awaitable< bool > > reclaim_idle_;
};

} // namespace homeblocks
