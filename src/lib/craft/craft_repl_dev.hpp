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
#include <homestore/replication/repl_dev.hpp>

#include <atomic>
#include <initializer_list>
#include <mutex>
#include <set>
#include <vector>

namespace homestore {
class home_log_store;
}

namespace homeblocks {

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
    int64_t commit_lsn{-1};      // contiguous committed prefix (== Synced)
    int64_t last_append_lsn{-1}; // highest appended dLSN (may be uncommitted)
    uint64_t client_token{0};    // token from the last successful InternalLogin
    uint64_t term{0};            // current session term
};

// One journal slot returned by fetch_data() (server-to-server resync; never crosses the CLIENT wire). Four-way:
// data (is_empty=false, all_zeros=false), zero write (all_zeros=true, no data), Empty (is_empty=true), or
// omitted from the response (not-present-here).
struct JournalSlot {
    int64_t lsn{-1};
    bool is_empty{false};
    bool all_zeros{false};
    lba_t lba{0};
    lba_count_t len{0};
    sisl::sg_list data{};
};

// ─── journal backend abstraction ─────────────────────────────────────────────
//
// Injected into CraftReplDev so unit tests can supply a mock without touching
// HomeStore. Production code passes HomeStoreCraftJournalBackend (defined in
// craft_repl_dev.cpp).

class CraftJournalBackend {
public:
    virtual async_status write_slot(int64_t lsn, lba_t lba, lba_count_t len,
                                   homestore::multi_blk_id blkid, bool all_zeros) = 0;
    virtual async_result< JournalSlot > read_slot(int64_t lsn) = 0;
    // Drop all entries with seq_num > lsn; lsn becomes the new journal tail.
    virtual async_status truncate_to(int64_t lsn) = 0;
    virtual ~CraftJournalBackend() = default;
};

// Factory that wraps a HomeStore log store. Used by volume.cpp when creating a CRAFT-mode volume.
// Tests inject MockCraftJournalBackend directly.
unique< CraftJournalBackend > make_homestore_journal_backend(shared< homestore::home_log_store > logstore);

// ─── CraftReplDev ─────────────────────────────────────────────────────────────
//
// One instance per CRAFT-mode volume. Implements the full CRAFT data plane
// (write, read, login, truncate, ...) on top of a HomeStore log store and
// index. Non-CRAFT volumes are unaffected.

class CraftReplDev {
public:
    explicit CraftReplDev(volume_id_t vol_id, unique< CraftJournalBackend > journal);
    ~CraftReplDev() = default;

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
    // that). An EMPTY `data` is a zero write (WRITE_ZEROES/unmap over [addr, addr+len)). The ack returns the
    // achieved {commit_lsn, last_append_lsn} snapshotted with the append -- every CRAFT IO response piggybacks the
    // watermarks (the wire's write_rsp), so any round-trip refreshes the client's model of this member.
    async_result< craft::lsn_pair > write(craft::client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                          sisl::sg_list data, bool all_zeros = false);

    // read_lsn is the horizon H: serve the latest version <= H for [addr, addr+len), from the LBA index if applied
    // or from the journal-tail overlay if only Appended (no index write on the read path). Never fetches from a
    // peer. Fills the caller-owned `dest` in place (data -> bytes, holes -> zeros) and returns craft::read_result:
    // the sparse layout (which byte sub-ranges were data vs holes) plus the piggybacked watermarks.
    async_result< craft::read_result > read(craft::client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len,
                                            sisl::sg_list dest);

    // Advance the frontier toward hdr.commit_lsn + reset the client-liveness watchdog -- which is WHY it is
    // term-fenced: a deposed client must not be able to keep its session alive. hdr.all_committed_lsn is the
    // client-computed set-wide min commit_lsn; the journal may be reclaimed below
    // min(all_committed_lsn, checkpointed apply frontier). Returns the achieved watermarks.
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

    // Return {commit_lsn, last_append_lsn} for the local partition.
    async_result< craft::lsn_pair > get_lsns(volume_id_t vol_id);

    // Alias of get_lsns exposed to peer servers during GetRSCommitLSN broadcast.
    async_result< craft::lsn_pair > get_rs_commit_lsn();

    // Drop all journal entries with dLSN > lsn; clear missing-set entries above lsn; clamp last_append_lsn.
    // Called only during login (quiesced -- no concurrent writes). commit_lsn is NOT changed.
    async_status truncate(int64_t lsn);

    // Propose a SyncRSCommitLSN RAFT entry (called by watchdog or leader during login).
    async_status append(int64_t sync_to, uint64_t client_token);

    // Return raw journal data for the requested LSNs. Empty slots return
    // JournalSlot{.is_empty=true} rather than an error.
    async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns);

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

#ifdef _PRERELEASE
    // Seeds partition watermarks and the missing set directly, bypassing write().
    // Only compiled when _PRERELEASE is defined; never present in production binaries.
    void seed_lsns(int64_t last_append, std::initializer_list< int64_t > missing = {});
    // Seeds commit_lsn independently of seed_lsns (which only touches last_append + missing).
    void seed_commit_lsn(int64_t commit);
    // Seeds the Empty-verdict set; does not affect missing_lsns_. Clears any prior seeded empties.
    void seed_empty(std::initializer_list< int64_t > empty);
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
        void on_restart() override {}
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
        CraftReplDev* owner_;
    };

    // Called from CraftRaftListener::on_commit after deserialising the entry type.
    void apply_sync_rs_commit_lsn(int64_t rs_commit_lsn, uint64_t client_token);
    void apply_internal_login(uint64_t client_token, uint64_t term);

    volume_id_t vol_id_;
    unique< CraftJournalBackend > journal_;
    CraftPartitionState state_;
    std::set< int64_t > missing_lsns_; // gaps between commit_lsn and last_append_lsn
    std::set< int64_t > empty_lsns_;   // slots positively verdicted Empty by a prior SyncRSCommitLSN (S5)
    mutable std::mutex missing_mu_;    // guards state_, missing_lsns_, and empty_lsns_
    bool login_in_progress_{false};
    std::mutex login_mu_;
    CraftRaftListener raft_listener_;
};

} // namespace homeblocks
