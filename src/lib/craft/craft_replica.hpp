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

// The per-replica CRAFT server surface -- one replica device of a partition. This is the internal
// contract behind a CRAFT-mode volume_handle: `volume` delegates its CRAFT data-plane calls to a
// `craft_replica`, and the public CRAFT free functions in home_blocks.hpp resolve a volume_handle to
// its backend and call these methods 1:1. The in-memory reference model (MemCraftReplica) implements
// this now; a production HomeStore-backed CraftReplDev can implement/adapt it later, independently.
//
// HomeStore-free: only home_blocks.hpp (which pulls the header-only homestore/error.hpp aliases) and
// std/sisl. Do NOT include the storage engine here.

#include <cstdint>
#include <vector>

#include <homeblks/home_blocks.hpp> // async_result/async_status + client-facing CRAFT types (LoginResult, io_extent, ...)

namespace homeblocks {

// ── internal / peer-only data types (deliberately NOT on the public client surface) ──

// Per-partition CRAFT state (internal to a replica implementation). Authoritative in memory; the
// production impl recovers it from the journal + superblock on restart (the reference model does not).
struct CraftPartitionState {
    int64_t  commit_lsn      {-1}; // contiguous committed prefix (== Synced)
    int64_t  last_append_lsn {-1}; // highest appended dLSN (may be uncommitted)
    uint64_t client_token    {0};  // token from the last successful InternalLogin
    uint64_t term            {0};  // current session term
};

// One journal slot returned by fetch_data() (server-to-server resync; not client-facing). Four-way:
// data (is_empty=false, all_zeros=false), zero write (all_zeros=true, no data), Empty (is_empty=true),
// or omitted from the response (not-present-here).
struct JournalSlot {
    int64_t       lsn{-1};
    bool          is_empty{false};
    bool          all_zeros{false};
    lba_t         lba{0};
    lba_count_t   len{0};
    sisl::sg_list data{};
};

class craft_replica {
public:
    virtual ~craft_replica() = default;

    // ── client-facing (what a CRAFT client issues against this one replica) ──

    // Leader-only: run the login orchestration and return the members, starting dLSN, and term.
    // A non-leader replica returns craft_error::NOT_LEADER.
    virtual async_result< LoginResult > login(uint64_t client_token) = 0;

    // Append one client-assigned write at slot `dlsn`. `addr`/`len` are BYTE offset/length, aligned to
    // the volume's lba_size (else std::errc::invalid_argument). `data` is a caller-owned (iomgr) buffer:
    // empty (size 0) => a zero write (WRITE_ZEROES / unmap; `len` is the range); non-empty => a data
    // write of exactly `len` bytes. Does NOT apply to the index; the frontier is advanced by
    // hdr.commit_lsn (piggybacked commit). craft_error::STALE_TERM if hdr.term != the session term.
    virtual async_status write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len, sisl::sg_list data) = 0;

    // Latest version <= read_lsn (horizon H) for [addr, addr+len) (BYTE offset/length, aligned). Fills the
    // caller-owned `dest` buffer in place (data sub-ranges get bytes, holes get zeros) and returns the
    // sparse layout (data vs holes). Advances the frontier to hdr.commit_lsn. STALE_TERM on term mismatch.
    virtual async_result< std::vector< io_extent > > read(client_hdr hdr, int64_t read_lsn, uint64_t addr,
                                                          uint64_t len, sisl::sg_list dest) = 0;

    // Advance the frontier toward hdr.commit_lsn + reset the client-liveness watchdog -- which is WHY
    // it is term-fenced: a stale client must not be able to keep the session alive. Returns the
    // achieved {commit_lsn, last_append_lsn}. No standalone commit verb; keep_alive is its carrier.
    virtual async_result< LSNPair > keep_alive(client_hdr hdr) = 0;

    // Snapshot {commit_lsn, last_append_lsn}.
    virtual async_result< LSNPair > get_lsns() = 0;

    // ── peer-facing (server-to-server; driven by the cold path / resync, never by a client) ──

    virtual async_result< LSNPair > get_rs_commit_lsn() = 0;
    virtual async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns) = 0;
    virtual async_status truncate(int64_t lsn) = 0;

    // This replica's endpoint id (for routing / membership).
    virtual peer_id_t id() const = 0;
};

} // namespace homeblocks
