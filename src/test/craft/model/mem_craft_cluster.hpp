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

// MemTransport -- the in-process stand-in for the CRAFT network AND the (RAFT) cold path, for the
// in-memory reference model. It routes replica<->replica cold-path calls, holds the membership and a
// designated leader, and is the fault-injection surface for tests (set_up / force_subquorum). It does
// NOT own the replicas' lifetime: they are co-owned by their volume_handles (production shape) or by
// the MemReplicaGroup below (model unit tests); MemTransport keeps raw pointers registered at creation.
//
// Membership, replica count, leader, and fault injection are control-plane / orchestration concerns
// that sit OUTSIDE the CRAFT protocol -- here they are just wired up directly (a CLI flag / harness).

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/functional/hash.hpp>

#include "mem_craft_replica.hpp"

namespace homeblocks::craft {

using uuid_hash = boost::hash< peer_id_t >;

class MemTransport {
public:
    MemTransport(std::vector< replica_endpoint > members, uint32_t lba_size);

    // Called by create_memory_volume / make_mem_replica_group as each replica is built.
    void register_replica(MemCraftReplica* r);

    // ── cold path: leader-only orchestration ──
    // login: GetRSCommitLSN -> SyncRSCommitLSN -> InternalLogin. A non-leader returns LoginResult with
    // term==0 and leader_hint set (not an error). NO_QUORUM / REPLICA_DOWN are errors.
    result< LoginResult > run_login(MemCraftReplica* caller, uint64_t client_token);

    // logout: InternalLogout applied to all live replicas. term-fenced. Returns NOT_LEADER if caller
    // is not the leader.
    status run_logout(MemCraftReplica* caller, uint64_t term);

    // ── membership / routing (consulted by the replicas' client-facing ops) ──
    peer_id_t leader() const;
    bool is_up(peer_id_t id) const;
    bool write_allowed(peer_id_t id) const; // false => this write is dropped (sub-quorum)
    std::size_t member_count() const { return members_.size(); }

    // ── fault injection (tests / CLI) ──
    void set_up(peer_id_t id, bool up);                  // bring a replica down / back up
    void force_subquorum(std::vector< peer_id_t > keep); // subsequent writes land ONLY on `keep`
    void clear_faults();
    void set_leader(peer_id_t id);

private:
    std::vector< MemCraftReplica* > live_replicas_locked() const; // requires mu_ held
    static std::size_t quorum(std::size_t n) { return n / 2 + 1; }

    std::vector< replica_endpoint > members_;
    std::unordered_map< peer_id_t, MemCraftReplica*, uuid_hash > by_id_;
    peer_id_t leader_{};
    uint64_t term_{0};
    uint32_t lba_size_{0}; // volume block size (bytes)
    std::unordered_set< peer_id_t, uuid_hash > down_;
    std::optional< std::unordered_set< peer_id_t, uuid_hash > > write_keep_;
    mutable std::mutex mu_;
};

// An N-replica in-process group with NO volumes attached -- the model layer used directly by unit
// tests (and the core of make_memory_replica_set, which additionally wraps each replica in a
// volume_handle). HomeStore-free.
struct MemReplicaGroup {
    std::shared_ptr< MemTransport > net;
    std::vector< std::shared_ptr< MemCraftReplica > > replicas; // index 0 is the default leader
};
MemReplicaGroup make_mem_replica_group(volume_id_t vol_id, uint32_t n = 3, uint32_t page_size = 4096);

// Deterministic per-replica id derived from the volume id + index (so tests are reproducible).
peer_id_t mem_replica_id(volume_id_t vol_id, uint32_t index);

} // namespace homeblocks::craft
