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

#include "mem_craft_cluster.hpp"

#include <algorithm>
#include <string>

namespace homeblocks::craft {

namespace {
auto fail(craft_error e) { return std::unexpected(make_error_condition(e)); }
} // namespace

MemTransport::MemTransport(std::vector< replica_endpoint > members, uint32_t lba_size) :
        members_{std::move(members)}, lba_size_{lba_size} {
    if (!members_.empty()) leader_ = members_.front().id;
}

void MemTransport::register_replica(MemCraftReplica* r) {
    std::lock_guard< std::mutex > g{mu_};
    by_id_[r->id()] = r;
}

peer_id_t MemTransport::leader() const {
    std::lock_guard< std::mutex > g{mu_};
    return leader_;
}
void MemTransport::set_leader(peer_id_t id) {
    std::lock_guard< std::mutex > g{mu_};
    leader_ = id;
}

bool MemTransport::is_up(peer_id_t id) const {
    std::lock_guard< std::mutex > g{mu_};
    return !down_.contains(id);
}
bool MemTransport::write_allowed(peer_id_t id) const {
    std::lock_guard< std::mutex > g{mu_};
    if (down_.contains(id)) return false;
    if (write_keep_ && !write_keep_->contains(id)) return false;
    return true;
}

void MemTransport::set_up(peer_id_t id, bool up) {
    std::lock_guard< std::mutex > g{mu_};
    if (up) {
        down_.erase(id);
    } else {
        down_.insert(id);
    }
}
void MemTransport::force_subquorum(std::vector< peer_id_t > keep) {
    std::lock_guard< std::mutex > g{mu_};
    write_keep_ = std::unordered_set< peer_id_t, uuid_hash >(keep.begin(), keep.end());
}
void MemTransport::clear_faults() {
    std::lock_guard< std::mutex > g{mu_};
    down_.clear();
    write_keep_.reset();
}

std::vector< MemCraftReplica* > MemTransport::live_replicas_locked() const {
    std::vector< MemCraftReplica* > live;
    for (auto const& m : members_) {
        if (down_.contains(m.id)) continue;
        if (auto it = by_id_.find(m.id); it != by_id_.end()) live.push_back(it->second);
    }
    return live;
}

result< LoginResult > MemTransport::run_login(MemCraftReplica* caller, uint64_t client_token) {
    std::vector< MemCraftReplica* > live;
    std::vector< replica_endpoint > members_copy;
    uint64_t nt{0};
    {
        std::lock_guard< std::mutex > g{mu_};
        if (down_.contains(caller->id())) return fail(craft_error::REPLICA_DOWN);
        if (caller->id() != leader_) return fail(craft_error::NOT_LEADER);
        live = live_replicas_locked();
        if (live.size() < quorum(members_.size())) return fail(craft_error::NO_QUORUM);
        nt = ++term_;
        members_copy = members_;
    }
    // Drive the cold path WITHOUT holding mu_ (each replica call takes only its own lock).
    // Phase 1: GetRSCommitLSN -> rs_commit_lsn = max(quorum.last_append_lsn).
    int64_t rs = -1;
    for (auto* r : live) rs = std::max(rs, r->peek_lsns().last_append_lsn);
    // Phase 2: SyncRSCommitLSN(rs) applied to all live members.
    for (auto* r : live) r->cold_apply_sync(rs, client_token);
    // Phase 3: InternalLogin(token, term+1) applied to all live members.
    for (auto* r : live) r->cold_apply_login(client_token, nt);
    // Phase 4: truncate any stale tail above rs.
    for (auto* r : live) {
        if (r->peek_lsns().last_append_lsn > rs) r->cold_truncate_above(rs);
    }
    return LoginResult{std::move(members_copy), rs, nt, lba_size_};
}

// ── group factory (model level; no volumes attached) ──

peer_id_t mem_replica_id(volume_id_t vol_id, uint32_t index) {
    peer_id_t id = vol_id; // copy the 16 bytes, then perturb the tail deterministically with `index`
    auto* p = reinterpret_cast< uint8_t* >(&id);
    p[12] ^= static_cast< uint8_t >(index & 0xff);
    p[13] ^= static_cast< uint8_t >((index >> 8) & 0xff);
    p[14] ^= static_cast< uint8_t >((index >> 16) & 0xff);
    p[15] ^= static_cast< uint8_t >((index >> 24) & 0xff);
    return id;
}

MemReplicaGroup make_mem_replica_group(volume_id_t vol_id, uint32_t n, uint32_t page_size) {
    std::vector< replica_endpoint > members;
    members.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        members.push_back(replica_endpoint{mem_replica_id(vol_id, i), "mem://replica-" + std::to_string(i)});
    }
    auto net = std::make_shared< MemTransport >(members, page_size);
    MemReplicaGroup group;
    group.net = net;
    group.replicas.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        auto r = std::make_shared< MemCraftReplica >(members[i], page_size, net);
        net->register_replica(r.get());
        group.replicas.push_back(std::move(r));
    }
    return group;
}

} // namespace homeblocks::craft
