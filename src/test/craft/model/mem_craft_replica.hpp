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

// In-memory, HomeStore-free implementation of ONE CRAFT replica device -- the reference model a
// client is built against. It owns an in-memory data journal (dLSN -> slot), an applied LBA index,
// and derives the journal-tail overlay on demand from the unapplied journal tail. All real work is
// synchronous under a per-replica mutex; the async_result/async_status methods are 1-line coroutine
// wrappers that complete inline (no iomgr reactor). NOT crash-resilient and NOT performant by design.
//
// Peer-to-peer concerns (leader election, login orchestration, resync, fault injection) live in
// MemTransport (mem_craft_cluster.hpp), the in-process stand-in for the network.

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "craft/craft_replica.hpp" // craft_replica interface + CRAFT data types

namespace homeblocks::craft {

class MemTransport; // in-process network + cold path

class MemCraftReplica final : public craft_replica {
public:
    MemCraftReplica(replica_endpoint ep, uint32_t page_size, std::shared_ptr< MemTransport > net);

    // ── craft_replica: client-facing ──
    async_result< LoginResult > login(uint64_t client_token) override;
    async_status logout(client_hdr hdr) override;
    async_status write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len, sisl::sg_list data) override;
    async_result< std::vector< io_extent > > read(client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len,
                                                  sisl::sg_list dest) override;
    async_result< LSNPair > keep_alive(client_hdr hdr) override;

    // ── craft_replica: peer-facing (driven by MemTransport) ──
    async_result< LSNPair > get_lsns() override;
    async_result< LSNPair > get_rs_commit_lsn() override;
    async_result< std::vector< JournalSlot > > fetch_data(std::vector< int64_t > lsns) override;
    async_status truncate(int64_t lsn) override;
    peer_id_t id() const override { return ep_.id; }

private:
    friend class MemTransport; // the cold path drives the cold_* / peek helpers below directly

    struct MemJournalSlot {
        uint64_t term{0};
        lba_t lba{0};
        lba_count_t len{0};
        bool all_zeros{false};
        bool is_empty{false};                            // Empty verdict (resync seam)
        std::shared_ptr< std::vector< uint8_t > > bytes; // len*page_size bytes; null iff all_zeros/empty
    };
    struct IndexCell {
        int64_t dlsn{-1};
        std::shared_ptr< std::vector< uint8_t > > buf; // one page at buf->data()+off
        std::size_t off{0};
    };

    // synchronous cores (each takes mu_); the public methods co_return these inline
    status do_write(client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len, sisl::sg_list);
    result< std::vector< io_extent > > do_read(client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len,
                                               sisl::sg_list dest);
    result< LSNPair > do_keep_alive(client_hdr hdr);
    result< LSNPair > do_lsns();
    status do_truncate(int64_t lsn);
    result< std::vector< JournalSlot > > do_fetch(std::vector< int64_t > const& lsns);

    // helpers (mu_ held by caller)

    void apply_up_to(int64_t target);
    void apply_slot(int64_t dlsn, MemJournalSlot const& s);
    // Fill `dest` for byte range [addr,addr+len) (data -> bytes, holes -> zeros) and return the layout.
    std::vector< io_extent > read_range(int64_t H, uint64_t addr, uint64_t len, sisl::sg_list const& dest);
    MemJournalSlot const* highest_slot_le(lba_t x, int64_t H) const; // journal-tail slot, honoring the horizon clamp

    // cold-path hooks used by MemTransport (each takes mu_)
    LSNPair peek_lsns();
    void cold_apply_sync(int64_t rs_commit_lsn, uint64_t client_token);
    void cold_apply_login(uint64_t client_token, uint64_t term);
    void cold_apply_logout();
    void cold_truncate_above(int64_t rs_commit_lsn);

    replica_endpoint ep_;
    uint32_t page_size_;
    std::shared_ptr< MemTransport > net_;
    CraftPartitionState state_;
    std::map< int64_t, MemJournalSlot > journal_; // dLSN -> slot (out-of-order arrival tolerated)
    std::map< lba_t, IndexCell > index_;          // applied prefix (<= commit_lsn); an absent LBA is a hole
    mutable std::mutex mu_;
};

} // namespace homeblocks::craft
