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

// craft_client: a reference CRAFT client over the PUBLIC volume_handle API -- assign a per-partition dLSN
// to each write, broadcast it, tally quorum, advance the commit frontier, pick a safe horizon per read.
//
// It calls ONLY the public free functions (login / async_write / async_read / keep_alive) against
// volume_handles, never the model internals, so swapping in a network shim is the only change needed.
// All dLSN bookkeeping lives in dlsn_tracker. See README.md.

#include <cstdint>
#include <optional>
#include <system_error>
#include <vector>

#include <homeblks/home_blocks.hpp>

#include "dlsn_tracker.hpp"

namespace homeblocks::craft {

class craft_client {
public:
    // One volume_handle per replica device. `leader` is where login is attempted first. `max_inflight` is
    // the caller's IO concurrency bound; it only sizes the tracker's winner-scan tripwire.
    explicit craft_client(std::vector< volume_handle > replicas, uint32_t leader = 0, uint32_t max_inflight = 128);

    // Establish the session, following NOT_LEADER redirects.
    async_status login(uint64_t client_token);

    // Broadcast a write at a fresh dLSN; commit advances once quorum acks. Empty `data` is a zero write.
    // A sub-quorum write leaves its slot unresolved, which pins the commit frontier -- as CRAFT requires.
    async_result< size_t > write(uint64_t addr, uint64_t len, sisl::sg_list data);

    // Fills `dest` in place (data bytes; holes -> zeros) and resolves to the byte count. Splits into
    // parallel sub-reads when blocks in the range need different horizons.
    async_result< size_t > read(uint64_t addr, uint64_t len, sisl::sg_list dest);

    // The dedicated commit carrier: advance the leader's frontier + reset its watchdog (keep_alive).
    async_status flush();

    uint32_t lba_size() const { return lba_size_; }
    uint64_t term() const { return term_; }
    int64_t commit_lsn() const { return tracker_.frontier(); }
    int64_t read_horizon() const { return tracker_.read_horizon(); }
    // Test hook: how many reads paid for the per-block winner pass.
    uint64_t winner_scans() const { return tracker_.winner_scans(); }

private:
    // The set-wide min commit_lsn floors journal reclaim on every replica. Our own frontier is an UPPER
    // bound on it, never the min, so sending that would let a replica reclaim journal a lagging peer still
    // needs. -1 (unknown) until a broadcast keep_alive can compute the real minimum.
    static constexpr int64_t k_all_committed_unknown = -1;

    client_hdr make_hdr() const;
    std::size_t quorum() const { return replicas_.size() / 2 + 1; }
    // The two guards every IO opens with; nullopt means the IO may proceed.
    std::optional< std::error_condition > precheck(uint64_t addr, uint64_t len) const;

    std::vector< volume_handle > replicas_;
    uint32_t leader_{0};
    uint64_t term_{0};
    uint32_t lba_size_{0};

    dlsn_tracker tracker_;
};

} // namespace homeblocks::craft
