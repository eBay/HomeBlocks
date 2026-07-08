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

// Bridge between the HomeStore-free CRAFT reference model (MemCraftReplica) and the public
// volume_handle surface. `create_memory_volume` wraps ONE in-process replica behind a volume_handle
// so it is drivable via the public CRAFT free functions (login/async_write/async_read/...), exactly
// like a production CRAFT volume_handle -- the API works whether the replicas are three servers or
// three objects in one process. `make_memory_replica_set` stands up the whole in-process set.
//
// This is a NON-public header (src/lib): it pulls in volume.hpp, so a TU using it links HomeStore.
// The model itself (mem_craft_replica/cluster) stays HomeStore-free and is testable on its own.

#include <cstdint>
#include <memory>
#include <vector>

#include <homeblks/home_blocks.hpp> // volume_handle, volume_info

#include "mem_craft_cluster.hpp" // MemTransport, MemCraftReplica, make_mem_replica_group

namespace homeblocks::craft {

// Wrap one MemCraftReplica behind an opaque volume_handle (via volume's memory-mode ctor).
volume_handle create_memory_volume(volume_info info, std::shared_ptr< MemCraftReplica > replica);

// Stand up an N-replica in-process set and return the N INDEPENDENT per-replica handles a client
// drives (index 0 is the default leader), plus the shared transport (fault injection only). There is
// deliberately NO aggregate / "whole volume" handle: each element is one replica device -- exactly what
// create_volume yields on each server in production -- and the client advances commits across them
// independently. This helper is just a harness loop over create_memory_volume (orchestration, outside
// the CRAFT protocol); `info` supplies the volume id / page_size / size.
struct MemReplicaHandles {
    std::vector< volume_handle >    handles; // one per replica device; the client drives each separately
    std::shared_ptr< MemTransport > net;     // in-process network + cold path + faults (no production analog)
};
MemReplicaHandles make_memory_replica_set(volume_info info, uint32_t n = 3);

} // namespace homeblocks::craft
