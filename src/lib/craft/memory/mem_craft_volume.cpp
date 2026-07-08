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

#include "mem_craft_volume.hpp"

#include "volume/volume.hpp" // full volume type + mem_craft_tag ctor

namespace homeblocks::craft {

volume_handle create_memory_volume(volume_info info, std::shared_ptr< MemCraftReplica > replica) {
    // volume's memory-mode ctor: HomeStore-free, stores `replica` as the CRAFT backend.
    return std::make_shared< volume >(std::move(info), std::move(replica), volume::mem_craft_tag{});
}

MemReplicaHandles make_memory_replica_set(volume_info info, uint32_t n) {
    auto group = make_mem_replica_group(info.id, n, static_cast< uint32_t >(info.page_size));

    MemReplicaHandles out;
    out.net = group.net;
    out.handles.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        // One volume_handle per replica device -- the same shape create_volume yields per server.
        volume_info vi{info.id, info.size_bytes, info.page_size, info.name};
        out.handles.push_back(create_memory_volume(std::move(vi), group.replicas[i]));
    }
    return out;
}

} // namespace homeblocks::craft
