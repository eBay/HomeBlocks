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

// Definitions of the public CRAFT data-plane free functions (declared in home_blocks.hpp). Each is a
// thin adapter: resolve the volume_handle to its per-replica craft_replica backend and forward 1:1.
// Dispatch is through the craft_replica INTERFACE, so this TU (part of libhomeblocks) does not depend
// on the in-memory model -- a production CraftReplDev-backed volume would work identically.

#include <homeblks/home_blocks.hpp>

#include "craft/craft_replica.hpp" // craft_replica interface
#include "volume/volume.hpp"       // volume::craft_backend()

namespace homeblocks {

namespace {
craft_replica* craft_backend_of(volume_handle const& v) { return v ? v->craft_backend() : nullptr; }
// A volume with no CRAFT backend (e.g. a HomeStore-backed volume) cannot serve the CRAFT data plane.
auto no_craft_backend() { return std::unexpected(std::make_error_condition(std::errc::not_supported)); }
} // namespace

async_result< LoginResult > login(volume_handle const& vol, uint64_t client_token) {
    auto* b = craft_backend_of(vol);
    if (!b) co_return no_craft_backend();
    co_return co_await b->login(client_token);
}

async_status async_write(volume_handle const& vol, client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                         sisl::sg_list data) {
    auto* b = craft_backend_of(vol);
    if (!b) co_return no_craft_backend();
    co_return co_await b->write(hdr, dlsn, addr, len, std::move(data));
}

async_result< std::vector< io_extent > > async_read(volume_handle const& vol, client_hdr hdr, int64_t read_lsn,
                                                    uint64_t addr, uint64_t len, sisl::sg_list dest) {
    auto* b = craft_backend_of(vol);
    if (!b) co_return no_craft_backend();
    co_return co_await b->read(hdr, read_lsn, addr, len, std::move(dest));
}

async_result< LSNPair > keep_alive(volume_handle const& vol, client_hdr hdr) {
    auto* b = craft_backend_of(vol);
    if (!b) co_return no_craft_backend();
    co_return co_await b->keep_alive(hdr);
}

} // namespace homeblocks
