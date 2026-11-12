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

// Definitions of the public CRAFT data-plane free functions (declared in home_blocks.hpp). Each resolves the
// volume_handle to that volume's CraftReplDev and forwards 1:1 -- there is nothing to translate, because
// CraftReplDev's client-facing verbs are already the wire's shape (byte-addressed, carrying craft::client_hdr).
//
// This is the FAR half of CRAFT: HomeBlocks is the backend. A CRAFT server (CraftConnector) terminates the wire,
// looks up the exposed volume, and calls exactly these functions with the handle -- so this file is the per-replica
// surface the transport binds to. HomeBlocks therefore implements no craft::craft_replica (that interface is the
// CLIENT's view of a member) and constructs no CRAFT client: make_client appears nowhere in this repo. The only
// thing taken from craft_client is <craft/types.hpp>, the vocabulary the wire is defined in.
//
// A volume with no CRAFT backend (a plain HomeStore volume, or a CRAFT volume not yet wired) returns
// std::errc::not_supported. A system restricted or shutting down, or a volume being destroyed, refuses the op with
// std::errc::operation_not_supported, and an offline volume with volume_error::OFFLINE. A write or read reaching past
// the end of the volume is std::errc::invalid_argument.

#include <optional>

#include <homeblks/home_blocks.hpp>

#include "craft/craft_repl_dev.hpp" // CraftReplDev: the concrete backend
#include "homeblks_impl.hpp"        // the system-level guards
#include "volume/volume.hpp"        // volume::craft_dev()

namespace homeblocks {

namespace {
auto no_craft_backend() { return std::unexpected(std::make_error_condition(std::errc::not_supported)); }

// Why `vol` can't serve a CRAFT op now, if it can't. Called with the op counted in flight on the volume (vol_io_guard):
// its destroy starts only once the volume is DESTROYING and no op is in flight, so it either waits for this op or is
// seen here, and the CraftReplDev outlives the op.
std::optional< std::error_condition > craft_unavailable(volume_handle const& vol, char const* op) {
    // The instance is gone once a shutdown finished, while a caller may still hold the handle.
    auto const hb = HomeBlocksImpl::instance();
    if (!hb || hb->is_restricted() || hb->is_shutting_down()) {
        LOGE("Can't serve CRAFT {}: the system is restricted or shut down.", op);
        return std::make_error_condition(std::errc::operation_not_supported);
    }
    if (vol->is_offline()) {
        LOGE("Can't serve CRAFT {}: volume {} is offline.", op, vol->id_str());
        return std::error_condition{volume_error::OFFLINE};
    }
    if (vol->is_destroying()) {
        LOGE("Can't serve CRAFT {}: volume {} is being destroyed.", op, vol->id_str());
        return std::make_error_condition(std::errc::operation_not_supported);
    }
    // Its group's destroy is under way: the replica goes at the reaper's next pass (a minute later in production), and
    // whatever it acknowledged until then would go with it.
    if (auto const& rd = vol->rd(); !rd || is_going_away(*rd)) {
        LOGE("Can't serve CRAFT {}: the RAFT group of volume {} is being destroyed.", op, vol->id_str());
        return std::make_error_condition(std::errc::operation_not_supported);
    }
    if (!vol->craft_dev()) { return std::make_error_condition(std::errc::not_supported); }
    return std::nullopt;
}

// Why `vol` can't serve [addr, addr + len): the range must lie within the volume. The CraftReplDev checks alignment.
std::optional< std::error_condition > craft_out_of_range(volume_handle const& vol, uint64_t addr, uint64_t len,
                                                         char const* op) {
    auto const size = vol->info()->size_bytes;
    if (len <= size && addr <= size - len) { return std::nullopt; } // addr + len <= size, without overflow
    LOGE("Can't serve CRAFT {}: [{}, +{}) is beyond the {} bytes of volume {}.", op, addr, len, size, vol->id_str());
    return std::make_error_condition(std::errc::invalid_argument);
}
} // namespace

async_result< craft::LoginResult > login(volume_handle const& vol, uint64_t client_token) {
    if (!vol) co_return no_craft_backend();
    vol_io_guard const guard{vol};
    if (auto const err = craft_unavailable(vol, "login")) co_return std::unexpected(*err);
    co_return co_await vol->craft_dev()->login(client_token);
}

async_status logout(volume_handle const& vol, craft::client_hdr hdr) {
    if (!vol) co_return no_craft_backend();
    vol_io_guard const guard{vol};
    if (auto const err = craft_unavailable(vol, "logout")) co_return std::unexpected(*err);
    co_return co_await vol->craft_dev()->logout(hdr);
}

async_result< craft::lsn_pair > async_write(volume_handle const& vol, craft::client_hdr hdr, int64_t dlsn,
                                            uint64_t addr, uint64_t len, sisl::sg_list data) {
    if (!vol) co_return no_craft_backend();
    vol_io_guard const guard{vol};
    if (auto const err = craft_unavailable(vol, "write")) co_return std::unexpected(*err);
    if (auto const err = craft_out_of_range(vol, addr, len, "write")) co_return std::unexpected(*err);
    co_return co_await vol->craft_dev()->write(hdr, dlsn, addr, len, std::move(data));
}

async_result< craft::read_result > async_read(volume_handle const& vol, craft::client_hdr hdr, int64_t read_lsn,
                                              uint64_t addr, uint64_t len, sisl::sg_list dest) {
    if (!vol) co_return no_craft_backend();
    vol_io_guard const guard{vol};
    if (auto const err = craft_unavailable(vol, "read")) co_return std::unexpected(*err);
    if (auto const err = craft_out_of_range(vol, addr, len, "read")) co_return std::unexpected(*err);
    co_return co_await vol->craft_dev()->read(hdr, read_lsn, addr, len, std::move(dest));
}

async_result< craft::lsn_pair > keep_alive(volume_handle const& vol, craft::client_hdr hdr) {
    if (!vol) co_return no_craft_backend();
    vol_io_guard const guard{vol};
    if (auto const err = craft_unavailable(vol, "keep_alive")) co_return std::unexpected(*err);
    co_return co_await vol->craft_dev()->keep_alive(hdr);
}

async_result< craft::resolution_result > request_resolution(volume_handle const& vol, craft::client_hdr hdr,
                                                            int64_t upto) {
    if (!vol) co_return no_craft_backend();
    vol_io_guard const guard{vol};
    if (auto const err = craft_unavailable(vol, "request_resolution")) co_return std::unexpected(*err);
    co_return co_await vol->craft_dev()->request_resolution(hdr, upto);
}

} // namespace homeblocks
