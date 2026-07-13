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
// std::errc::not_supported.

#include <homeblks/home_blocks.hpp>

#include "craft/craft_repl_dev.hpp" // CraftReplDev: the concrete backend
#include "volume/volume.hpp"        // volume::craft_dev()

namespace homeblocks {

namespace {
CraftReplDev* craft_dev_of(volume_handle const& v) { return v ? v->craft_dev() : nullptr; }
auto no_craft_backend() { return std::unexpected(std::make_error_condition(std::errc::not_supported)); }
} // namespace

async_result< craft::LoginResult > login(volume_handle const& vol, uint64_t client_token) {
    auto* d = craft_dev_of(vol);
    if (!d) co_return no_craft_backend();
    co_return co_await d->login(client_token);
}

async_status logout(volume_handle const& vol, craft::client_hdr hdr) {
    auto* d = craft_dev_of(vol);
    if (!d) co_return no_craft_backend();
    co_return co_await d->logout(hdr);
}

async_result< craft::lsn_pair > async_write(volume_handle const& vol, craft::client_hdr hdr, int64_t dlsn,
                                            uint64_t addr, uint64_t len, sisl::sg_list data) {
    auto* d = craft_dev_of(vol);
    if (!d) co_return no_craft_backend();
    co_return co_await d->write(hdr, dlsn, addr, len, std::move(data));
}

async_result< craft::read_result > async_read(volume_handle const& vol, craft::client_hdr hdr, int64_t read_lsn,
                                              uint64_t addr, uint64_t len, sisl::sg_list dest) {
    auto* d = craft_dev_of(vol);
    if (!d) co_return no_craft_backend();
    co_return co_await d->read(hdr, read_lsn, addr, len, std::move(dest));
}

async_result< craft::lsn_pair > keep_alive(volume_handle const& vol, craft::client_hdr hdr) {
    auto* d = craft_dev_of(vol);
    if (!d) co_return no_craft_backend();
    co_return co_await d->keep_alive(hdr);
}

async_result< craft::resolution_result > request_resolution(volume_handle const& vol, craft::client_hdr hdr,
                                                            int64_t upto) {
    auto* d = craft_dev_of(vol);
    if (!d) co_return no_craft_backend();
    co_return co_await d->request_resolution(hdr, upto);
}

} // namespace homeblocks
