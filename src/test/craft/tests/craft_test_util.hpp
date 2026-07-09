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

// Shared scaffolding for the CRAFT tests. HomeStore-free, so test_craft_memory can link without the
// engine. Everything here is a convenience over the public CRAFT surface; nothing encodes a rule.

#include <cstdint>
#include <utility>
#include <vector>

#include <sisl/fds/buffer.hpp>

#include <homeblks/home_blocks.hpp>

#include "coro_helpers.hpp"

namespace homeblocks::craft::test {

constexpr uint32_t PAGE = 512;

// block index/count -> byte offset/length
constexpr uint64_t blk(uint64_t n) { return n * uint64_t{PAGE}; }

// Drive a coroutine to completion off-reactor.
template < class T >
auto rg(T&& t) {
    return detail::sync_get(std::forward< T >(t));
}

// A single-buffer sg_list over `b`. The buffer must outlive the IO.
inline sisl::sg_list one_iov(std::vector< uint8_t >& b) {
    sisl::sg_list s;
    s.size = b.size();
    s.iovs.push_back(iovec{b.data(), b.size()});
    return s;
}

inline std::vector< uint8_t > page_of(uint8_t fill, uint64_t nblk = 1) {
    return std::vector< uint8_t >(nblk * PAGE, fill);
}

// all_committed_lsn stays -1 (unknown): the tests never drive journal reclaim.
inline client_hdr chdr(uint64_t term, int64_t commit = -1) { return client_hdr{term, commit, -1}; }

} // namespace homeblocks::craft::test
