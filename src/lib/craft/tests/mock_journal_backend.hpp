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

#include "craft/craft_repl_dev.hpp"

// Shared free_slot body for CraftJournalBackend test mocks that record write_slot calls into a
// std::map< int64_t, JournalSlot > slots member. test_craft_write.cpp, test_craft_raft_entries.cpp,
// and test_craft_peer_exchange.cpp all need identical behavior here: read back the recorded slot,
// skip it if all_zeros (nothing was ever allocated), otherwise delegate to the mock's own free_data
// so any per-test free_data call counters still fire.
namespace homeblocks {

// Shared read_slot body for the same mocks: look up the recorded slot by lsn, or
// no_such_file_or_directory if write_slot was never called for it.
template < typename Mock >
async_result< JournalSlot > mock_read_slot(Mock& mock, int64_t lsn) {
    auto it = mock.slots.find(lsn);
    if (it == mock.slots.end())
        co_return std::unexpected(std::make_error_condition(std::errc::no_such_file_or_directory));
    co_return it->second;
}

template < typename Mock >
async_status mock_free_slot(Mock& mock, int64_t lsn) {
    auto it = mock.slots.find(lsn);
    if (it == mock.slots.end())
        co_return std::unexpected(std::make_error_condition(std::errc::no_such_file_or_directory));
    if (it->second.all_zeros) co_return ok();
    co_return co_await mock.free_data(homestore::multi_blk_id{});
}

} // namespace homeblocks