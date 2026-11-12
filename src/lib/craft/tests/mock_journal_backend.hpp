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

#include <functional>

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

// Leader of a 3-member group by default. propose() records each entry and succeeds unless `fail` is set;
// on_commit, if set, runs with the entry as RAFT would (typically the device's own listener on_commit).
class MockCraftRaftProposer : public CraftRaftProposer {
public:
    bool leader{true};
    uint32_t members{3};
    bool fail{false};
    std::vector< std::pair< std::vector< uint8_t >, std::vector< uint8_t > > > proposed; // {header, key}
    std::function< void(sisl::blob const&, sisl::blob const&) > on_commit;

    bool is_leader() const override { return leader; }
    uint32_t member_count() const override { return members; }

    async_status propose(sisl::blob header, sisl::blob key) override {
        if (fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        proposed.emplace_back(std::vector< uint8_t >(header.cbytes(), header.cbytes() + header.size()),
                              std::vector< uint8_t >(key.cbytes(), key.cbytes() + key.size()));
        if (on_commit) on_commit(header, key);
        co_return ok();
    }
};

// Decodes one entry MockCraftRaftProposer recorded.
struct ProposedSync {
    CraftEntryType type;
    int64_t rs_commit_lsn;
    uint64_t client_token;
    std::vector< int64_t > empty_slots;
};

inline ProposedSync decode(std::pair< std::vector< uint8_t >, std::vector< uint8_t > > const& entry) {
    auto const* p = reinterpret_cast< SyncRSCommitLSNPayload const* >(entry.second.data());
    auto empties = parse_empty_slots(
        sisl::blob{const_cast< uint8_t* >(entry.second.data()), static_cast< uint32_t >(entry.second.size())});
    return {reinterpret_cast< CraftEntryHeader const* >(entry.first.data())->type, p->rs_commit_lsn, p->client_token,
            empties.value_or(std::vector< int64_t >{})};
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