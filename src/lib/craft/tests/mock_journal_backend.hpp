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
#include <optional>
#include <utility>

#include <gtest/gtest.h>

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

// Deterministic, non-nil member id: MockCraftRaftProposer is test_member_id(0), and the mock peer fetchers tag
// their i-th reply test_member_id(i + 1).
inline craft::peer_id_t test_member_id(uint32_t i) {
    craft::peer_id_t id{};
    id.begin()[15] = static_cast< uint8_t >(i + 1);
    return id;
}

// Leader of a 3-voter group by default (this member is test_member_id(0)). propose() records each entry and
// succeeds unless `fail` or `time_out` is set; on_commit, if set, runs with the entry as RAFT would (typically the
// device's own listener on_commit).
class MockCraftRaftProposer : public CraftRaftProposer {
public:
    bool leader{true};
    uint32_t members{3};
    craft::peer_id_t self{test_member_id(0)}; // set outside the voters to model a leader missing from its own list
    bool fail{false};
    // As the adapter's deadline would: the entry is recorded (it may still commit) but not applied, and propose()
    // returns timed_out.
    bool time_out{false};
    std::vector< std::pair< std::vector< uint8_t >, std::vector< uint8_t > > > proposed; // {header, key}
    std::function< void(sisl::blob const&, sisl::blob const&) > on_commit;
    std::function< void() > on_propose; // runs first, whichever way the proposal ends
    // What a committed proposal returns, as the adapter returns its apply's outcome: on_commit sets it through
    // loop_back(); ok if it doesn't.
    std::optional< status > commit_outcome;

    bool is_leader() const override { return leader; }
    craft::peer_id_t my_id() const override { return self; }
    // Voters are test_member_id(0..members-1); members = 0 is an unknown membership. Any other id is a learner.
    std::vector< craft::peer_id_t > voters() const override {
        std::vector< craft::peer_id_t > ids;
        for (uint32_t i = 0; i < members; ++i)
            ids.push_back(test_member_id(i));
        return ids;
    }

    async_status propose(sisl::blob header, sisl::blob key) override {
        if (on_propose) on_propose();
        if (fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        proposed.emplace_back(std::vector< uint8_t >(header.cbytes(), header.cbytes() + header.size()),
                              std::vector< uint8_t >(key.cbytes(), key.cbytes() + key.size()));
        if (time_out) co_return std::unexpected(std::make_error_condition(std::errc::timed_out));
        if (on_commit) on_commit(header, key);
        co_return std::exchange(commit_outcome, std::nullopt).value_or(ok());
    }
};

// Commits a proposed entry on `dev` as RAFT would, and hands the proposer its apply's outcome to return. An apply
// still waiting in a paused queue has none yet, so the proposal returns ok.
inline void loop_back(MockCraftRaftProposer& proposer, CraftReplDev& dev, sisl::blob const& header,
                      sisl::blob const& key) {
    auto const ctx = make_test_proposal_ctx();
    dev.raft_listener().on_commit(1, header, key, {}, ctx);
    proposer.commit_outcome = take_test_proposal_outcome(ctx);
}

// A SyncRSCommitLSN entry MockCraftRaftProposer recorded, decoded.
struct ProposedSync {
    CraftEntryType type;
    int64_t rs_commit_lsn;
    uint64_t client_token;
    uint64_t term;
    std::vector< int64_t > empty_slots;
};

// A header or key that doesn't parse fails the test, so an assertion on what it returns ({rs_commit_lsn = -1, no Empty
// slots}) can't pass on a malformed entry.
inline ProposedSync decode(std::pair< std::vector< uint8_t >, std::vector< uint8_t > > const& entry) {
    if (entry.first.size() < sizeof(CraftEntryHeader)) {
        ADD_FAILURE() << "proposed entry's header is " << entry.first.size() << " bytes";
        return {CraftEntryType{}, -1, 0, 0, {}};
    }
    auto const type = reinterpret_cast< CraftEntryHeader const* >(entry.first.data())->type;
    auto sync = parse_sync_rs_commit_lsn(
        sisl::blob{const_cast< uint8_t* >(entry.second.data()), static_cast< uint32_t >(entry.second.size())});
    if (!sync) {
        ADD_FAILURE() << "proposed entry's key isn't a well-formed SyncRSCommitLSN (" << entry.second.size()
                      << " bytes)";
        return {type, -1, 0, 0, {}};
    }
    return {type, sync->rs_commit_lsn, sync->client_token, sync->term, std::move(sync->empty_slots)};
}

// In-memory CraftStateStore. Keeps the last image persisted and hands it back through the real codec on load(), so
// restart tests exercise encode/decode too. hold suspends the next persist() until gate is completed.
class MockCraftStateStore : public CraftStateStore {
public:
    std::optional< CraftDurableState > saved;
    int persist_calls{0};
    bool fail_persist{false};
    bool fail_load{false};
    bool hold{false};
    sisl::async::value_awaitable< bool > gate;
    // The next persist() writes even if the partition faults while it is held, as the real store's write does when
    // it holds its lock first: a concurrent mark_faulted() waits for it.
    bool write_through_fault{false};

    bool faulted{false}; // mark_faulted()
    int mark_calls{0};

    result< std::optional< CraftDurableState > > load() override {
        if (fail_load) return std::unexpected(std::make_error_condition(std::errc::io_error));
        if (faulted) return std::unexpected(std::make_error_condition(std::errc::state_not_recoverable));
        if (!saved) return std::optional< CraftDurableState >{};
        auto const bytes = encode_durable_state(*saved);
        auto d = decode_durable_state(bytes.data(), bytes.size());
        if (!d) return std::unexpected(std::make_error_condition(std::errc::illegal_byte_sequence));
        return d;
    }

    async_status persist(CraftDurableState const& s) override {
        ++persist_calls;
        bool const through = std::exchange(write_through_fault, false) && !fail_persist && !faulted;
        if (std::exchange(hold, false)) co_await gate;
        if (!through && (fail_persist || faulted))
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        saved = s;
        co_return ok();
    }

    void destroy() override { saved.reset(); }
    void mark_faulted() override {
        ++mark_calls;
        faulted = true;
    }
};

template < typename Mock >
async_status mock_free_slot(Mock& mock, int64_t lsn) {
    auto it = mock.slots.find(lsn);
    if (it == mock.slots.end())
        co_return std::unexpected(std::make_error_condition(std::errc::no_such_file_or_directory));
    if (it->second.all_zeros) co_return ok();
    co_return co_await mock.free_data(homestore::multi_blk_id{});
}

} // namespace homeblocks