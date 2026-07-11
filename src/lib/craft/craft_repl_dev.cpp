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

#include "craft_repl_dev.hpp"

namespace homeblocks {

// ─── constructor ──────────────────────────────────────────────────────────────

CraftReplDev::CraftReplDev(volume_id_t vol_id, unique< CraftJournalBackend > journal) :
        vol_id_{vol_id}, journal_{std::move(journal)}, raft_listener_{this} {}

// ─── get_lsns / get_rs_commit_lsn ────────────────────────────────────────────
// These are real: they just snapshot the in-memory partition state.

async_result< craft::lsn_pair > CraftReplDev::get_lsns(volume_id_t /* vol_id */) {
    co_return craft::lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}

async_result< craft::lsn_pair > CraftReplDev::get_rs_commit_lsn() {
    co_return craft::lsn_pair{state_.commit_lsn, state_.last_append_lsn};
}

// ─── stubs (S2–S7 will implement these) ──────────────────────────────────────

async_result< craft::LoginResult > CraftReplDev::login(uint64_t /* client_token */, volume_id_t /* vol_id */) {
    LOGW("CraftReplDev::login not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_status CraftReplDev::write(uint64_t /* term */, int64_t /* lsn */, lba_t /* lba */, lba_count_t /* len */,
                                 sisl::sg_list /* data */) {
    LOGW("CraftReplDev::write not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< sisl::sg_list > CraftReplDev::read(uint64_t /* term */, int64_t /* read_lsn */, lba_t /* lba */,
                                                 lba_count_t /* len */) {
    LOGW("CraftReplDev::read not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::lsn_pair > CraftReplDev::commit(uint64_t /* term */, int64_t /* lsn */) {
    LOGW("CraftReplDev::commit not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::lsn_pair > CraftReplDev::keep_alive(int64_t /* commit_lsn */, int64_t /* all_committed_lsn */) {
    LOGW("CraftReplDev::keep_alive not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_status CraftReplDev::truncate(int64_t /* lsn */) {
    LOGW("CraftReplDev::truncate not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_status CraftReplDev::append(int64_t /* sync_to */, uint64_t /* client_token */) {
    LOGW("CraftReplDev::append not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< std::vector< JournalSlot > > CraftReplDev::fetch_data(std::vector< int64_t > /* lsns */) {
    LOGW("CraftReplDev::fetch_data not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

// ─── RAFT listener ────────────────────────────────────────────────────────────

void CraftReplDev::CraftRaftListener::on_commit(int64_t lsn, sisl::blob const& /* header */,
                                                sisl::blob const& /* key */,
                                                std::vector< homestore::multi_blk_id > const& /* blkids */,
                                                cintrusive< homestore::repl_req_ctx >& /* ctx */) {
    // S5 will parse the entry type from `header` and dispatch to
    // owner_->apply_sync_rs_commit_lsn() or owner_->apply_internal_login().
    LOGD("CraftRaftListener::on_commit lsn={} (entry dispatch not yet implemented)", lsn);
}

// ─── RAFT apply helpers (S5 will implement) ───────────────────────────────────

void CraftReplDev::apply_sync_rs_commit_lsn(int64_t rs_commit_lsn, uint64_t /* client_token */) {
    // Advance commit_lsn to rs_commit_lsn, calling fetch_data() for any missing slots.
    LOGD("apply_sync_rs_commit_lsn rs_commit_lsn={} (not yet implemented)", rs_commit_lsn);
}

void CraftReplDev::apply_internal_login(uint64_t client_token, uint64_t term) {
    // Update state_.client_token and state_.term; subsequent IOs with a different
    // term will be rejected with ETERM.
    LOGD("apply_internal_login client_token={} term={} (not yet implemented)", client_token, term);
}

} // namespace homeblocks