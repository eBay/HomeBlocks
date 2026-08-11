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
#include "../coro_helpers.hpp"

#include <homestore/logstore/log_store.hpp> // home_log_store, logstore_seq_num_t

namespace homeblocks {

// ─── HomeStore journal backend ────────────────────────────────────────────────
//
// Production backend: wraps a HomeStore home_log_store. write_slot / read_slot
// are stubs implemented in S2 (PR #165). truncate_to() is implemented here (S4).

class HomeStoreCraftJournalBackend : public CraftJournalBackend {
public:
    explicit HomeStoreCraftJournalBackend(shared< homestore::home_log_store > logstore) :
            logstore_{std::move(logstore)} {}

    async_status write_slot(int64_t lsn, lba_t /* lba */, lba_count_t /* len */, sisl::sg_list /* data */) override {
        LOGW("HomeStoreCraftJournalBackend::write_slot lsn={} not yet implemented", lsn);
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override {
        LOGW("HomeStoreCraftJournalBackend::read_slot lsn={} not yet implemented", lsn);
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    // Drop all journal entries with seq_num > lsn; lsn becomes the new tail.
    // home_log_store::rollback(to_lsn) removes everything ABOVE to_lsn, which is exactly what we need.
    async_status truncate_to(int64_t lsn) override {
        if (!logstore_->rollback(static_cast< homestore::logstore_seq_num_t >(lsn)))
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return ok();
    }

private:
    shared< homestore::home_log_store > logstore_;
};

unique< CraftJournalBackend > make_homestore_journal_backend(shared< homestore::home_log_store > logstore) {
    return std::make_unique< HomeStoreCraftJournalBackend >(std::move(logstore));
}

// ─── constructor ──────────────────────────────────────────────────────────────

CraftReplDev::CraftReplDev(volume_id_t vol_id, unique< CraftJournalBackend > journal) :
        vol_id_{vol_id}, journal_{std::move(journal)}, raft_listener_{this} {}

// ─── get_lsns / get_rs_commit_lsn ────────────────────────────────────────────
// Snapshot the in-memory partition state under missing_mu_ for consistency with
// write() which updates state_ under the same lock.

async_result< craft::lsn_pair > CraftReplDev::get_lsns(volume_id_t /* vol_id */) {
    craft::lsn_pair pair{};
    {
        std::lock_guard lk{missing_mu_};
        pair = {state_.commit_lsn, state_.last_append_lsn};
    }
    co_return pair;
}

async_result< craft::lsn_pair > CraftReplDev::get_rs_commit_lsn() {
    craft::lsn_pair pair{};
    {
        std::lock_guard lk{missing_mu_};
        pair = {state_.commit_lsn, state_.last_append_lsn};
    }
    co_return pair;
}

// ─── truncate (S4) ────────────────────────────────────────────────────────────
//
// Called only during the login sequence, while no writes are in-flight (the
// CRAFT write path is quiesced by login serialisation). Three atomic steps:
//   1. Journal rollback: drop all entries with dLSN > lsn (tail truncation).
//   2. Clamp last_append_lsn to lsn if it is higher.
//   3. Erase all missing-set entries above lsn.
// commit_lsn is not touched: the new rs_commit_lsn passed by the caller is the
// dLSN up to which RAFT consensus has RESOLVED entries. Entries above that are
// the ones being dropped. The missing set tracks gaps in [commit_lsn+1,
// last_append_lsn]; after truncation every entry > lsn is gone from both the
// journal and the missing set.

async_status CraftReplDev::truncate(int64_t lsn) {
    // Guard before touching the journal: truncating below commit_lsn would drop
    // committed entries and break the commit_lsn <= last_append_lsn invariant.
    {
        std::lock_guard lk{missing_mu_};
        DEBUG_ASSERT_GE(lsn, state_.commit_lsn, "truncate below committed prefix");
    }

    // Step 1: journal rollback — synchronous; fails fast on I/O error.
    if (auto r = co_await journal_->truncate_to(lsn); !r) co_return r;

    // Steps 2 + 3 under the same mutex write() uses.
    {
        std::lock_guard lk{missing_mu_};
        if (state_.last_append_lsn > lsn) state_.last_append_lsn = lsn;
        missing_lsns_.erase(missing_lsns_.upper_bound(lsn), missing_lsns_.end());
    }
    co_return ok();
}

#ifdef _PRERELEASE
void CraftReplDev::seed_lsns(int64_t last_append, std::initializer_list< int64_t > missing) {
    std::lock_guard lk{missing_mu_};
    state_.last_append_lsn = last_append;
    missing_lsns_.clear();
    missing_lsns_.insert(missing);
}

void CraftReplDev::seed_commit_lsn(int64_t commit) {
    std::lock_guard lk{missing_mu_};
    state_.commit_lsn = commit;
}

void CraftReplDev::seed_empty(std::initializer_list< int64_t > empty) {
    std::lock_guard lk{missing_mu_};
    empty_lsns_.clear();
    empty_lsns_.insert(empty);
}
#endif

// ─── stubs (S1/S2/S3/S5/S6/S7 implement these) ───────────────────────────────

async_result< craft::LoginResult > CraftReplDev::login(uint64_t /* client_token */) {
    LOGW("CraftReplDev::login not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_status CraftReplDev::logout(craft::client_hdr /* hdr */) {
    LOGW("CraftReplDev::logout not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::lsn_pair > CraftReplDev::write(craft::client_hdr hdr, int64_t dlsn, uint64_t addr, uint64_t len,
                                                    sisl::sg_list data) {
    if (hdr.term != state_.term) {
        LOGW("write rejected: stale term want={} got={} dlsn={}", state_.term, hdr.term, dlsn);
        co_return std::unexpected(make_error_condition(volume_error::STALE_TERM));
    }

    {
        std::lock_guard lock{missing_mu_};
        for (int64_t gap = state_.last_append_lsn + 1; gap < dlsn; ++gap)
            missing_lsns_.insert(gap);
        if ((dlsn > state_.last_append_lsn) || missing_lsns_.contains(dlsn)) missing_lsns_.insert(dlsn);
        state_.last_append_lsn = std::max(state_.last_append_lsn, dlsn);
    }

    auto res = co_await journal_->write_slot(dlsn, static_cast< lba_t >(addr), static_cast< lba_count_t >(len),
                                             std::move(data));
    if (!res) {
        LOGE("write_slot failed dlsn={} addr={} len={}: {}", dlsn, addr, len, res.error().message());
        co_return std::unexpected(res.error());
    }

    craft::lsn_pair snapshot;
    {
        std::lock_guard lock{missing_mu_};
        missing_lsns_.erase(dlsn);
        snapshot = {state_.commit_lsn, state_.last_append_lsn};
    }
    LOGT("write ok dlsn={} addr={} len={}", dlsn, addr, len);
    co_return snapshot;
}

async_result< craft::read_result > CraftReplDev::read(craft::client_hdr /* hdr */, int64_t /* read_lsn */,
                                                      uint64_t /* addr */, uint64_t /* len */,
                                                      sisl::sg_list /* dest */) {
    LOGW("CraftReplDev::read not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::lsn_pair > CraftReplDev::keep_alive(craft::client_hdr /* hdr */) {
    LOGW("CraftReplDev::keep_alive not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_result< craft::resolution_result > CraftReplDev::request_resolution(craft::client_hdr /* hdr */,
                                                                          int64_t /* upto */) {
    LOGW("CraftReplDev::request_resolution not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

async_status CraftReplDev::append(int64_t /* sync_to */, uint64_t /* client_token */) {
    LOGW("CraftReplDev::append not yet implemented");
    co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
}

// ─── fetch_data (S6) ──────────────────────────────────────────────────────────
//
// Four-way response per requested LSN (in request order; omitted slots absent):
//   is_empty=true  : slot positively verdicted Empty by a prior SyncRSCommitLSN (S5); Empty beats data
//   present+data   : slot in journal, all_zeros=false; data payload present
//   present+zero   : slot in journal, all_zeros=true; no data payload (WRITE_ZEROES / range-unmap)
//   omitted        : slot not locally held — in missing_lsns_ or above last_append_lsn
//
// The Empty verdict is leader-only (S5 pre-resolution); this responder only reports state.
// empty_lsns_ is checked first: a slot in both empty_lsns_ and the journal returns is_empty=true
// (Empty beats data, the reconciliation invariant from S5).
//
// The missing_mu_ lock is dropped before each co_await read_slot() call to avoid holding a mutex
// across a suspension point. Callers are serialised by the login sequence (no concurrent writes
// while fetch_data runs), so the snapshot taken under the lock is stable.
//
// A read_slot() I/O error aborts the batch immediately (fail-fast); the partial result is discarded.

async_result< std::vector< JournalSlot > > CraftReplDev::fetch_data(std::vector< int64_t > lsns) {
    std::vector< JournalSlot > result;
    result.reserve(lsns.size());

    for (int64_t lsn : lsns) {
        enum class SlotKind { Empty, Present, Absent };
        SlotKind kind;
        {
            std::lock_guard lk{missing_mu_};
            if (empty_lsns_.contains(lsn)) {
                kind = SlotKind::Empty;
            } else if (lsn >= 0 && lsn <= state_.last_append_lsn && !missing_lsns_.contains(lsn)) {
                kind = SlotKind::Present;
            } else {
                kind = SlotKind::Absent;
            }
        }

        if (kind == SlotKind::Empty) {
            result.push_back(JournalSlot{.lsn = lsn, .is_empty = true});
        } else if (kind == SlotKind::Present) {
            auto slot_r = co_await journal_->read_slot(lsn);
            if (!slot_r) co_return std::unexpected(slot_r.error());
            slot_r->lsn = lsn;
            result.push_back(std::move(*slot_r));
        }
        // Absent: omit from result (not-present-here)
    }

    co_return result;
}

// ─── RAFT listener ────────────────────────────────────────────────────────────

void CraftReplDev::CraftRaftListener::on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                                std::vector< homestore::multi_blk_id > const& /* blkids */,
                                                cintrusive< homestore::repl_req_ctx >& /* ctx */) {
    if (header.size() < sizeof(CraftEntryHeader)) {
        LOGE("on_commit lsn={} header too small ({} bytes)", lsn, header.size());
        return;
    }
    const auto* entry_hdr = reinterpret_cast< const CraftEntryHeader* >(header.cbytes());

    switch (entry_hdr->type) {
    case CraftEntryType::SyncRSCommitLSN: {
        if (key.size() < sizeof(SyncRSCommitLSNPayload)) {
            LOGE("on_commit lsn={} SyncRSCommitLSN key too small ({} bytes)", lsn, key.size());
            return;
        }
        const auto* payload = reinterpret_cast< const SyncRSCommitLSNPayload* >(key.cbytes());
        auto empty_slots    = parse_empty_slots(key);
        if (!empty_slots) {
            LOGE("on_commit lsn={} SyncRSCommitLSN malformed empty_slots", lsn);
            return;
        }
        // apply_sync_rs_commit_lsn co_awaits peer fetch + journal writes; on_commit itself is a synchronous
        // HomeStore callback, so fire-and-forget it.
        detail::detach(owner_->apply_sync_rs_commit_lsn(payload->rs_commit_lsn, payload->client_token,
                                                        std::move(*empty_slots)));
        break;
    }
    case CraftEntryType::InternalLogin:
        // Dispatch lands with 22887.
        LOGD("on_commit lsn={} InternalLogin (dispatch not yet implemented)", lsn);
        break;
    default:
        LOGE("on_commit lsn={} unrecognized CraftEntryType={}", lsn, static_cast< uint8_t >(entry_hdr->type));
        break;
    }
}

// ─── RAFT apply helpers (S5 implements) ──────────────────────────────────────
//
// apply_sync_rs_commit_lsn (22886): client_token is verified against the current session first -- a mismatch
// gates the ENTIRE apply (no reconciliation, no catch-up, no watermark advance), since a RAFT entry whose
// token doesn't match the live session shouldn't be trusted to describe it. Once the token matches, every
// other step is best-effort forward progress: empty_slots are reconciled and the newly-spanned range is
// marked missing, catch-up attempts to fill in what it can from a peer, and commit_lsn/last_append_lsn
// advance regardless of whether catch-up fully succeeded -- mirroring truncate()'s invariant that apply
// never reverts the watermark, only advances it.

async_status CraftReplDev::apply_sync_rs_commit_lsn(int64_t rs_commit_lsn, uint64_t client_token,
                                                    std::vector< int64_t > empty_slots) {
    std::vector< int64_t > to_fetch;
    {
        std::lock_guard lk{missing_mu_};
        if (client_token != state_.client_token) {
            LOGW("apply_sync_rs_commit_lsn: client_token mismatch want={} got={} rs_commit_lsn={} -- skipping apply",
                state_.client_token, client_token, rs_commit_lsn);
            co_return std::unexpected(make_error_condition(volume_error::WRONG_TOKEN));
        }

        for (int64_t lsn : empty_slots) {
            empty_lsns_.insert(lsn);
            missing_lsns_.erase(lsn);
        }

        // Everything newly spanned by this advance that isn't Empty-verdicted is a gap until catch-up
        // (below) resolves it -- same idiom write() uses for gaps opened by an out-of-order dlsn.
        for (int64_t lsn = state_.last_append_lsn + 1; lsn <= rs_commit_lsn; ++lsn) {
            if (!empty_lsns_.contains(lsn)) missing_lsns_.insert(lsn);
        }
        state_.last_append_lsn = std::max(state_.last_append_lsn, rs_commit_lsn);

        for (int64_t lsn : missing_lsns_) {
            if (lsn <= rs_commit_lsn) to_fetch.push_back(lsn);
        }
    }

    if (!to_fetch.empty()) {
        if (peer_fetcher_ == nullptr) {
            LOGW("apply_sync_rs_commit_lsn: {} lsn(s) missing but no peer_fetcher_ wired -- leaving as missing",
                to_fetch.size());
        } else if (auto fetched = co_await peer_fetcher_->fetch_from_peer(to_fetch); !fetched) {
            LOGE("apply_sync_rs_commit_lsn: fetch_from_peer failed: {} -- leaving {} lsn(s) as missing",
                fetched.error().message(), to_fetch.size());
        } else {
            for (auto& slot : *fetched) {
                if (slot.is_empty) {
                    std::lock_guard lk{missing_mu_};
                    empty_lsns_.insert(slot.lsn);
                    missing_lsns_.erase(slot.lsn);
                    continue;
                }
                auto res = co_await journal_->write_slot(slot.lsn, slot.lba, slot.len, std::move(slot.data));
                if (!res) {
                    LOGE("apply_sync_rs_commit_lsn: write_slot failed lsn={}: {} -- leaving as missing", slot.lsn,
                        res.error().message());
                    continue;
                }
                std::lock_guard lk{missing_mu_};
                missing_lsns_.erase(slot.lsn);
            }
        }
    }

    // Unconditional: commit_lsn is a replica-set-wide watermark RAFT already agreed on, independent of
    // whether this replica's local catch-up succeeded.
    {
        std::lock_guard lk{missing_mu_};
        state_.commit_lsn = std::max(state_.commit_lsn, rs_commit_lsn);
    }
    LOGT("apply_sync_rs_commit_lsn ok rs_commit_lsn={} client_token={}", rs_commit_lsn, client_token);
    co_return ok();
}

void CraftReplDev::apply_internal_login(uint64_t client_token, uint64_t term) {
    LOGD("apply_internal_login client_token={} term={} (not yet implemented)", client_token, term);
}

} // namespace homeblocks
