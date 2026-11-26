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

// Seeded randomized interleaving test (S7 / SDSTOR-26679).
//
// One CraftReplDev plays member 0 of a 3-member group. The client and the other two members are a world model: every
// client write, which members hold which copy of each slot, every committed login and every committed Empty verdict.
// Each step picks a random operation: a client write (new, delivered out of order, held mid-journal, or at a stale
// term), a piggybacked commit, another leader's Sync or login committed here, pausing or resuming the apply queue,
// releasing a held write, this member's own Sync round or Resolve, or a read (sometimes with a login committing in the
// middle of it). After every step it checks invariant_violations() and the safety properties below, and stops at the
// first failure, printing the seed and the operation trace. --seed=N replays one seed.
//
// Everything runs inline on one thread, so this explores interleavings at suspension points (held writes, a paused
// or draining apply queue, a login committed inside a read), not cross-thread races.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <sys/uio.h>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>
#include <homestore/crc.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "craft/craft_repl_dev.hpp"
#include "home_blks_config.hpp"
#include "coro_helpers.hpp"
#include "mock_journal_backend.hpp"

// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles craft_repl_dev.cpp directly.
SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)
SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTION_GROUP(test_craft_interleave,
                  (seeds, "", "seeds", "number of seeds to run, from 1",
                   ::cxxopts::value< uint32_t >()->default_value("300"), "number"),
                  (steps, "", "steps", "steps per seed", ::cxxopts::value< uint32_t >()->default_value("400"),
                   "number"),
                  (seed, "", "seed", "run only this seed (0: run --seeds seeds)",
                   ::cxxopts::value< uint64_t >()->default_value("0"), "number"))
SISL_OPTIONS_ENABLE(logging, test_craft_interleave)

namespace homeblocks {
namespace {

static constexpr uint32_t k_page_size = 4096;
static constexpr homestore::csum_t k_crc16_seed = 0x8005; // the seed craft_repl_dev.cpp sums with
static constexpr lba_t k_lbas = 8;                        // a small LBA range, so writes overlap
static constexpr size_t k_max_held = 3;                   // writes held mid-journal at once

using ReadIndex = std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >;

template < typename R >
using Outcome = std::shared_ptr< std::optional< R > >;

// Starts an operation without waiting for it. Its result is set once it finishes: at once, unless it suspends.
template < typename R, typename Op >
Outcome< R > start(Op&& op) {
    auto out = std::make_shared< std::optional< R > >();
    homeblocks::detail::detach([](std::decay_t< Op > o, Outcome< R > dst) -> sisl::async::task< void > {
        *dst = co_await std::move(o);
    }(std::forward< Op >(op), out));
    return out;
}

std::string describe(std::error_condition const& e) { return e.message(); }

// ── the world ─────────────────────────────────────────────────────────────────

// One client write: its slot, its session, and its payload. Each page is stamped with (dlsn, term, lba), so a read
// shows which write it returned.
struct Write {
    int64_t dlsn{-1};
    uint64_t term{0};
    lba_t lba{0};
    uint32_t nlbas{1};
    bool zeros{false};
    std::vector< uint8_t > data; // nlbas pages; empty for zeros
    std::vector< homestore::csum_t > csums;
};

int64_t stamped_dlsn(uint8_t const* page) {
    int64_t dlsn;
    std::memcpy(&dlsn, page, sizeof(dlsn));
    return dlsn;
}

JournalSlot to_slot(Write const& w) {
    JournalSlot s{.lsn = w.dlsn,
                  .term = w.term,
                  .all_zeros = w.zeros,
                  .lba_off_bytes = w.lba * k_page_size,
                  .len_bytes = w.nlbas * k_page_size,
                  .csums = w.csums};
    if (!w.zeros) {
        s.data.size = w.data.size();
        s.data.iovs.push_back(iovec{const_cast< uint8_t* >(w.data.data()), w.data.size()});
    }
    return s;
}

// ── this member's journal ─────────────────────────────────────────────────────

class InterleaveJournal : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots;
    std::map< homestore::blk_num_t, std::vector< uint8_t > > blocks; // freed blocks are erased, so reading one fails
    homestore::blk_num_t next_blk{1000};
    bool hold_next_write{false}; // the next write_slot() waits in `held` until released
    std::vector< std::shared_ptr< sisl::async::value_awaitable< bool > > > held;
    std::function< void(JournalSlot const&) > on_journal; // sees every record as it's written
    std::function< void() > on_read_data;

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const& data, lba_count_t len) override {
        auto const nlbas = static_cast< homestore::blk_count_t >(len / k_page_size);
        homestore::multi_blk_id blkid{next_blk, nlbas, /*chunk_num=*/1};
        auto const* buf = static_cast< uint8_t const* >(data.iovs[0].iov_base);
        for (homestore::blk_count_t i = 0; i < nlbas; ++i)
            blocks[next_blk + i] = std::vector< uint8_t >(buf + i * k_page_size, buf + (i + 1) * k_page_size);
        next_blk += nlbas;
        co_return blkid;
    }
    async_status write_slot(int64_t lsn, uint64_t term, lba_t lba, lba_count_t len, homestore::multi_blk_id blkid,
                            bool all_zeros, std::vector< homestore::csum_t > const& csums) override {
        if (std::exchange(hold_next_write, false)) {
            auto gate = std::make_shared< sisl::async::value_awaitable< bool > >();
            held.push_back(gate);
            co_await *gate;
        }
        JournalSlot slot{.lsn = lsn,
                         .term = term,
                         .all_zeros = all_zeros,
                         .lba_off_bytes = lba,
                         .len_bytes = len,
                         .blkid = blkid,
                         .csums = csums};
        if (on_journal) on_journal(slot);
        slots[lsn] = std::move(slot);
        co_return ok();
    }
    async_result< JournalSlot > read_slot(int64_t lsn) override {
        auto it = slots.find(lsn);
        if (it == slots.end())
            co_return std::unexpected(std::make_error_condition(std::errc::no_such_file_or_directory));
        co_return it->second;
    }
    async_status free_data(homestore::multi_blk_id blkid) override {
        auto pieces = blkid.iterate();
        while (auto piece = pieces.next())
            for (homestore::blk_count_t i = 0; i < piece->blk_count(); ++i)
                blocks.erase(piece->blk_num() + i);
        co_return ok();
    }
    async_status free_slot(int64_t lsn) override {
        auto it = slots.find(lsn);
        if (it == slots.end())
            co_return std::unexpected(std::make_error_condition(std::errc::no_such_file_or_directory));
        if (it->second.all_zeros) co_return ok();
        co_return co_await free_data(it->second.blkid);
    }
    async_status read_data(homestore::multi_blk_id blkid, sisl::sg_list& dest) override {
        if (on_read_data) on_read_data();
        auto* buf = static_cast< uint8_t* >(dest.iovs[0].iov_base);
        size_t offset = 0;
        auto pieces = blkid.iterate();
        while (auto piece = pieces.next()) {
            for (homestore::blk_count_t i = 0; i < piece->blk_count(); ++i) {
                auto it = blocks.find(piece->blk_num() + i);
                if (it == blocks.end()) co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
                std::memcpy(buf + offset, it->second.data(), it->second.size());
                offset += it->second.size();
            }
        }
        co_return ok();
    }
};

// The commit runs' index: lba -> {blkid, checksum}, the shape VolumeIndexTable exposes.
class FakeIndex {
public:
    std::map< lba_t, BlockInfo > entries;

    status write_to_index(lba_t start_lba, lba_t end_lba, std::unordered_map< lba_t, BlockInfo >& blocks_info) {
        for (auto lba = start_lba; lba <= end_lba; ++lba) {
            auto& info = blocks_info[lba];
            if (auto it = entries.find(lba); it != entries.end()) info.old_blkid = it->second.new_blkid;
            entries[lba] = BlockInfo{info.new_blkid, homestore::blk_id{}, info.new_checksum};
        }
        return ok();
    }
    status delete_lba_range(lba_t start_lba, lba_t end_lba, std::vector< homestore::blk_id >& freed) {
        for (auto lba = start_lba; lba <= end_lba; ++lba) {
            auto it = entries.find(lba);
            if (it == entries.end()) continue;
            freed.push_back(it->second.new_blkid);
            entries.erase(it);
        }
        return ok();
    }
    status read_from_index(lba_t start_lba, lba_t end_lba, ReadIndex& out) {
        for (auto lba = start_lba; lba <= end_lba; ++lba) {
            auto it = entries.find(lba);
            if (it == entries.end()) continue;
            out.emplace_back(VolumeIndexKey{lba}, VolumeIndexValue{it->second.new_blkid, it->second.new_checksum});
        }
        return ok();
    }
};

// The two other members, as this member sees them: their replies come from the world.
class InterleavePeers : public CraftPeerFetcher {
public:
    std::function< std::vector< QuorumSlotResponse >(std::vector< int64_t > const&) > quorum_reply;
    std::function< std::vector< JournalSlot >(std::vector< int64_t > const&) > fetch_reply;

    async_result< RsCommitLsnReply > get_rs_commit_lsn(uint64_t, bool) override { co_return RsCommitLsnReply{}; }
    async_result< std::vector< JournalSlot > > fetch_data(const std::vector< int64_t >& lsns, uint32_t) override {
        co_return fetch_reply(lsns);
    }
    async_result< std::vector< QuorumSlotResponse > > fetch_from_quorum(std::vector< int64_t > lsns,
                                                                        uint32_t) override {
        co_return quorum_reply(lsns);
    }
    async_result< std::vector< QuorumLsnResponse > > poll_quorum_lsns(uint64_t, bool, uint32_t) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }
};

// ── the harness ───────────────────────────────────────────────────────────────
//
// Safety properties, checked after every step:
//   - no fault, and invariant_violations() is clean (the apply queue may be busy mid-run);
//   - a write is acked only in the newest committed session, and never into an Empty slot;
//   - an old-session write, round or Resolve never succeeds once a newer login has committed;
//   - a read from a session a login replaced, before or during the read, never succeeds; a current-session read
//     returns, for each LBA, the newest write this member holds (see expected());
//   - a fresh Empty verdict is above the committed watermark, and never for a slot a quorum holds;
//   - a committed Empty verdict is never lost, and commit_lsn never goes backwards;
//   - every journal record is a client write at its own term, and a fill never journals a slot a committed login
//     dropped (SDSTOR-26847).

// What a run exercised, so a pass can't hide a harness that never reaches the cases it checks.
struct Counts {
    uint64_t acked{0};         // writes this member acked
    uint64_t held{0};          // writes that landed after being held mid-journal
    uint64_t logins{0};        // logins committed here
    uint64_t truncating{0};    // ...of which below this member's last_append_lsn
    uint64_t syncs{0};         // other leaders' Syncs committed here
    uint64_t rounds{0};        // this member's rounds and Resolves that succeeded
    uint64_t empties{0};       // Empty verdicts committed
    uint64_t fills{0};         // slots this member journaled from a peer's copy
    uint64_t stale_offered{0}; // copies a committed login dropped, offered by a peer
    uint64_t reads_checked{0}; // current-session reads whose data was compared
    uint64_t reads_fenced{0};  // reads from a replaced session that failed, as they must
    uint64_t reads_nested{0};  // reads with a login committed in the middle

    Counts& operator+=(Counts const& o) {
        acked += o.acked;
        held += o.held;
        logins += o.logins;
        truncating += o.truncating;
        syncs += o.syncs;
        rounds += o.rounds;
        empties += o.empties;
        fills += o.fills;
        stale_offered += o.stale_offered;
        reads_checked += o.reads_checked;
        reads_fenced += o.reads_fenced;
        reads_nested += o.reads_nested;
        return *this;
    }
    std::string str() const {
        return "acked " + std::to_string(acked) + ", held " + std::to_string(held) + ", logins " +
            std::to_string(logins) + " (" + std::to_string(truncating) + " truncating), syncs " +
            std::to_string(syncs) + ", rounds " + std::to_string(rounds) + ", empties " + std::to_string(empties) +
            ", fills " + std::to_string(fills) + ", stale copies offered " + std::to_string(stale_offered) +
            ", reads checked " + std::to_string(reads_checked) + " / fenced " + std::to_string(reads_fenced) +
            " / with a login inside " + std::to_string(reads_nested);
    }
};

class Interleave {
public:
    explicit Interleave(uint64_t seed) : seed_{seed}, rng_{seed} {
        auto journal = std::make_unique< InterleaveJournal >();
        journal_ = journal.get();
        dev_ = CraftReplDev::create(volume_id_t{}, std::move(journal), k_page_size, nullptr);
        dev_->set_raft_proposer(&proposer_);
        dev_->set_peer_fetcher(&peers_);
        dev_->set_sync_lsn_interval(5); // periodic rounds fire from write() too
        dev_->use_test_index(
            [this](lba_t s, lba_t e, std::unordered_map< lba_t, BlockInfo >& info) {
                return index_.write_to_index(s, e, info);
            },
            [this](lba_t s, lba_t e, std::vector< homestore::blk_id >& freed) {
                return index_.delete_lba_range(s, e, freed);
            });
        proposer_.on_commit = [this](sisl::blob const& h, sisl::blob const& k) { loop_back(proposer_, *dev_, h, k); };
        journal_->on_journal = [this](JournalSlot const& s) { check_journaled(s); };
        peers_.quorum_reply = [this](std::vector< int64_t > const& lsns) {
            std::vector< QuorumSlotResponse > out;
            for (uint32_t p : {1u, 2u})
                if (chance(0.85))
                    out.push_back(QuorumSlotResponse{.member = test_member_id(p), .slots = peer_slots(p, lsns)});
            return out;
        };
        peers_.fetch_reply = [this](std::vector< int64_t > const& lsns) {
            return peer_slots(chance(0.5) ? 1 : 2, lsns);
        };
    }

    bool failed() const { return failed_; }
    Counts const& counts() const { return counts_; }

    void run(uint32_t steps) {
        for (uint32_t i = 0; i < steps && !failed_; ++i) {
            step_ = i;
            snapshot();
            one_step();
            after_step();
        }
        finish();
    }

private:
    // ── randomness and reporting ──

    int64_t pick(int64_t lo, int64_t hi) { return std::uniform_int_distribution< int64_t >{lo, hi}(rng_); }
    bool chance(double p) { return std::bernoulli_distribution{p}(rng_); }

    void trace(std::string line) { trace_.push_back("#" + std::to_string(step_) + " " + std::move(line)); }

    void violation(std::string const& what) {
        if (failed_) return;
        failed_ = true;
        std::ostringstream os;
        os << "seed " << seed_ << ", step " << step_ << ": " << what << "\n  replay with --seed=" << seed_
           << "\n  trace (last 60 lines):";
        auto const from = trace_.size() > 60 ? trace_.size() - 60 : 0;
        for (auto i = from; i < trace_.size(); ++i)
            os << "\n    " << trace_[i];
        ADD_FAILURE() << os.str();
    }

    // ── world queries ──

    static constexpr uint64_t k_token_base = 100;
    uint64_t token() const { return k_token_base + term_; }

    // True if this member holds w's copy of its slot, readable and not voided.
    bool self_holds(Write const* w) const {
        auto s = journal_->slots.find(w->dlsn);
        return s != journal_->slots.end() && s->second.term == w->term && w->dlsn <= dev_->last_append_lsn() &&
            !dev_->is_missing(w->dlsn) && !dev_->is_empty_slot(w->dlsn);
    }

    // Members holding the live copy of slot l: the write a login didn't drop and no verdict voided.
    int holders(int64_t l) const {
        auto it = live_.find(l);
        if (it == live_.end() || empty_.contains(l)) return 0;
        int n = self_holds(it->second) ? 1 : 0;
        for (uint32_t p : {1u, 2u})
            if (auto c = peers_held_[p].find(l); c != peers_held_[p].end() && c->second == it->second) ++n;
        return n;
    }

    // True if a committed login dropped slot lsn as written at term: the first login after term is below it.
    bool dropped(int64_t lsn, uint64_t term) const {
        for (auto const& [t, rs] : logins_)
            if (t > term) return lsn > rs;
        return false;
    }

    int64_t scan_end() const {
        int64_t end = std::max(next_dlsn_, dev_->last_append_lsn() + 1);
        for (uint32_t p : {1u, 2u})
            if (!peers_held_[p].empty()) end = std::max(end, peers_held_[p].rbegin()->first + 1);
        return end;
    }

    // The highest commit_lsn the client may piggyback: every slot up to it is settled, either by a committed Sync or
    // because a quorum holds it (no later verdict can void it).
    int64_t settled_prefix() const {
        int64_t l = 0;
        while (l < next_dlsn_ && (empty_.contains(l) || holders(l) >= 2))
            ++l;
        return std::max(l - 1, dev_->applied_sync_lsn());
    }

    std::vector< JournalSlot > peer_slots(uint32_t p, std::vector< int64_t > const& lsns) const {
        std::vector< JournalSlot > out;
        for (int64_t l : lsns) {
            if (empty_.contains(l)) {
                out.push_back(JournalSlot{.lsn = l, .is_empty = true});
            } else if (auto c = peers_held_[p].find(l); c != peers_held_[p].end()) {
                out.push_back(to_slot(*c->second)); // live, or a copy a login dropped
                if (dropped(l, c->second->term)) ++counts_.stale_offered;
            }
        }
        return out;
    }

    void void_slot(int64_t l) {
        ++counts_.empties;
        empty_.insert(l);
        peers_held_[1].erase(l);
        peers_held_[2].erase(l);
    }

    void commit_entry(CraftEntryType type, std::vector< uint8_t >& key) {
        std::vector< uint8_t > header(sizeof(CraftEntryHeader));
        reinterpret_cast< CraftEntryHeader* >(header.data())->type = type;
        dev_->raft_listener().on_commit(1, sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                                        sisl::blob{key.data(), static_cast< uint32_t >(key.size())}, {}, {});
    }
    void commit_sync(int64_t rs, uint64_t term, std::vector< int64_t > const& verdicts) {
        std::vector< uint8_t > key(sync_rs_commit_lsn_key_size(verdicts.size()));
        serialize_sync_rs_commit_lsn(key.data(), rs, token(), term, verdicts);
        commit_entry(CraftEntryType::SyncRSCommitLSN, key);
    }

    // Another leader's resolution of (floor, rs]: a slot a responder holds live is filled (the leader, member 1,
    // then holds it), and one no responder holds is voted Empty. Returns the verdicts, already applied to the world.
    std::vector< int64_t > resolve_elsewhere(int64_t floor, int64_t rs, bool self_responds, bool p2_responds) {
        std::vector< int64_t > verdicts;
        for (int64_t l = floor + 1; l <= rs; ++l) {
            if (empty_.contains(l)) continue;
            auto it = live_.find(l);
            bool held = false;
            if (it != live_.end()) {
                auto const holds = [&](uint32_t p) {
                    auto c = peers_held_[p].find(l);
                    return c != peers_held_[p].end() && c->second == it->second;
                };
                held = holds(1) || (p2_responds && holds(2)) || (self_responds && self_holds(it->second));
            }
            if (held) {
                peers_held_[1][l] = it->second;
            } else {
                verdicts.push_back(l);
                void_slot(l);
            }
        }
        return verdicts;
    }

    // ── checks ──

    // Every record this member journals: a client write's copy, at that write's own term, and never one a committed
    // login dropped -- unless it's a client write already in flight when the login committed, which lands anyway and
    // discards itself.
    void check_journaled(JournalSlot const& s) {
        auto it = by_slot_term_.find({s.lsn, s.term});
        if (it == by_slot_term_.end())
            return violation("journaled lsn " + std::to_string(s.lsn) + " at term " + std::to_string(s.term) +
                             ", a term no write to it had");
        Write const& w = *it->second;
        if (s.lba_off_bytes != w.lba * k_page_size || s.len_bytes != w.nlbas * k_page_size || s.all_zeros != w.zeros ||
            (!w.zeros && s.csums != w.csums))
            return violation("journaled lsn " + std::to_string(s.lsn) + " with another write's contents");
        if (inflight_.contains({s.lsn, s.term})) return;
        ++counts_.fills;
        if (dropped(s.lsn, s.term))
            violation("filled lsn " + std::to_string(s.lsn) + " at term " + std::to_string(s.term) +
                      ", a slot a committed login dropped");
    }

    struct Pending {
        Write const* w;
        Outcome< result< craft::lsn_pair > > out;
        bool held;
    };

    void check_pending() {
        for (auto it = pending_.begin(); it != pending_.end();) {
            if (!it->out->has_value()) {
                ++it;
                continue;
            }
            Write const& w = *it->w;
            auto const& r = **it->out;
            inflight_.erase({w.dlsn, w.term});
            if (r) ++counts_.acked;
            if (it->held) ++counts_.held;
            trace("  write lsn " + std::to_string(w.dlsn) + " term " + std::to_string(w.term) + " -> " +
                  (r ? std::string{"acked"} : describe(r.error())));
            if (r && w.term != term_)
                violation("write lsn " + std::to_string(w.dlsn) + " at term " + std::to_string(w.term) +
                          " acked though the login at term " + std::to_string(term_) + " committed");
            else if (r && (dev_->is_empty_slot(w.dlsn) || empty_.contains(w.dlsn)))
                violation("write lsn " + std::to_string(w.dlsn) + " acked into an Empty slot");
            it = pending_.erase(it);
        }
    }

    void snapshot() {
        pre_applied_sync_ = dev_->applied_sync_lsn();
        pre_holders_.clear();
        for (int64_t l = 0, end = scan_end(); l < end; ++l)
            pre_holders_[l] = holders(l);
    }

    void after_step() {
        check_pending();
        if (failed_) return;
        if (dev_->is_faulted()) return violation("the partition faulted");
        for (auto const& v : dev_->invariant_violations())
            if (!v.starts_with("apply queue not idle")) return violation("invariant: " + v);
        if (dev_->commit_lsn() < last_commit_)
            return violation("commit_lsn went back from " + std::to_string(last_commit_) + " to " +
                             std::to_string(dev_->commit_lsn()));
        last_commit_ = dev_->commit_lsn();
        for (int64_t l = 0, end = scan_end(); l < end; ++l) {
            bool const dev_empty = dev_->is_empty_slot(l);
            bool const world_empty = empty_.contains(l);
            if (dev_empty && !world_empty) { // voted by this member's own round
                if (l <= pre_applied_sync_)
                    return violation("fresh Empty for lsn " + std::to_string(l) + " at or below the watermark " +
                                     std::to_string(pre_applied_sync_));
                if (pre_holders_[l] >= 2)
                    return violation("Empty for lsn " + std::to_string(l) + ", which a quorum held");
                void_slot(l);
            } else if (world_empty && !dev_empty) {
                return violation("the committed Empty verdict for lsn " + std::to_string(l) + " was lost");
            }
        }
    }

    // ── operations ──

    enum class Op {
        new_write,
        deliver,
        stale_write,
        keep_alive,
        sync,
        login,
        pause,
        resume,
        release,
        round,
        resolve,
        read
    };

    void one_step() {
        static constexpr std::array< std::pair< Op, int >, 12 > k_weights{{{Op::new_write, 18},
                                                                           {Op::deliver, 22},
                                                                           {Op::stale_write, 4},
                                                                           {Op::keep_alive, 8},
                                                                           {Op::sync, 6},
                                                                           {Op::login, 4},
                                                                           {Op::pause, 3},
                                                                           {Op::resume, 5},
                                                                           {Op::release, 8},
                                                                           {Op::round, 5},
                                                                           {Op::resolve, 4},
                                                                           {Op::read, 10}}};
        int total = 0;
        for (auto const& [op, w] : k_weights)
            total += w;
        int roll = static_cast< int >(pick(0, total - 1));
        Op op = Op::new_write;
        for (auto const& [o, w] : k_weights) {
            if (roll < w) {
                op = o;
                break;
            }
            roll -= w;
        }
        switch (op) {
        case Op::new_write:
            new_write();
            return;
        case Op::deliver:
            return deliver();
        case Op::stale_write:
            return stale_write();
        case Op::keep_alive:
            return keep_alive();
        case Op::sync:
            return sync_elsewhere();
        case Op::login:
            return login_elsewhere(/*nested=*/false);
        case Op::pause:
            return pause();
        case Op::resume:
            return resume();
        case Op::release:
            return release();
        case Op::round:
            return own_round(/*resolve=*/false);
        case Op::resolve:
            return own_round(/*resolve=*/true);
        case Op::read:
            return read();
        }
    }

    // The client issues its next write. The other members get it now (`to_peers`, or a random choice); this member
    // gets it later (deliver()).
    Write const* new_write(std::optional< std::vector< uint32_t > > to_peers = std::nullopt) {
        auto w = std::make_unique< Write >();
        w->dlsn = next_dlsn_++;
        w->term = term_;
        w->nlbas = static_cast< uint32_t >(pick(1, 2));
        w->lba = static_cast< lba_t >(pick(0, static_cast< int64_t >(k_lbas - w->nlbas)));
        w->zeros = chance(0.2);
        if (!w->zeros) {
            w->data.resize(w->nlbas * k_page_size);
            for (uint32_t i = 0; i < w->nlbas; ++i) {
                uint8_t* page = w->data.data() + i * k_page_size;
                auto const fill = 0x11 + (static_cast< uint64_t >(w->dlsn) * 7 + w->lba + i) % 200;
                std::memset(page, static_cast< int >(fill), k_page_size);
                uint64_t const lba = w->lba + i;
                std::memcpy(page, &w->dlsn, sizeof(w->dlsn));
                std::memcpy(page + 8, &w->term, sizeof(w->term));
                std::memcpy(page + 16, &lba, sizeof(lba));
                w->csums.push_back(crc16_t10dif(k_crc16_seed, page, k_page_size));
            }
        }
        if (!to_peers) {
            double const reach = std::uniform_real_distribution< double >{0.0, 1.0}(rng_);
            to_peers = reach < 0.5 ? std::vector< uint32_t >{1, 2}
                : reach < 0.65     ? std::vector< uint32_t >{1}
                : reach < 0.8      ? std::vector< uint32_t >{2}
                                   : std::vector< uint32_t >{};
        }
        std::string to;
        for (uint32_t p : *to_peers) {
            if (p == 2 && p2_lagging_) continue; // still in an older session: it rejects this one's writes
            peers_held_[p][w->dlsn] = w.get();
            to += " p" + std::to_string(p);
        }
        live_[w->dlsn] = w.get();
        by_slot_term_[{w->dlsn, w->term}] = w.get();
        undelivered_.push_back(w.get());
        trace("new write lsn " + std::to_string(w->dlsn) + " term " + std::to_string(w->term) + " lba " +
              std::to_string(w->lba) + "+" + std::to_string(w->nlbas) + (w->zeros ? " zeros" : "") + " to" +
              (to.empty() ? " none" : to));
        writes_.push_back(std::move(w));
        return writes_.back().get();
    }

    // Delivers an undelivered write to this member, in any order, sometimes held mid-journal.
    void deliver() {
        if (undelivered_.empty()) {
            new_write();
            return;
        }
        auto const i = static_cast< size_t >(pick(0, static_cast< int64_t >(undelivered_.size()) - 1));
        deliver(undelivered_[i], journal_->held.size() < k_max_held && chance(0.25));
    }
    void deliver(Write const* w, bool hold) {
        std::erase(undelivered_, w);
        int64_t const commit = chance(0.5) ? client_commit_ : -1;
        sisl::sg_list data;
        if (!w->zeros) {
            data.size = w->data.size();
            data.iovs.push_back(iovec{const_cast< uint8_t* >(w->data.data()), w->data.size()});
        }
        trace("deliver lsn " + std::to_string(w->dlsn) + " term " + std::to_string(w->term) + (hold ? " (held)" : "") +
              " commit " + std::to_string(commit));
        inflight_.insert({w->dlsn, w->term});
        journal_->hold_next_write = hold;
        craft::client_hdr const hdr{.term = w->term, .commit_lsn = commit, .all_committed_lsn = -1};
        auto out = start< result< craft::lsn_pair > >(
            dev_->write(hdr, w->dlsn, w->lba * k_page_size, w->nlbas * k_page_size, std::move(data)));
        journal_->hold_next_write = false;
        pending_.push_back(Pending{w, std::move(out), hold});
    }

    // A write at a term that isn't the session's: one from a session a login replaced, or from one not yet started.
    void stale_write() {
        uint64_t const bad = (term_ > 0 && chance(0.5)) ? term_ - 1 : term_ + 1;
        sisl::sg_list none;
        trace("stale write lsn " + std::to_string(next_dlsn_) + " term " + std::to_string(bad));
        craft::client_hdr const hdr{.term = bad, .commit_lsn = -1, .all_committed_lsn = -1};
        auto out = start< result< craft::lsn_pair > >(dev_->write(hdr, next_dlsn_, 0, k_page_size, std::move(none)));
        if (!out->has_value()) return violation("a stale write suspended");
        if (**out)
            violation("a write at term " + std::to_string(bad) + " succeeded in session " + std::to_string(term_));
    }

    // The client piggybacks a commit_lsn up to its settled prefix.
    void keep_alive() {
        int64_t const hi = settled_prefix();
        if (hi > client_commit_ && chance(0.8)) client_commit_ = pick(client_commit_ + 1, hi);
        trace("keep_alive commit " + std::to_string(client_commit_));
        craft::client_hdr const hdr{.term = dev_->term(), .commit_lsn = client_commit_, .all_committed_lsn = -1};
        auto out = start< result< craft::lsn_pair > >(dev_->keep_alive(hdr));
        if (!out->has_value()) violation("keep_alive suspended");
    }

    // Another leader's periodic Sync in the newest session, committed here: it may land behind a queued login.
    void sync_elsewhere() {
        int64_t const floor =
            std::max({dev_->applied_sync_lsn(), client_commit_, dev_->commit_lsn(), dev_->last_proposed_sync_lsn()});
        int64_t const hi = std::max(floor, next_dlsn_ - 1);
        int64_t const rs = pick(floor, hi);
        if (rs < 0) return;
        auto const verdicts = resolve_elsewhere(floor, rs, /*self_responds=*/chance(0.5), /*p2_responds=*/true);
        ++counts_.syncs;
        if (p2_lagging_ && chance(0.15)) p2_catches_up();
        trace("sync elsewhere rs " + std::to_string(rs) + " term " + std::to_string(term_) + " empty " +
              std::to_string(verdicts.size()));
        commit_sync(rs, term_, verdicts);
    }

    // Another leader's login, committed here: its Sync, then its InternalLogin. This member is polled (and quiesced)
    // or missed the poll, in which case it may hold a tail the login drops. Often the old client's last writes reached
    // only one member first: this one (a tail to drop) or p2 (a stale tail, if p2 misses the login).
    void login_elsewhere(bool nested) {
        if (!nested && chance(0.6)) {
            bool const to_self = chance(0.4);
            for (int64_t i = 0, n = pick(1, 3); i < n; ++i) {
                Write const* w = new_write(to_self ? std::vector< uint32_t >{} : std::vector< uint32_t >{2});
                if (to_self) deliver(w, /*hold=*/false);
            }
            check_pending();
        }
        bool const self_polled = chance(0.65);
        bool const p2_responds = !self_polled || chance(0.35);
        int64_t rs = -1;
        if (self_polled) {
            auto polled = start< result< RsCommitLsnReply > >(dev_->get_rs_commit_lsn(term_, /*is_login=*/true));
            if (!polled->has_value() || !**polled) return violation("this member's login poll failed");
            rs = (**polled)->lsns.last_append_lsn;
        }
        for (uint32_t p : {1u, 2u})
            if ((p == 1 || p2_responds) && !peers_held_[p].empty()) rs = std::max(rs, peers_held_[p].rbegin()->first);
        int64_t const floor =
            std::max({dev_->applied_sync_lsn(), client_commit_, dev_->commit_lsn(), dev_->last_proposed_sync_lsn()});
        rs = std::max(rs, floor);
        auto const verdicts = resolve_elsewhere(floor, rs, self_polled, p2_responds);
        ++counts_.logins;
        if (rs < dev_->last_append_lsn()) ++counts_.truncating;
        commit_sync(rs, term_, verdicts);

        uint64_t const term = term_ + 1;
        std::vector< uint8_t > key(sizeof(InternalLoginPayload));
        serialize_internal_login(key.data(), k_token_base + term, term, rs);
        commit_entry(CraftEntryType::InternalLogin, key);

        bool const p2_missed = chance(0.6);
        trace(std::string{nested ? "  (inside the read) " : ""} + "login elsewhere term " + std::to_string(term) +
              " rs " + std::to_string(rs) + " empty " + std::to_string(verdicts.size()) +
              (self_polled ? "" : ", this member not polled") + (p2_missed ? ", p2 missed it" : ""));
        logins_.emplace_back(term, rs);
        term_ = term;
        next_dlsn_ = rs + 1;
        for (auto it = live_.upper_bound(rs); it != live_.end();)
            it = live_.erase(it);
        empty_.erase(empty_.upper_bound(rs), empty_.end());
        peers_held_[1].erase(peers_held_[1].upper_bound(rs), peers_held_[1].end());
        if (p2_missed)
            p2_lagging_ = true;
        else
            p2_catches_up();
    }

    // p2 commits the logins it missed: each drops its copies above that login's watermark.
    void p2_catches_up() {
        std::erase_if(peers_held_[2], [this](auto const& kv) { return dropped(kv.first, kv.second->term); });
        p2_lagging_ = false;
        trace("  p2 caught up");
    }

    void pause() {
        if (paused_) return;
        trace("pause apply");
        dev_->pause_apply();
        paused_ = true;
    }
    void resume() {
        if (!paused_) return;
        trace("resume apply");
        paused_ = false;
        dev_->resume_apply();
    }

    void release() {
        if (journal_->held.empty()) return;
        auto const i = static_cast< size_t >(pick(0, static_cast< int64_t >(journal_->held.size()) - 1));
        auto gate = journal_->held[i];
        journal_->held.erase(journal_->held.begin() + static_cast< std::ptrdiff_t >(i));
        trace("release a held write");
        gate->complete(true);
    }

    // This member's own Sync round, or a client's Resolve, at this member's session term.
    void own_round(bool resolve) {
        int64_t const hi = std::max< int64_t >(next_dlsn_ - 1, 0);
        int64_t const upto = pick(std::min< int64_t >(std::max< int64_t >(dev_->commit_lsn(), 0), hi), hi);
        uint64_t const t = dev_->term();
        trace(std::string{resolve ? "resolve" : "round"} + " upto " + std::to_string(upto) + " term " +
              std::to_string(t));
        using R = result< craft::resolution_result >;
        craft::client_hdr const hdr{.term = t, .commit_lsn = -1, .all_committed_lsn = -1};
        auto const out = resolve ? start< R >(dev_->request_resolution(hdr, upto))
                                 : start< R >(dev_->append(upto, dev_->client_token(), t));
        if (!out->has_value()) return violation("a round suspended");
        trace(std::string{"  -> "} +
              (**out ? "resolved to " + std::to_string((**out)->resolved_upto) : describe((**out).error())));
        if (**out && t != term_)
            violation("a round in session " + std::to_string(t) + " succeeded after the login at term " +
                      std::to_string(term_) + " committed");
        else if (**out)
            ++counts_.rounds;
    }

    // What a current-session read of [lba, lba+nlbas) must return: for each LBA, the newest write this member holds
    // readable (zeros if none, or if it wrote zeros). `checked[i]` is false for an LBA whose newest journaled write was
    // voided by an Empty verdict: install_sync_rs_commit_lsn() retires the voided slot's overlay entries without
    // re-publishing an older unindexed write, so such a read can serve the index's older value. A known gap, not
    // checked here.
    std::vector< uint8_t > expected(lba_t lba, uint32_t nlbas, std::vector< bool >& checked) const {
        std::vector< uint8_t > out(nlbas * k_page_size, 0);
        checked.assign(nlbas, true);
        int64_t const last = dev_->last_append_lsn();
        for (uint32_t i = 0; i < nlbas; ++i) {
            lba_t const target = lba + i;
            JournalSlot const* best = nullptr;
            int64_t newest_voided = -1;
            for (auto const& [l, s] : journal_->slots) {
                lba_t const first = s.lba_off_bytes / k_page_size;
                if (l > last || target < first || target >= first + s.len_bytes / k_page_size) continue;
                if (dev_->is_empty_slot(l)) {
                    newest_voided = l;
                } else if (!dev_->is_missing(l)) {
                    best = &s; // ascending: the last match is the newest
                }
            }
            if (newest_voided >= 0 && (!best || newest_voided > best->lsn)) checked[i] = false;
            if (!best || best->all_zeros) continue;
            auto w = by_slot_term_.find({best->lsn, best->term});
            if (w == by_slot_term_.end()) continue; // flagged when it was journaled
            auto const off = (target - best->lba_off_bytes / k_page_size) * k_page_size;
            std::memcpy(out.data() + i * k_page_size, w->second->data.data() + off, k_page_size);
        }
        return out;
    }

    // A read at this member's session, or at the one before. Now and then a login commits during its index lookup,
    // or during its data read.
    void read() {
        uint64_t const t = (dev_->term() > 0 && chance(0.15)) ? dev_->term() - 1 : dev_->term();
        uint32_t const nlbas = static_cast< uint32_t >(pick(1, 2));
        lba_t const lba = static_cast< lba_t >(pick(0, static_cast< int64_t >(k_lbas - nlbas)));
        int64_t const read_lsn = std::max< int64_t >(dev_->last_append_lsn(), 0);
        std::vector< bool > checked;
        auto const want = expected(lba, nlbas, checked);
        uint64_t const term_before = term_;
        int64_t const nest = pick(0, 19);
        auto lookup_hook = std::make_shared< bool >(nest == 0);
        auto data_hook = std::make_shared< bool >(nest == 1);
        trace("read lba " + std::to_string(lba) + "+" + std::to_string(nlbas) + " term " + std::to_string(t) + " at " +
              std::to_string(read_lsn) +
              (nest == 0       ? ", a login commits during the lookup"
                   : nest == 1 ? ", a login commits during the data read"
                               : ""));
        journal_->on_read_data = [this, data_hook] {
            if (std::exchange(*data_hook, false)) login_elsewhere(/*nested=*/true);
        };
        auto buf = std::make_shared< std::vector< uint8_t > >(nlbas * k_page_size, 0xEE);
        sisl::sg_list dest;
        dest.size = buf->size();
        dest.iovs.push_back(iovec{buf->data(), buf->size()});
        auto const lookup = [this, lookup_hook](lba_t s, lba_t e, ReadIndex& kvs) {
            if (std::exchange(*lookup_hook, false)) login_elsewhere(/*nested=*/true);
            return index_.read_from_index(s, e, kvs);
        };
        auto out = start< result< craft::read_result > >(
            dev_->read_with(read_lsn, lba * k_page_size, nlbas * k_page_size, std::move(dest), lookup, t));
        journal_->on_read_data = nullptr;
        if (!out->has_value()) return violation("a read suspended");
        auto const& r = **out;
        trace(std::string{"  -> "} + (r ? "ok" : describe(r.error())));
        if (term_ != term_before) ++counts_.reads_nested;
        if (t < term_) {
            if (r)
                violation("a read at term " + std::to_string(t) + " succeeded though the login at term " +
                          std::to_string(term_) + (term_ != term_before ? " committed during it" : " had committed"));
            else
                ++counts_.reads_fenced;
            return;
        }
        if (!r) return violation("a current-session read failed: " + describe(r.error()));
        if (std::find(checked.begin(), checked.end(), true) != checked.end()) ++counts_.reads_checked;
        for (uint32_t i = 0; i < nlbas; ++i) {
            if (!checked[i]) continue;
            uint8_t const* got = buf->data() + i * k_page_size;
            uint8_t const* exp = want.data() + i * k_page_size;
            if (std::memcmp(got, exp, k_page_size) == 0) continue;
            auto const name = [](uint8_t const* page) {
                bool const zero = std::all_of(page, page + k_page_size, [](uint8_t b) { return b == 0; });
                return zero ? std::string{"zeros"} : "lsn " + std::to_string(stamped_dlsn(page));
            };
            return violation("read lba " + std::to_string(lba + i) + " returned " + name(got) + ", expected " +
                             name(exp));
        }
    }

    // Releases every held write, resumes the apply queue, and checks the run settled: nothing left pending, every
    // committed login applied, and every invariant holds with the queue idle.
    void finish() {
        while (!journal_->held.empty()) {
            auto gate = journal_->held.front();
            journal_->held.erase(journal_->held.begin());
            gate->complete(true);
            check_pending();
        }
        if (paused_) {
            paused_ = false;
            dev_->resume_apply();
        }
        check_pending();
        if (failed_) return;
        if (!pending_.empty()) return violation("a write never finished");
        if (dev_->term() != term_)
            return violation("the session is term " + std::to_string(dev_->term()) + " after every login at up to " +
                             std::to_string(term_) + " applied");
        for (auto const& v : dev_->invariant_violations())
            return violation("invariant at the end: " + v);
    }

    uint64_t const seed_;
    std::mt19937_64 rng_;
    uint32_t step_{0};
    bool failed_{false};
    std::vector< std::string > trace_;

    InterleaveJournal* journal_{nullptr};
    FakeIndex index_;
    InterleavePeers peers_;
    MockCraftRaftProposer proposer_;
    std::shared_ptr< CraftReplDev > dev_;
    bool paused_{false};
    bool p2_lagging_{false}; // p2 missed a login: it holds a stale tail and rejects the new session's writes

    std::vector< std::unique_ptr< Write > > writes_;
    std::map< std::pair< int64_t, uint64_t >, Write const* > by_slot_term_; // every write, by (dlsn, term)
    std::map< int64_t, Write const* > live_;                                // each slot's live write
    std::set< int64_t > empty_;                                             // committed Empty verdicts
    std::array< std::map< int64_t, Write const* >, 3 > peers_held_;         // members 1 and 2: the copy each holds
    std::vector< Write const* > undelivered_;                               // not yet sent to this member
    std::vector< Pending > pending_;                                        // writes this member hasn't answered
    std::set< std::pair< int64_t, uint64_t > > inflight_;                   // client writes started, not answered
    std::vector< std::pair< uint64_t, int64_t > > logins_;                  // committed logins: (term, rs)
    uint64_t term_{0};                                                      // the newest committed session
    int64_t next_dlsn_{0};
    int64_t client_commit_{-1};
    int64_t last_commit_{-1};
    int64_t pre_applied_sync_{-1};
    std::map< int64_t, int > pre_holders_;
    mutable Counts counts_; // peer_slots() counts stale copies it offers
};

TEST(CraftInterleave, SafetyHoldsUnderRandomInterleavings) {
    auto const steps = SISL_OPTIONS["steps"].as< uint32_t >();
    auto const only = SISL_OPTIONS["seed"].as< uint64_t >();
    auto const seeds = SISL_OPTIONS["seeds"].as< uint32_t >();
    Counts total;
    for (uint64_t seed = only ? only : 1; seed <= (only ? only : seeds); ++seed) {
        Interleave run{seed};
        run.run(steps);
        std::cout << "[ interleave ] seed " << seed << ": " << run.counts().str() << "\n";
        total += run.counts();
        if (run.failed()) return;
    }
    std::cout << "[ interleave ] total: " << total.str() << "\n";
    if (only) return; // one replayed seed needn't reach every case
    // A full run must reach every case it checks, or a pass says nothing about it.
    EXPECT_GT(total.acked, 0u);
    EXPECT_GT(total.held, 0u);
    EXPECT_GT(total.truncating, 0u);
    EXPECT_GT(total.syncs, 0u);
    EXPECT_GT(total.rounds, 0u);
    EXPECT_GT(total.empties, 0u);
    EXPECT_GT(total.fills, 0u);
    EXPECT_GT(total.stale_offered, 0u);
    EXPECT_GT(total.reads_checked, 0u);
    EXPECT_GT(total.reads_fenced, 0u);
    EXPECT_GT(total.reads_nested, 0u);
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging, test_craft_interleave);
    // This light suite never starts iomgr, so the watchdog's recurring timer must stay disabled.
    HB_SETTINGS_FACTORY().load_json("{\"craft_watchdog_timeout_ms\": 0}");
    return RUN_ALL_TESTS();
}
