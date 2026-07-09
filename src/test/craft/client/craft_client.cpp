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
#include "craft_client.hpp"

#include <utility>
#include <vector>

#include <sisl/async/when_all.hpp>
#include <sisl/fds/buffer.hpp> // sisl::sg_iterator

namespace homeblocks::craft {

craft_client::craft_client(std::vector< volume_handle > replicas, uint32_t leader, uint32_t max_inflight) :
        replicas_(std::move(replicas)), leader_(leader), tracker_(max_inflight) {}

client_hdr craft_client::make_hdr() const {
    // Every IO piggybacks the commit frontier: CRAFT has no standalone commit verb.
    return client_hdr{term_, tracker_.frontier(), k_all_committed_unknown};
}

// Fail fast rather than burn a dLSN on an IO the replicas will reject anyway. They enforce alignment too.
std::optional< std::error_condition > craft_client::precheck(uint64_t addr, uint64_t len) const {
    if (term_ == 0) return make_error_condition(std::errc::not_connected);
    bool const aligned = (lba_size_ != 0) && (len != 0) && ((addr % lba_size_) == 0) && ((len % lba_size_) == 0);
    if (!aligned) return make_error_condition(std::errc::invalid_argument);
    return std::nullopt;
}

async_status craft_client::login(uint64_t client_token) {
    std::size_t const n = replicas_.size();
    std::error_condition last_err{};

    // A follower does not fail the call: it returns term == 0 plus leader_hint. We have no id -> handle map
    // (LoginResult::members is empty on a redirect), so walk the handles. A hop that errors is skipped, not
    // fatal -- a down replica must not block login.
    for (std::size_t hop = 0; hop < n; ++hop) {
        auto const target = static_cast< uint32_t >((leader_ + hop) % n);
        auto lr = co_await homeblocks::login(replicas_[target], client_token);
        if (!lr.has_value()) {
            last_err = lr.error();
            continue;
        }
        if (lr->term == 0) continue; // NOT_LEADER redirect

        leader_ = target;
        term_ = lr->term;
        lba_size_ = lr->lba_size;
        tracker_.reset_at(lr->dLSN, lr->lba_size);
        co_return ok();
    }
    co_return std::unexpected(last_err ? last_err : make_error_condition(craft_error::NOT_LEADER));
}

async_result< size_t > craft_client::write(uint64_t addr, uint64_t len, sisl::sg_list data) {
    if (auto const e = precheck(addr, len)) co_return std::unexpected(*e);

    // Reserve the dLSN and record its range BEFORE the broadcast: any replica that can hold this slot
    // implies a concurrent read's scan can already see it, which is what makes the horizon safe.
    int64_t const dlsn = tracker_.reserve(addr, len);
    client_hdr const hdr = make_hdr();

    std::size_t acks = 0;
    std::size_t refused = 0; // deterministic rejections: that replica provably did not journal it
    std::error_condition last_err{};
    auto const tally = [&](auto const& r) {
        if (r.has_value()) {
            ++acks;
        } else {
            last_err = r.error();
            if (r.error() == std::make_error_condition(std::errc::invalid_argument)) ++refused;
        }
    };

    if (replicas_.size() == 1) {
        tally(co_await homeblocks::async_write(replicas_[0], hdr, dlsn, addr, len, std::move(data)));
    } else {
        // Each task gets its own sg_list descriptor copy; all point at the same caller-owned source buffer,
        // which the caller keeps alive across the co_await.
        std::vector< async_status > futs;
        futs.reserve(replicas_.size());
        for (auto& h : replicas_) {
            futs.push_back(homeblocks::async_write(h, hdr, dlsn, addr, len, data));
        }
        for (auto const& r : co_await sisl::async::when_all(std::move(futs))) {
            tally(r);
        }
    }

    if (acks < quorum()) {
        // If EVERY replica refused deterministically, the slot is provably absent set-wide: a resolved
        // no-op that must not pin the commit frontier. Otherwise some replica may hold it, so the slot
        // stays unresolved until the leader fills or Empties it, and commit correctly stalls there.
        tracker_.resolve(dlsn, (refused == replicas_.size()) ? slot_outcome::empty : slot_outcome::failed);
        co_return std::unexpected(last_err ? last_err : make_error_condition(craft_error::NO_QUORUM));
    }
    tracker_.resolve(dlsn, slot_outcome::acked);
    co_return len;
}

async_result< size_t > craft_client::read(uint64_t addr, uint64_t len, sisl::sg_list dest) {
    if (auto const e = precheck(addr, len)) co_return std::unexpected(*e);

    auto const plan = co_await tracker_.plan_read(addr, len);
    if (!plan.has_value()) co_return std::unexpected(plan.error());

    client_hdr const hdr = make_hdr();
    // (N: route to an eligible holder using the per-replica Missing map -- future work.)
    auto& target = replicas_[leader_];

    // `dest` is filled in place (data extents + zero-filled holes), so the caller needs only the byte count.
    if (plan->size() == 1) {
        auto layout = co_await homeblocks::async_read(target, hdr, plan->front().H, addr, len, std::move(dest));
        if (!layout.has_value()) co_return std::unexpected(layout.error());
        co_return len;
    }

    // Split read: sg_iterator walks `dest` once, in order, carving one descriptor per segment. Each is
    // passed by value into the callee's coroutine frame, so no descriptor of ours outlives this loop.
    sisl::sg_iterator slicer{dest.iovs};
    std::vector< async_result< std::vector< io_extent > > > futs;
    futs.reserve(plan->size());
    for (auto const& seg : *plan) {
        sisl::sg_list sub;
        sub.size = seg.len;
        sub.iovs = slicer.next_iovs(static_cast< uint32_t >(seg.len));
        futs.push_back(homeblocks::async_read(target, hdr, seg.H, seg.addr, seg.len, std::move(sub)));
    }
    for (auto const& r : co_await sisl::async::when_all(std::move(futs))) {
        if (!r.has_value()) co_return std::unexpected(r.error());
    }
    co_return len;
}

async_status craft_client::flush() {
    auto r = co_await homeblocks::keep_alive(replicas_[leader_], make_hdr());
    if (!r.has_value()) co_return std::unexpected(r.error());
    co_return ok();
}

} // namespace homeblocks::craft
