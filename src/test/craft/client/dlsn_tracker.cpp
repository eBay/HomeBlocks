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
#include "dlsn_tracker.hpp"

#include <algorithm>

#include <boost/container/small_vector.hpp>

namespace homeblocks::craft {

namespace {

// Compile-time policy for the tracker. Reaching any of these caps means a write has hung or the client is
// issuing far more overlapping in-flight writes than a healthy one does.
constexpr uint64_t k_max_blocks = 4096;    // 512 KiB (ublk's per-tag buffer) even at 128B blocks
constexpr size_t k_max_segments = 8;       // most sub-reads we will fan out for one IO
constexpr int64_t k_trunc_batch = 512;     // resolves per exclusive-locked StreamTracker::truncate
constexpr int64_t k_scan_cap_floor = 1024; // Ha - umin past this means a write has hung
constexpr int64_t k_scan_cap_per_inflight = 8;

constexpr bool overlaps(uint64_t a_off, uint64_t a_len, uint64_t b_off, uint64_t b_len) {
    return (a_off < b_off + b_len) && (b_off < a_off + a_len);
}

// `addr`/`len` are written once, before the slot's active bit is set, and AtomicBitset publishes that bit
// with release and reads it with acquire -- so once foreach_all_active hands us the slot, a plain read of
// its range is ordered. `outcome` is mutated after publication, so it must go through atomic_ref.
struct slot_view {
    uint64_t addr{0};
    uint64_t len{0};
    slot_outcome outcome{slot_outcome::in_flight};
};

slot_view read_slot(dlsn_slot& s) {
    return slot_view{
        s.addr, s.len,
        static_cast< slot_outcome >(std::atomic_ref< uint8_t >(s.outcome).load(std::memory_order_relaxed))};
}

} // namespace

dlsn_tracker::dlsn_tracker(uint32_t max_inflight) :
        scan_cap_{std::max(k_scan_cap_floor, k_scan_cap_per_inflight * static_cast< int64_t >(max_inflight))} {}

void dlsn_tracker::reset_at(int64_t login_dlsn, uint32_t lba_size) {
    lba_size_ = lba_size;
    next_dlsn_.store(login_dlsn + 1, std::memory_order_relaxed);
    last_trunc_.store(login_dlsn, std::memory_order_relaxed);
    frontier_.store(login_dlsn, std::memory_order_release);
    highest_acked_.store(login_dlsn, std::memory_order_release);
    // StreamTracker(name, start_idx) bases its window at start_idx + 1, i.e. the first dLSN we will issue.
    tracker_.emplace("craft_dlsn", login_dlsn);
}

int64_t dlsn_tracker::reserve(uint64_t addr, uint64_t len) {
    int64_t const d = next_dlsn_.fetch_add(1, std::memory_order_relaxed);
    tracker_->create(d, dlsn_slot{addr, len, static_cast< uint8_t >(slot_outcome::in_flight)});
    return d;
}

void dlsn_tracker::resolve(int64_t dlsn, slot_outcome oc) {
    bool const resolved = is_resolved(oc);

    // On `true`, update() sets the completion bit -- a release store, which publishes this outcome to any
    // concurrent scanner that acquires that bit.
    tracker_->update(dlsn, [oc, resolved](dlsn_slot& s) {
        std::atomic_ref< uint8_t >(s.outcome).store(static_cast< uint8_t >(oc), std::memory_order_relaxed);
        return resolved;
    });

    if (oc == slot_outcome::acked) {
        // A CAS (not a store), so every publication of Ha joins one release sequence: a reader that
        // acquire-loads ANY later Ha also sees this slot's outcome. That is what lets the winner pass trust
        // `acked` for every slot <= Ha. (sisl::atomic_update_max cannot express this: it funnels one
        // memory_order into both the load and the CAS, and its acq_rel default is invalid for a load.)
        int64_t cur = highest_acked_.load(std::memory_order_relaxed);
        while (dlsn > cur &&
               !highest_acked_.compare_exchange_weak(cur, dlsn, std::memory_order_release, std::memory_order_relaxed)) {
        }
    }
    if (resolved) advance_frontier();

    gate_.signal();
}

void dlsn_tracker::advance_frontier() {
    int64_t f = frontier_.load(std::memory_order_relaxed);
    for (;;) {
        int64_t const issued = next_dlsn_.load(std::memory_order_relaxed) - 1;
        // completed_upto(hint) is the last index of the contiguous completed run at or above `hint`; the
        // hint keeps it O(1) instead of rescanning from the window base. Clamp against `issued` because
        // _upto() reports the last ALLOCATED slot when every bit it scanned was set.
        int64_t const nf = std::min(tracker_->completed_upto(f + 1), issued);
        if (nf <= f) return;
        if (frontier_.compare_exchange_weak(f, nf, std::memory_order_release, std::memory_order_relaxed)) {
            // Truncate only ever to a PUBLISHED frontier, so the window base never rises above F+1 --
            // exactly where scanners start, and every slot it skips is resolved.
            int64_t lt = last_trunc_.load(std::memory_order_relaxed);
            if ((nf - lt >= k_trunc_batch) &&
                last_trunc_.compare_exchange_strong(lt, nf, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                tracker_->truncate(nf);
            }
            return;
        }
    }
}

dlsn_tracker::plan_outcome dlsn_tracker::compute(uint64_t addr, uint64_t len) {
    int64_t const F = frontier_.load(std::memory_order_acquire);
    int64_t const Ha = highest_acked_.load(std::memory_order_acquire);

    // Every slot <= F is resolved, so once F catches up to Ha nothing unresolved is at or below the
    // horizon. Ha can also trail F (a slot resolved Empty without ever acking), hence max().
    if (F >= Ha) return one_segment(addr, len, std::max(F, Ha));

    // completed_upto(h) is the last index of the completed run at or above h, so its successor is the first
    // NON-completed index >= h. Chaining it visits exactly the unresolved slots -- a count bounded by
    // out-of-order write depth -- so one straggling write never makes every read pay for the gap it opens.
    int64_t umin = -1;
    bool any_failed = false;
    bool any_gap = false;
    for (int64_t h = F + 1; h <= Ha;) {
        int64_t const u = tracker_->completed_upto(h) + 1;
        if (u > Ha) break;
        if (u < h) break; // unreachable (the scan starts at h); guards against a hang if that ever changes

        slot_view sv;
        bool active = false;
        tracker_->foreach_all_active(u, [&](int64_t idx, dlsn_slot& s) {
            if (idx == u) {
                active = true;
                sv = read_slot(s);
            }
            return false; // we only want the first active index at or above u
        });

        // An inactive index in (F, Ha] was issued (dLSNs are dense) but its create() is not visible yet.
        // Its range is unknown, so it fences conservatively -- and it resolves within nanoseconds.
        bool const hits = !active || overlaps(addr, len, sv.addr, sv.len);
        if (hits) {
            if (umin < 0) umin = u;
            if (!active) any_gap = true;
            if (sv.outcome == slot_outcome::failed) any_failed = true;
        }
        h = u + 1;
    }

    if (umin < 0) return one_segment(addr, len, Ha); // nothing unresolved overlaps

    // A gap has no range to plan against, and a window this wide means a write has hung.
    if (any_gap || (Ha - umin > scan_cap_)) return fenced(any_failed);

    return split(addr, len, Ha, umin, any_failed);
}

// A replica serves the highest dLSN <= H per LBA, so walking the overlapping slots in ASCENDING dLSN order
// answers each block directly. What survives in `ustar[b]` is the lowest unresolved slot covering b that
// sits above every acked one -- exactly the slot that block's horizon must duck under. Blocks with no entry
// are safe at the full horizon.
dlsn_tracker::plan_outcome dlsn_tracker::split(uint64_t addr, uint64_t len, int64_t Ha, int64_t umin, bool any_failed) {
    winner_scans_.fetch_add(1, std::memory_order_relaxed);

    uint64_t const nblk = (lba_size_ != 0) ? (len / lba_size_) : 0;
    if ((nblk == 0) || (nblk > k_max_blocks)) return fenced(any_failed);

    boost::container::small_vector< int64_t, 512 > ustar(nblk, -1);
    tracker_->foreach_all_active(umin, [&](int64_t idx, dlsn_slot& s) {
        if (idx > Ha) return false;
        auto const sv = read_slot(s);
        // An Empty slot is a resolved no-op: it writes nothing, so it neither fences nor supersedes.
        if (sv.outcome == slot_outcome::empty) return true;
        if (!overlaps(addr, len, sv.addr, sv.len)) return true;

        uint64_t const lo = std::max(addr, sv.addr);
        uint64_t const hi = std::min(addr + len, sv.addr + sv.len);
        uint64_t const b0 = (lo - addr) / lba_size_;
        uint64_t const b1 = std::min(nblk - 1, (hi - 1 - addr) / lba_size_);

        bool const acked = (sv.outcome == slot_outcome::acked);
        for (uint64_t b = b0; b <= b1; ++b) {
            if (acked) {
                ustar[b] = -1; // durable: supersedes every unresolved slot beneath it on this block
            } else if (ustar[b] < 0) {
                ustar[b] = idx; // first unresolved slot above the last durable one
            }
        }
        return true;
    });

    auto const horizon_of = [&](uint64_t b) { return (ustar[b] < 0) ? Ha : ustar[b] - 1; };
    plan_outcome plan;
    uint64_t seg_start = 0;
    int64_t cur = horizon_of(0);
    for (uint64_t b = 1; b <= nblk; ++b) {
        if ((b < nblk) && (horizon_of(b) == cur)) continue;
        if (plan.segs.size() == k_max_segments) return fenced(any_failed);
        plan.segs.push_back(read_segment{addr + (seg_start * lba_size_), (b - seg_start) * lba_size_, cur});
        seg_start = b;
        if (b < nblk) cur = horizon_of(b);
    }
    return plan;
}

async_result< read_plan > dlsn_tracker::plan_read(uint64_t addr, uint64_t len) {
    auto p = compute(addr, len);
    while (p.stall()) {
        // Capture the gate's current event BEFORE re-testing: a resolve landing in between completes this
        // very event, so the wakeup cannot be lost.
        auto ev = gate_.arm();
        gate_.enter();
        p = compute(addr, len);
        if (!p.stall()) {
            gate_.leave();
            break;
        }
        co_await *ev;
        gate_.leave();
    }
    if (p.degraded()) co_return std::unexpected(make_error_condition(craft_error::NO_QUORUM));
    co_return std::move(p.segs);
}

} // namespace homeblocks::craft
