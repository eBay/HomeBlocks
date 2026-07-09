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

// dlsn_tracker: the client-side dLSN bookkeeping for one CRAFT partition -- the dLSN counter, the
// contiguous commit frontier F (what client_hdr.commit_lsn piggybacks), and the per-read horizon H.
//
// THE INVARIANT everything rests on: the spine is a sisl::StreamTracker keyed by dLSN, and its "completed"
// bit means RESOLVED = {acked, empty}, NOT "acked". So every slot <= F is resolved, "is anything unresolved
// at or below H?" collapses to "F < H", and a failed write pins F -- which is exactly the commit_lsn CRAFT
// wants, since commit must not pass a slot the leader has not yet filled or Emptied.
//
// See README.md for the per-block horizon derivation, the split read, and the memory-ordering rationale.

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <variant>

#include <boost/container/small_vector.hpp>
#include <sisl/async/shared_awaitable.hpp>
#include <sisl/fds/stream_tracker.hpp>

#include <homeblks/home_blocks.hpp> // async_result / result / craft_error / ok()

namespace homeblocks::craft {

// `acked` (durable, shadows lower dLSNs) and `empty` (provably absent set-wide; writes nothing, shadows
// nothing) are RESOLVED and set the completion bit. `failed` (sub-quorum; some replica MAY hold it) and
// `in_flight` are not, and pin F.
enum class slot_outcome : uint8_t { in_flight = 0, acked = 1, failed = 2, empty = 3 };

inline constexpr bool is_resolved(slot_outcome o) { return (o == slot_outcome::acked) || (o == slot_outcome::empty); }

// Trivially copyable, as StreamTracker requires (it callocs the slot array and memmoves it on compaction).
// `outcome` is mutated after publication so every access goes through atomic_ref; see read_slot().
struct dlsn_slot {
    uint64_t addr{0};
    uint64_t len{0};
    uint8_t outcome{static_cast< uint8_t >(slot_outcome::in_flight)};
};
static_assert(std::is_trivially_copyable_v< dlsn_slot >);

// A re-armable, multi-waiter event: shared_awaitable is single-shot, so each signal swaps in a fresh one.
// Waiters capture the current event BEFORE re-testing the condition, so a signal landing in between
// completes the event they are about to await -- no lost wakeup.
class resolution_gate {
public:
    using event_t = sisl::async::shared_awaitable< std::monostate >;

    resolution_gate() : cur_{std::make_shared< event_t >()} {}
    resolution_gate(resolution_gate const&) = delete;
    resolution_gate& operator=(resolution_gate const&) = delete;

    std::shared_ptr< event_t > arm() const { return cur_.load(std::memory_order_acquire); }

    // enter()/signal() form a Dekker interlock: each stores its flag then reads the other's state, so the
    // seq_cst fence between them is what lets signal() skip its allocation when nobody is waiting without
    // ever losing a wakeup. The fence must sit between the counter bump and the caller's read of state.
    void enter() {
        waiters_.fetch_add(1, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }
    void leave() { waiters_.fetch_sub(1, std::memory_order_relaxed); }

    // Called after every resolve, once the new state is published. Resumes waiters inline.
    void signal() {
        std::atomic_thread_fence(std::memory_order_seq_cst);
        if (waiters_.load(std::memory_order_relaxed) == 0) return; // nobody stalled: no allocation, no lock
        cur_.exchange(std::make_shared< event_t >(), std::memory_order_acq_rel)->complete({});
    }

private:
    mutable std::atomic< std::shared_ptr< event_t > > cur_;
    std::atomic< uint32_t > waiters_{0};
};

// One sub-range of a read, with the horizon it may carry. The safe horizon is per-LBA, so a range whose
// blocks disagree partitions into segments issued in parallel (CRAFT-Design.md, "Split reads" / "Case (b)").
struct read_segment {
    uint64_t addr{0};
    uint64_t len{0};
    int64_t H{-1};
};
using read_plan = boost::container::small_vector< read_segment, 4 >;

class dlsn_tracker {
public:
    // `max_inflight` is an opaque IO-concurrency bound; it only sizes the winner-scan tripwire. Nothing
    // here assumes a queue count, and dLSN assignment is a plain fetch_add from any thread.
    explicit dlsn_tracker(uint32_t max_inflight);

    // (Re-)establish a session. `login_dlsn` is the set's rs_commit_lsn: everything <= it is durable, so it
    // seeds BOTH watermarks -- seeding only next_dlsn leaves H at -1 and a recovery login reads zeros.
    void reset_at(int64_t login_dlsn, uint32_t lba_size);

    // Assign a dLSN and record its range. MUST complete before the write is broadcast, so that any replica
    // able to hold this slot implies a scanner can already see it.
    int64_t reserve(uint64_t addr, uint64_t len);

    void resolve(int64_t dlsn, slot_outcome oc);

    int64_t frontier() const { return frontier_.load(std::memory_order_acquire); }
    int64_t read_horizon() const { return std::max(frontier(), highest_acked_.load(std::memory_order_acquire)); }

    // How to read [addr, addr+len) safely. Only the pathological branches suspend on the gate, and only a
    // *failed* slot in one of those fails -- nothing resolves it without a leader resolution round.
    async_result< read_plan > plan_read(uint64_t addr, uint64_t len);

    // Test hook: how many reads paid for the per-block winner pass.
    uint64_t winner_scans() const { return winner_scans_.load(std::memory_order_relaxed); }

private:
    // An empty plan means the range is fenced: by an in_flight slot (retry after the next resolve) or by a
    // failed one (nothing will resolve it, so the read fails).
    struct plan_outcome {
        read_plan segs{};
        bool failed{false};
        bool stall() const { return segs.empty() && !failed; }
        bool degraded() const { return segs.empty() && failed; }
    };

    static plan_outcome fenced(bool any_failed) { return plan_outcome{{}, any_failed}; }
    static plan_outcome one_segment(uint64_t addr, uint64_t len, int64_t H) {
        plan_outcome o;
        o.segs.push_back(read_segment{addr, len, H});
        return o;
    }

    plan_outcome compute(uint64_t addr, uint64_t len);
    // Ascending per-block winner pass over [umin, Ha]. Only runs when an unresolved slot overlaps.
    plan_outcome split(uint64_t addr, uint64_t len, int64_t Ha, int64_t umin, bool any_failed);
    void advance_frontier();

    std::atomic< int64_t > next_dlsn_{0};
    std::atomic< int64_t > frontier_{-1};      // F: contiguous RESOLVED prefix == commit_lsn
    std::atomic< int64_t > highest_acked_{-1}; // Ha: max quorum-acked dLSN
    std::atomic< int64_t > last_trunc_{-1};
    std::atomic< uint64_t > winner_scans_{0};

    std::optional< sisl::StreamTracker< dlsn_slot > > tracker_; // constructed at login
    resolution_gate gate_;
    uint32_t lba_size_{0};
    int64_t scan_cap_{0};
};

} // namespace homeblocks::craft
