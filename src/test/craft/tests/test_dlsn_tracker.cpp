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

// Unit test for dlsn_tracker in isolation -- no replicas, no model, no HomeStore. It drives
// reserve/resolve directly so every branch of the horizon rule is reachable deterministically. The last
// test drives readers and writers concurrently and is meant to run under TSAN. test_craft_client covers
// the same rules end to end against the reference model.

#include <chrono>
#include <cstdint>
#include <random>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "coro_helpers.hpp"
#include "client/dlsn_tracker.hpp"

using namespace homeblocks;
using namespace homeblocks::craft;

namespace {
constexpr uint32_t BS = 512; // block size

constexpr uint64_t blk(uint64_t n) { return n * uint64_t{BS}; }

// Reserve a write covering blocks [first, first + count).
int64_t issue(dlsn_tracker& t, uint64_t first, uint64_t count = 1) { return t.reserve(blk(first), blk(count)); }

// How the tracker says a read of blocks [first, first + count) must be issued.
result< read_plan > plan(dlsn_tracker& t, uint64_t first, uint64_t count = 1) {
    return detail::sync_get(t.plan_read(blk(first), blk(count)));
}

// Every plan must partition the requested range exactly: contiguous, in order, no gaps or overlaps.
void expect_covers(read_plan const& p, uint64_t first, uint64_t count) {
    ASSERT_FALSE(p.empty());
    uint64_t off = blk(first);
    for (auto const& seg : p) {
        EXPECT_EQ(seg.addr, off);
        EXPECT_GT(seg.len, 0u);
        off += seg.len;
    }
    EXPECT_EQ(off, blk(first + count));
}

// The horizon of a read that needs no split. Fails the test if the range partitions.
int64_t horizon(dlsn_tracker& t, uint64_t first, uint64_t count = 1) {
    auto p = plan(t, first, count);
    EXPECT_TRUE(p.has_value());
    if (!p.has_value() || (p->size() != 1)) {
        ADD_FAILURE() << "expected a single segment";
        return -99;
    }
    return p->front().H;
}

// Every test but the recovery-login one starts a fresh session at dLSN -1.
class DlsnTracker : public ::testing::Test {
protected:
    dlsn_tracker t{128};
    void SetUp() override { t.reset_at(-1, BS); }
};
} // namespace

// --- the contiguous frontier ---

TEST_F(DlsnTracker, OutOfOrderResolvesFoldIntoContiguousPrefix) {
    EXPECT_EQ(t.frontier(), -1);

    auto const d0 = issue(t, 0);
    auto const d1 = issue(t, 1);
    auto const d2 = issue(t, 2);
    auto const d3 = issue(t, 3);
    EXPECT_EQ(d0, 0);
    EXPECT_EQ(d3, 3);

    t.resolve(d2, slot_outcome::acked);
    EXPECT_EQ(t.frontier(), -1) << "a gap at 0,1 must hold the frontier back";
    t.resolve(d1, slot_outcome::acked);
    EXPECT_EQ(t.frontier(), -1);
    t.resolve(d0, slot_outcome::acked);
    EXPECT_EQ(t.frontier(), 2) << "filling the gap folds the whole contiguous run in at once";
    t.resolve(d3, slot_outcome::acked);
    EXPECT_EQ(t.frontier(), 3);
    EXPECT_EQ(t.read_horizon(), 3);
}

TEST_F(DlsnTracker, FailedWritePinsTheFrontier) {

    auto const d0 = issue(t, 0);
    auto const d1 = issue(t, 1);
    t.resolve(d0, slot_outcome::failed);
    t.resolve(d1, slot_outcome::acked);

    EXPECT_EQ(t.frontier(), -1) << "a failed slot is unresolved; commit must not pass it";
    EXPECT_EQ(t.read_horizon(), 1);
}

TEST_F(DlsnTracker, EmptyResolvesAndReleasesTheFrontier) {

    auto const d0 = issue(t, 0);
    auto const d1 = issue(t, 1);
    t.resolve(d0, slot_outcome::empty); // provably absent set-wide: a resolved no-op
    t.resolve(d1, slot_outcome::acked);
    EXPECT_EQ(t.frontier(), 1);
}

TEST_F(DlsnTracker, RecoveryLoginSeedsBothWatermarks) {
    t.reset_at(1000, BS); // rs_commit_lsn from a recovery login
    EXPECT_EQ(t.frontier(), 1000);
    EXPECT_EQ(t.read_horizon(), 1000) << "reads must see the durable prefix, not -1 (zeros)";
    EXPECT_EQ(issue(t, 0), 1001);
}

// --- the read horizon ---

TEST_F(DlsnTracker, FastPathSkipsTheScanEntirely) {

    t.resolve(issue(t, 0), slot_outcome::acked);
    t.resolve(issue(t, 1), slot_outcome::acked);

    EXPECT_EQ(horizon(t, 0), 1);
    EXPECT_EQ(t.winner_scans(), 0u) << "F == Ha: two atomic loads, no tracker walk";
}

TEST_F(DlsnTracker, NonOverlappingUnresolvedWriteDoesNotFenceOrScan) {

    issue(t, 0);                                 // dLSN 0 left in flight, block 0
    t.resolve(issue(t, 5), slot_outcome::acked); // dLSN 1, block 5

    // A read of block 5 is fenced by nothing: the in-flight slot covers a different range. The winner
    // pass must not run -- this is what keeps one straggling write from taxing every unrelated read.
    EXPECT_EQ(horizon(t, 5), 1);
    EXPECT_EQ(t.winner_scans(), 0u);
}

TEST_F(DlsnTracker, ClampsBelowAnUnresolvedOverlap) {

    t.resolve(issue(t, 0), slot_outcome::acked);  // dLSN 0: the durable value at block 0
    t.resolve(issue(t, 0), slot_outcome::failed); // dLSN 1: sub-quorum, some replica may hold it
    t.resolve(issue(t, 9), slot_outcome::acked);  // dLSN 2: pushes Ha above the hole

    // Reading at Ha=2 would let a replica that journaled dLSN 1 serve it, and dLSN 1 may still be
    // Empty'd -- a read-stability violation. Clamp below it and serve the last durable version.
    EXPECT_EQ(horizon(t, 0), 0);
    EXPECT_EQ(t.winner_scans(), 1u);
    EXPECT_EQ(t.frontier(), 0) << "commit stays pinned under the unresolved slot";
}

TEST_F(DlsnTracker, AckedWriteFullyShadowsAnUnresolvedSlot) {

    t.resolve(issue(t, 0), slot_outcome::acked);  // dLSN 0
    t.resolve(issue(t, 0), slot_outcome::failed); // dLSN 1: failed at block 0
    t.resolve(issue(t, 0), slot_outcome::acked);  // dLSN 2: overwrites block 0 and acks

    // The replica serves the highest dLSN <= H per block, so dLSN 2 hides dLSN 1 completely. No fence.
    EXPECT_EQ(horizon(t, 0), 2) << "a fully shadowed unresolved slot can never be the version a read sees";
    EXPECT_EQ(t.frontier(), 0);
}

TEST_F(DlsnTracker, EmptySlotShadowsNothing) {

    issue(t, 0);                                 // dLSN 0: in flight at block 0
    t.resolve(issue(t, 0), slot_outcome::empty); // dLSN 1: resolved, but writes nothing
    t.resolve(issue(t, 9), slot_outcome::acked); // dLSN 2: Ha = 2

    // dLSN 1 is resolved, so it does not fence -- but it carries no data either, so it cannot shadow
    // dLSN 0. The in-flight slot still owns block 0 and the horizon clamps below it.
    EXPECT_EQ(horizon(t, 0), -1);
}

TEST_F(DlsnTracker, SplitsWhenBlocksInOneRangeNeedDifferentHorizons) {

    t.resolve(issue(t, 0, 2), slot_outcome::failed); // dLSN 0: blocks 0-1, sub-quorum
    t.resolve(issue(t, 0), slot_outcome::acked);     // dLSN 1: block 0 only

    // Block 0's newest version is the acked dLSN 1, which supersedes dLSN 0 there: safe at the horizon.
    // Block 1's newest version IS the unresolved dLSN 0, so it must duck under it. One read carries one
    // horizon, so the range partitions -- and neither answer requires waiting for dLSN 0 to resolve.
    auto const p = plan(t, 0, 2);
    ASSERT_TRUE(p.has_value());
    ASSERT_EQ(p->size(), 2u);
    expect_covers(*p, 0, 2);
    EXPECT_EQ((*p)[0].addr, blk(0));
    EXPECT_EQ((*p)[0].len, blk(1));
    EXPECT_EQ((*p)[0].H, 1) << "block 0: the acked write is visible";
    EXPECT_EQ((*p)[1].addr, blk(1));
    EXPECT_EQ((*p)[1].len, blk(1));
    EXPECT_EQ((*p)[1].H, -1) << "block 1: clamp below the unresolved write";
}

TEST_F(DlsnTracker, InFlightAndFailedSlotsFenceIdentically) {
    // The same geometry as above, but dLSN 0 is still in flight rather than failed. Because a per-block
    // horizon always exists, neither case waits and neither errors -- the two outcomes are interchangeable
    // for read planning, which is why nothing here needs a resolution round to make progress.
    issue(t, 0, 2);                              // dLSN 0: blocks 0-1, still in flight
    t.resolve(issue(t, 0), slot_outcome::acked); // dLSN 1: block 0 only

    auto const p = plan(t, 0, 2);
    ASSERT_TRUE(p.has_value());
    ASSERT_EQ(p->size(), 2u);
    expect_covers(*p, 0, 2);
    EXPECT_EQ((*p)[0].H, 1);
    EXPECT_EQ((*p)[1].H, -1);
}

TEST_F(DlsnTracker, StallsOnlyWhenTheRangeWouldFanOutTooFar) {

    // Alternate single-block in-flight writes across a 32-block range. Every other block needs its own
    // clamped horizon, so the plan would exceed the fan-out cap. That is the one case left that waits.
    std::vector< int64_t > holes;
    for (uint64_t b = 0; b < 20; b += 2) {
        holes.push_back(issue(t, b));
    }
    auto const tail = issue(t, 31); // acked, so Ha rises above the in-flight slots
    t.resolve(tail, slot_outcome::acked);

    std::atomic< bool > resolved{false};
    std::thread resolver{[&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        resolved.store(true, std::memory_order_release);
        for (auto d : holes) {
            t.resolve(d, slot_outcome::acked);
        }
    }};

    auto const p = plan(t, 0, 32); // blocks until the fan-out drops under the cap
    EXPECT_TRUE(resolved.load(std::memory_order_acquire)) << "the plan returned before any write resolved";
    resolver.join();

    // It waits only while the range would fan out past the cap -- not until everything resolves. As soon as
    // enough writes land that the plan fits, it proceeds with whatever split is left.
    ASSERT_TRUE(p.has_value());
    EXPECT_LE(p->size(), 8u) << "the fan-out cap is what it waited for";
    expect_covers(*p, 0, 32);

    // With every write resolved, the same range collapses back to a single segment at the full horizon.
    auto const q = plan(t, 0, 32);
    ASSERT_TRUE(q.has_value());
    ASSERT_EQ(q->size(), 1u);
    EXPECT_EQ(q->front().H, t.read_horizon());
    EXPECT_EQ(t.frontier(), t.read_horizon());
}

// --- the straggler: a pinned frontier must not tax unrelated reads ---

TEST_F(DlsnTracker, PinnedFrontierDoesNotMakeUnrelatedReadsScan) {

    issue(t, 0); // dLSN 0 straggles at block 0, pinning F at -1 forever

    // Thousands of unrelated writes come and go. Ha - F grows without bound, which is exactly the case
    // a window walk would choke on: the span is issue_rate x straggler_latency, not queue depth.
    for (uint64_t i = 0; i < 4000; ++i) {
        t.resolve(issue(t, 100 + (i % 50)), slot_outcome::acked);
    }
    EXPECT_EQ(t.frontier(), -1);
    EXPECT_EQ(t.read_horizon(), 4000);

    EXPECT_EQ(horizon(t, 120), 4000);
    EXPECT_EQ(t.winner_scans(), 0u) << "enumerating unresolved slots is bounded by in-flight depth";
}

// --- concurrency: readers scanning slots that writers are concurrently creating ---

// RUN THIS UNDER TSAN: it is the test sisl's AtomicBitset release/acquire ordering exists for (see
// README.md "Concurrency notes"). Readers scan slots that writers are concurrently creating, so the slot
// bit is the only thing publishing the payload; with relaxed bits this reports ~12 races.
//
// It also exercises what the single-threaded tests cannot: the frontier CAS under contention, truncation
// racing live scans, the reserve/create gap that makes a reader fence conservatively, and the
// resolution_gate's Dekker interlock -- a reader that decides to stall just as the last writer resolves
// must not sleep forever, which would hang this test.
TEST_F(DlsnTracker, ConcurrentPlanAndResolveIsRaceFree) {
    constexpr int kWriters = 4;
    constexpr int kReaders = 4;
    constexpr int kOpsPerWriter = 2000;
    constexpr uint64_t kSpace = 64; // blocks; small, so reads and writes overlap constantly

    std::atomic< int > writers_live{kWriters};

    std::vector< std::thread > threads;
    for (int w = 0; w < kWriters; ++w) {
        threads.emplace_back([&, w] {
            std::mt19937 rng(0x51ED + w);
            for (int i = 0; i < kOpsPerWriter; ++i) {
                uint64_t const first = rng() % (kSpace - 4);
                uint64_t const count = 1 + (rng() % 4);
                int64_t const d = t.reserve(blk(first), blk(count));
                if ((rng() % 8) == 0) std::this_thread::yield(); // widen the in-flight window
                t.resolve(d, ((rng() % 32) == 0) ? slot_outcome::empty : slot_outcome::acked);
            }
            writers_live.fetch_sub(1, std::memory_order_release);
        });
    }

    for (int r = 0; r < kReaders; ++r) {
        threads.emplace_back([&, r] {
            std::mt19937 rng(0xBEEF ^ (r * 7919));
            while (writers_live.load(std::memory_order_acquire) > 0) {
                uint64_t const first = rng() % (kSpace - 8);
                uint64_t const count = 1 + (rng() % 8);

                // The frontier only grows, and no segment may ever be planned below it: every slot <= F is
                // resolved, so a horizon under F would be needlessly stale, and one under a *stale* F would
                // mean the winner pass lost a resolved slot.
                int64_t const f0 = t.frontier();
                auto const p = plan(t, first, count);
                if (!p.has_value()) {
                    ADD_FAILURE() << "plan_read failed with no failed slots in play";
                    continue;
                }
                expect_covers(*p, first, count);
                for (auto const& seg : *p) {
                    EXPECT_GE(seg.H, f0);
                }
                EXPECT_GE(t.frontier(), f0);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Every slot resolved, so the frontier caught up to the last dLSN issued and a read of any range needs
    // no split at all.
    EXPECT_EQ(t.frontier(), static_cast< int64_t >(kWriters * kOpsPerWriter) - 1);
    EXPECT_EQ(t.frontier(), t.read_horizon());
    auto const p = plan(t, 0, kSpace);
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->size(), 1u);
}
