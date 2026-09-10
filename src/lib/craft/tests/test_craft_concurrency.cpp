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

// Real multi-threaded tests for CraftReplDev's own internal locking (missing_mu_, overlay_mu_,
// commit_running_) -- every other CRAFT test drives calls from a single thread via sync_get(), so
// none of them ever exercise genuine concurrent contention on these locks.
//
// This stays light (no HomeStore/iomgr): MockCraftJournalBackend's coroutine bodies never suspend
// across a real async boundary (co_return only), so stdexec::sync_wait (sync_get) runs the whole
// commit_impl()/write() coroutine synchronously on WHICHEVER OS thread calls it -- real parallel
// std::thread callers therefore drive genuinely concurrent execution of CraftReplDev's own code,
// with no iomgr reactor required. The mock/fake below are made thread-safe purely as test
// scaffolding (so a real locking bug in CraftReplDev surfaces as a wrong count/state rather than a
// crash inside the test double itself); CraftReplDev's own locks are what's actually under test.
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles
// craft_repl_dev.cpp directly (same pattern as test_craft_truncate.cpp).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <homestore/crc.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "craft/craft_repl_dev.hpp"
#include "home_blks_config.hpp"
#include "coro_helpers.hpp"

SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)
SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)

namespace homeblocks {
namespace {

static constexpr uint32_t k_page_size = 4096;

// ── thread-safe journal mock ────────────────────────────────────────────────────
//
// Same shape as test_craft_commit.cpp's MockCraftJournalBackend, but every access to shared state is
// guarded -- multiple real threads call alloc_write_data/write_slot/read_data concurrently here.

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::mutex mu;
    std::map< int64_t, JournalSlot > slots;
    homestore::blk_num_t next_blk_num{1000};
    std::map< homestore::blk_num_t, std::vector< uint8_t > > block_data;

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const& data, lba_count_t len) override {
        auto nlbas = static_cast< homestore::blk_count_t >(len / k_page_size);
        auto const* buf = static_cast< uint8_t const* >(data.iovs[0].iov_base);
        std::lock_guard lk{mu};
        homestore::multi_blk_id blkid{next_blk_num, nlbas, /* chunk = */ 1};
        for (homestore::blk_count_t i = 0; i < nlbas; ++i)
            block_data[next_blk_num + i] = std::vector< uint8_t >(buf + i * k_page_size, buf + (i + 1) * k_page_size);
        next_blk_num += nlbas;
        co_return blkid;
    }

    async_status write_slot(int64_t lsn, uint64_t /* term */, lba_t lba, lba_count_t len, homestore::multi_blk_id blkid,
                            bool all_zeros, std::vector< homestore::csum_t > const& csums) override {
        std::lock_guard lk{mu};
        slots[lsn] = JournalSlot{
            .lsn = lsn, .all_zeros = all_zeros, .lba_off_bytes = lba, .len_bytes = len, .blkid = blkid, .csums = csums};
        co_return ok();
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override {
        std::lock_guard lk{mu};
        auto it = slots.find(lsn);
        if (it == slots.end())
            co_return std::unexpected(std::make_error_condition(std::errc::no_such_file_or_directory));
        co_return it->second;
    }

    async_status truncate_to(int64_t) override { co_return ok(); }
    async_status free_data(homestore::multi_blk_id) override { co_return ok(); }

    async_status read_data(homestore::multi_blk_id blkid, sisl::sg_list& dest) override {
        auto* buf = static_cast< uint8_t* >(dest.iovs[0].iov_base);
        size_t offset = 0;
        std::lock_guard lk{mu};
        auto pieces = blkid.iterate();
        while (auto piece = pieces.next()) {
            for (homestore::blk_count_t i = 0; i < piece->blk_count(); ++i) {
                auto it = block_data.find(piece->blk_num() + i);
                if (it == block_data.end())
                    co_return std::unexpected(make_error_condition(volume_error::INTERNAL_ERROR));
                std::memcpy(buf + offset, it->second.data(), it->second.size());
                offset += it->second.size();
            }
        }
        co_return ok();
    }

    void add_data_slot(int64_t lsn, lba_t lba, uint32_t nlbas, homestore::blk_num_t blk_num,
                       std::vector< homestore::csum_t > csums) {
        homestore::multi_blk_id blkid{blk_num, static_cast< homestore::blk_count_t >(nlbas), /* chunk = */ 1};
        std::lock_guard lk{mu};
        slots[lsn] = JournalSlot{.lsn = lsn,
                                 .lba_off_bytes = lba * k_page_size,
                                 .len_bytes = nlbas * k_page_size,
                                 .blkid = blkid,
                                 .csums = std::move(csums)};
    }
};

// ── thread-safe fake index ──────────────────────────────────────────────────────

class FakeIndex {
public:
    std::mutex mu;
    std::map< lba_t, BlockInfo > entries;

    status write_to_index(lba_t start_lba, lba_t end_lba, std::unordered_map< lba_t, BlockInfo >& blocks_info) {
        std::lock_guard lk{mu};
        for (auto lba = start_lba; lba <= end_lba; ++lba) {
            auto& info = blocks_info[lba];
            if (auto it = entries.find(lba); it != entries.end()) info.old_blkid = it->second.new_blkid;
            entries[lba] = BlockInfo{info.new_blkid, homestore::blk_id{}, info.new_checksum};
        }
        return ok();
    }

    status delete_lba_range(lba_t start_lba, lba_t end_lba, std::vector< homestore::blk_id >& out_freed_blkids) {
        std::lock_guard lk{mu};
        for (auto lba = start_lba; lba <= end_lba; ++lba) {
            auto it = entries.find(lba);
            if (it == entries.end()) continue;
            out_freed_blkids.push_back(it->second.new_blkid);
            entries.erase(it);
        }
        return ok();
    }

    status read_from_index(lba_t start_lba, lba_t end_lba,
                           std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >& out) {
        std::lock_guard lk{mu};
        for (auto lba = start_lba; lba <= end_lba; ++lba) {
            auto it = entries.find(lba);
            if (it == entries.end()) continue;
            out.emplace_back(VolumeIndexKey{lba}, VolumeIndexValue{it->second.new_blkid, it->second.new_checksum});
        }
        return ok();
    }
};

// ── test fixture ─────────────────────────────────────────────────────────────

class CraftConcurrencyTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto mock = std::make_unique< MockCraftJournalBackend >();
        journal_ = mock.get();
        dev_ = std::make_unique< CraftReplDev >(volume_id_t{}, std::move(mock), k_page_size, nullptr);
    }

    auto do_write_data(int64_t dlsn, lba_t lba, uint8_t fill) {
        std::vector< uint8_t > buf(k_page_size, fill);
        sisl::sg_list data;
        data.size = buf.size();
        data.iovs.push_back(iovec{buf.data(), buf.size()});
        return homeblocks::detail::sync_get(
            dev_->write(craft::client_hdr{0, -1, -1}, dlsn, lba * k_page_size, k_page_size, std::move(data)));
    }

    auto do_write_zeros(int64_t dlsn, lba_t lba) {
        sisl::sg_list empty_data{};
        return homeblocks::detail::sync_get(
            dev_->write(craft::client_hdr{0, -1, -1}, dlsn, lba * k_page_size, k_page_size, std::move(empty_data)));
    }

    auto do_read(int64_t read_lsn, lba_t lba, std::vector< uint8_t >& dest_buf) {
        dest_buf.assign(k_page_size, 0xFF);
        sisl::sg_list dest;
        dest.size = dest_buf.size();
        dest.iovs.push_back(iovec{dest_buf.data(), dest_buf.size()});
        // read_with() (not the public read()): dev_ is constructed with indx_tbl_=nullptr in this
        // light fixture, and read() itself would unconditionally reject with not_supported before
        // ever reaching read_impl -- matching test_craft_commit.cpp's do_read() pattern.
        return homeblocks::detail::sync_get(dev_->read_with(
            read_lsn, lba * k_page_size, k_page_size, std::move(dest),
            [this](lba_t s, lba_t e, std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >& out) {
                return index_.read_from_index(s, e, out);
            }));
    }

    MockCraftJournalBackend* journal_{nullptr};
    FakeIndex index_;
    std::unique_ptr< CraftReplDev > dev_;
};

// N real threads each write a disjoint subset of a shared dLSN/LBA space concurrently (thread t
// handles every dlsn where dlsn % kThreads == t, submitted in increasing order per thread but with
// arrival order across threads left to the OS scheduler). After joining, missing_lsns_ must be fully
// drained and every LBA's overlay entry must reflect its own dlsn -- proving missing_lsns_/
// last_append_lsn/overlay_ stay consistent under real concurrent write() calls, not just the
// single-threaded out-of-order sequencing every other write test exercises.
TEST_F(CraftConcurrencyTest, ConcurrentDisjointWritesConverge) {
    constexpr int kThreads = 8;
    constexpr int kPerThread = 60;
    constexpr int kTotal = kThreads * kPerThread;

    std::vector< std::thread > threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([this, t]() {
            for (int k = 0; k < kPerThread; ++k) {
                int64_t dlsn = static_cast< int64_t >(k * kThreads + t);
                auto r = do_write_data(dlsn, /* lba = */ static_cast< lba_t >(dlsn), /* fill = */ 0xAB);
                EXPECT_TRUE(r.has_value()) << "dlsn=" << dlsn;
            }
        });
    }
    for (auto& th : threads)
        th.join();

    EXPECT_EQ(dev_->missing_count(), 0u);
    EXPECT_EQ(dev_->last_append_lsn(), kTotal - 1);
    for (int i = 0; i < kTotal; ++i) {
        EXPECT_EQ(dev_->overlay_lsn_for(static_cast< lba_t >(i)), i) << "lba=" << i;
    }
}

// N real threads all call write() for the exact SAME dlsn/LBA concurrently (a malformed/retried
// request -- a well-behaved client never does this for one dlsn, but a buggy or adversarial one
// might). Without in_flight_write_dlsns_' dedup guard, every thread would pass the idempotency check
// before any of them advance last_append_lsn, and every thread would proceed to alloc_write_data --
// doubly (N-ly) allocating real blocks for one dlsn, with only whichever write_slot call lands last
// ever referenced by the journal and the rest silently leaked. Proves: exactly one thread's write
// succeeds, every other thread is rejected with operation_in_progress (never silently duplicated),
// and MockCraftJournalBackend's block allocator only ever advances by ONE write's worth of blocks.
TEST_F(CraftConcurrencyTest, ConcurrentSameDlsnWritesNoDoubleAllocation) {
    constexpr int64_t kDlsn = 42;
    constexpr lba_t kLba = 7;
    constexpr int kThreads = 12;

    homestore::blk_num_t const next_blk_before = journal_->next_blk_num;

    std::atomic< int > succeeded{0};
    std::atomic< int > rejected_in_progress{0};
    std::atomic< int > unexpected_errors{0};
    std::vector< std::thread > threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([this, t, &succeeded, &rejected_in_progress, &unexpected_errors]() {
            auto r = do_write_data(kDlsn, kLba, static_cast< uint8_t >(t + 1));
            if (r.has_value()) {
                ++succeeded;
            } else if (r.error() == std::errc::operation_in_progress) {
                ++rejected_in_progress;
            } else {
                ++unexpected_errors;
            }
        });
    }
    for (auto& th : threads)
        th.join();

    EXPECT_EQ(unexpected_errors.load(), 0);
    // Every thread races the SAME dlsn: at least one must win (whichever gets there first when
    // last_append_lsn is still -1), and every other concurrent attempt must be rejected as in-flight
    // rather than silently duplicating the allocation. A thread that happens to run strictly after an
    // earlier one has already completed would instead see the idempotent path (also a success) --
    // the real threading (all kThreads launched together, no serialization point before the race
    // begins) makes at least one non-idempotent rejection overwhelmingly likely, but this test's
    // actual safety property is unexpected_errors==0 and the allocation-count check below.
    EXPECT_GE(succeeded.load(), 1);
    EXPECT_EQ(succeeded.load() + rejected_in_progress.load(), kThreads);

    // The one true test: exactly one dlsn=42 write's worth of blocks (1 LBA) was ever allocated,
    // no matter how many threads raced to write it.
    EXPECT_EQ(journal_->next_blk_num - next_blk_before, 1u);

    // The dlsn is fully resolved and locally readable afterward, same as any other successful write.
    EXPECT_EQ(dev_->last_append_lsn(), kDlsn);
    EXPECT_EQ(dev_->overlay_lsn_for(kLba), kDlsn);
}

// One thread continuously overwrites a single LBA at increasing dLSNs (each write's content tagged
// with a distinct byte value) while another thread concurrently, repeatedly reads that same LBA --
// proving overlay_mu_'s snapshot-under-lock discipline never lets a reader observe a torn/mixed
// OverlayEntry (which would surface as either non-uniform bytes within one page, or a spurious
// CRC_MISMATCH from a blkid/csum pair that doesn't actually correspond to each other).
TEST_F(CraftConcurrencyTest, ConcurrentWriteAndReadNoTornReads) {
    constexpr int kWrites = 500;
    std::atomic< bool > stop{false};
    std::atomic< int > torn_reads{0};
    std::atomic< int > read_errors{0};

    std::thread writer([this]() {
        for (int i = 0; i < kWrites; ++i) {
            auto r = do_write_data(/* dlsn = */ i, /* lba = */ 0, /* fill = */ static_cast< uint8_t >(i % 256));
            EXPECT_TRUE(r.has_value()) << "dlsn=" << i;
        }
    });

    std::thread reader([this, &stop, &torn_reads, &read_errors]() {
        std::vector< uint8_t > dest;
        while (!stop.load(std::memory_order_relaxed)) {
            // read_lsn far ahead of any possible dlsn so every overlay entry is always within horizon.
            auto r = do_read(/* read_lsn = */ 1'000'000, /* lba = */ 0, dest);
            if (!r.has_value()) {
                ++read_errors;
                continue;
            }
            if (r->extents.empty() || r->extents[0].hole) continue; // no write has landed yet
            uint8_t const first = dest[0];
            bool const uniform = std::all_of(dest.begin(), dest.end(), [first](uint8_t b) { return b == first; });
            if (!uniform) ++torn_reads;
        }
    });

    writer.join();
    stop.store(true, std::memory_order_relaxed);
    reader.join();

    EXPECT_EQ(read_errors.load(), 0);
    EXPECT_EQ(torn_reads.load(), 0);
}

// N real threads all call commit_with() targeting the same upto_lsn concurrently against a shared,
// pre-seeded journal -- commit_running_ (guarded by missing_mu_) must ensure the apply loop's
// write_fn runs for each LBA EXACTLY ONCE across all callers combined, never zero (stall) and never
// twice (double-apply), regardless of which thread actually "wins" the race. write_fn sleeps briefly
// to widen the window so concurrent callers genuinely overlap with the winner's in-flight run rather
// than only ever seeing it already finished.
TEST_F(CraftConcurrencyTest, ConcurrentCommitWithSerializesNoDoubleApply) {
    constexpr int kSlots = 40;
    dev_->seed_lsns(kSlots - 1, {});
    for (int i = 0; i < kSlots; ++i)
        journal_->add_data_slot(i, /* lba = */ static_cast< lba_t >(i), /* nlbas = */ 1,
                                /* blk_num = */ static_cast< homestore::blk_num_t >(1000 + i), {uint16_t(i)});

    std::mutex applied_mu;
    std::set< lba_t > applied_lbas;
    int duplicate_applies = 0;

    auto write_fn = [&](lba_t s, lba_t e, std::unordered_map< lba_t, BlockInfo >& info) -> status {
        std::this_thread::sleep_for(std::chrono::milliseconds(1)); // widen the concurrent-caller window
        auto r = index_.write_to_index(s, e, info);
        std::lock_guard lk{applied_mu};
        for (auto l = s; l <= e; ++l) {
            if (!applied_lbas.insert(l).second) ++duplicate_applies;
        }
        return r;
    };
    auto delete_fn = [this](lba_t s, lba_t e, std::vector< homestore::blk_id >& freed) {
        return index_.delete_lba_range(s, e, freed);
    };

    constexpr int kThreads = 8;
    std::vector< std::thread > threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([this, &write_fn, &delete_fn]() {
            auto r = homeblocks::detail::sync_get(dev_->commit_with(kSlots - 1, write_fn, delete_fn));
            EXPECT_TRUE(r.has_value());
        });
    }
    for (auto& th : threads)
        th.join();

    EXPECT_EQ(dev_->commit_lsn(), kSlots - 1);
    EXPECT_EQ(duplicate_applies, 0);                               // no LBA was ever applied by more than one caller
    EXPECT_EQ(applied_lbas.size(), static_cast< size_t >(kSlots)); // every LBA was applied by exactly one
}

// ── comprehensive mixed-workload stress ───────────────────────────────────────
//
// A wholistic stress test combining every operation CraftReplDev allows to run concurrently in
// production (write, commit/keep_alive-equivalent, read -- NOT truncate(), which is documented as
// login-only/quiesced and would violate its own precondition if raced against concurrent writes) --
// all firing simultaneously from many real threads for a sustained run, against a single shared
// instance. Every one of missing_mu_, overlay_mu_, and commit_running_ is under real contention at
// once, not in isolation the way the more focused tests above exercise them one at a time.
//
// Correctness is checked two ways: (1) no operation anywhere in the run returns an unexpected error
// or produces a torn/non-uniform read, and (2) the FINAL state is verified against a ground truth
// computed purely arithmetically from the (deterministic) work assignment -- independent of real-time
// execution order, because write()'s highest-dLSN-wins rule is defined by dlsn VALUE, not arrival
// time. This lets the test assert an exact expected outcome despite the interleaving being
// intentionally chaotic.
TEST_F(CraftConcurrencyTest, ComprehensiveMixedWorkloadStress) {
    constexpr int kNumLbas = 30;
    constexpr int64_t kTotalDlsn = 1500;
    constexpr int kWriterThreads = 12;
    constexpr int kReaderThreads = 4;
    constexpr int kCommitterThreads = 3;

    // Deterministic work assignment: dlsn -> lba, and whether it's an all_zeros (unmap) write.
    auto lba_for = [](int64_t dlsn) { return static_cast< lba_t >(dlsn % kNumLbas); };
    auto is_zero_write = [](int64_t dlsn) { return dlsn % 7 == 0; };
    auto fill_for = [](int64_t dlsn) { return static_cast< uint8_t >(dlsn % 256); };

    // Ground truth: for each lba, the winning dlsn is whichever assigned dlsn is numerically
    // highest (write()'s overlay/index apply order is entirely determined by dlsn value, never by
    // real-time arrival), computed here with no dependence on how the threads below actually
    // interleave.
    std::vector< int64_t > winner_dlsn(kNumLbas, -1);
    for (int64_t d = 0; d < kTotalDlsn; ++d) {
        auto l = lba_for(d);
        if (winner_dlsn[l] < d) winner_dlsn[l] = d;
    }

    // Deliberately NOT seeding last_append_lsn ahead of these writes: doing so would make write()'s
    // own idempotent short-circuit (dlsn <= last_append_lsn && not missing) treat every dlsn as
    // "already written" and skip real journaling/overlay population entirely -- the same mistake
    // fixed in TruncateRemovesOverlayEntriesAboveLsn. Starting from the default -1 lets every write
    // advance last_append_lsn for real.
    std::atomic< bool > writers_done{false};
    std::atomic< int > unexpected_errors{0};
    std::atomic< int > torn_reads{0};

    // Writers: thread t handles every dlsn where dlsn % kWriterThreads == t, in increasing order --
    // real-time arrival across threads is left entirely to the OS scheduler.
    std::vector< std::thread > writers;
    for (int t = 0; t < kWriterThreads; ++t) {
        writers.emplace_back([this, t, &lba_for, &is_zero_write, &fill_for, &unexpected_errors]() {
            for (int64_t d = t; d < kTotalDlsn; d += kWriterThreads) {
                auto r = is_zero_write(d) ? do_write_zeros(d, lba_for(d)) : do_write_data(d, lba_for(d), fill_for(d));
                if (!r.has_value()) ++unexpected_errors;
            }
        });
    }

    // Committers: repeatedly drive commit_with() toward the final target while writers are still in
    // flight -- commit_running_ must serialize these against each other AND against whichever
    // writers are concurrently populating the overlay for lsns not yet applied.
    auto write_fn = [this](lba_t s, lba_t e, std::unordered_map< lba_t, BlockInfo >& info) {
        return index_.write_to_index(s, e, info);
    };
    auto delete_fn = [this](lba_t s, lba_t e, std::vector< homestore::blk_id >& freed) {
        return index_.delete_lba_range(s, e, freed);
    };
    std::vector< std::thread > committers;
    for (int c = 0; c < kCommitterThreads; ++c) {
        committers.emplace_back([this, &writers_done, &unexpected_errors, &write_fn, &delete_fn]() {
            while (!writers_done.load(std::memory_order_relaxed)) {
                auto r = homeblocks::detail::sync_get(dev_->commit_with(kTotalDlsn - 1, write_fn, delete_fn));
                if (!r.has_value()) ++unexpected_errors;
            }
        });
    }

    // Readers: continuously read random LBAs throughout -- read_lsn is far beyond any possible dlsn
    // so every overlay entry is always within horizon; only uniformity (no torn read) and absence of
    // errors are checked here, not exact content (which the post-join ground-truth check covers).
    std::vector< std::thread > readers;
    for (int r_idx = 0; r_idx < kReaderThreads; ++r_idx) {
        readers.emplace_back([this, r_idx, &writers_done, &unexpected_errors, &torn_reads]() {
            std::mt19937 rng(static_cast< uint32_t >(r_idx) + 1);
            std::uniform_int_distribution< int > lba_dist(0, kNumLbas - 1);
            std::vector< uint8_t > dest;
            while (!writers_done.load(std::memory_order_relaxed)) {
                auto r = do_read(/* read_lsn = */ kTotalDlsn * 10, static_cast< lba_t >(lba_dist(rng)), dest);
                if (!r.has_value()) {
                    ++unexpected_errors;
                    continue;
                }
                if (r->extents.empty() || r->extents[0].hole) continue;
                uint8_t const first = dest[0];
                bool const uniform = std::all_of(dest.begin(), dest.end(), [first](uint8_t b) { return b == first; });
                if (!uniform) ++torn_reads;
            }
        });
    }

    for (auto& th : writers)
        th.join();
    writers_done.store(true, std::memory_order_relaxed);
    for (auto& th : committers)
        th.join();
    for (auto& th : readers)
        th.join();

    ASSERT_EQ(unexpected_errors.load(), 0);
    ASSERT_EQ(torn_reads.load(), 0);

    // Drain: every dlsn in [0, kTotalDlsn) was assigned to exactly one writer thread, so
    // missing_lsns_ must now be fully empty -- this must reach the target with no stall.
    auto final_commit = homeblocks::detail::sync_get(dev_->commit_with(kTotalDlsn - 1, write_fn, delete_fn));
    ASSERT_TRUE(final_commit.has_value());
    EXPECT_EQ(dev_->missing_count(), 0u);
    EXPECT_EQ(dev_->commit_lsn(), kTotalDlsn - 1);

    // Ground-truth verification: every LBA's final index state must reflect its winning dlsn --
    // absent (unmapped) if the winner was an all_zeros write, or the winner's exact content
    // checksum otherwise -- and its overlay entry must be fully retired (everything committed).
    for (int l = 0; l < kNumLbas; ++l) {
        EXPECT_EQ(dev_->overlay_lsn_for(static_cast< lba_t >(l)), -1) << "lba=" << l;
        if (is_zero_write(winner_dlsn[l])) {
            EXPECT_FALSE(index_.entries.count(static_cast< lba_t >(l))) << "lba=" << l << " should be unmapped";
        } else {
            ASSERT_TRUE(index_.entries.count(static_cast< lba_t >(l))) << "lba=" << l;
            std::vector< uint8_t > expected(k_page_size, fill_for(winner_dlsn[l]));
            auto expected_csum = crc16_t10dif(0x8005, expected.data(), expected.size());
            EXPECT_EQ(index_.entries[static_cast< lba_t >(l)].new_checksum, expected_csum) << "lba=" << l;
        }
    }
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    HB_SETTINGS_FACTORY().load_json("{\"craft_watchdog_timeout_ms\": 0}");
    return RUN_ALL_TESTS();
}
