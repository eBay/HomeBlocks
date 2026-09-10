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

// Unit tests for CraftReplDev::commit() (S3: Commit Path).
//
// Exercises commit_with() -- the test seam behind commit() -- against a fake, std::map-backed
// index instead of a real VolumeIndexTable, so this stays a light test (no HomeStore bring-up).
// commit_with() runs the exact same apply-one-slot algorithm (commit_impl) that commit() binds to
// indx_tbl_'s real methods; only the write/delete callbacks differ.
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles
// craft_repl_dev.cpp directly (same pattern as test_craft_truncate.cpp).

#include <condition_variable>
#include <gtest/gtest.h>
#include <map>
#include <mutex>
#include <optional>
#include <thread>
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
// Must match k_craft_crc16_seed in craft_repl_dev.cpp -- same constant test_craft_write.cpp uses.
static constexpr homestore::csum_t k_test_crc16_seed = 0x8005;

// ── journal mock ──────────────────────────────────────────────────────────────
//
// A capable mock (unlike the stub ones in test_craft_truncate.cpp/test_craft_peer_exchange.cpp):
// write_slot actually records slots so real dev_->write() calls can be used to seed the overlay,
// and alloc_write_data returns a real, decomposable multi_blk_id sized to the request.

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots;
    int free_data_calls{0};
    homestore::blk_num_t next_blk_num{1000};
    std::optional< int64_t > fail_on_read; // if set, read_slot for that lsn returns an error
    // Opt-in blocking gate for read_slot(), default open (no blocking) -- lets a test create a real,
    // observable window during rebuild_overlay()'s walk (e.g. to verify recovering_'s client-I/O gate
    // actually holds while a restart-triggered rebuild is still in flight), since this mock's other
    // methods all resolve synchronously with no real suspension to race against otherwise.
    std::mutex read_gate_mu;
    std::condition_variable read_gate_cv;
    bool read_gate_open{true};
    void close_read_gate() {
        std::lock_guard lk{read_gate_mu};
        read_gate_open = false;
    }
    void open_read_gate() {
        {
            std::lock_guard lk{read_gate_mu};
            read_gate_open = true;
        }
        read_gate_cv.notify_all();
    }
    // Bytes backing each single-block blk_num -- populated automatically by alloc_write_data (so any
    // real write() call is readable back via read_data with no separate seeding step) and directly by
    // read()-test cases that construct index/overlay state without going through write().
    std::map< homestore::blk_num_t, std::vector< uint8_t > > block_data;

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const& data, lba_count_t len) override {
        auto nlbas = static_cast< homestore::blk_count_t >(len / k_page_size);
        homestore::multi_blk_id blkid{next_blk_num, nlbas, /* chunk = */ 1};
        auto const* buf = static_cast< uint8_t const* >(data.iovs[0].iov_base);
        for (homestore::blk_count_t i = 0; i < nlbas; ++i)
            block_data[next_blk_num + i] = std::vector< uint8_t >(buf + i * k_page_size, buf + (i + 1) * k_page_size);
        next_blk_num += nlbas;
        co_return blkid;
    }

    async_status write_slot(int64_t lsn, uint64_t /* term */, lba_t lba, lba_count_t len, homestore::multi_blk_id blkid,
                            bool all_zeros, std::vector< homestore::csum_t > const& csums) override {
        slots[lsn] = JournalSlot{
            .lsn = lsn, .all_zeros = all_zeros, .lba_off_bytes = lba, .len_bytes = len, .blkid = blkid, .csums = csums};
        co_return ok();
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override {
        {
            // Plain (non-coroutine) blocking wait: this mock's coroutines never suspend across a real
            // async boundary, so whatever thread calls into read_slot() (directly, or transitively via
            // rebuild_overlay()'s detached run_recovery()) physically blocks here until released --
            // exactly what a test needs to create an observable in-progress-recovery window.
            std::unique_lock lk{read_gate_mu};
            read_gate_cv.wait(lk, [this] { return read_gate_open; });
        }
        if (fail_on_read && *fail_on_read == lsn)
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        auto it = slots.find(lsn);
        if (it == slots.end())
            co_return std::unexpected(std::make_error_condition(std::errc::no_such_file_or_directory));
        co_return it->second;
    }

    async_status truncate_to(int64_t) override { co_return ok(); }
    async_status free_data(homestore::multi_blk_id) override {
        ++free_data_calls;
        co_return ok();
    }
    // Same error category HomeStoreCraftJournalBackend::read_data returns for a real async_read
    // failure -- a missing block_data entry here means the test forgot to seed it, not a
    // filesystem-shaped condition, so this mirrors production's actual error vocabulary rather than
    // borrowing an unrelated POSIX errno.
    async_status read_data(homestore::multi_blk_id blkid, sisl::sg_list& dest) override {
        auto* buf = static_cast< uint8_t* >(dest.iovs[0].iov_base);
        size_t offset = 0;
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

    // Seed a data slot directly (bypassing write()) for tests that need precise control over
    // missing_lsns_/last_append_lsn without exercising write()'s own logic.
    void add_data_slot(int64_t lsn, lba_t lba, uint32_t nlbas, homestore::blk_num_t blk_num,
                       std::vector< homestore::csum_t > csums) {
        homestore::multi_blk_id blkid{blk_num, static_cast< homestore::blk_count_t >(nlbas), /* chunk = */ 1};
        slots[lsn] = JournalSlot{.lsn = lsn,
                                 .lba_off_bytes = lba * k_page_size,
                                 .len_bytes = nlbas * k_page_size,
                                 .blkid = blkid,
                                 .csums = std::move(csums)};
    }

    // Seed block_data directly for read()-test cases that construct index/overlay state without a
    // real write() call (e.g. index entries built straight against FakeIndex).
    void seed_block(homestore::blk_num_t blk_num, std::vector< uint8_t > data) {
        block_data[blk_num] = std::move(data);
    }
};

// ── fake index ────────────────────────────────────────────────────────────────
//
// Backed by a plain std::map, matching the shape commit_impl's write/delete callbacks expect --
// the same two operations a real VolumeIndexTable exposes (write_to_index, delete_lba_range).

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

    status delete_lba_range(lba_t start_lba, lba_t end_lba, std::vector< homestore::blk_id >& out_freed_blkids) {
        for (auto lba = start_lba; lba <= end_lba; ++lba) {
            auto it = entries.find(lba);
            if (it == entries.end()) continue;
            out_freed_blkids.push_back(it->second.new_blkid);
            entries.erase(it);
        }
        return ok();
    }

    // Same shape as the real VolumeIndexTable::read_from_index: absent LBAs are simply omitted
    // (holes), not an error.
    status read_from_index(lba_t start_lba, lba_t end_lba,
                           std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >& out) {
        for (auto lba = start_lba; lba <= end_lba; ++lba) {
            auto it = entries.find(lba);
            if (it == entries.end()) continue;
            out.emplace_back(VolumeIndexKey{lba}, VolumeIndexValue{it->second.new_blkid, it->second.new_checksum});
        }
        return ok();
    }
};

// ── test fixture ─────────────────────────────────────────────────────────────

class CraftCommitTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto mock = std::make_unique< MockCraftJournalBackend >();
        journal_ = mock.get();
        dev_ = std::make_unique< CraftReplDev >(volume_id_t{}, std::move(mock), k_page_size, nullptr);
    }

    auto do_commit(int64_t upto_lsn) {
        return homeblocks::detail::sync_get(dev_->commit_with(
            upto_lsn,
            [this](lba_t s, lba_t e, std::unordered_map< lba_t, BlockInfo >& info) {
                return index_.write_to_index(s, e, info);
            },
            [this](lba_t s, lba_t e, std::vector< homestore::blk_id >& freed) {
                return index_.delete_lba_range(s, e, freed);
            }));
    }

    // Real write() call -- the only way to populate the overlay (write()'s post-flight block does
    // it unconditionally, regardless of indx_tbl_). Backed by a shared static buffer since write()
    // computes a per-LBA CRC over it.
    auto do_write_data(uint64_t term, int64_t dlsn, lba_t lba, uint32_t nlbas) {
        static std::vector< uint8_t > buf(16 * k_page_size, 0xAB);
        sisl::sg_list data;
        data.size = nlbas * k_page_size;
        data.iovs.push_back(iovec{buf.data(), data.size});
        return homeblocks::detail::sync_get(dev_->write(craft::client_hdr{term, -1, -1}, dlsn, lba * k_page_size,
                                                        nlbas * k_page_size, std::move(data)));
    }

    // Real write() call for an all_zeros (unmap) write -- populates the overlay's all_zeros marker.
    auto do_write_zeros(uint64_t term, int64_t dlsn, lba_t lba, uint32_t nlbas) {
        sisl::sg_list empty_data{};
        return homeblocks::detail::sync_get(dev_->write(craft::client_hdr{term, -1, -1}, dlsn, lba * k_page_size,
                                                        nlbas * k_page_size, std::move(empty_data)));
    }

    // Directly seed a committed index entry with real backing bytes, bypassing write()/commit() --
    // for read()-test cases that only care about index-sourced state.
    void seed_index_entry(lba_t lba, homestore::blk_num_t blk_num, std::vector< uint8_t > data) {
        auto csum = crc16_t10dif(k_test_crc16_seed, data.data(), data.size());
        index_.entries[lba] = BlockInfo{homestore::blk_id{blk_num, 1, /* chunk = */ 1}, homestore::blk_id{}, csum};
        journal_->seed_block(blk_num, std::move(data));
    }

    auto do_read(int64_t read_lsn, lba_t lba, uint32_t nlbas) {
        dest_buf_.assign(nlbas * k_page_size, 0xFF); // non-zero filler so hole-zeroing is actually observable
        sisl::sg_list dest;
        dest.size = dest_buf_.size();
        dest.iovs.push_back(iovec{dest_buf_.data(), dest_buf_.size()});
        return homeblocks::detail::sync_get(dev_->read_with(
            read_lsn, lba * k_page_size, nlbas * k_page_size, std::move(dest),
            [this](lba_t s, lba_t e, std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >& out) {
                return index_.read_from_index(s, e, out);
            }));
    }

    std::vector< uint8_t > dest_buf_;
    MockCraftJournalBackend* journal_{nullptr};
    FakeIndex index_;
    std::unique_ptr< CraftReplDev > dev_;
};

// ── tests ─────────────────────────────────────────────────────────────────────

// A single in-order slot applies cleanly: index gains an entry per LBA, commit_lsn advances.
TEST_F(CraftCommitTest, InOrderApply) {
    dev_->seed_lsns(0, {});
    journal_->add_data_slot(0, /*lba=*/0, /*nlbas=*/2, /*blk_num=*/100, {11, 22});

    auto r = do_commit(0);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 0);
    EXPECT_EQ(dev_->commit_lsn(), 0);
    ASSERT_TRUE(index_.entries.count(0));
    ASSERT_TRUE(index_.entries.count(1));
    EXPECT_EQ(index_.entries[0].new_checksum, 11);
    EXPECT_EQ(index_.entries[1].new_checksum, 22);
}

// commit() must stall at the first gap in missing_lsns_ rather than error -- entries before the
// gap apply; the gap and everything after it are left untouched.
TEST_F(CraftCommitTest, StallAtFirstMissingHole) {
    dev_->seed_lsns(5, {2});
    journal_->add_data_slot(0, 0, 1, 100, {11});
    journal_->add_data_slot(1, 1, 1, 101, {12});
    // lsn=2 deliberately has no journal slot -- matches its missing_lsns_ entry.
    journal_->add_data_slot(3, 3, 1, 103, {14});
    journal_->add_data_slot(4, 4, 1, 104, {15});
    journal_->add_data_slot(5, 5, 1, 105, {16});

    auto r = do_commit(5);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 1); // stalls right before the hole at lsn=2
    EXPECT_EQ(dev_->commit_lsn(), 1);
    EXPECT_TRUE(index_.entries.count(0));
    EXPECT_TRUE(index_.entries.count(1));
    EXPECT_FALSE(index_.entries.count(3)); // never reached
}

// An Empty-verdicted lsn is skipped without a journal read -- if commit_impl tried to read_slot it,
// the call would fail (no slot seeded there) and the whole commit would error instead of succeeding.
TEST_F(CraftCommitTest, EmptySlotSkip) {
    dev_->seed_lsns(2, {});
    dev_->seed_empty({1});
    journal_->add_data_slot(0, 0, 1, 100, {11});
    journal_->add_data_slot(2, 2, 1, 102, {13});

    auto r = do_commit(2);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 2);
    EXPECT_EQ(dev_->commit_lsn(), 2);
    EXPECT_TRUE(index_.entries.count(0));
    EXPECT_TRUE(index_.entries.count(2));
}

// szmyd (PR #175 review, on the record but not filed inline): commit_impl's is_empty branch skips
// overlay retirement alongside the apply. Reachable via "Empty beats data" reconciliation
// (request_resolution's doc comment in home_blocks.hpp): a replica can have already journaled real
// data (and thus a real overlay entry) for an lsn that the CLUSTER-WIDE resolution round still
// verdicts Empty (e.g. the leader's round didn't successfully use this replica as a holder). The
// index correctly never applies it, but without retiring the overlay, this replica would keep
// SERVING that data on reads -- directly contradicting the Empty verdict, not just leaving harmless
// stale bookkeeping.
TEST_F(CraftCommitTest, EmptyVerdictRetiresOverlayEvenWhenThisReplicaHadTheData) {
    ASSERT_TRUE(do_write_data(0, /* dlsn = */ 0, /* lba = */ 5, /* nlbas = */ 1).has_value());
    ASSERT_EQ(dev_->overlay_lsn_for(5), 0); // write() created a real overlay entry

    dev_->seed_empty({0}); // cluster-wide Empty verdict despite this replica having the data

    auto r = do_commit(0);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 0);
    EXPECT_FALSE(index_.entries.count(5)); // never applied -- Empty beats data
    EXPECT_EQ(dev_->overlay_lsn_for(5), -1); // retired -- must not still be served on reads
}

// all_zeros apply removes the index entry and reclaims its block.
TEST_F(CraftCommitTest, AllZerosApplyRemovesEntryAndFreesBlock) {
    index_.entries[0] = BlockInfo{homestore::blk_id{500, 1, 1}, homestore::blk_id{}, 99};
    dev_->seed_lsns(0, {});
    journal_->slots[0] = JournalSlot{.lsn = 0, .all_zeros = true, .lba_off_bytes = 0, .len_bytes = k_page_size};

    auto r = do_commit(0);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 0);
    EXPECT_FALSE(index_.entries.count(0));
    EXPECT_EQ(journal_->free_data_calls, 1);
}

// A data apply over an already-mapped LBA writes the new entry and reclaims the superseded block.
TEST_F(CraftCommitTest, DataApplyWritesEntryAndFreesSupersededBlock) {
    index_.entries[0] = BlockInfo{homestore::blk_id{500, 1, 1}, homestore::blk_id{}, 77};
    dev_->seed_lsns(0, {});
    journal_->add_data_slot(0, 0, 1, /*blk_num=*/999, {222});

    auto r = do_commit(0);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 0);
    ASSERT_TRUE(index_.entries.count(0));
    EXPECT_EQ(index_.entries[0].new_checksum, 222);
    EXPECT_EQ(journal_->free_data_calls, 1); // old blk_id{500,1,1} reclaimed
}

// Crash-replay of an already-applied slot before commit_lsn was durably persisted past it: the
// index already holds this exact blkid from the first apply (a separate, already-flushed
// checkpoint), so re-applying the same slot must NOT free it -- old_blkid == new_blkid means
// "already applied" (a no-op replay), not "superseded by a newer write".
TEST_F(CraftCommitTest, CrashReplayOfAppliedSlotDoesNotFreeLiveBlock) {
    dev_->seed_lsns(0, {});
    journal_->add_data_slot(0, /* lba = */ 0, /* nlbas = */ 1, /* blk_num = */ 999, {222});

    auto r1 = do_commit(0);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 0);
    ASSERT_TRUE(index_.entries.count(0));
    EXPECT_EQ(journal_->free_data_calls, 0); // no prior entry to supersede on the first apply

    // Simulate a crash before commit_lsn was durably persisted past this slot: commit_lsn regresses
    // to before lsn=0 (as if superblock recovery restored an older value), while the index (already
    // flushed at its own checkpoint) still has lsn=0's write applied.
    dev_->seed_commit_lsn(-1);

    auto r2 = do_commit(0);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(dev_->commit_lsn(), 0);
    EXPECT_EQ(journal_->free_data_calls, 0); // must NOT free blk_num=999 -- index still references it
}

// LBA written at dLSN 3 and 5 (5 superseding 3 in the overlay, highest-dLSN-wins); committing
// through 3 must not retire the overlay entry, since its recorded lsn is 5, not 3.
TEST_F(CraftCommitTest, OverlayRetiresOnlyIfLsnMatches) {
    dev_->seed_lsns(2, {});
    dev_->seed_commit_lsn(2);
    ASSERT_TRUE(do_write_data(0, 3, /*lba=*/0, /*nlbas=*/1).has_value());
    ASSERT_TRUE(do_write_data(0, 4, /*lba=*/1, /*nlbas=*/1).has_value());
    ASSERT_TRUE(do_write_data(0, 5, /*lba=*/0, /*nlbas=*/1).has_value());
    ASSERT_EQ(dev_->overlay_lsn_for(0), 5); // highest-dLSN-wins already, before any commit

    auto r = do_commit(3);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 3);
    EXPECT_EQ(dev_->commit_lsn(), 3);
    EXPECT_EQ(dev_->overlay_lsn_for(0), 5); // survives: recorded lsn (5) != lsn just applied (3)
}

// write()'s overlay population must also handle all_zeros writes (not just data writes) --
// highest-dLSN-wins applies identically to the all_zeros marker entries.
TEST_F(CraftCommitTest, OverlayPopulatesForAllZerosWrites) {
    sisl::sg_list empty_data{};
    auto r = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{0, -1, -1}, /* dlsn = */ 0, /* addr = */ 0, k_page_size, std::move(empty_data)));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->overlay_lsn_for(0), 0);
}

// A slot's blkid may have more than one piece (multi_blk_id::iterate() walks each of them);
// verify the decomposition maps every piece's blocks to consecutive LBAs, not just the first piece.
TEST_F(CraftCommitTest, DataApplyDecomposesMultiPieceBlkid) {
    homestore::multi_blk_id blkid{/* blk_num = */ 500, /* nblks = */ 2, /* chunk = */ 1};
    blkid.add(/* blk_num = */ 900, /* nblks = */ 1, /* chunk = */ 1); // second, non-contiguous piece
    journal_->slots[0] =
        JournalSlot{.lsn = 0, .lba_off_bytes = 0, .len_bytes = 3 * k_page_size, .blkid = blkid, .csums = {11, 22, 33}};
    dev_->seed_lsns(0, {});

    auto r = do_commit(0);
    ASSERT_TRUE(r.has_value());
    ASSERT_TRUE(index_.entries.count(0));
    ASSERT_TRUE(index_.entries.count(1));
    ASSERT_TRUE(index_.entries.count(2));
    EXPECT_EQ(index_.entries[0].new_checksum, 11);
    EXPECT_EQ(index_.entries[1].new_checksum, 22);
    EXPECT_EQ(index_.entries[2].new_checksum, 33);
    // lba=0,1 come from the first piece (blk_num 500,501); lba=2 from the second, non-contiguous
    // piece (blk_num 900) -- proves iterate() walked both pieces, not just the first.
    EXPECT_EQ(index_.entries[0].new_blkid.blk_num(), 500u);
    EXPECT_EQ(index_.entries[1].new_blkid.blk_num(), 501u);
    EXPECT_EQ(index_.entries[2].new_blkid.blk_num(), 900u);
}

// A concurrent (here: reentrant, called synchronously from inside the outer commit's own write_fn
// callback -- so commit_running_ is still true) commit invocation must be a safe no-op, not a
// second, overlapping application of the same range.
TEST_F(CraftCommitTest, ConcurrentCommitIsNoOp) {
    dev_->seed_lsns(0, {});
    journal_->add_data_slot(0, 0, 1, 100, {11});

    int reentrant_write_calls = 0;
    auto reentrant_write_fn = [&](lba_t s, lba_t e, std::unordered_map< lba_t, BlockInfo >& info) {
        auto nested = homeblocks::detail::sync_get(dev_->commit_with(
            0,
            [&](lba_t, lba_t, std::unordered_map< lba_t, BlockInfo >&) {
                ++reentrant_write_calls;
                return ok();
            },
            [](lba_t, lba_t, std::vector< homestore::blk_id >&) { return ok(); }));
        EXPECT_TRUE(nested.has_value());
        EXPECT_EQ(*nested, -1); // commit_lsn hasn't advanced yet -- the outer run is still in progress
        return index_.write_to_index(s, e, info);
    };

    auto outer = homeblocks::detail::sync_get(
        dev_->commit_with(0, reentrant_write_fn, [this](lba_t s, lba_t e, std::vector< homestore::blk_id >& freed) {
            return index_.delete_lba_range(s, e, freed);
        }));
    ASSERT_TRUE(outer.has_value());
    EXPECT_EQ(*outer, 0);
    EXPECT_EQ(reentrant_write_calls, 0);  // the nested call never touched the index
    EXPECT_TRUE(index_.entries.count(0)); // the outer call's own apply still went through
}

// A genuine index-write failure aborts the commit immediately; commit_lsn does not advance past
// the last successfully-applied lsn.
TEST_F(CraftCommitTest, WriteFnErrorAbortsCommit) {
    dev_->seed_lsns(1, {});
    journal_->add_data_slot(0, 0, 1, 100, {11});
    journal_->add_data_slot(1, 1, 1, 101, {12});

    auto r = homeblocks::detail::sync_get(dev_->commit_with(
        1,
        [](lba_t, lba_t, std::unordered_map< lba_t, BlockInfo >&) -> status {
            return std::unexpected(volume_error::INDEX_ERROR);
        },
        [](lba_t, lba_t, std::vector< homestore::blk_id >&) { return ok(); }));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), -1); // nothing applied
}

// commit() clamps to last_append_lsn even if asked to commit further than what has actually been
// appended locally (e.g. the client's own view of commit_lsn is ahead of this replica).
TEST_F(CraftCommitTest, ClampsToLastAppendLsn) {
    dev_->seed_lsns(1, {});
    journal_->add_data_slot(0, 0, 1, 100, {11});
    journal_->add_data_slot(1, 1, 1, 101, {12});

    auto r = do_commit(/* upto_lsn = */ 100); // far beyond last_append_lsn=1
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 1);
    EXPECT_EQ(dev_->commit_lsn(), 1);
}

// A second commit() call past an already-fully-applied range is a true no-op: no further index
// writes happen (proves repeated best-effort commit() calls from every write() don't re-apply).
TEST_F(CraftCommitTest, RepeatedCommitIsNoOp) {
    dev_->seed_lsns(0, {});
    journal_->add_data_slot(0, 0, 1, 100, {11});
    ASSERT_TRUE(do_commit(0).has_value());
    ASSERT_EQ(dev_->commit_lsn(), 0);

    int write_calls = 0;
    auto r = homeblocks::detail::sync_get(dev_->commit_with(
        0,
        [&](lba_t s, lba_t e, std::unordered_map< lba_t, BlockInfo >& info) {
            ++write_calls;
            return index_.write_to_index(s, e, info);
        },
        [this](lba_t s, lba_t e, std::vector< homestore::blk_id >& freed) {
            return index_.delete_lba_range(s, e, freed);
        }));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 0);
    EXPECT_EQ(write_calls, 0); // nothing left to apply
}

// Defense-in-depth: write() rejects len==0 at the client boundary (see CraftWriteTest.ZeroLenRejected),
// but a stale/legacy on-disk record could still have one. commit() must abort cleanly rather than
// let nlbas=0 underflow the end_lba computation into a near-UINT64_MAX range.
TEST_F(CraftCommitTest, MalformedZeroLenSlotAborts) {
    dev_->seed_lsns(0, {});
    journal_->slots[0] = JournalSlot{.lsn = 0, .lba_off_bytes = 0, .len_bytes = 0};

    auto r = do_commit(0);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), -1);
}

// ── read() ────────────────────────────────────────────────────────────────────

// A committed (index-only) LBA reads back as a single data extent with the exact bytes seeded.
TEST_F(CraftCommitTest, ReadIndexOnly) {
    std::vector< uint8_t > content(k_page_size, 0xCD);
    seed_index_entry(/* lba = */ 0, /* blk_num = */ 500, content);

    auto r = do_read(/* read_lsn = */ 10, /* lba = */ 0, /* nlbas = */ 1);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_EQ(r->extents[0].addr, 0u);
    EXPECT_EQ(r->extents[0].len, k_page_size);
    EXPECT_FALSE(r->extents[0].hole);
    EXPECT_EQ(dest_buf_, content);
}

// An appended-but-not-yet-committed write is still locally readable via the overlay -- no commit()
// call happens in this test at all.
TEST_F(CraftCommitTest, ReadOverlayOnly) {
    ASSERT_TRUE(do_write_data(0, /* dlsn = */ 5, /* lba = */ 0, /* nlbas = */ 1).has_value());

    auto r = do_read(/* read_lsn = */ 5, /* lba = */ 0, /* nlbas = */ 1);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_FALSE(r->extents[0].hole);
    EXPECT_EQ(dest_buf_, std::vector< uint8_t >(k_page_size, 0xAB)); // do_write_data's fixed fill
}

// An overlay entry above read_lsn is held but never served -- the index's older, still-valid-as-of-
// read_lsn value must be used instead, not treated as a hole.
TEST_F(CraftCommitTest, HorizonClampServesIndexNotOverlayAboveReadLsn) {
    std::vector< uint8_t > old_content(k_page_size, 0xCD);
    seed_index_entry(/* lba = */ 0, /* blk_num = */ 500, old_content);
    dev_->seed_commit_lsn(3);

    ASSERT_TRUE(do_write_data(0, /* dlsn = */ 5, /* lba = */ 0, /* nlbas = */ 1).has_value());

    auto r = do_read(/* read_lsn = */ 4, /* lba = */ 0, /* nlbas = */ 1); // 4 < overlay's lsn=5
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_FALSE(r->extents[0].hole);
    EXPECT_EQ(dest_buf_, old_content); // NOT the overlay's 0xAB fill
}

// Absent from both the index and the overlay -- reads as a hole (zero-filled), not an error.
TEST_F(CraftCommitTest, AbsentRangeIsHole) {
    auto r = do_read(/* read_lsn = */ 10, /* lba = */ 0, /* nlbas = */ 1);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_TRUE(r->extents[0].hole);
    EXPECT_EQ(dest_buf_, std::vector< uint8_t >(k_page_size, 0));
}

// An all_zeros overlay entry (unapplied WRITE_ZEROES) reads as a hole -- no block to read at all.
TEST_F(CraftCommitTest, AllZerosOverlayIsHole) {
    ASSERT_TRUE(do_write_zeros(0, /* dlsn = */ 5, /* lba = */ 0, /* nlbas = */ 1).has_value());

    auto r = do_read(/* read_lsn = */ 5, /* lba = */ 0, /* nlbas = */ 1);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_TRUE(r->extents[0].hole);
    EXPECT_EQ(dest_buf_, std::vector< uint8_t >(k_page_size, 0));
}

// A data write whose actual payload happens to be all-zero bytes must collapse to a hole at READ
// time -- the index still says "data" (this is not an all_zeros/unmap entry), so this proves the
// scan runs on read, not on write.
TEST_F(CraftCommitTest, DataWriteOfAllZeroBytesCollapsesAtReadTimeNotWriteTime) {
    std::vector< uint8_t > zero_content(k_page_size, 0x00);
    seed_index_entry(/* lba = */ 0, /* blk_num = */ 500, zero_content);

    auto r = do_read(/* read_lsn = */ 10, /* lba = */ 0, /* nlbas = */ 1);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_TRUE(r->extents[0].hole);
    EXPECT_EQ(dest_buf_, zero_content);
}

// Corrupted bytes behind a valid index entry (checksum no longer matches) must fail the read rather
// than silently return bad data.
TEST_F(CraftCommitTest, CrcMismatchFails) {
    std::vector< uint8_t > content(k_page_size, 0xCD);
    seed_index_entry(/* lba = */ 0, /* blk_num = */ 500, content);
    journal_->seed_block(500, std::vector< uint8_t >(k_page_size, 0xEE)); // corrupt after csum was computed

    auto r = do_read(/* read_lsn = */ 10, /* lba = */ 0, /* nlbas = */ 1);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::CRC_MISMATCH));
}

// A bit-flip that happens to zero out an LBA whose checksum was computed over real (non-zero)
// content must be reported as CRC_MISMATCH, not silently collapsed to an indistinguishable hole --
// the all-zero read-time collapse must never bypass the CRC check.
TEST_F(CraftCommitTest, AllZeroCorruptionFailsCrcInsteadOfCollapsing) {
    std::vector< uint8_t > content(k_page_size, 0xCD); // checksum computed over non-zero content
    seed_index_entry(/* lba = */ 0, /* blk_num = */ 500, content);
    journal_->seed_block(500, std::vector< uint8_t >(k_page_size, 0x00)); // corrupted to all-zero

    auto r = do_read(/* read_lsn = */ 10, /* lba = */ 0, /* nlbas = */ 1);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::CRC_MISMATCH));
}

// A multi-LBA read spanning a contiguous data run followed by a hole must merge the run into one
// extent (one batched read, not one per LBA) and report the hole as a separate, correctly-offset
// extent.
TEST_F(CraftCommitTest, MultiLbaReadMergesAdjacentExtents) {
    std::vector< uint8_t > content0(k_page_size, 0xCD);
    std::vector< uint8_t > content1(k_page_size, 0xCE);
    seed_index_entry(0, /* blk_num = */ 500, content0);
    seed_index_entry(1, /* blk_num = */ 501, content1); // contiguous blk_num -- merges with lba=0's run
    // lba=2 left absent -> hole

    auto r = do_read(/* read_lsn = */ 10, /* lba = */ 0, /* nlbas = */ 3);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 2u);
    EXPECT_FALSE(r->extents[0].hole);
    EXPECT_EQ(r->extents[0].addr, 0u);
    EXPECT_EQ(r->extents[0].len, 2 * k_page_size);
    EXPECT_TRUE(r->extents[1].hole);
    EXPECT_EQ(r->extents[1].addr, 2 * k_page_size);
    EXPECT_EQ(r->extents[1].len, k_page_size);

    std::vector< uint8_t > expected(content0);
    expected.insert(expected.end(), content1.begin(), content1.end());
    expected.insert(expected.end(), k_page_size, 0);
    EXPECT_EQ(dest_buf_, expected);
}

// A single contiguous data run larger than blk_count_t's max (65535 -- uint16_t) must be split
// across multiple read_data batches rather than let the run_nlbas -> blk_count_t static_cast
// silently truncate (65536 wraps to 0, anything larger wraps to some smaller-than-intended count) --
// either would misread real data. Uses a tiny 8-byte lba_size (rather than k_page_size) so a run
// this large still fits under this test binary's craft_max_io_len_mb=1 MiB cap; constructs its own
// dev_/journal_/index_ locally since CraftCommitTest's own fixture is fixed at k_page_size.
TEST_F(CraftCommitTest, LargeContiguousRunSplitsAcrossBlkCountTLimit) {
    constexpr uint32_t k_tiny_lba_size = 8;
    constexpr uint32_t k_nlbas = 65536; // one more than blk_count_t's max (65535)

    auto mock = std::make_unique< MockCraftJournalBackend >();
    auto* journal = mock.get();
    FakeIndex index;
    auto dev = std::make_unique< CraftReplDev >(volume_id_t{}, std::move(mock), k_tiny_lba_size, nullptr);

    std::vector< uint8_t > content(k_tiny_lba_size, 0xAB);
    auto const csum = crc16_t10dif(k_test_crc16_seed, content.data(), content.size());
    for (uint32_t lba = 0; lba < k_nlbas; ++lba) {
        homestore::blk_num_t const blk_num = 1000 + lba; // one contiguous run across all k_nlbas LBAs
        index.entries[lba] = BlockInfo{homestore::blk_id{blk_num, 1, /* chunk = */ 1}, homestore::blk_id{}, csum};
        journal->seed_block(blk_num, content);
    }

    std::vector< uint8_t > dest_buf(static_cast< size_t >(k_nlbas) * k_tiny_lba_size, 0xFF);
    sisl::sg_list dest;
    dest.size = dest_buf.size();
    dest.iovs.push_back(iovec{dest_buf.data(), dest_buf.size()});
    auto r = homeblocks::detail::sync_get(dev->read_with(
        /* read_lsn = */ 10, /* addr = */ 0, dest.size, std::move(dest),
        [&index](lba_t s, lba_t e, std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >& out) {
            return index.read_from_index(s, e, out);
        }));
    ASSERT_TRUE(r.has_value());
    // The split is invisible to the caller: both batches return the same content, so the final
    // adjacent-extent merge collapses them back into a single extent.
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_FALSE(r->extents[0].hole);
    for (uint32_t lba = 0; lba < k_nlbas; ++lba)
        EXPECT_EQ(
            std::memcmp(dest_buf.data() + static_cast< size_t >(lba) * k_tiny_lba_size, content.data(), k_tiny_lba_size),
            0);
}

// An in-horizon overlay entry must win over an EXISTING committed index entry for the same LBA
// (not just "no index entry at all", which ReadOverlayOnly already covers) -- the overlay is
// strictly newer, so its content must be served, not the index's stale one.
TEST_F(CraftCommitTest, OverlayWinsOverCommittedIndexForSameLba) {
    std::vector< uint8_t > old_content(k_page_size, 0xCD);
    seed_index_entry(/* lba = */ 0, /* blk_num = */ 500, old_content);
    dev_->seed_commit_lsn(2);

    ASSERT_TRUE(do_write_data(0, /* dlsn = */ 3, /* lba = */ 0, /* nlbas = */ 1).has_value());

    auto r = do_read(/* read_lsn = */ 3, /* lba = */ 0, /* nlbas = */ 1);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_FALSE(r->extents[0].hole);
    EXPECT_EQ(dest_buf_, std::vector< uint8_t >(k_page_size, 0xAB)); // overlay's fill, NOT old_content
}

// The horizon clamp is inclusive: an overlay entry whose lsn EQUALS read_lsn must be served, not
// treated as "above the horizon" -- HorizonClampServesIndexNotOverlayAboveReadLsn only covers the
// strictly-greater-than case.
TEST_F(CraftCommitTest, HorizonBoundaryEqualLsnServed) {
    dev_->seed_commit_lsn(2);
    ASSERT_TRUE(do_write_data(0, /* dlsn = */ 3, /* lba = */ 0, /* nlbas = */ 1).has_value());

    auto r = do_read(/* read_lsn = */ 3, /* lba = */ 0, /* nlbas = */ 1); // read_lsn == overlay's lsn
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_FALSE(r->extents[0].hole);
    EXPECT_EQ(dest_buf_, std::vector< uint8_t >(k_page_size, 0xAB)); // served, not clamped away
}

// read_lsn below commit_lsn is unanswerable: the index has already blind-overwritten past commit_lsn
// (apply is destructive by design), so a caller asking for a horizon strictly below the frontier must
// be rejected rather than silently served the too-new committed value.
TEST_F(CraftCommitTest, ReadRejectsStaleHorizon) {
    dev_->seed_commit_lsn(10);

    auto r = do_read(/* read_lsn = */ 5, /* lba = */ 0, /* nlbas = */ 1); // 5 < commit_lsn=10
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::HORIZON_STALE));
}

// A single read spanning three distinct sources in one call: committed index data, an absent hole,
// and an in-horizon overlay entry -- proving the per-LBA source resolution and extent-building logic
// handle all three simultaneously, not just any two at a time.
TEST_F(CraftCommitTest, ThreeWayMixedExtentRead) {
    std::vector< uint8_t > index_content(k_page_size, 0xCD);
    seed_index_entry(/* lba = */ 0, /* blk_num = */ 500, index_content);
    // lba=1 left absent -> hole
    dev_->seed_commit_lsn(2);
    ASSERT_TRUE(do_write_data(0, /* dlsn = */ 3, /* lba = */ 2, /* nlbas = */ 1).has_value());

    auto r = do_read(/* read_lsn = */ 3, /* lba = */ 0, /* nlbas = */ 3);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 3u);
    EXPECT_FALSE(r->extents[0].hole); // lba=0: index
    EXPECT_EQ(r->extents[0].addr, 0u);
    EXPECT_EQ(r->extents[0].len, k_page_size);
    EXPECT_TRUE(r->extents[1].hole); // lba=1: absent
    EXPECT_EQ(r->extents[1].addr, k_page_size);
    EXPECT_FALSE(r->extents[2].hole); // lba=2: overlay
    EXPECT_EQ(r->extents[2].addr, 2 * k_page_size);

    std::vector< uint8_t > expected(index_content);
    expected.insert(expected.end(), k_page_size, 0);
    expected.insert(expected.end(), k_page_size, 0xAB); // do_write_data's fixed fill
    EXPECT_EQ(dest_buf_, expected);
}

// A write whose LBA range only PARTIALLY overlaps existing committed entries: the untouched LBA
// must still read from the index, while the overlapping AND newly-covered LBAs read from the
// overlay -- proving per-LBA resolution isn't confused by a write that straddles a boundary rather
// than exactly matching prior LBA ranges.
TEST_F(CraftCommitTest, PartialOverlapWriteResolvesPerLba) {
    seed_index_entry(0, /* blk_num = */ 500, std::vector< uint8_t >(k_page_size, 0xA0));
    seed_index_entry(1, /* blk_num = */ 501, std::vector< uint8_t >(k_page_size, 0xA1));
    seed_index_entry(2, /* blk_num = */ 502, std::vector< uint8_t >(k_page_size, 0xA2));
    dev_->seed_commit_lsn(5);

    // Overlaps committed lba=1,2 and additionally covers new lba=3.
    ASSERT_TRUE(do_write_data(0, /* dlsn = */ 6, /* lba = */ 1, /* nlbas = */ 3).has_value());

    auto r = do_read(/* read_lsn = */ 6, /* lba = */ 0, /* nlbas = */ 4);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u); // index-data and overlay-data are both "data" -- one merged extent
    EXPECT_FALSE(r->extents[0].hole);
    EXPECT_EQ(r->extents[0].len, 4 * k_page_size);

    // lba=0: untouched by the write -- still the index's original content.
    std::vector< uint8_t > lba0(dest_buf_.begin(), dest_buf_.begin() + k_page_size);
    EXPECT_EQ(lba0, std::vector< uint8_t >(k_page_size, 0xA0));
    // lba=1,2,3: all served from the overlay now, including the two that were previously committed.
    for (uint32_t i = 1; i < 4; ++i) {
        std::vector< uint8_t > seg(dest_buf_.begin() + i * k_page_size, dest_buf_.begin() + (i + 1) * k_page_size);
        EXPECT_EQ(seg, std::vector< uint8_t >(k_page_size, 0xAB)) << "lba=" << i;
    }
}

// The end-to-end visibility transition: before commit, a write is only readable via the overlay;
// after commit, the overlay entry is retired and the SAME content is now served from the index.
TEST_F(CraftCommitTest, CommitTransitionsReadFromOverlayToIndex) {
    dev_->seed_lsns(-1, {});
    ASSERT_TRUE(do_write_data(0, /* dlsn = */ 0, /* lba = */ 0, /* nlbas = */ 1).has_value());

    auto r1 = do_read(/* read_lsn = */ 0, /* lba = */ 0, /* nlbas = */ 1);
    ASSERT_TRUE(r1.has_value());
    EXPECT_FALSE(r1->extents[0].hole);
    EXPECT_EQ(dest_buf_, std::vector< uint8_t >(k_page_size, 0xAB));
    EXPECT_NE(dev_->overlay_lsn_for(0), -1); // still only in the overlay

    ASSERT_TRUE(do_commit(0).has_value());
    EXPECT_EQ(dev_->overlay_lsn_for(0), -1); // retired

    auto r2 = do_read(/* read_lsn = */ 0, /* lba = */ 0, /* nlbas = */ 1);
    ASSERT_TRUE(r2.has_value());
    EXPECT_FALSE(r2->extents[0].hole);
    EXPECT_EQ(dest_buf_, std::vector< uint8_t >(k_page_size, 0xAB)); // same content, now from the index
}

// read()'s own term fencing -- mirrors write()'s TermRejection. The term check runs before the
// !indx_tbl_ guard, so this is reachable even with indx_tbl_ == nullptr in this fixture.
TEST_F(CraftCommitTest, ReadRejectsStaleTerm) {
    dev_->seed_term(7);
    std::vector< uint8_t > buf(k_page_size, 0xFF);
    sisl::sg_list dest;
    dest.size = buf.size();
    dest.iovs.push_back(iovec{buf.data(), buf.size()});

    auto r = homeblocks::detail::sync_get(
        dev_->read(craft::client_hdr{/* term = */ 3, -1, -1}, /* read_lsn = */ 0, 0, k_page_size, std::move(dest)));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
}

// read()'s own boundary validation -- mirrors write()'s ZeroLenRejected/UnalignedLenRejected/
// UnalignedAddrRejected. read_impl has the identical nlbas=0 underflow risk write() guards against.
TEST_F(CraftCommitTest, ReadZeroLenRejected) {
    auto r = do_read(/* read_lsn = */ 10, /* lba = */ 0, /* nlbas = */ 0);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(std::errc::invalid_argument));
}

TEST_F(CraftCommitTest, ReadUnalignedLenRejected) {
    dest_buf_.assign(1, 0xFF);
    sisl::sg_list dest;
    dest.size = dest_buf_.size();
    dest.iovs.push_back(iovec{dest_buf_.data(), dest_buf_.size()});
    auto r = homeblocks::detail::sync_get(
        dev_->read_with(10, 0, /* len = */ k_page_size / 2, std::move(dest),
                        [this](lba_t s, lba_t e, std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >& out) {
                            return index_.read_from_index(s, e, out);
                        }));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(std::errc::invalid_argument));
}

TEST_F(CraftCommitTest, ReadUnalignedAddrRejected) {
    dest_buf_.assign(k_page_size, 0xFF);
    sisl::sg_list dest;
    dest.size = dest_buf_.size();
    dest.iovs.push_back(iovec{dest_buf_.data(), dest_buf_.size()});
    auto r = homeblocks::detail::sync_get(
        dev_->read_with(10, /* addr = */ 1, k_page_size, std::move(dest),
                        [this](lba_t s, lba_t e, std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >& out) {
                            return index_.read_from_index(s, e, out);
                        }));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(std::errc::invalid_argument));
}

// ── rebuild_overlay() ─────────────────────────────────────────────────────────

// A fresh instance's overlay_ is reconstructed from the journal alone: every present entry above
// commit_lsn populates the overlay exactly as write()'s own post-flight update would have.
TEST_F(CraftCommitTest, RebuildOverlayPopulatesFromJournal) {
    dev_->seed_commit_lsn(2);
    dev_->seed_lsns(5, {});
    journal_->add_data_slot(3, /* lba = */ 0, /* nlbas = */ 1, /* blk_num = */ 700, {31});
    journal_->add_data_slot(4, /* lba = */ 1, /* nlbas = */ 1, /* blk_num = */ 701, {32});
    journal_->add_data_slot(5, /* lba = */ 2, /* nlbas = */ 1, /* blk_num = */ 702, {33});

    auto r = homeblocks::detail::sync_get(dev_->rebuild_overlay());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->overlay_lsn_for(0), 3);
    EXPECT_EQ(dev_->overlay_lsn_for(1), 4);
    EXPECT_EQ(dev_->overlay_lsn_for(2), 5);
}

// Missing and Empty-verdicted lsns within the walked range are skipped (no journal read attempted --
// if the walk tried to read_slot a missing lsn, MockCraftJournalBackend would error and the whole
// rebuild would fail), not treated as a stop condition like commit_impl()'s gap handling.
TEST_F(CraftCommitTest, RebuildOverlaySkipsMissingAndEmpty) {
    dev_->seed_commit_lsn(0);
    dev_->seed_lsns(4, {2}); // lsn=2 missing -- deliberately has no journal slot
    dev_->seed_empty({3});   // lsn=3 Empty-verdicted -- also has no journal slot
    journal_->add_data_slot(1, /* lba = */ 0, /* nlbas = */ 1, /* blk_num = */ 700, {11});
    journal_->add_data_slot(4, /* lba = */ 1, /* nlbas = */ 1, /* blk_num = */ 701, {14});

    auto r = homeblocks::detail::sync_get(dev_->rebuild_overlay());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->overlay_lsn_for(0), 1); // before the gap
    EXPECT_EQ(dev_->overlay_lsn_for(1), 4); // after both the gap and the Empty slot
}

// Two entries touching the same LBA at different lsns within the walked range: the higher-dLSN one
// must win, same rule write() itself already applies.
TEST_F(CraftCommitTest, RebuildOverlayHighestDlsnWins) {
    dev_->seed_commit_lsn(0);
    dev_->seed_lsns(2, {});
    journal_->add_data_slot(1, /* lba = */ 0, /* nlbas = */ 1, /* blk_num = */ 700, {11});
    journal_->add_data_slot(2, /* lba = */ 0, /* nlbas = */ 1, /* blk_num = */ 701, {12}); // supersedes lsn=1

    auto r = homeblocks::detail::sync_get(dev_->rebuild_overlay());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->overlay_lsn_for(0), 2);
}

// An all_zeros entry within the walked range populates the overlay's all_zeros marker, not a data
// (blkid+csum) entry.
TEST_F(CraftCommitTest, RebuildOverlayHandlesAllZeros) {
    dev_->seed_commit_lsn(0);
    dev_->seed_lsns(1, {});
    journal_->slots[1] = JournalSlot{.lsn = 1, .all_zeros = true, .lba_off_bytes = 0, .len_bytes = k_page_size};

    auto r = homeblocks::detail::sync_get(dev_->rebuild_overlay());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->overlay_lsn_for(0), 1);

    // Confirm it reads as a hole (the all_zeros marker, not stray data) -- same read path Read tests
    // already exercise for a live all_zeros overlay entry.
    auto read_r = do_read(/* read_lsn = */ 1, /* lba = */ 0, /* nlbas = */ 1);
    ASSERT_TRUE(read_r.has_value());
    EXPECT_TRUE(read_r->extents[0].hole);
}

// A read_slot() failure for a present (non-missing/non-empty) lsn propagates out of rebuild_overlay()
// rather than silently leaving a partially-reconstructed overlay.
TEST_F(CraftCommitTest, RebuildOverlayPropagatesReadSlotError) {
    dev_->seed_commit_lsn(0);
    dev_->seed_lsns(1, {});
    journal_->add_data_slot(1, /* lba = */ 0, /* nlbas = */ 1, /* blk_num = */ 700, {11});
    journal_->fail_on_read = 1;

    auto r = homeblocks::detail::sync_get(dev_->rebuild_overlay());
    ASSERT_FALSE(r.has_value());
}

// ── restart recovery gate (SDSTOR-22905) ──────────────────────────────────────

// Per subtasks.md/SDSTOR-22905, the overlay must be fully rebuilt before the volume accepts new
// client I/O. trigger_on_restart() drives the EXACT production path (CraftRaftListener::on_restart()
// -> run_recovery() -> rebuild_overlay()), with the mock's read_slot() gated closed so the rebuild
// genuinely blocks -- proving write()/read()/keep_alive() all reject with OFFLINE while recovering_
// is set, and the gate is gone (recovering_ false, overlay actually rebuilt) once it completes.
//
// Uses the real public write()/read()/keep_alive() here, not do_write_data()/do_read() (which go
// through commit_with()/read_with()'s test seams and would bypass the gate entirely, since it lives
// in write()/read()/keep_alive() themselves, not commit_impl()/read_impl()). This fixture's indx_tbl_
// is null, so read() still fails not_supported once the gate is gone -- that's expected and besides
// the point here; what matters is that it's a DIFFERENT error, proving OFFLINE was the gate, not an
// unrelated rejection that happened to look similar.
TEST_F(CraftCommitTest, RestartRecoveryGatesClientIoUntilOverlayRebuilt) {
    dev_->seed_term(7);
    dev_->seed_commit_lsn(0);
    dev_->seed_lsns(1, {});
    journal_->add_data_slot(1, /* lba = */ 0, /* nlbas = */ 1, /* blk_num = */ 700, {11});
    journal_->close_read_gate();

    EXPECT_FALSE(dev_->is_recovering());
    std::thread restart_thread([this] { dev_->trigger_on_restart(); });

    // trigger_on_restart() sets recovering_ BEFORE detaching run_recovery() (which is what then blocks
    // on the closed read gate) -- so this becomes true almost immediately; a short, generous wait
    // avoids a hard poll loop without making the test slow in the common case.
    for (int i = 0; i < 1000 && !dev_->is_recovering(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(dev_->is_recovering());

    static std::vector< uint8_t > buf(k_page_size, 0xAA);
    sisl::sg_list wdata;
    wdata.size = k_page_size;
    wdata.iovs.push_back(iovec{buf.data(), wdata.size});
    auto write_r =
        homeblocks::detail::sync_get(dev_->write(craft::client_hdr{7, -1, -1}, /* dlsn = */ 10,
                                                 /* addr = */ 5 * k_page_size, k_page_size, std::move(wdata)));
    ASSERT_FALSE(write_r.has_value());
    EXPECT_EQ(write_r.error(), volume_error::OFFLINE);

    dest_buf_.assign(k_page_size, 0xFF);
    sisl::sg_list rdata;
    rdata.size = dest_buf_.size();
    rdata.iovs.push_back(iovec{dest_buf_.data(), dest_buf_.size()});
    auto read_r = homeblocks::detail::sync_get(dev_->read(craft::client_hdr{7, -1, -1}, /* read_lsn = */ 0,
                                                          /* addr = */ 5 * k_page_size, k_page_size, std::move(rdata)));
    ASSERT_FALSE(read_r.has_value());
    EXPECT_EQ(read_r.error(), volume_error::OFFLINE);

    auto ka_r = homeblocks::detail::sync_get(dev_->keep_alive(craft::client_hdr{7, -1, -1}));
    ASSERT_FALSE(ka_r.has_value());
    EXPECT_EQ(ka_r.error(), volume_error::OFFLINE);

    journal_->open_read_gate();
    restart_thread.join();

    EXPECT_FALSE(dev_->is_recovering());
    EXPECT_EQ(dev_->overlay_lsn_for(0), 1); // the rebuild actually completed, not just unblocked

    // The gate is gone: keep_alive() (which needs no index) succeeds again.
    auto ka_r2 = homeblocks::detail::sync_get(dev_->keep_alive(craft::client_hdr{7, -1, -1}));
    EXPECT_TRUE(ka_r2.has_value());

    // read() now fails for the ordinary, unrelated reason (no index configured) -- not OFFLINE.
    dest_buf_.assign(k_page_size, 0xFF);
    sisl::sg_list rdata2;
    rdata2.size = dest_buf_.size();
    rdata2.iovs.push_back(iovec{dest_buf_.data(), dest_buf_.size()});
    auto read_r2 =
        homeblocks::detail::sync_get(dev_->read(craft::client_hdr{7, -1, -1}, /* read_lsn = */ 0,
                                                /* addr = */ 5 * k_page_size, k_page_size, std::move(rdata2)));
    ASSERT_FALSE(read_r2.has_value());
    EXPECT_EQ(read_r2.error(), std::make_error_condition(std::errc::not_supported));
}

// A restart recovery whose rebuild_overlay() FAILS (e.g. a corrupt journal record) must permanently
// fault the partition rather than clearing the gate and resuming I/O against a silently incomplete
// overlay -- entries at and above the failing lsn were never populated, so a naive "always clear
// recovering_" would let a subsequent read for an LBA only covered by one of those entries silently
// fall through to stale, already-superseded state with no error. recovering_ itself is deliberately
// left set (never cleared) on this path too -- recovery_faulted_ is the flag that actually governs
// client-facing rejection from here on (checked first, ahead of recovering_, by every entry point), but
// leaving recovering_ set as well is a defensive belt-and-suspenders, not a load-bearing distinction.
TEST_F(CraftCommitTest, FailedRestartRecoveryPermanentlyFaultsThePartition) {
    dev_->seed_term(7);
    dev_->seed_commit_lsn(0);
    dev_->seed_lsns(1, {});
    journal_->add_data_slot(1, /* lba = */ 0, /* nlbas = */ 1, /* blk_num = */ 700, {11});
    journal_->fail_on_read = 1;

    EXPECT_FALSE(dev_->is_recovery_faulted());
    // read_gate stays open (default) -- run_recovery() runs to completion (with a failure) synchronously
    // within this call, same as every other trigger_on_restart() use in this file.
    dev_->trigger_on_restart();
    EXPECT_TRUE(dev_->is_recovery_faulted());

    static std::vector< uint8_t > buf(k_page_size, 0xAA);
    sisl::sg_list wdata;
    wdata.size = k_page_size;
    wdata.iovs.push_back(iovec{buf.data(), wdata.size});
    auto write_r =
        homeblocks::detail::sync_get(dev_->write(craft::client_hdr{7, -1, -1}, /* dlsn = */ 10,
                                                 /* addr = */ 5 * k_page_size, k_page_size, std::move(wdata)));
    ASSERT_FALSE(write_r.has_value());
    EXPECT_EQ(write_r.error(), volume_error::INTERNAL_ERROR);

    dest_buf_.assign(k_page_size, 0xFF);
    sisl::sg_list rdata;
    rdata.size = dest_buf_.size();
    rdata.iovs.push_back(iovec{dest_buf_.data(), dest_buf_.size()});
    auto read_r = homeblocks::detail::sync_get(dev_->read(craft::client_hdr{7, -1, -1}, /* read_lsn = */ 0,
                                                          /* addr = */ 5 * k_page_size, k_page_size, std::move(rdata)));
    ASSERT_FALSE(read_r.has_value());
    EXPECT_EQ(read_r.error(), volume_error::INTERNAL_ERROR);

    auto ka_r = homeblocks::detail::sync_get(dev_->keep_alive(craft::client_hdr{7, -1, -1}));
    ASSERT_FALSE(ka_r.has_value());
    EXPECT_EQ(ka_r.error(), volume_error::INTERNAL_ERROR);
}

// ── truncate() overlay pruning ────────────────────────────────────────────────

// truncate() must prune any overlay entry referencing a now-rolled-back dLSN -- otherwise a
// subsequent read could serve stale data from a write that no longer exists in the journal
// (subtasks.md's S3 AC: the overlay is "updated on append/apply/truncate").
TEST_F(CraftCommitTest, TruncateRemovesOverlayEntriesAboveLsn) {
    // Deliberately NOT seeding last_append_lsn ahead of these writes: doing so would make write()'s
    // own idempotent short-circuit (dlsn <= last_append_lsn && not missing) treat dlsn=3 as "already
    // written" and skip overlay population entirely. Starting from the default -1 lets both writes
    // advance last_append_lsn for real.
    ASSERT_TRUE(do_write_data(0, /* dlsn = */ 3, /* lba = */ 0, /* nlbas = */ 1).has_value());
    ASSERT_TRUE(do_write_data(0, /* dlsn = */ 5, /* lba = */ 1, /* nlbas = */ 1).has_value());
    ASSERT_EQ(dev_->overlay_lsn_for(0), 3);
    ASSERT_EQ(dev_->overlay_lsn_for(1), 5);

    auto r = homeblocks::detail::sync_get(dev_->truncate(4));
    ASSERT_TRUE(r.has_value());

    EXPECT_EQ(dev_->overlay_lsn_for(0), 3);  // survives: 3 <= truncation point 4
    EXPECT_EQ(dev_->overlay_lsn_for(1), -1); // pruned: 5 > truncation point 4, rolled back
}

// ── commit() resume after a gap fills ─────────────────────────────────────────

// After a previously-missing dLSN is filled, a subsequent commit() must apply through it and reach
// the SAME final state a straight no-gap in-order run would have produced (subtasks.md's S3 AC:
// "in-order apply after the hole fills (same stable state as a no-hole run)").
TEST_F(CraftCommitTest, InOrderApplyAfterHoleFillsMatchesNoGapRun) {
    dev_->seed_lsns(2, {1}); // lsn=1 missing
    journal_->add_data_slot(0, 0, 1, 100, {11});
    journal_->add_data_slot(2, 2, 1, 102, {13});
    // lsn=1 has no slot yet -- matches its missing_lsns_ entry.

    auto stalled = do_commit(2);
    ASSERT_TRUE(stalled.has_value());
    EXPECT_EQ(*stalled, 0); // stalls right before the hole

    // The gap fills: lsn=1 is now written and removed from missing_lsns_ (mirroring what a real
    // write() call does internally when a gap-filling dlsn arrives).
    journal_->add_data_slot(1, 1, 1, 101, {12});
    dev_->seed_lsns(2, {}); // last_append_lsn unchanged; missing_lsns_ now empty

    auto resumed = do_commit(2);
    ASSERT_TRUE(resumed.has_value());
    EXPECT_EQ(*resumed, 2);
    EXPECT_EQ(dev_->commit_lsn(), 2);
    ASSERT_TRUE(index_.entries.count(0));
    ASSERT_TRUE(index_.entries.count(1));
    ASSERT_TRUE(index_.entries.count(2));
    EXPECT_EQ(index_.entries[0].new_checksum, 11);
    EXPECT_EQ(index_.entries[1].new_checksum, 12);
    EXPECT_EQ(index_.entries[2].new_checksum, 13);
}

// ── keep_alive() all_committed_lsn capture ────────────────────────────────────

// keep_alive() must capture hdr.all_committed_lsn into partition state (max-monotonic) so it's
// available for S8's eventual journal reclaim -- the reclaim action itself is not implemented here.
TEST_F(CraftCommitTest, KeepAliveCapturesAllCommittedLsnMonotonically) {
    ASSERT_EQ(dev_->all_committed_lsn(), -1);

    auto r1 = homeblocks::detail::sync_get(dev_->keep_alive(craft::client_hdr{0, -1, /* all_committed_lsn = */ 5}));
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(dev_->all_committed_lsn(), 5);

    // A stale/reordered message with a LOWER all_committed_lsn must not regress the floor.
    auto r2 = homeblocks::detail::sync_get(dev_->keep_alive(craft::client_hdr{0, -1, /* all_committed_lsn = */ 2}));
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(dev_->all_committed_lsn(), 5);

    auto r3 = homeblocks::detail::sync_get(dev_->keep_alive(craft::client_hdr{0, -1, /* all_committed_lsn = */ 9}));
    ASSERT_TRUE(r3.has_value());
    EXPECT_EQ(dev_->all_committed_lsn(), 9);
}

// A malformed all_committed_lsn (negative, but not the -1 "unset" sentinel) must be ignored rather
// than corrupting the floor -- the call itself still succeeds (this is best-effort floor metadata,
// not core I/O semantics that should fail the whole call).
TEST_F(CraftCommitTest, KeepAliveIgnoresMalformedNegativeAllCommittedLsn) {
    auto r1 = homeblocks::detail::sync_get(dev_->keep_alive(craft::client_hdr{0, -1, /* all_committed_lsn = */ 5}));
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(dev_->all_committed_lsn(), 5);

    auto r2 = homeblocks::detail::sync_get(dev_->keep_alive(craft::client_hdr{0, -1, /* all_committed_lsn = */ -7}));
    ASSERT_TRUE(r2.has_value()); // the call itself still succeeds
    EXPECT_EQ(dev_->all_committed_lsn(), 5); // floor unchanged -- malformed value ignored, not applied
}

// ── delete_fn error path ──────────────────────────────────────────────────────

// A delete_fn failure during an all_zeros apply must propagate the error and leave commit_lsn
// unchanged -- mirrors WriteFnErrorAbortsCommit for the delete side of commit_impl().
TEST_F(CraftCommitTest, DeleteFnErrorAbortsCommit) {
    index_.entries[0] = BlockInfo{homestore::blk_id{500, 1, 1}, homestore::blk_id{}, 99};
    dev_->seed_lsns(0, {});
    journal_->slots[0] = JournalSlot{.lsn = 0, .all_zeros = true, .lba_off_bytes = 0, .len_bytes = k_page_size};

    auto r = homeblocks::detail::sync_get(dev_->commit_with(
        0, [](lba_t, lba_t, std::unordered_map< lba_t, BlockInfo >&) { return ok(); },
        [](lba_t, lba_t, std::vector< homestore::blk_id >&) -> status {
            return std::unexpected(volume_error::INDEX_ERROR);
        }));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), -1); // nothing applied
}

// ── read() read_fn error path ─────────────────────────────────────────────────

// A read_fn failure in read_impl() propagates as an error -- proves read() does not silently
// treat index errors as holes (an absent LBA and an index-read error are distinct conditions).
TEST_F(CraftCommitTest, ReadFnErrorPropagates) {
    dest_buf_.assign(k_page_size, 0xFF);
    sisl::sg_list dest;
    dest.size = dest_buf_.size();
    dest.iovs.push_back(iovec{dest_buf_.data(), dest_buf_.size()});

    auto r = homeblocks::detail::sync_get(dev_->read_with(
        /* read_lsn = */ 10, 0, k_page_size, std::move(dest),
        [](lba_t, lba_t, std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >&) -> status {
            return std::unexpected(volume_error::INDEX_ERROR);
        }));
    ASSERT_FALSE(r.has_value());
}

// The pre-index-read horizon check only catches a commit_lsn that advanced BEFORE the snapshot was
// taken. read_fn itself takes no lock, so a concurrent commit_impl can still advance commit_lsn past
// read_lsn WHILE read_fn is executing -- simulated here by having read_fn itself perform the advance,
// standing in for a commit_impl run on another thread interleaving with this query. The post-read_fn
// re-check must catch this and reject, not just the pre-check.
TEST_F(CraftCommitTest, ReadRejectsHorizonAdvancedDuringIndexQuery) {
    dest_buf_.assign(k_page_size, 0xFF);
    sisl::sg_list dest;
    dest.size = dest_buf_.size();
    dest.iovs.push_back(iovec{dest_buf_.data(), dest_buf_.size()});

    auto r = homeblocks::detail::sync_get(dev_->read_with(
        /* read_lsn = */ 5, 0, k_page_size, std::move(dest),
        [this](lba_t s, lba_t e, std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >& out) {
            // Stands in for a concurrent commit_impl run advancing the frontier past read_lsn while
            // this index query is in flight.
            dev_->seed_commit_lsn(10);
            return index_.read_from_index(s, e, out);
        }));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::HORIZON_STALE));
}

// ── read() negative read_lsn ──────────────────────────────────────────────────

// A negative read_lsn must be rejected: lsn=-1 would make every overlay entry's lsn<=read_lsn
// check silently false (no real lsn is negative), serving a committed-only view with no error.
TEST_F(CraftCommitTest, ReadNegativeLsnRejected) {
    auto r = do_read(/* read_lsn = */ -1, /* lba = */ 0, /* nlbas = */ 1);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(std::errc::invalid_argument));
}

// ── keep_alive() term fencing ─────────────────────────────────────────────────

// keep_alive() must reject a stale term -- mirrors write()'s TermRejection.
TEST_F(CraftCommitTest, KeepAliveRejectsStaleTerm) {
    dev_->seed_term(5);
    auto r = homeblocks::detail::sync_get(dev_->keep_alive(craft::client_hdr{/* term = */ 3, -1, -1}));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
}

// ── C1: csum OOB guard in commit_impl() ───────────────────────────────────────

// A slot whose persisted csums array is shorter than the blkid piece count must abort the commit
// rather than access csums[csum_idx] out of bounds -- exercises the new C1 defense-in-depth guard.
TEST_F(CraftCommitTest, CommitCsumShortArrayAborts) {
    dev_->seed_lsns(0, {});
    // Slot claims 2 LBAs (nlbas=2) but supplies only 1 checksum -- simulates a corrupt on-disk record.
    homestore::multi_blk_id blkid{500, /* nblks = */ 2, /* chunk = */ 1};
    journal_->slots[0] =
        JournalSlot{.lsn = 0, .lba_off_bytes = 0, .len_bytes = 2 * k_page_size, .blkid = blkid, .csums = {11}};

    auto r = do_commit(0);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), -1); // commit aborted before any index write
}

// ── C2: nlbas==0 guard in rebuild_overlay() ───────────────────────────────────

// A slot with len_bytes=0 (nlbas=0) within the walked range must be skipped with a log error --
// exercises the new C2 guard, matching commit_impl()'s identical protection. The slot after the
// malformed one must still be processed (skip, not abort).
TEST_F(CraftCommitTest, RebuildOverlaySkipsNlbasZeroSlot) {
    dev_->seed_commit_lsn(0);
    dev_->seed_lsns(2, {});
    // lsn=1: malformed zero-len record; lsn=2: valid data slot covering lba=5.
    journal_->slots[1] = JournalSlot{.lsn = 1, .lba_off_bytes = 0, .len_bytes = 0};
    journal_->add_data_slot(2, /* lba = */ 5, /* nlbas = */ 1, /* blk_num = */ 800, {42});

    auto r = homeblocks::detail::sync_get(dev_->rebuild_overlay());
    ASSERT_TRUE(r.has_value());             // rebuild succeeds despite the malformed slot
    EXPECT_EQ(dev_->overlay_lsn_for(5), 2); // valid slot after the bad one was still processed
}

// ── C5: dest.iovs.empty() guard in read_impl() ───────────────────────────────

// A dest sg_list with no iovecs must be rejected before read_impl() accesses dest.iovs[0] --
// exercises the new C5 guard. The len check passes (aligned, non-zero), but the iovec guard fires.
TEST_F(CraftCommitTest, ReadDestEmptyIovsRejected) {
    sisl::sg_list dest;
    dest.size = k_page_size; // non-zero size claimed but no backing iovec

    auto r = homeblocks::detail::sync_get(dev_->read_with(
        /* read_lsn = */ 0, 0, k_page_size, std::move(dest),
        [this](lba_t s, lba_t e, std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >& out) {
            return index_.read_from_index(s, e, out);
        }));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(std::errc::invalid_argument));
}

// A dest iovec that's non-empty but too small for the requested range must be rejected -- an
// undersized iov_len would otherwise have the memset/read_data calls write past the buffer's end.
TEST_F(CraftCommitTest, ReadDestUndersizedIovLenRejected) {
    std::vector< uint8_t > small_buf(k_page_size / 2); // half the size a 1-page read needs
    sisl::sg_list dest;
    dest.size = k_page_size; // claims a full page, but the backing iovec is only half that
    dest.iovs.push_back(iovec{small_buf.data(), small_buf.size()});

    auto r = homeblocks::detail::sync_get(dev_->read_with(
        /* read_lsn = */ 0, 0, k_page_size, std::move(dest),
        [this](lba_t s, lba_t e, std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >& out) {
            return index_.read_from_index(s, e, out);
        }));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(std::errc::invalid_argument));
}

// ── R1-1: all_committed_lsn captured by write() ──────────────────────────────

// write() must advance all_committed_lsn max-monotonically (the R1-1 fix) -- without this,
// only keep_alive() messages advance the reclaim floor, leaving write-only partitions permanently
// stuck at -1 even after the cluster has durably committed far ahead of them.
TEST_F(CraftCommitTest, WriteCapturesAllCommittedLsn) {
    ASSERT_EQ(dev_->all_committed_lsn(), -1);

    // A write with all_committed_lsn piggybacked advances the floor.
    static std::vector< uint8_t > buf(k_page_size, 0xAB);
    sisl::sg_list data;
    data.size = k_page_size;
    data.iovs.push_back(iovec{buf.data(), k_page_size});
    auto r = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{/* term = */ 0, /* commit_lsn = */ -1, /* all_committed_lsn = */ 7},
                    /* dlsn = */ 0, 0, k_page_size, std::move(data)));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->all_committed_lsn(), 7);

    // A subsequent write with a LOWER all_committed_lsn must not regress the floor.
    static std::vector< uint8_t > buf2(k_page_size, 0xAB);
    sisl::sg_list data2;
    data2.size = k_page_size;
    data2.iovs.push_back(iovec{buf2.data(), k_page_size});
    auto r2 = homeblocks::detail::sync_get(dev_->write(craft::client_hdr{0, -1, /* all_committed_lsn = */ 3},
                                                       /* dlsn = */ 1, k_page_size, k_page_size, std::move(data2)));
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(dev_->all_committed_lsn(), 7); // not regressed
}

// ── R1-1: all_committed_lsn captured by read() ───────────────────────────────

// read() must capture hdr.all_committed_lsn (the R1-1 fix applies to read() too). The real
// entry point is used (not read_with()) because only read() accepts a client_hdr. Even though
// read() returns not_supported (no index configured in this fixture), the capture happens in the
// missing_mu_ block ahead of the !indx_tbl_ check, so the floor is advanced before the error.
TEST_F(CraftCommitTest, ReadCapturesAllCommittedLsn) {
    ASSERT_EQ(dev_->all_committed_lsn(), -1);

    dest_buf_.assign(k_page_size, 0xFF);
    sisl::sg_list dest;
    dest.size = dest_buf_.size();
    dest.iovs.push_back(iovec{dest_buf_.data(), dest_buf_.size()});
    auto r = homeblocks::detail::sync_get(
        dev_->read(craft::client_hdr{/* term = */ 0, /* commit_lsn = */ -1, /* all_committed_lsn = */ 4},
                   /* read_lsn = */ 0, /* addr = */ 0, k_page_size, std::move(dest)));
    EXPECT_FALSE(r.has_value()); // fails (not_supported: no index), but floor was captured first
    EXPECT_EQ(dev_->all_committed_lsn(), 4);
}

// ── craft_max_io_len_mb enforcement in read() ────────────────────────────────

// read() enforces the same craft_max_io_len_mb cap as write(). craft_max_io_len_mb is set to 1 in
// main(), so a len of 1 MiB + one page (page-aligned, non-zero) exceeds the limit and is rejected.
TEST_F(CraftCommitTest, ReadMaxIoLenEnforced) {
    constexpr uint64_t k_over_limit = 1024 * 1024 + k_page_size;
    dest_buf_.assign(k_page_size, 0xFF); // small real buffer -- len check fires before any read
    sisl::sg_list dest;
    dest.size = k_over_limit;
    dest.iovs.push_back(iovec{dest_buf_.data(), k_over_limit}); // oversized claim, real check fires first
    auto r = homeblocks::detail::sync_get(dev_->read_with(
        /* read_lsn = */ 0, 0, k_over_limit, std::move(dest),
        [this](lba_t s, lba_t e, std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >& out) {
            return index_.read_from_index(s, e, out);
        }));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(std::errc::invalid_argument));
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    // craft_watchdog_timeout_ms=0: watchdog disabled (no iomgr needed).
    // craft_max_io_len_mb=1: small cap for MaxIoLen tests; all other tests use <= 12 KiB, well below 1 MiB.
    HB_SETTINGS_FACTORY().load_json("{\"craft_watchdog_timeout_ms\": 0, \"craft_max_io_len_mb\": 1}");
    return RUN_ALL_TESTS();
}
