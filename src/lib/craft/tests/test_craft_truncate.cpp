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

// Unit tests for CraftReplDev::truncate (S4: Truncate Path).
//
// Scope: truncate() drops every slot above lsn from the replica's state (last_append_lsn, the missing and Empty sets)
// and frees the blocks of the ones it held, without touching commit_lsn. It is logical: the journal records stay, so
// the mock never removes a seeded slot. Below a slot already indexed it fails and changes nothing.
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles
// craft_repl_dev.cpp directly (not via the ${PROJECT_NAME}_craft OBJECT lib which is linked
// into the main library together with homeblks_impl.cpp that owns the definition normally).

#include <map>
#include <vector>

#include <gtest/gtest.h>
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

// ── minimal journal mock ──────────────────────────────────────────────────────
//
// Records every block freed. truncate() frees through free_slot(), which frees the block a seeded slot's record
// references, as the real backend does.

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots; // seeded directly by tests -- see add_data_slot/add_all_zeros_slot
    std::vector< homestore::multi_blk_id > freed_blkids; // every free_data() call, in order

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const& /* data */,
                                                             lba_count_t /* len */) override {
        co_return homestore::multi_blk_id{};
    }

    async_status write_slot(int64_t, uint64_t, lba_t, lba_count_t, homestore::multi_blk_id, bool,
                            std::vector< homestore::csum_t > const&) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override {
        auto it = slots.find(lsn);
        if (it == slots.end()) co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
        co_return it->second;
    }

    async_status free_data(homestore::multi_blk_id blkid) override {
        freed_blkids.push_back(blkid);
        co_return ok();
    }

    // A slot seeded only through seed_lsns() has no record, so there's nothing to free.
    async_status free_slot(int64_t lsn) override {
        auto it = slots.find(lsn);
        if (it == slots.end() || it->second.all_zeros) co_return ok();
        co_return co_await free_data(it->second.blkid);
    }
    async_status read_data(homestore::multi_blk_id, sisl::sg_list&) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    // Seed a data (non-all_zeros) slot referencing a single-block blkid.
    void add_data_slot(int64_t lsn, homestore::blk_num_t blk_num) {
        slots[lsn] = JournalSlot{.lsn = lsn, .blkid = homestore::multi_blk_id{blk_num, 1, /* chunk = */ 1}};
    }

    // Seed an all_zeros slot -- no blkid, nothing to free.
    void add_all_zeros_slot(int64_t lsn) { slots[lsn] = JournalSlot{.lsn = lsn, .all_zeros = true}; }
};

// ── test fixture ─────────────────────────────────────────────────────────────

class CraftTruncateTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto mock = std::make_unique< MockCraftJournalBackend >();
        journal_ = mock.get();
        dev_ = CraftReplDev::create(volume_id_t{}, std::move(mock), k_page_size, nullptr);
    }
    void TearDown() override { EXPECT_EQ(dev_->invariant_violations(), std::vector< std::string >{}); }

    auto do_truncate(int64_t lsn) { return homeblocks::detail::sync_get(dev_->truncate(lsn)); }

    MockCraftJournalBackend* journal_{nullptr};
    std::shared_ptr< CraftReplDev > dev_;
};

// ── the frontier and the missing set ──────────────────────────────────────────

// lsn < last_append: last_append is clamped to lsn, and the gaps above it go.
TEST_F(CraftTruncateTest, TruncateClampedLastAppend) {
    dev_->seed_lsns(100, {85, 92});
    auto r = do_truncate(80);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 80);
    EXPECT_EQ(dev_->missing_count(), 0u);
}

// lsn == last_append: nothing to drop.
TEST_F(CraftTruncateTest, TruncateAtLastAppend) {
    dev_->seed_lsns(100, {});
    journal_->add_data_slot(100, /* blk_num = */ 500);
    auto r = do_truncate(100);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 100);
    EXPECT_TRUE(journal_->freed_blkids.empty());
}

// lsn > last_append: last_append is already below; it must NOT be raised.
TEST_F(CraftTruncateTest, TruncateAboveLastAppend) {
    dev_->seed_lsns(80, {});
    auto r = do_truncate(100);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 80); // unchanged
}

// Missing entries strictly above the truncation point are removed; those at or below survive.
TEST_F(CraftTruncateTest, MissingSetPartiallyErased) {
    dev_->seed_lsns(100, {70, 80, 91, 95});
    auto r = do_truncate(90);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 90);
    EXPECT_EQ(dev_->missing_count(), 2u); // 70 and 80 survive
    EXPECT_TRUE(dev_->is_missing(70));
    EXPECT_TRUE(dev_->is_missing(80));
    EXPECT_FALSE(dev_->is_missing(91));
    EXPECT_FALSE(dev_->is_missing(95));
}

// All missing entries are above the truncation point → missing set fully cleared.
TEST_F(CraftTruncateTest, MissingSetFullyErased) {
    dev_->seed_lsns(100, {91, 95, 99});
    auto r = do_truncate(90);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 90);
    EXPECT_EQ(dev_->missing_count(), 0u);
}

// A missing entry exactly at the truncation point is kept: upper_bound(lsn) erases > lsn only.
TEST_F(CraftTruncateTest, MissingEntryAtTruncationPointKept) {
    dev_->seed_lsns(100, {80, 90, 95});
    auto r = do_truncate(90);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->missing_count(), 2u); // 80 and 90 survive; 95 erased
    EXPECT_TRUE(dev_->is_missing(90));
    EXPECT_FALSE(dev_->is_missing(95));
}

// An Empty verdict above the truncation point goes with the slot; one at or below it stays.
TEST_F(CraftTruncateTest, EmptyMarksAboveLsnErased) {
    dev_->seed_lsns(100, {});
    dev_->seed_empty({85, 95});
    auto r = do_truncate(90);
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(dev_->is_empty_slot(85));
    EXPECT_FALSE(dev_->is_empty_slot(95));
}

// ── blocks: truncate() frees the blocks of the slots it drops ─────────────────
//
// The records stay (a logical truncate), so nothing else will ever free these blocks. A slot this replica never held
// (missing) or already voided (Empty) has none to free, even if a record sits at its dLSN.

// Two dropped data entries above lsn: both blocks must be freed.
TEST_F(CraftTruncateTest, TruncateFreesBlocksForDroppedDataEntries) {
    dev_->seed_lsns(100, {});
    journal_->add_data_slot(91, /* blk_num = */ 500);
    journal_->add_data_slot(95, /* blk_num = */ 501);

    auto r = do_truncate(90);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(journal_->freed_blkids.size(), 2u);
    EXPECT_EQ(journal_->freed_blkids[0].blk_num(), 500u);
    EXPECT_EQ(journal_->freed_blkids[1].blk_num(), 501u);
}

// An entry at lsn survives and must not be freed.
TEST_F(CraftTruncateTest, TruncateDoesNotFreeEntriesAtOrBelowLsn) {
    dev_->seed_lsns(100, {});
    journal_->add_data_slot(90, /* blk_num = */ 400); // at lsn -- survives, must not be freed
    journal_->add_data_slot(91, /* blk_num = */ 500); // above lsn -- dropped, must be freed

    auto r = do_truncate(90);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(journal_->freed_blkids.size(), 1u);
    EXPECT_EQ(journal_->freed_blkids[0].blk_num(), 500u);
}

// An all_zeros entry above lsn has no block to free.
TEST_F(CraftTruncateTest, TruncateSkipsFreeForAllZerosEntries) {
    dev_->seed_lsns(100, {});
    journal_->add_all_zeros_slot(91);

    auto r = do_truncate(90);
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(journal_->freed_blkids.empty());
}

// Records sit at 92 (missing here) and 93 (voted Empty, its blocks already freed): freeing either would free blocks
// this replica doesn't hold, so only 91's go.
TEST_F(CraftTruncateTest, FreesOnlySlotsThisReplicaHolds) {
    dev_->seed_lsns(100, {92});
    dev_->seed_empty({93});
    journal_->add_data_slot(91, /* blk_num = */ 500);
    journal_->add_data_slot(92, /* blk_num = */ 501);
    journal_->add_data_slot(93, /* blk_num = */ 502);

    auto r = do_truncate(90);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(journal_->freed_blkids.size(), 1u);
    EXPECT_EQ(journal_->freed_blkids[0].blk_num(), 500u);
}

// Truncating again to the same point frees nothing twice: the records are still there, but last_append no longer
// reaches them.
TEST_F(CraftTruncateTest, SecondTruncateFreesNothingAgain) {
    dev_->seed_lsns(100, {});
    journal_->add_data_slot(91, /* blk_num = */ 500);

    ASSERT_TRUE(do_truncate(90).has_value());
    ASSERT_TRUE(do_truncate(90).has_value());

    EXPECT_EQ(journal_->freed_blkids.size(), 1u);
    EXPECT_EQ(dev_->last_append_lsn(), 90);
}

// ── commit_lsn ────────────────────────────────────────────────────────────────

// commit_lsn is invariant: truncate() must not alter it.
TEST_F(CraftTruncateTest, CommitLsnNotTouched) {
    dev_->seed_lsns(50, {});
    auto r = do_truncate(30);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), -1); // default; truncate must not touch it
}

// Below commit_lsn the slots are indexed: truncate() fails in every build and changes nothing. At commit_lsn it's
// fine.
TEST_F(CraftTruncateTest, BelowCommitFailsAndChangesNothing) {
    dev_->seed_lsns(100, {80, 90});
    dev_->seed_commit_lsn(50);
    journal_->add_data_slot(91, /* blk_num = */ 500);

    auto r = do_truncate(40);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), std::make_error_condition(std::errc::invalid_argument));
    EXPECT_EQ(dev_->last_append_lsn(), 100);
    EXPECT_EQ(dev_->missing_count(), 2u);
    EXPECT_TRUE(journal_->freed_blkids.empty());

    ASSERT_TRUE(do_truncate(50).has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 50);
    EXPECT_EQ(dev_->commit_lsn(), 50);
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    HB_SETTINGS_FACTORY().load_json("{\"craft_watchdog_timeout_ms\": 0}");
    return RUN_ALL_TESTS();
}
