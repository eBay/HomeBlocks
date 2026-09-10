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
// Scope: truncate() drops journal entries above lsn, clamps last_append_lsn, and erases
// missing-set entries above lsn, without touching commit_lsn. A journal I/O failure must
// leave partition state unmodified.
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
// Captures the lsn passed to truncate_to() and can be armed to return an I/O error
// so tests can verify the error-path invariant (state unchanged on failure).

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    bool should_fail{false};
    int64_t truncated_to{INT64_MIN};
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

    // Looks up a seeded slot; a never-seeded lsn returns not_supported -- matches the ordinary case
    // of a gap (an lsn this replica never journaled) and lets truncate()'s pre-free scan skip it.
    async_result< JournalSlot > read_slot(int64_t lsn) override {
        auto it = slots.find(lsn);
        if (it == slots.end()) co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
        co_return it->second;
    }

    async_status truncate_to(int64_t lsn) override {
        truncated_to = lsn;
        if (should_fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return ok();
    }
    async_status free_data(homestore::multi_blk_id blkid) override {
        freed_blkids.push_back(blkid);
        co_return ok();
    }

    async_status free_slot(int64_t) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }
    async_status read_data(homestore::multi_blk_id, sisl::sg_list&) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    // Seed a data (non-all_zeros) slot referencing a single-block blkid -- for truncate()'s
    // pre-rollback free scan tests.
    void add_data_slot(int64_t lsn, homestore::blk_num_t blk_num) {
        slots[lsn] = JournalSlot{.lsn = lsn, .blkid = homestore::multi_blk_id{blk_num, 1, /* chunk = */ 1}};
    }

    // Seed an all_zeros slot -- no blkid, nothing for the pre-free scan to free.
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

    auto do_truncate(int64_t lsn) { return homeblocks::detail::sync_get(dev_->truncate(lsn)); }

    MockCraftJournalBackend* journal_{nullptr};
    std::shared_ptr< CraftReplDev > dev_;
};

// ── tests ─────────────────────────────────────────────────────────────────────

// lsn < last_append: last_append is clamped to lsn; entries above lsn are gone.
TEST_F(CraftTruncateTest, TruncateClampedLastAppend) {
    dev_->seed_lsns(100, {85, 92});
    auto r = do_truncate(80);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(journal_->truncated_to, 80);
    EXPECT_EQ(dev_->last_append_lsn(), 80);
    EXPECT_EQ(dev_->missing_count(), 0u);
}

// lsn == last_append: nothing to clamp; journal is still called.
TEST_F(CraftTruncateTest, TruncateAtLastAppend) {
    dev_->seed_lsns(100, {});
    auto r = do_truncate(100);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(journal_->truncated_to, 100);
    EXPECT_EQ(dev_->last_append_lsn(), 100);
}

// lsn > last_append: last_append is already below; it must NOT be raised.
TEST_F(CraftTruncateTest, TruncateAboveLastAppend) {
    dev_->seed_lsns(80, {});
    auto r = do_truncate(100);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(journal_->truncated_to, 100);
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

// ── S4 block leak: truncate() must free blocks referenced by dropped entries ─────────────────
//
// home_log_store::rollback (behind truncate_to) only removes journal RECORDS -- it has no idea
// about the data-service blocks those records reference (HS_DATA_LINKED: block lifecycle is this
// class's job). Every entry above lsn that truncate() drops must have its block freed BEFORE the
// record is gone, or it leaks permanently from the block allocator.

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

// A dropped entry AT OR BELOW lsn (still committed, not being dropped) must not be freed.
TEST_F(CraftTruncateTest, TruncateDoesNotFreeEntriesAtOrBelowLsn) {
    dev_->seed_lsns(100, {});
    journal_->add_data_slot(90, /* blk_num = */ 400); // at lsn -- survives, must not be freed
    journal_->add_data_slot(91, /* blk_num = */ 500); // above lsn -- dropped, must be freed

    auto r = do_truncate(90);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(journal_->freed_blkids.size(), 1u);
    EXPECT_EQ(journal_->freed_blkids[0].blk_num(), 500u);
}

// An all_zeros entry above lsn has no block to free -- the scan must not call free_data for it.
TEST_F(CraftTruncateTest, TruncateSkipsFreeForAllZerosEntries) {
    dev_->seed_lsns(100, {});
    journal_->add_all_zeros_slot(91);

    auto r = do_truncate(90);
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(journal_->freed_blkids.empty());
}

// A gap above lsn (never journaled locally, no slot seeded) must be skipped silently -- read_slot's
// failure is the ordinary case here, not an error, and must not abort the truncate.
TEST_F(CraftTruncateTest, TruncateSkipsFreeForGapsAboveLsn) {
    dev_->seed_lsns(100, {92}); // 92 is a real gap -- no slot seeded for it
    journal_->add_data_slot(91, /* blk_num = */ 500);

    auto r = do_truncate(90);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(journal_->freed_blkids.size(), 1u); // only 91's real block, nothing for the gap at 92
    EXPECT_EQ(journal_->freed_blkids[0].blk_num(), 500u);
}

// commit_lsn is invariant: truncate() must not alter it.
TEST_F(CraftTruncateTest, CommitLsnNotTouched) {
    dev_->seed_lsns(50, {});
    auto r = do_truncate(30);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->commit_lsn(), -1); // default; truncate must not touch it
}

// A journal I/O error propagates out and leaves partition state completely unmodified.
TEST_F(CraftTruncateTest, JournalErrorShieldsState) {
    journal_->should_fail = true;
    dev_->seed_lsns(100, {80, 90});
    auto r = do_truncate(70);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 100); // unchanged
    EXPECT_EQ(dev_->missing_count(), 2u);    // unchanged
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    HB_SETTINGS_FACTORY().load_json("{\"craft_watchdog_timeout_ms\": 0}");
    return RUN_ALL_TESTS();
}
