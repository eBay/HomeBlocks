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

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>

#include "craft/craft_repl_dev.hpp"
#include "coro_helpers.hpp"

SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)
SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)

namespace homeblocks {
namespace {

// ── minimal journal mock ──────────────────────────────────────────────────────
//
// Captures the lsn passed to truncate_to() and can be armed to return an I/O error
// so tests can verify the error-path invariant (state unchanged on failure).

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    bool should_fail{false};
    int64_t truncated_to{INT64_MIN};

    async_status write_slot(int64_t, lba_t, lba_count_t, homestore::multi_blk_id, bool) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    async_result< JournalSlot > read_slot(int64_t) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    async_status truncate_to(int64_t lsn) override {
        truncated_to = lsn;
        if (should_fail) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return ok();
    }
};

// ── test fixture ─────────────────────────────────────────────────────────────

class CraftTruncateTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto mock = std::make_unique< MockCraftJournalBackend >();
        journal_ = mock.get();
        dev_ = std::make_unique< CraftReplDev >(volume_id_t{}, std::move(mock));
    }

    auto do_truncate(int64_t lsn) { return homeblocks::detail::sync_get(dev_->truncate(lsn)); }

    MockCraftJournalBackend* journal_{nullptr};
    std::unique_ptr< CraftReplDev > dev_;
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
    return RUN_ALL_TESTS();
}
