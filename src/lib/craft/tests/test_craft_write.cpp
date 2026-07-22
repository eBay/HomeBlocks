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

// Unit tests for CraftReplDev::write (S2: Write Path).
//
// Tests verify:
//   - in-order writes update last_append_lsn and clear the missing set
//   - out-of-order writes fill the missing set for gaps then clear on fill
//   - stale-term writes are rejected with volume_error::STALE_TERM
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles
// craft_repl_dev.cpp directly (same pattern as test_craft_truncate.cpp).

#include <gtest/gtest.h>
#include <map>
#include <sisl/logging/logging.h>

#include "craft/craft_repl_dev.hpp"
#include "coro_helpers.hpp"

SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)
SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)

namespace homeblocks {
namespace {

// ── journal mock ──────────────────────────────────────────────────────────────

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots;
    std::set< int64_t > fail_lsns; // write_slot returns io_error for these lsns

    async_status write_slot(int64_t lsn, lba_t lba, lba_count_t len,
                            homestore::multi_blk_id /* blkid */, bool all_zeros) override {
        if (fail_lsns.count(lsn))
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        slots[lsn] = JournalSlot{lsn, false, all_zeros, lba, len, {}};
        co_return ok();
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override {
        auto it = slots.find(lsn);
        if (it == slots.end())
            co_return std::unexpected(std::make_error_condition(std::errc::no_such_file_or_directory));
        co_return it->second;
    }

    async_status truncate_to(int64_t) override { co_return ok(); }

    bool has_slot(int64_t lsn) const { return slots.count(lsn) > 0; }
    size_t slot_count() const { return slots.size(); }
};

// ── test fixture ─────────────────────────────────────────────────────────────

class CraftWriteTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto mock = std::make_unique< MockCraftJournalBackend >();
        journal_ = mock.get();
        dev_ = std::make_unique< CraftReplDev >(volume_id_t{}, std::move(mock));
    }

    auto do_write(uint64_t term, int64_t lsn, bool all_zeros = false) {
        return homeblocks::detail::sync_get(
            dev_->write(craft::client_hdr{term, -1, -1}, lsn, 0, 4096, sisl::sg_list{}, all_zeros));
    }

    MockCraftJournalBackend* journal_{nullptr};
    std::unique_ptr< CraftReplDev > dev_;
};

// Each lsn arrives exactly one step ahead: no gap, no missing entries after each write.
TEST_F(CraftWriteTest, InOrderWrites) {
    auto r0 = do_write(0, 0);
    ASSERT_TRUE(r0.has_value());
    EXPECT_EQ(r0->last_append_lsn, 0);
    EXPECT_EQ(dev_->missing_count(), 0u);
    EXPECT_TRUE(journal_->has_slot(0));

    auto r1 = do_write(0, 1);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->last_append_lsn, 1);
    EXPECT_EQ(dev_->missing_count(), 0u);
    EXPECT_TRUE(journal_->has_slot(1));
}

// lsn=2 arrives before lsn=1, creating a gap. Gap enters the missing set and clears when filled.
TEST_F(CraftWriteTest, OutOfOrderWrites) {
    ASSERT_TRUE(do_write(0, 0).has_value());

    auto r2 = do_write(0, 2);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->last_append_lsn, 2);
    EXPECT_EQ(dev_->missing_count(), 1u);
    EXPECT_TRUE(dev_->is_missing(1));
    EXPECT_TRUE(journal_->has_slot(2));

    auto r1 = do_write(0, 1);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(dev_->missing_count(), 0u);
    EXPECT_FALSE(dev_->is_missing(1));
    EXPECT_TRUE(journal_->has_slot(1));
}

// A write with the wrong term is rejected immediately; no journal entry and no state change.
TEST_F(CraftWriteTest, TermRejection) {
    // Default state_.term == 0; writing with term=1 must fail.
    auto r = do_write(1, 0);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::STALE_TERM));
    EXPECT_EQ(journal_->slot_count(), 0u);
    EXPECT_EQ(dev_->missing_count(), 0u);
}

// all_zeros=true skips data allocation; journal slot is marked all_zeros.
TEST_F(CraftWriteTest, AllZerosWrite) {
    auto r = do_write(0, 0, /*all_zeros=*/true);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->last_append_lsn, 0);
    EXPECT_EQ(dev_->missing_count(), 0u);
    ASSERT_TRUE(journal_->has_slot(0));
    EXPECT_TRUE(journal_->slots[0].all_zeros);
}

// A failed write_slot must leave the lsn in missing_lsns_ (pre-insert invariant).
TEST_F(CraftWriteTest, WriteSlotFails_LsnRemainsInMissing) {
    journal_->fail_lsns.insert(0);
    auto r = do_write(0, 0);
    ASSERT_FALSE(r.has_value());
    EXPECT_TRUE(dev_->is_missing(0));
    EXPECT_EQ(journal_->slot_count(), 0u);
}

// A write into an Empty-verdicted slot is permanently rejected (Empty beats data).
TEST_F(CraftWriteTest, EmptySlotRejectsWrite) {
    dev_->seed_empty({5});
    auto r = do_write(0, 5);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(volume_error::EMPTY_SLOT));
    EXPECT_EQ(journal_->slot_count(), 0u);
    EXPECT_FALSE(dev_->is_missing(5));
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
