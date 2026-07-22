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

// Unit tests for CraftReplDev::get_lsns(), get_rs_commit_lsn(), and fetch_data()
// (S6: Peer Data Exchange APIs).
//
// fetch_data() implements a four-way response per slot:
//   present+data : slot in journal, all_zeros=false
//   present+zero : slot in journal, all_zeros=true (WRITE_ZEROES / range-unmap)
//   omitted      : slot not locally held (in missing_lsns_ or above last_append_lsn)
//   is_empty     : slot positively verdicted Empty via a prior S5 SyncRSCommitLSN apply
//
// The Empty verdict is leader-only (S5 pre-resolution); fetch_data responders
// report state only — they never declare a slot Empty themselves.
//
// This TU defines SISL_LOGGING_DEF for the homeblocks module because it compiles
// craft_repl_dev.cpp directly (same pattern as test_craft_truncate.cpp).

#include <gtest/gtest.h>
#include <map>
#include <optional>
#include <sisl/logging/logging.h>

#include "craft/craft_repl_dev.hpp"
#include "coro_helpers.hpp"

SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)
SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)

namespace homeblocks {
namespace {

// ── journal mock ──────────────────────────────────────────────────────────────
//
// Backed by a std::map so tests can seed exact JournalSlot values per LSN.
// fail_on_read optionally injects an I/O error for a specific LSN to exercise
// the error-propagation path of fetch_data.

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots;
    std::optional< int64_t > fail_on_read; // if set, read_slot for that LSN returns io_error

    async_status write_slot(int64_t, lba_t, lba_count_t, homestore::multi_blk_id, bool) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override {
        if (fail_on_read && *fail_on_read == lsn)
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        auto it = slots.find(lsn);
        if (it == slots.end())
            co_return std::unexpected(std::make_error_condition(std::errc::no_such_file_or_directory));
        co_return it->second;
    }

    async_status truncate_to(int64_t) override { co_return ok(); }
};

// ── test fixture ─────────────────────────────────────────────────────────────

class CraftPeerExchangeTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto mock = std::make_unique< MockCraftJournalBackend >();
        journal_ = mock.get();
        dev_ = std::make_unique< CraftReplDev >(volume_id_t{}, std::move(mock));
    }

    auto do_get_lsns() { return homeblocks::detail::sync_get(dev_->get_lsns(volume_id_t{})); }
    auto do_get_rs_commit_lsn() { return homeblocks::detail::sync_get(dev_->get_rs_commit_lsn()); }
    auto do_fetch_data(std::vector< int64_t > lsns) {
        return homeblocks::detail::sync_get(dev_->fetch_data(std::move(lsns)));
    }

    // Seed a data slot into the mock journal. lba and len default to small non-zero values.
    void add_slot(int64_t lsn, lba_t lba = 0, lba_count_t len = 4, bool all_zeros = false) {
        journal_->slots[lsn] = JournalSlot{.lsn = lsn, .all_zeros = all_zeros, .lba = lba, .len = len};
    }

    MockCraftJournalBackend* journal_{nullptr};
    std::unique_ptr< CraftReplDev > dev_;
};

// ── get_lsns / get_rs_commit_lsn ─────────────────────────────────────────────

// Fresh device: both watermarks default to -1 (uninitialized sentinel).
TEST_F(CraftPeerExchangeTest, GetLsnsDefaultState) {
    auto r = do_get_lsns();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->commit_lsn, -1);
    EXPECT_EQ(r->last_append_lsn, -1);
}

// After seeding, get_lsns reflects both watermarks correctly.
TEST_F(CraftPeerExchangeTest, GetLsnsAfterSeed) {
    dev_->seed_lsns(50, {30, 40});
    dev_->seed_commit_lsn(25);
    auto r = do_get_lsns();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->commit_lsn, 25);
    EXPECT_EQ(r->last_append_lsn, 50);
}

// get_rs_commit_lsn is an alias of get_lsns; both must return the same snapshot.
TEST_F(CraftPeerExchangeTest, GetRsCommitLsnMatchesGetLsns) {
    dev_->seed_lsns(100, {});
    dev_->seed_commit_lsn(80);
    auto lsns_r = do_get_lsns();
    auto rs_r = do_get_rs_commit_lsn();
    ASSERT_TRUE(lsns_r.has_value());
    ASSERT_TRUE(rs_r.has_value());
    EXPECT_EQ(lsns_r->commit_lsn, rs_r->commit_lsn);
    EXPECT_EQ(lsns_r->last_append_lsn, rs_r->last_append_lsn);
}

// ── fetch_data ────────────────────────────────────────────────────────────────

// Requesting zero LSNs returns an empty result vector, not an error.
TEST_F(CraftPeerExchangeTest, FetchDataEmptyRequest) {
    auto r = do_fetch_data({});
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty());
}

// A slot present in the journal (not missing, within last_append) returns with its data fields.
TEST_F(CraftPeerExchangeTest, FetchDataPresentData) {
    add_slot(5, /*lba=*/10, /*len=*/4);
    dev_->seed_lsns(5, {});

    auto r = do_fetch_data({5});
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->size(), 1u);
    EXPECT_EQ((*r)[0].lsn, 5);
    EXPECT_FALSE((*r)[0].is_empty);
    EXPECT_FALSE((*r)[0].all_zeros);
    EXPECT_EQ((*r)[0].lba, 10u);
    EXPECT_EQ((*r)[0].len, 4u);
}

// A zero-write slot (all_zeros=true) is returned with that flag set and no data payload.
TEST_F(CraftPeerExchangeTest, FetchDataPresentZeroWrite) {
    add_slot(7, /*lba=*/20, /*len=*/8, /*all_zeros=*/true);
    dev_->seed_lsns(7, {});

    auto r = do_fetch_data({7});
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->size(), 1u);
    EXPECT_EQ((*r)[0].lsn, 7);
    EXPECT_FALSE((*r)[0].is_empty);
    EXPECT_TRUE((*r)[0].all_zeros);
}

// Slots both at or below commit_lsn (committed) and above it (appended-only) are returned;
// fetch_data does not gate on the commit boundary — it returns any locally-held slot.
TEST_F(CraftPeerExchangeTest, FetchDataAcrossCommitBoundary) {
    add_slot(3, 0, 4); // lsn <= commit_lsn=5: committed and applied to index
    add_slot(7, 0, 4); // lsn in (5, 10]: appended, not yet committed
    add_slot(10, 0, 4);
    dev_->seed_commit_lsn(5);
    dev_->seed_lsns(10, {});

    auto r = do_fetch_data({3, 7, 10});
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->size(), 3u);
    EXPECT_EQ((*r)[0].lsn, 3);
    EXPECT_EQ((*r)[1].lsn, 7);
    EXPECT_EQ((*r)[2].lsn, 10);
}

// A slot in missing_lsns_ is not locally held; it must be omitted from the response.
TEST_F(CraftPeerExchangeTest, FetchDataMissingOmitted) {
    dev_->seed_lsns(10, {6}); // lsn 6 is in the missing set

    auto r = do_fetch_data({6});
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty()); // not-present-here: omitted
}

// A requested LSN above last_append_lsn is not locally held; it must be omitted.
TEST_F(CraftPeerExchangeTest, FetchDataAboveLastAppendOmitted) {
    dev_->seed_lsns(5, {});

    auto r = do_fetch_data({10}); // 10 > last_append_lsn=5
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->empty());
}

// A slot positively verdicted Empty (by a prior S5 apply) is returned with is_empty=true,
// even when the slot is also in missing_lsns_ (the peer never received the write data).
TEST_F(CraftPeerExchangeTest, FetchDataEmptySlot) {
    dev_->seed_lsns(10, {8}); // lsn 8 is missing (write never arrived)
    dev_->seed_empty({8});    // S5 also applied an Empty verdict for lsn 8

    auto r = do_fetch_data({8});
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->size(), 1u);
    EXPECT_EQ((*r)[0].lsn, 8);
    EXPECT_TRUE((*r)[0].is_empty);
}

// Empty beats data: if a slot is in both empty_lsns_ and the journal, is_empty=true is returned.
// This is the "Empty wins" reconciliation invariant from S5 pre-resolution.
TEST_F(CraftPeerExchangeTest, FetchDataEmptyBeatsPresent) {
    add_slot(5, 0, 4);
    dev_->seed_lsns(5, {});
    dev_->seed_empty({5}); // S5 applied an Empty verdict over an existing data slot

    auto r = do_fetch_data({5});
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->size(), 1u);
    EXPECT_TRUE((*r)[0].is_empty);
    EXPECT_EQ((*r)[0].lsn, 5);
}

// Mixed request: present, missing, Empty, and above-last-append — each classified correctly.
// Only present and Empty slots appear in the result; missing and above-last-append are omitted.
TEST_F(CraftPeerExchangeTest, FetchDataMixedRequest) {
    add_slot(2, 0, 4);
    add_slot(4, 0, 4);
    dev_->seed_lsns(10, {3}); // lsn 3 is missing
    dev_->seed_empty({6});    // lsn 6 is Empty-verdicted
    // lsn 15 > last_append_lsn=10 → absent

    auto r = do_fetch_data({2, 3, 4, 6, 15});
    ASSERT_TRUE(r.has_value());
    // 3 (missing) and 15 (above last_append) are omitted → 3 results in request order
    ASSERT_EQ(r->size(), 3u);
    EXPECT_EQ((*r)[0].lsn, 2);
    EXPECT_FALSE((*r)[0].is_empty);
    EXPECT_EQ((*r)[1].lsn, 4);
    EXPECT_FALSE((*r)[1].is_empty);
    EXPECT_EQ((*r)[2].lsn, 6);
    EXPECT_TRUE((*r)[2].is_empty);
}

// A read_slot() I/O error aborts fetch_data immediately; subsequent LSNs are not processed.
TEST_F(CraftPeerExchangeTest, FetchDataReadErrorPropagates) {
    add_slot(1, 0, 4);
    add_slot(2, 0, 4);
    dev_->seed_lsns(3, {});
    journal_->fail_on_read = 2; // reading lsn=2 returns io_error

    auto r = do_fetch_data({1, 2, 3});
    EXPECT_FALSE(r.has_value()); // error propagated; partial result discarded
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
