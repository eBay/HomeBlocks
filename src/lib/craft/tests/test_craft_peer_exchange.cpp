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

// Unit tests for CraftReplDev::get_rs_commit_lsn() and fetch_data() (S6: Peer Data Exchange APIs).
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
#include <sisl/options/options.h>

#include "craft/craft_repl_dev.hpp"
#include "home_blks_config.hpp"
#include "coro_helpers.hpp"
#include "mock_journal_backend.hpp"

SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)
SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)

namespace homeblocks {
namespace {

static constexpr uint32_t k_page_size = 4096;

// ── journal mock ──────────────────────────────────────────────────────────────
//
// Backed by a std::map so tests can seed exact JournalSlot values per LSN.
// fail_on_read optionally injects an I/O error for a specific LSN to exercise
// the error-propagation path of fetch_data.

class MockCraftJournalBackend : public CraftJournalBackend {
public:
    std::map< int64_t, JournalSlot > slots;
    std::optional< int64_t > fail_on_read; // if set, read_slot for that LSN returns io_error
    int reads{0};                          // read_slot calls

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const& /* data */,
                                                             lba_count_t /* len */) override {
        co_return homestore::multi_blk_id{};
    }

    async_status write_slot(int64_t, uint64_t, lba_t, lba_count_t, homestore::multi_blk_id, bool,
                            std::vector< homestore::csum_t > const&) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override {
        ++reads;
        if (fail_on_read && *fail_on_read == lsn)
            co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return co_await mock_read_slot(*this, lsn);
    }

    async_status truncate_to(int64_t) override { co_return ok(); }
    async_status free_data(homestore::multi_blk_id) override { co_return ok(); }

    async_status free_slot(int64_t lsn) override { return mock_free_slot(*this, lsn); }
    async_status read_data(homestore::multi_blk_id, sisl::sg_list&) override {
        co_return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }
};

// ── test fixture ─────────────────────────────────────────────────────────────

class CraftPeerExchangeTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto mock = std::make_unique< MockCraftJournalBackend >();
        journal_ = mock.get();
        dev_ = CraftReplDev::create(volume_id_t{}, std::move(mock), k_page_size, nullptr);
    }

    auto do_get_rs_commit_lsn() { return homeblocks::detail::sync_get(dev_->get_rs_commit_lsn(0, false)); }
    auto do_fetch_data(std::vector< int64_t > lsns) {
        return homeblocks::detail::sync_get(dev_->fetch_data(std::move(lsns)));
    }

    // Seed a data slot into the mock journal. lba and len default to small non-zero values.
    void add_slot(int64_t lsn, lba_t lba = 0, lba_count_t len = 4, bool all_zeros = false) {
        journal_->slots[lsn] = JournalSlot{.lsn = lsn, .all_zeros = all_zeros, .lba_off_bytes = lba, .len_bytes = len};
    }

    MockCraftJournalBackend* journal_{nullptr};
    std::shared_ptr< CraftReplDev > dev_;
};

// ── get_rs_commit_lsn ────────────────────────────────────────────────────────

// Fresh device: both watermarks default to -1 (uninitialized sentinel).
TEST_F(CraftPeerExchangeTest, GetRsCommitLsnDefaultState) {
    auto r = do_get_rs_commit_lsn();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->commit_lsn, -1);
    EXPECT_EQ(r->last_append_lsn, -1);
}

// After seeding, get_rs_commit_lsn reflects both watermarks correctly.
TEST_F(CraftPeerExchangeTest, GetRsCommitLsnAfterSeed) {
    dev_->seed_lsns(50, {30, 40});
    dev_->seed_commit_lsn(25);
    auto r = do_get_rs_commit_lsn();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->commit_lsn, 25);
    EXPECT_EQ(r->last_append_lsn, 50);
}

// ── fetch_data ────────────────────────────────────────────────────────────────

// ── fetch_range: ranged, size-bounded callee (docs/craft/resync.md §4.3, §4.7) ─────────────────────────

namespace {
auto do_fetch_range(CraftReplDev& d, std::vector< LsnRange > ranges, uint64_t max_bytes) {
    return homeblocks::detail::sync_get(d.fetch_range(std::move(ranges), max_bytes));
}
std::vector< int64_t > lsns_of(FetchRangeResult const& r) {
    std::vector< int64_t > v;
    for (auto const& s : r.slots)
        v.push_back(s.lsn);
    return v;
}
} // namespace

TEST_F(CraftPeerExchangeTest, FetchRangeServesEverythingWithinBudget) {
    for (int64_t l : {2, 3, 5})
        add_slot(l, 0, /*len=*/4);
    dev_->seed_lsns(5, {4});

    auto r = do_fetch_range(*dev_, {{2, 5}}, /*max_bytes=*/1024);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(lsns_of(*r), (std::vector< int64_t >{2, 3, 5})); // 4 is missing here: omitted
    EXPECT_EQ(r->next, -1);
    EXPECT_EQ(r->reclaimed_below, -1);
}

TEST_F(CraftPeerExchangeTest, FetchRangeStopsAtBudgetWithContinuation) {
    for (int64_t l = 0; l <= 4; ++l)
        add_slot(l, 0, /*len=*/4);
    dev_->seed_lsns(4, {});

    auto r = do_fetch_range(*dev_, {{0, 4}}, /*max_bytes=*/8);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(lsns_of(*r), (std::vector< int64_t >{0, 1}));
    EXPECT_EQ(r->next, 2);
    // Reads stop at the budget: slots 0 and 1 fill it exactly, so slot 2 is never read -- a batched read loaded every
    // slot of the batch first (review on #200).
    EXPECT_EQ(journal_->reads, 2);
}

// A slot larger than the whole budget is still returned (alone), so a fetch always makes progress.
TEST_F(CraftPeerExchangeTest, FetchRangeAlwaysReturnsAtLeastOneSlot) {
    add_slot(0, 0, /*len=*/100);
    add_slot(1, 0, /*len=*/4);
    dev_->seed_lsns(1, {});

    auto r = do_fetch_range(*dev_, {{0, 1}}, /*max_bytes=*/10);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(lsns_of(*r), (std::vector< int64_t >{0}));
    EXPECT_EQ(r->next, 1);
}

// Empty and zero-write slots cost no payload, so they never exhaust the budget.
TEST_F(CraftPeerExchangeTest, FetchRangeEmptyAndZeroSlotsAreFree) {
    add_slot(0, 0, /*len=*/8, /*all_zeros=*/true);
    dev_->seed_lsns(2, {});
    dev_->seed_empty({1});
    add_slot(2, 0, /*len=*/4);

    auto r = do_fetch_range(*dev_, {{0, 2}}, /*max_bytes=*/4);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(lsns_of(*r), (std::vector< int64_t >{0, 1, 2}));
    EXPECT_TRUE(r->slots[1].is_empty);
}

// When the budget runs out at the end of one range, the continuation is the start of the next range.
TEST_F(CraftPeerExchangeTest, FetchRangeContinuesIntoNextRange) {
    for (int64_t l : {1, 2, 7, 8})
        add_slot(l, 0, /*len=*/4);
    dev_->seed_lsns(8, {});

    auto r = do_fetch_range(*dev_, {{1, 2}, {7, 8}}, /*max_bytes=*/8);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(lsns_of(*r), (std::vector< int64_t >{1, 2}));
    EXPECT_EQ(r->next, 7);
}

// A huge sparse range is served in pieces: at most k_fetch_range_max_scan dLSNs are examined per call.
TEST_F(CraftPeerExchangeTest, FetchRangeCapsTheScan) {
    dev_->seed_lsns(-1, {});
    auto r = do_fetch_range(*dev_, {{0, CraftReplDev::k_fetch_range_max_scan + 10}}, /*max_bytes=*/1024);
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->slots.empty());
    EXPECT_EQ(r->next, CraftReplDev::k_fetch_range_max_scan);
}

TEST_F(CraftPeerExchangeTest, FetchRangeReadErrorPropagates) {
    add_slot(0, 0, /*len=*/4);
    dev_->seed_lsns(0, {});
    journal_->fail_on_read = 0;

    auto r = do_fetch_range(*dev_, {{0, 0}}, /*max_bytes=*/1024);
    EXPECT_FALSE(r.has_value());
}

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
    EXPECT_EQ((*r)[0].lba_off_bytes, 10u);
    EXPECT_EQ((*r)[0].len_bytes, 4u);
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
    SISL_OPTIONS_LOAD(argc, argv, logging);
    HB_SETTINGS_FACTORY().load_json("{\"craft_watchdog_timeout_ms\": 0}");
    return RUN_ALL_TESTS();
}
