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
#include <functional>
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
    std::optional< int64_t > fail_on_read;   // if set, read_slot for that LSN returns io_error
    std::function< void(int64_t) > on_write; // runs inside write_slot, while the write is in flight

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const& /* data */,
                                                             lba_count_t /* len */) override {
        co_return homestore::multi_blk_id{};
    }

    async_status write_slot(int64_t lsn, uint64_t, lba_t lba, lba_count_t len, homestore::multi_blk_id blkid,
                            bool all_zeros, std::vector< homestore::csum_t > const& csums) override {
        if (on_write) on_write(lsn);
        slots[lsn] = JournalSlot{
            .lsn = lsn, .all_zeros = all_zeros, .lba_off_bytes = lba, .len_bytes = len, .blkid = blkid, .csums = csums};
        co_return ok();
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override {
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
    void TearDown() override { EXPECT_EQ(dev_->invariant_violations(), std::vector< std::string >{}); }

    auto do_get_rs_commit_lsn() { return homeblocks::detail::sync_get(dev_->get_rs_commit_lsn(0, false)); }
    auto do_fetch_data(std::vector< int64_t > lsns) {
        return homeblocks::detail::sync_get(dev_->fetch_data(std::move(lsns)));
    }

    // Seed a data slot into the mock journal. lba and len default to small non-zero values.
    void add_slot(int64_t lsn, lba_t lba = 0, lba_count_t len = 4, bool all_zeros = false) {
        journal_->slots[lsn] = JournalSlot{.lsn = lsn, .all_zeros = all_zeros, .lba_off_bytes = lba, .len_bytes = len};
    }

    // A login poll at term: quiesces this member's writes at or below it.
    auto do_quiesce(uint64_t term) {
        return homeblocks::detail::sync_get(dev_->get_rs_commit_lsn(term, /*is_login=*/true));
    }
    // A one-block all_zeros write at dlsn (no payload needed).
    auto do_write(int64_t dlsn, uint64_t term) {
        return homeblocks::detail::sync_get(
            dev_->write(craft::client_hdr{term, -1, -1}, dlsn, /*addr=*/0, k_page_size, sisl::sg_list{}));
    }
    // Commits an InternalLogin at term, as RAFT would on this member; it applies unless the apply queue is paused.
    void commit_login(uint64_t term) {
        std::vector< uint8_t > header(sizeof(CraftEntryHeader));
        reinterpret_cast< CraftEntryHeader* >(header.data())->type = CraftEntryType::InternalLogin;
        std::vector< uint8_t > key(sizeof(InternalLoginPayload));
        *reinterpret_cast< InternalLoginPayload* >(key.data()) = InternalLoginPayload{.client_token = 7, .term = term};
        cintrusive< homestore::repl_req_ctx > ctx{};
        dev_->raft_listener().on_commit(1, sisl::blob{header.data(), static_cast< uint32_t >(header.size())},
                                        sisl::blob{key.data(), static_cast< uint32_t >(key.size())}, {}, ctx);
    }

    MockCraftJournalBackend* journal_{nullptr};
    std::shared_ptr< CraftReplDev > dev_;
};

// ── get_rs_commit_lsn ────────────────────────────────────────────────────────

// Fresh device: both watermarks default to -1 (uninitialized sentinel).
TEST_F(CraftPeerExchangeTest, GetRsCommitLsnDefaultState) {
    auto r = do_get_rs_commit_lsn();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->lsns.commit_lsn, -1);
    EXPECT_EQ(r->lsns.last_append_lsn, -1);
    EXPECT_EQ(r->term, 0u);
}

// After seeding, get_rs_commit_lsn reflects both watermarks correctly.
TEST_F(CraftPeerExchangeTest, GetRsCommitLsnAfterSeed) {
    dev_->seed_lsns(50, {30, 40});
    dev_->seed_commit_lsn(25);
    auto r = do_get_rs_commit_lsn();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->lsns.commit_lsn, 25);
    EXPECT_EQ(r->lsns.last_append_lsn, 50);
}

// The reply carries this member's own session term, not the one the poll asked about.
TEST_F(CraftPeerExchangeTest, GetRsCommitLsnReportsOwnTerm) {
    dev_->seed_term(3);
    auto r = do_quiesce(/*term=*/5);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->term, 3u);
}

// ...counting a login it has committed but not yet applied: else a leader that hasn't learned of that commit could
// poll at the replaced session's term and pass.
TEST_F(CraftPeerExchangeTest, GetRsCommitLsnReportsACommittedLogin) {
    dev_->seed_term(3);
    dev_->pause_apply();
    commit_login(/*term=*/5);
    ASSERT_EQ(dev_->term(), 3u); // committed, not applied

    auto r = do_quiesce(/*term=*/3);
    dev_->resume_apply();

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->term, 5u);
}

// ── login quiesce (SDSTOR-22889) ──────────────────────────────────────────────
//
// A login poll (is_login) fences writes whose term is at or below the poll's. The fence matrix in
// test_craft_pre_resolution.cpp covers what each entry point does under it; these cover the write's two checks.

// A write arriving after the poll is rejected before its journal write: nothing journaled, last_append unmoved.
TEST_F(CraftPeerExchangeTest, QuiesceRejectsLaterWriteBeforeJournaling) {
    dev_->seed_lsns(2, {});
    ASSERT_TRUE(do_quiesce(/*term=*/0).has_value());

    auto w = do_write(/*dlsn=*/5, /*term=*/0);

    ASSERT_FALSE(w.has_value());
    EXPECT_EQ(w.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_FALSE(journal_->slots.contains(5));
    EXPECT_EQ(dev_->last_append_lsn(), 2);
    EXPECT_EQ(dev_->missing_count(), 0u);
}

// A write already in flight when the poll arrives isn't acked, but keeps its slot: journaled, readable, and reported
// present to peers. The poll's last_append counts it.
TEST_F(CraftPeerExchangeTest, QuiesceDuringJournalWriteKeepsSlotWithoutAck) {
    dev_->seed_lsns(2, {});
    std::optional< int64_t > polled_last_append;
    journal_->on_write = [&](int64_t) {
        if (auto r = do_quiesce(/*term=*/0); r) polled_last_append = r->lsns.last_append_lsn;
    };

    auto w = do_write(/*dlsn=*/3, /*term=*/0);

    ASSERT_FALSE(w.has_value());
    EXPECT_EQ(w.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
    EXPECT_EQ(polled_last_append, 3);
    EXPECT_FALSE(dev_->is_missing(3));
    EXPECT_EQ(dev_->overlay_lsn_for(0), 3);
    auto f = do_fetch_data({3});
    ASSERT_TRUE(f.has_value());
    ASSERT_EQ(f->size(), 1u);
    EXPECT_FALSE((*f)[0].is_empty);
}

// The fence only rises: a later poll at a lower term doesn't lift it.
TEST_F(CraftPeerExchangeTest, QuiesceFenceOnlyRises) {
    dev_->seed_term(2);
    ASSERT_TRUE(do_quiesce(/*term=*/2).has_value());
    ASSERT_TRUE(do_quiesce(/*term=*/1).has_value());

    auto w = do_write(/*dlsn=*/0, /*term=*/2);

    ASSERT_FALSE(w.has_value());
    EXPECT_EQ(w.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
}

// The poll fences up to its own term, not the replica's, which may lag behind a login it hasn't applied: a replica
// still at term 3 keeps fencing session 5 once it applies that session's login.
TEST_F(CraftPeerExchangeTest, QuiesceFencesThePollTermNotTheReplicaTerm) {
    dev_->seed_term(3);
    ASSERT_TRUE(do_quiesce(/*term=*/5).has_value());
    commit_login(/*term=*/5);
    ASSERT_EQ(dev_->term(), 5u);

    auto w = do_write(/*dlsn=*/0, /*term=*/5);

    ASSERT_FALSE(w.has_value());
    EXPECT_EQ(w.error(), craft::make_error_condition(craft::craft_error::STALE_TERM));
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
