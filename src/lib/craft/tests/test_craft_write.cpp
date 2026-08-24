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

#include <array>
#include <gtest/gtest.h>
#include <map>
#include <set>
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
    std::map< int64_t, uint64_t > slot_terms; // term passed to write_slot, keyed by lsn
    std::set< int64_t > fail_lsns;            // write_slot returns io_error for these lsns
    bool fail_alloc{false};                   // alloc_write_data returns io_error when set
    int alloc_write_data_calls{0};
    int free_data_calls{0};
    void const* last_alloc_data_ptr{nullptr}; // iov_base seen by the most recent alloc_write_data call

    async_result< homestore::multi_blk_id > alloc_write_data(sisl::sg_list const& data,
                                                             lba_count_t /* len */) override {
        ++alloc_write_data_calls;
        last_alloc_data_ptr = data.iovs.empty() ? nullptr : data.iovs[0].iov_base;
        if (fail_alloc) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        co_return homestore::multi_blk_id{};
    }

    async_status write_slot(int64_t lsn, uint64_t term, lba_t lba, lba_count_t len, homestore::multi_blk_id /* blkid */,
                            bool all_zeros) override {
        if (fail_lsns.contains(lsn)) co_return std::unexpected(std::make_error_condition(std::errc::io_error));
        slots[lsn] = JournalSlot{lsn, false, all_zeros, lba, len, {}};
        slot_terms[lsn] = term;
        co_return ok();
    }

    async_result< JournalSlot > read_slot(int64_t lsn) override {
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

    bool has_slot(int64_t lsn) const { return slots.contains(lsn); }
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

    auto do_write(uint64_t term, int64_t lsn, bool all_zeros = true) {
        return homeblocks::detail::sync_get(
            dev_->write(craft::client_hdr{term, -1, -1}, lsn, 0, 4096, sisl::sg_list{}, all_zeros));
    }

    // Variant that marks data.size > 0 so the alloc_write_data branch is exercised.
    // The mock ignores the actual iovecs; only data.size matters for the branch guard.
    auto do_write_with_data(uint64_t term, int64_t lsn) {
        sisl::sg_list data;
        data.size = 4096;
        return homeblocks::detail::sync_get(
            dev_->write(craft::client_hdr{term, -1, -1}, lsn, 0, 4096, std::move(data), false));
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
    EXPECT_EQ(journal_->alloc_write_data_calls, 0); // no block allocation for all_zeros writes
}

// A failed write_slot must leave the lsn in missing_lsns_ (pre-insert invariant).
TEST_F(CraftWriteTest, WriteSlotFails_LsnRemainsInMissing) {
    journal_->fail_lsns.insert(0);
    auto r = do_write(0, 0);
    ASSERT_FALSE(r.has_value());
    EXPECT_TRUE(dev_->is_missing(0));
    EXPECT_EQ(journal_->slot_count(), 0u);
    EXPECT_EQ(dev_->last_append_lsn(), 0); // last_append_lsn is set before write_slot; not rolled back on failure
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

// Empty verdict on a slot already in missing_lsns_ must remove it so the gap is resolved.
// Without this, the slot stalls commit advancement permanently.
TEST_F(CraftWriteTest, EmptyVerdictClearsMissingEntry) {
    // Write lsn=10 with the journal injecting a failure → lsn 10 and gaps 0-9 all land in missing.
    journal_->fail_lsns.insert(10);
    auto r10 = do_write(0, 10);
    ASSERT_FALSE(r10.has_value());
    EXPECT_TRUE(dev_->is_missing(5));
    EXPECT_TRUE(dev_->is_missing(10));

    // S5 verdicts lsn=5 Empty: must be evicted from missing_lsns_.
    dev_->seed_empty({5});
    EXPECT_FALSE(dev_->is_missing(5));   // gap resolved
    EXPECT_TRUE(dev_->is_empty_slot(5)); // permanently empty

    // A late write to lsn=5 is still rejected with EMPTY_SLOT.
    auto r5 = do_write(0, 5);
    ASSERT_FALSE(r5.has_value());
    EXPECT_EQ(r5.error(), make_error_condition(volume_error::EMPTY_SLOT));
}

// A negative dlsn is rejected before any state is touched.
TEST_F(CraftWriteTest, NegativeDlsnRejected) {
    auto r = do_write(0, -1);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(journal_->slot_count(), 0u);
    EXPECT_EQ(dev_->missing_count(), 0u);
    EXPECT_EQ(dev_->last_append_lsn(), -1);
}

// A dlsn far ahead of last_append_lsn is rejected to prevent unbounded gap allocation.
TEST_F(CraftWriteTest, ExcessiveGapRejected) {
    auto r = do_write(0, 1'000'001);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(dev_->missing_count(), 0u);
    EXPECT_EQ(dev_->last_append_lsn(), -1);
}

// Gap of more than 1: writing lsn=4 after lsn=0 creates gaps {1,2,3} in missing_lsns_.
TEST_F(CraftWriteTest, OutOfOrderWritesLargerGap) {
    ASSERT_TRUE(do_write(0, 0).has_value());
    auto r4 = do_write(0, 4);
    ASSERT_TRUE(r4.has_value());
    EXPECT_EQ(r4->last_append_lsn, 4);
    EXPECT_EQ(dev_->missing_count(), 3u);
    EXPECT_TRUE(dev_->is_missing(1));
    EXPECT_TRUE(dev_->is_missing(2));
    EXPECT_TRUE(dev_->is_missing(3));
    EXPECT_FALSE(dev_->is_missing(4)); // 4 was successfully written

    // Fill the gap one by one and verify the missing set drains.
    ASSERT_TRUE(do_write(0, 2).has_value());
    EXPECT_EQ(dev_->missing_count(), 2u);
    ASSERT_TRUE(do_write(0, 1).has_value());
    ASSERT_TRUE(do_write(0, 3).has_value());
    EXPECT_EQ(dev_->missing_count(), 0u);
}

// A retry of an already-written dlsn returns success idempotently without a second write_slot call.
// Invariant: after a successful write, dlsn is absent from missing_lsns_; a second write with the
// same dlsn must not violate the pre-insert invariant by calling write_slot without a prior insert.
// Also covers cross-type retries: the original slot type (all_zeros vs data) is preserved; the
// retry type is irrelevant — the slot is immutable once written.
TEST_F(CraftWriteTest, DuplicateWriteIsIdempotent) {
    // same-type: all_zeros then all_zeros at dLSN=0
    ASSERT_TRUE(do_write(0, 0).has_value());
    EXPECT_EQ(journal_->slot_count(), 1u);
    EXPECT_FALSE(dev_->is_missing(0));

    auto r2 = do_write(0, 0);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(journal_->slot_count(), 1u); // write_slot not called again
    EXPECT_FALSE(dev_->is_missing(0));

    // cross-type: data write first, then all_zeros retry at dLSN=1 — all_zeros is discarded
    auto r_data = do_write_with_data(0, 1);
    ASSERT_TRUE(r_data.has_value());
    EXPECT_EQ(journal_->slot_count(), 2u);
    EXPECT_EQ(journal_->alloc_write_data_calls, 1);
    EXPECT_FALSE(journal_->slots[1].all_zeros); // data slot preserved

    auto r_zero = do_write(0, 1, /*all_zeros=*/true);
    ASSERT_TRUE(r_zero.has_value());
    EXPECT_EQ(journal_->slot_count(), 2u);          // write_slot not called again
    EXPECT_EQ(journal_->alloc_write_data_calls, 1); // no extra alloc for the all_zeros retry

    // cross-type: all_zeros write first, then data retry at dLSN=2 — data is discarded
    auto r_zero2 = do_write(0, 2, /*all_zeros=*/true);
    ASSERT_TRUE(r_zero2.has_value());
    EXPECT_EQ(journal_->slot_count(), 3u);
    EXPECT_EQ(journal_->alloc_write_data_calls, 1); // all_zeros never allocates
    EXPECT_TRUE(journal_->slots[2].all_zeros);      // zero slot preserved

    auto r_data2 = do_write_with_data(0, 2);
    ASSERT_TRUE(r_data2.has_value());
    EXPECT_EQ(journal_->slot_count(), 3u);          // write_slot not called again
    EXPECT_EQ(journal_->alloc_write_data_calls, 1); // idempotent path skips alloc_write_data
}

// The gap loop must not re-introduce Empty-verdicted LSNs into missing_lsns_.
// Without this filter, an Empty-resolved gap stalls commit advancement just like a real missing entry.
TEST_F(CraftWriteTest, GapLoopSkipsEmptyVerdicts) {
    dev_->seed_empty({3});
    ASSERT_FALSE(dev_->is_missing(3));

    // Writing dlsn=5 on a fresh device triggers the gap loop for lsns 0-4; lsn=3 is Empty-verdicted.
    auto r5 = do_write(0, 5);
    ASSERT_TRUE(r5.has_value());
    // lsn=3 must NOT have been re-inserted — it is Empty-resolved, not a gap.
    EXPECT_FALSE(dev_->is_missing(3));
    EXPECT_TRUE(dev_->is_empty_slot(3));
    EXPECT_EQ(dev_->missing_count(), 4u); // gaps {0,1,2,4}; lsn=3 is Empty, lsn=5 written
    EXPECT_TRUE(dev_->is_missing(0));
    EXPECT_TRUE(dev_->is_missing(1));
    EXPECT_TRUE(dev_->is_missing(2));
    EXPECT_TRUE(dev_->is_missing(4));
    EXPECT_FALSE(dev_->is_missing(5)); // successfully written
}

// Cap fencepost from a seeded non-initial state: verify the > (not >=) boundary and that Guard 1
// (overflow protection when dlsn is near INT64_MAX) fires correctly too.
TEST_F(CraftWriteTest, GapCapFenceposts) {
    dev_->seed_lsns(1'000'000, {});

    // gap = 1,000,001 (one over cap) → rejected; state unchanged.
    ASSERT_FALSE(do_write(0, 2'000'001).has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 1'000'000);
    EXPECT_EQ(dev_->missing_count(), 0u);

    // Substantially over cap → also rejected.
    ASSERT_FALSE(do_write(0, 3'000'000).has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 1'000'000);

    // Guard 1: dlsn near INT64_MAX triggers the overflow-safe guard before the subtraction.
    ASSERT_FALSE(do_write(0, INT64_MAX).has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 1'000'000);

    // Small gap (2) from the seeded state → accepted; only 1 gap entry created.
    ASSERT_TRUE(do_write(0, 1'000'002).has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 1'000'002);
    EXPECT_EQ(dev_->missing_count(), 1u); // gap: 1,000,001
}

// Guard 2 (per-write gap cap) only bounds a single write's contribution to missing_lsns_. A client
// that walks the watermark forward in cap-sized increments (+1,000,000 repeatedly) never trips Guard
// 2, yet missing_lsns_ grows without bound across writes. Guard 3 closes this: it rejects a
// gap-creating write once the CUMULATIVE set size already reached its cap, independent of this
// write's own gap distance. Three walks of exactly k_max_ooo_gap each succeed (Guard 3's check uses
// the size BEFORE this write, which only crosses the cap after the third); the fourth is rejected.
TEST_F(CraftWriteTest, CumulativeMissingCapRejectsSustainedWalk) {
    dev_->seed_lsns(0, {});

    ASSERT_TRUE(do_write(0, 1'000'000).has_value());
    EXPECT_EQ(dev_->missing_count(), 999'999u);

    ASSERT_TRUE(do_write(0, 2'000'000).has_value());
    EXPECT_EQ(dev_->missing_count(), 1'999'998u);

    ASSERT_TRUE(do_write(0, 3'000'000).has_value());
    EXPECT_EQ(dev_->missing_count(), 2'999'997u); // now over the 2,000,000 cap

    // A further gap-creating jump is rejected even though its own gap distance (1,000,000) is well
    // within the per-write Guard 2 cap — Guard 3 is what stops the sustained walk.
    auto r = do_write(0, 4'000'000);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(std::errc::value_too_large));
    EXPECT_EQ(dev_->last_append_lsn(), 3'000'000); // rejected write must not advance state
    EXPECT_EQ(dev_->missing_count(), 2'999'997u);  // unchanged
}

// Exact >= fencepost for Guard 3: walk missing_lsns_ up to precisely cap-1 (still accepted), then to
// exactly cap (still accepted -- the write that REACHES the cap is not itself rejected, only the
// next one that finds the cap already reached), then verify the following gap-creating write is
// rejected. Mirrors GapCapFenceposts' precision for Guard 2.
TEST_F(CraftWriteTest, CumulativeMissingCapFencepost) {
    dev_->seed_lsns(0, {});
    ASSERT_TRUE(do_write(0, 1'000'000).has_value());
    ASSERT_TRUE(do_write(0, 2'000'000).has_value());
    EXPECT_EQ(dev_->missing_count(), 1'999'998u);

    // Two +2 gap-creating writes walk the count up one entry at a time to the exact boundary.
    ASSERT_TRUE(do_write(0, 2'000'002).has_value()); // pre-check size=1,999,998 (< cap) -> passes
    EXPECT_EQ(dev_->missing_count(), 1'999'999u);    // cap - 1

    ASSERT_TRUE(do_write(0, 2'000'004).has_value()); // pre-check size=1,999,999 (< cap) -> passes
    EXPECT_EQ(dev_->missing_count(), 2'000'000u);    // == cap exactly

    // The set is now exactly at cap; the next gap-creating write's pre-check sees size == cap.
    auto r = do_write(0, 2'000'006);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(std::errc::value_too_large));
    EXPECT_EQ(dev_->last_append_lsn(), 2'000'004);
    EXPECT_EQ(dev_->missing_count(), 2'000'000u);
}

// A write that FILLS an existing gap must succeed even when missing_lsns_ is already AT the cap --
// otherwise the set could never drain back down once full, turning the mitigation into a permanent
// replica lockup.
TEST_F(CraftWriteTest, CumulativeMissingCapExemptsGapFill) {
    dev_->seed_lsns(0, {});
    ASSERT_TRUE(do_write(0, 1'000'000).has_value());
    ASSERT_TRUE(do_write(0, 2'000'000).has_value());
    ASSERT_TRUE(do_write(0, 2'000'002).has_value());
    ASSERT_TRUE(do_write(0, 2'000'004).has_value());
    ASSERT_EQ(dev_->missing_count(), 2'000'000u); // exactly at cap

    // lsn=1 has been in missing_lsns_ since the very first jump (gap 1..999,999).
    ASSERT_TRUE(dev_->is_missing(1));
    auto r = do_write(0, 1);
    ASSERT_TRUE(r.has_value());
    EXPECT_FALSE(dev_->is_missing(1));
    EXPECT_EQ(dev_->missing_count(), 1'999'999u); // shrank by exactly one
}

// A strictly in-order write (dlsn == last_append_lsn + 1) creates no gap entry and must succeed even
// when missing_lsns_ is already AT the cap. This is the corner case the dlsn > last_append_lsn + 1
// condition (vs. the weaker dlsn > last_append_lsn) exists to protect: an in-order write is "ahead of
// last_append_lsn" too, but adds nothing to the set, so it must not be blocked by unrelated OOO
// activity that happened to fill the set elsewhere.
TEST_F(CraftWriteTest, CumulativeMissingCapExemptsInOrderWrite) {
    dev_->seed_lsns(0, {});
    ASSERT_TRUE(do_write(0, 1'000'000).has_value());
    ASSERT_TRUE(do_write(0, 2'000'000).has_value());
    ASSERT_TRUE(do_write(0, 2'000'002).has_value());
    ASSERT_TRUE(do_write(0, 2'000'004).has_value());
    ASSERT_EQ(dev_->missing_count(), 2'000'000u); // exactly at cap
    ASSERT_EQ(dev_->last_append_lsn(), 2'000'004);

    auto r = do_write(0, 2'000'005);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(dev_->last_append_lsn(), 2'000'005);
    EXPECT_EQ(dev_->missing_count(), 2'000'000u); // unchanged: no gap created
}

// A write with data.size > 0 (non-zero content) must call alloc_write_data exactly once.
// This verifies the HS_DATA_LINKED branch is entered when the payload is non-empty.
TEST_F(CraftWriteTest, NonZeroWriteCallsAllocWriteData) {
    auto r = do_write_with_data(0, 0);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(journal_->alloc_write_data_calls, 1);
    EXPECT_TRUE(journal_->has_slot(0));
}

// Zero-copy (SDSTOR-22873): the sg_list's iovec must reach alloc_write_data unchanged -- same
// buffer pointer, not a copy. do_write_with_data (used above) sets data.size without real iovs, so
// it cannot exercise this; this test builds a real buffer and checks pointer identity survives the
// full path (write() -> alloc_write_data), which is the only way a copy would be detectable.
TEST_F(CraftWriteTest, NonZeroWriteIsZeroCopy) {
    std::array< uint8_t, 4096 > buf{};
    sisl::sg_list data;
    data.size = buf.size();
    data.iovs.push_back(iovec{buf.data(), buf.size()});

    auto r = homeblocks::detail::sync_get(dev_->write(craft::client_hdr{0, -1, -1}, /* dlsn = */ 0, 0, buf.size(),
                                                      std::move(data), /* all_zeros = */ false));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(journal_->last_alloc_data_ptr, static_cast< void const* >(buf.data()));
}

// alloc_write_data failure propagates; write_slot must not be called and no block is freed
// (there is nothing to free — allocation never succeeded).
TEST_F(CraftWriteTest, AllocWriteDataFails_WriteRejected) {
    journal_->fail_alloc = true;
    auto r = do_write_with_data(0, 0);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(journal_->alloc_write_data_calls, 1);
    EXPECT_EQ(journal_->slot_count(), 0u);   // write_slot not called
    EXPECT_EQ(journal_->free_data_calls, 0); // nothing to free
    EXPECT_TRUE(dev_->is_missing(0));        // pre-insert invariant holds
}

// write_slot failure after a successful alloc_write_data must call free_data exactly once
// (Finding 1 fix). This test exercises blkid_allocated=true path.
TEST_F(CraftWriteTest, WriteSlotFailsWithData_BlocksFreed) {
    journal_->fail_lsns.insert(0);
    auto r = do_write_with_data(0, 0);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(journal_->alloc_write_data_calls, 1); // alloc succeeded
    EXPECT_EQ(journal_->slot_count(), 0u);          // write_slot returned error
    EXPECT_EQ(journal_->free_data_calls, 1);        // blocks freed after write_slot failure
    EXPECT_TRUE(dev_->is_missing(0));               // pre-insert invariant holds
}

// all_zeros=false with an empty sg_list (data.size==0) was previously a RELEASE_ASSERT (process abort).
// After the fix it returns invalid_argument so a malformed client frame cannot kill the replica.
TEST_F(CraftWriteTest, AllZerosFalseWithEmptyDataRejected) {
    sisl::sg_list empty_data{};
    auto r = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{0, -1, -1}, 0, 0, 4096, std::move(empty_data), /*all_zeros=*/false));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(std::errc::invalid_argument));
    EXPECT_EQ(journal_->slot_count(), 0u);  // write_slot not reached
    EXPECT_EQ(dev_->last_append_lsn(), -1); // no state mutation
}

// The inverse bad combination: all_zeros=true with a non-empty sg_list. Previously silently
// accepted and dropped the payload (the guard only covered !all_zeros && data.size == 0); now
// rejected symmetrically, since WRITE_ZEROES/unmap names a range and must not also carry data.
TEST_F(CraftWriteTest, AllZerosTrueWithNonEmptyDataRejected) {
    sisl::sg_list data;
    data.size = 4096;
    auto r = homeblocks::detail::sync_get(
        dev_->write(craft::client_hdr{0, -1, -1}, /* dlsn = */ 0, 0, 4096, std::move(data), /* all_zeros = */ true));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), make_error_condition(std::errc::invalid_argument));
    EXPECT_EQ(journal_->slot_count(), 0u);  // write_slot not reached
    EXPECT_EQ(dev_->last_append_lsn(), -1); // no state mutation
}

// write_slot must receive the term from the client_hdr so CraftJournalEntry.term is populated correctly.
// This is the on-disk term used to detect and skip stale-tail entries on recovery.
TEST_F(CraftWriteTest, WriteSlotReceivesCorrectTerm) {
    constexpr uint64_t k_term = 7;
    dev_->seed_term(k_term);
    auto r = do_write(k_term, 0);
    ASSERT_TRUE(r.has_value());
    ASSERT_TRUE(journal_->has_slot(0));
    EXPECT_EQ(journal_->slot_terms[0], k_term);
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
