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

// Exercises HomeStoreCraftJournalBackend::write_slot against a REAL HomeStore home_log_store --
// the only place in the CRAFT test suite that runs this code instead of MockCraftJournalBackend.
// The value_awaitable/run_on_forget/INLINE-safety bridge in write_slot is the subtlest code in
// this backend and was previously verified only by comments, never executed.
//
// make_homestore_journal_backend has no production call site yet, so this deliberately drives the
// backend directly rather than through CraftReplDev or a volume -- the narrowest test that still
// runs the real completion path.
//
// Also exercises make_homestore_checkpoint_trigger_fn's result (SDSTOR-22888) against the REAL
// homestore::cp_mgr() -- same rationale: MockCraftCheckpointTrigger (test_craft_raft_entries.cpp)
// covers CraftReplDev's own gating logic, but the wrapper's factory -> cp_mgr().trigger_cp_flush()
// -> async_status conversion chain had never been compiled and run against a live CPManager.
//
// Links the full homeblocks library (unlike the other craft tests, which compile
// craft_repl_dev.cpp directly to avoid HomeStore bring-up) because a real home_log_store requires
// a running HomeStore instance.

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/async/value_awaitable.hpp>
#include <sisl/async/when_all.hpp>
#include <sisl/options/options.h>
#include <homestore/logstore_service.hpp>
#include <iomgr/iomgr.hpp>

#include "hb_internal.hpp"
#include "homeblks_impl.hpp"
#include "home_blks_config.hpp"
#include "craft/craft_repl_dev.hpp"
#include "coro_helpers.hpp"
#include "test_common.hpp"

SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTIONS_ENABLE(logging, test_common_setup)
SISL_LOGGING_DECL(test_craft_homestore_backend)

std::unique_ptr< test_common::HBTestHelper > g_helper;

using namespace homeblocks;

static constexpr uint32_t k_page_size = 4096;

// Writes a raw blob directly to the log store, bypassing HomeStoreCraftJournalBackend::write_slot's
// own encoding -- used to simulate a corrupted/malformed on-disk record for read_slot's validate-
// before-trust tests. Mirrors write_slot's OWN write_async/value_awaitable completion bridge rather
// than using home_log_store::write_and_flush(): write_and_flush leaves the logdev's completion
// bookkeeping inconsistent with what a graceful homestore shutdown expects, hanging teardown
// indefinitely (confirmed by bisection -- every test using it hangs in isolation; every test using
// this bridge, or write_slot itself, does not).
void write_raw_blob(shared< homestore::home_log_store > const& logstore, int64_t lsn, sisl::io_blob_safe const& blob) {
    auto va = std::make_shared< sisl::async::value_awaitable< bool > >();
    auto write_ret = logstore->write_async(
        static_cast< homestore::logstore_seq_num_t >(lsn), blob, nullptr,
        [va](homestore::logstore_seq_num_t, sisl::io_blob&, homestore::logdev_key, void*) mutable {
            iomanager.run_on_forget(iomgr::reactor_regex::least_busy_io,
                                    [va = std::move(va)]() mutable { va->complete(true); });
        });
    ASSERT_GE(write_ret, 0);
    homeblocks::detail::sync_get([va]() -> homestore::async_status {
        co_await *va;
        co_return homestore::ok();
    }());
}

class CraftHomeStoreBackendTest : public ::testing::Test {
protected:
    // Each test gets its own logdev/log_store so writes/rollbacks in one test cannot affect another.
    // TIMER | INLINE matches solo_repl_dev's configuration: the mode where write_async's completion
    // can fire before await_suspend returns, the INLINE-safety hazard write_slot's comment documents.
    shared< homestore::home_log_store > make_logstore() {
        auto flush_mode =
            static_cast< homestore::flush_mode_t >(static_cast< uint32_t >(homestore::flush_mode_t::TIMER) |
                                                   static_cast< uint32_t >(homestore::flush_mode_t::INLINE));
        auto logdev_id = homestore::logstore_service().create_new_logdev(flush_mode);
        return homestore::logstore_service().create_new_log_store(logdev_id, /* append_mode = */ false);
    }
};

// A hang here (rather than a pass) is exactly the deadlock/UB this test exists to catch.
TEST_F(CraftHomeStoreBackendTest, WriteSlotCompletesInlineWithoutHanging) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    auto r = homeblocks::detail::sync_get(
        backend->write_slot(/* lsn = */ 0, /* term = */ 1, /* lba = */ 0, /* len = */ 4096, homestore::multi_blk_id{},
                            /* all_zeros = */ true, std::vector< homestore::csum_t >{}));
    ASSERT_TRUE(r.has_value());
}

// A write HomeStore hasn't written by its deadline fails timed_out, and HomeStore writes the entry later, whole: its
// buffer outlives the write. This log flushes only when asked, so the entry is still pending when the deadline passes.
TEST_F(CraftHomeStoreBackendTest, AWritePastItsDeadlineIsWrittenWholeLater) {
    auto const logdev_id = homestore::logstore_service().create_new_logdev(homestore::flush_mode_t::EXPLICIT);
    auto logstore = homestore::logstore_service().create_new_log_store(logdev_id, /* append_mode = */ false);
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    auto const timeout = HB_DYNAMIC_CONFIG(craft_journal_write_timeout_ms);
    HB_SETTINGS_FACTORY().modifiable_settings([](auto& s) { s.craft_journal_write_timeout_ms = 1; });
    std::optional< status > written;
    homeblocks::detail::detach([](CraftJournalBackend* b, std::optional< status >& out) -> async_status {
        out = co_await b->write_slot(/* lsn = */ 0, /* term = */ 1, /* lba = */ 0, /* len = */ 4096,
                                     homestore::multi_blk_id{}, /* all_zeros = */ true, {});
        co_return ok();
    }(backend.get(), written));
    HB_SETTINGS_FACTORY().modifiable_settings([timeout](auto& s) { s.craft_journal_write_timeout_ms = timeout; });
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    backend->expire_overdue();
    ASSERT_TRUE(written.has_value());
    ASSERT_FALSE(*written);
    EXPECT_EQ(written->error(), std::errc::timed_out);

    ASSERT_TRUE(logstore->flush());
    auto const slot = homeblocks::detail::sync_get(backend->read_slot(0));
    ASSERT_TRUE(slot) << slot.error().message();
    EXPECT_EQ(slot->lsn, 0);
    EXPECT_TRUE(slot->all_zeros);
    EXPECT_EQ(slot->len_bytes, 4096u);
}

// truncate_to(lsn) must drop journal entries above lsn from the real logdev's tail -- verified via
// the backend's tail_lsn().
TEST_F(CraftHomeStoreBackendTest, TruncateToRollsBackRealLogStore) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    for (int64_t lsn = 0; lsn <= 4; ++lsn) {
        auto r = homeblocks::detail::sync_get(backend->write_slot(lsn, /* term = */ 1, /* lba = */ 0, /* len = */ 4096,
                                                                  homestore::multi_blk_id{}, /* all_zeros = */ true,
                                                                  std::vector< homestore::csum_t >{}));
        ASSERT_TRUE(r.has_value());
    }
    ASSERT_EQ(homeblocks::detail::sync_get(backend->tail_lsn()), 4);

    auto r = homeblocks::detail::sync_get(backend->truncate_to(2));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(homeblocks::detail::sync_get(backend->tail_lsn()), 2);
}

// Journal reclaim against the real log store: entries <= upto become unreadable, later ones stay, and the reclaim
// point is reported back (a restart reads it from here). Never past the last entry: the tail stays.
TEST_F(CraftHomeStoreBackendTest, ReclaimUptoDropsTheJournalPrefix) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);
    for (int64_t lsn = 0; lsn <= 4; ++lsn) {
        auto r = homeblocks::detail::sync_get(backend->write_slot(lsn, /* term = */ 1, /* lba = */ 0, /* len = */ 4096,
                                                                  homestore::multi_blk_id{}, /* all_zeros = */ true,
                                                                  std::vector< homestore::csum_t >{}));
        ASSERT_TRUE(r.has_value());
    }
    EXPECT_EQ(backend->reclaimed_upto(), -1);
    ASSERT_TRUE(homeblocks::detail::sync_get(backend->reclaim_upto(2)).has_value());
    EXPECT_EQ(backend->reclaimed_upto(), 2);
    EXPECT_FALSE(homeblocks::detail::sync_get(backend->read_slot(2)).has_value());
    EXPECT_TRUE(homeblocks::detail::sync_get(backend->read_slot(3)).has_value());

    ASSERT_TRUE(homeblocks::detail::sync_get(backend->reclaim_upto(10)).has_value());
    EXPECT_EQ(backend->reclaimed_upto(), 4);
    EXPECT_EQ(homeblocks::detail::sync_get(backend->tail_lsn()), 4);
}

// A journal never written to has tail -1: restart recovery takes max(tail, ...) for last_append_lsn, so any other
// value would invent appended slots.
TEST_F(CraftHomeStoreBackendTest, TailLsnOfANewJournalIsMinusOne) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);
    EXPECT_EQ(homeblocks::detail::sync_get(backend->tail_lsn()), -1);
}

// read_slot must parse back exactly what write_slot wrote: header fields, the csum array, and the blkid.
TEST_F(CraftHomeStoreBackendTest, ReadSlotReturnsCorrectData) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    homestore::multi_blk_id blkid{/* blk_num = */ 42, /* nblks = */ 2, /* chunk_num = */ 7};
    std::vector< homestore::csum_t > csums{111, 222};

    auto w = homeblocks::detail::sync_get(backend->write_slot(/* lsn = */ 3, /* term = */ 5, /* lba = */ 100,
                                                              /* len = */ 2 * k_page_size, blkid,
                                                              /* all_zeros = */ false, csums));
    ASSERT_TRUE(w.has_value());

    auto r = homeblocks::detail::sync_get(backend->read_slot(3));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->lsn, 3);
    EXPECT_FALSE(r->all_zeros);
    EXPECT_EQ(r->lba_off_bytes, 100u);
    EXPECT_EQ(r->len_bytes, 2 * k_page_size);
    EXPECT_EQ(r->csums, csums);
    EXPECT_TRUE(r->blkid == blkid);
}

// A never-appended lsn must fail cleanly (the log store throws std::out_of_range internally).
TEST_F(CraftHomeStoreBackendTest, ReadSlotUnknownLsnFails) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    auto r = homeblocks::detail::sync_get(backend->read_slot(99));
    ASSERT_FALSE(r.has_value());
}

// read_slot must round-trip an all_zeros slot correctly: nlbas=0 (empty csum array), and the
// still-present (empty) serialized multi_blk_id region.
TEST_F(CraftHomeStoreBackendTest, ReadSlotRoundTripsAllZerosSlot) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    auto w =
        homeblocks::detail::sync_get(backend->write_slot(/* lsn = */ 0, /* term = */ 1, /* lba = */ 200,
                                                         /* len = */ 2 * k_page_size, homestore::multi_blk_id{},
                                                         /* all_zeros = */ true, std::vector< homestore::csum_t >{}));
    ASSERT_TRUE(w.has_value());

    auto r = homeblocks::detail::sync_get(backend->read_slot(0));
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->all_zeros);
    EXPECT_EQ(r->lba_off_bytes, 200u);
    EXPECT_EQ(r->len_bytes, 2 * k_page_size);
    EXPECT_TRUE(r->csums.empty());
}

// A blob too small for even the fixed 34-byte header must fail cleanly, not crash or misread
// garbage as a header.
TEST_F(CraftHomeStoreBackendTest, ReadSlotTooSmallForHeaderFails) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    sisl::io_blob_safe tiny{4};
    std::memset(tiny.bytes(), 0, tiny.size());
    write_raw_blob(logstore, 0, tiny);

    auto r = homeblocks::detail::sync_get(backend->read_slot(0));
    ASSERT_FALSE(r.has_value());
}

// A record with a correctly-sized blob but a stomped magic must be rejected rather than trusted.
TEST_F(CraftHomeStoreBackendTest, ReadSlotBadMagicFails) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    // Write a real all_zeros slot to get a correctly-sized, otherwise-valid blob.
    auto w =
        homeblocks::detail::sync_get(backend->write_slot(0, 1, 0, k_page_size, homestore::multi_blk_id{},
                                                         /* all_zeros = */ true, std::vector< homestore::csum_t >{}));
    ASSERT_TRUE(w.has_value());

    // Corrupt the leading bytes (the on-disk magic field starts at offset 0) and rewrite at a
    // fresh lsn -- read_slot must reject it rather than trust a garbage header.
    auto raw = logstore->read_sync(0);
    sisl::io_blob_safe corrupted{raw.size()};
    std::memcpy(corrupted.bytes(), raw.bytes(), raw.size());
    std::memset(corrupted.bytes(), 0xFF, 4);
    write_raw_blob(logstore, 1, corrupted);

    auto r = homeblocks::detail::sync_get(backend->read_slot(1));
    ASSERT_FALSE(r.has_value());
}

// A record with a correct magic but a stomped version byte must be rejected -- this is the version
// field's entire purpose: a layout change (e.g. the checksum-array region added between the header
// and the blkid) bumps k_journal_version so a record from a different layout fails this check
// instead of being misparsed. version (uint8_t) sits at offset 4, right after the 4-byte magic.
TEST_F(CraftHomeStoreBackendTest, ReadSlotBadVersionFails) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    auto w =
        homeblocks::detail::sync_get(backend->write_slot(0, 1, 0, k_page_size, homestore::multi_blk_id{},
                                                         /* all_zeros = */ true, std::vector< homestore::csum_t >{}));
    ASSERT_TRUE(w.has_value());

    auto raw = logstore->read_sync(0);
    sisl::io_blob_safe corrupted{raw.size()};
    std::memcpy(corrupted.bytes(), raw.bytes(), raw.size());
    corrupted.bytes()[4] = 99; // stomp version, leave magic (bytes 0-3) intact
    write_raw_blob(logstore, 1, corrupted);

    auto r = homeblocks::detail::sync_get(backend->read_slot(1));
    ASSERT_FALSE(r.has_value());
}

// A record whose embedded hdr.lsn does not match the seq_num it was read back at must be rejected
// rather than silently trusted -- simulated here by writing a valid slot at lsn=0, then re-writing
// that SAME (otherwise perfectly valid) blob verbatim at lsn=1: the blob's own hdr.lsn field still
// says 0, so read_slot(1) must fail the cross-check rather than return a slot claiming to be lsn=1.
TEST_F(CraftHomeStoreBackendTest, ReadSlotLsnMismatchFails) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    auto w =
        homeblocks::detail::sync_get(backend->write_slot(0, 1, 0, k_page_size, homestore::multi_blk_id{},
                                                         /* all_zeros = */ true, std::vector< homestore::csum_t >{}));
    ASSERT_TRUE(w.has_value());

    auto raw = logstore->read_sync(0);
    sisl::io_blob_safe replayed{raw.size()};
    std::memcpy(replayed.bytes(), raw.bytes(), raw.size());
    write_raw_blob(logstore, 1, replayed);

    auto r = homeblocks::detail::sync_get(backend->read_slot(1));
    ASSERT_FALSE(r.has_value());
}

// A non-all_zeros record whose hdr.len is not a positive multiple of lba_size (corrupted/malformed)
// must be rejected before it's used to derive nlbas -- otherwise an unaligned len silently
// misaligns the csum-array and blkid offsets that follow, deserializing garbage rather than
// failing cleanly. hdr.len is a lba_count_t (uint32_t) sitting at a fixed offset (29 bytes) within
// CraftJournalEntry's packed 34-byte layout: magic(4)+version(1)+term(8)+lsn(8)+lba(8)+len(4)+all_zeros(1).
TEST_F(CraftHomeStoreBackendTest, ReadSlotMisalignedLenFails) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    homestore::multi_blk_id blkid{42, 1, 7};
    std::vector< homestore::csum_t > csums{111};
    auto w =
        homeblocks::detail::sync_get(backend->write_slot(0, 1, 0, k_page_size, blkid, /* all_zeros = */ false, csums));
    ASSERT_TRUE(w.has_value());

    auto raw = logstore->read_sync(0);
    sisl::io_blob_safe corrupted{raw.size()};
    std::memcpy(corrupted.bytes(), raw.bytes(), raw.size());
    uint32_t const bad_len = 1; // nonzero, but not a multiple of k_page_size
    std::memcpy(corrupted.bytes() + 29, &bad_len, sizeof(bad_len));
    write_raw_blob(logstore, 1, corrupted);

    auto r = homeblocks::detail::sync_get(backend->read_slot(1));
    ASSERT_FALSE(r.has_value());
}

// A truncated blob (real header/magic, but cut short of even the fixed header size) must be
// rejected rather than read past the end of the buffer.
TEST_F(CraftHomeStoreBackendTest, ReadSlotTruncatedBlobFails) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    homestore::multi_blk_id blkid{42, 2, 7};
    std::vector< homestore::csum_t > csums{111, 222};
    auto w = homeblocks::detail::sync_get(
        backend->write_slot(0, 1, 0, 2 * k_page_size, blkid, /* all_zeros = */ false, csums));
    ASSERT_TRUE(w.has_value());

    auto raw = logstore->read_sync(0);
    ASSERT_GT(raw.size(), 10u);
    sisl::io_blob_safe truncated{10}; // smaller than even the fixed 34-byte header
    std::memcpy(truncated.bytes(), raw.bytes(), 10);
    write_raw_blob(logstore, 1, truncated);

    auto r = homeblocks::detail::sync_get(backend->read_slot(1));
    ASSERT_FALSE(r.has_value());
}

// alloc_write_data's application_hint routes allocation through VolumeChunkSelector by vol_ordinal.
// No volume was created in this test (make_homestore_journal_backend has no production call site
// yet, so there is no real ordinal to allocate against), so this hits VolumeChunkSelector with an
// unregistered ordinal -- previously a null-pointer dereference (select_chunk indexed
// m_volume_chunks[ordinal] and dereferenced the null slot without checking), now fixed. This is a
// regression test for that fix: alloc_write_data must fail cleanly, not crash.
TEST_F(CraftHomeStoreBackendTest, AllocWriteDataFailsCleanlyForUnregisteredOrdinal) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    constexpr uint32_t k_len = 4096;
    sisl::io_blob_safe buf{k_len, 512};
    std::memset(buf.bytes(), 0xab, k_len);
    sisl::sg_list data{.size = k_len, .iovs = {iovec{buf.bytes(), k_len}}};

    auto alloc_r = homeblocks::detail::sync_get(backend->alloc_write_data(data, static_cast< lba_count_t >(k_len)));
    ASSERT_FALSE(alloc_r.has_value());
}

// free_slot reads the raw entry back off the log store and validates magic/version/lsn before trusting it.
TEST_F(CraftHomeStoreBackendTest, FreeSlotSucceedsForRealEntry) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    auto w = homeblocks::detail::sync_get(
        backend->write_slot(/* lsn = */ 0, /* term = */ 1, /* lba = */ 0, /* len = */ 4096, homestore::multi_blk_id{},
                            /* all_zeros = */ true, std::vector< homestore::csum_t >{}));
    ASSERT_TRUE(w.has_value());

    auto r = homeblocks::detail::sync_get(backend->free_slot(0));
    ASSERT_TRUE(r.has_value());
}

// Writes a raw blob directly to the log store (bypassing write_slot's serialization entirely) that
// doesn't conform to CraftJournalEntry's magic/version -- simulates a corrupt or foreign record.
// free_slot must reject it rather than misreading garbage bytes as a valid blkid.
TEST_F(CraftHomeStoreBackendTest, FreeSlotRejectsCorruptEntry) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0, k_page_size);

    std::vector< uint8_t > garbage(64, 0xEE); // larger than sizeof(CraftJournalEntry); not its magic/version
    sisl::io_blob raw_blob{garbage.data(), static_cast< uint32_t >(garbage.size()), /* is_aligned = */ false};

    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    auto write_ret = logstore->write_async(
        /* seq_num = */ 0, raw_blob, nullptr,
        [&](homestore::logstore_seq_num_t, sisl::io_blob&, homestore::logdev_key, void*) {
            std::lock_guard< std::mutex > lk{mu};
            done = true;
            cv.notify_one();
        });
    ASSERT_GE(write_ret, 0) << "write_async rejected -- log store or logdev is stopping, callback will not fire";

    std::unique_lock< std::mutex > lk{mu};
    ASSERT_TRUE(cv.wait_for(lk, std::chrono::seconds(5), [&] { return done; }))
        << "write_async callback never fired -- lost completion (see craft_repl_dev.cpp:139-150)";
    lk.unlock();

    auto r = homeblocks::detail::sync_get(backend->free_slot(0));
    ASSERT_FALSE(r.has_value());
}

// force=false: the value apply_sync_rs_commit_lsn's periodic trigger actually passes today.
TEST_F(CraftHomeStoreBackendTest, CheckpointTriggerFlushesRealCPManager) {
    auto trigger = make_homestore_checkpoint_trigger_fn();

    auto r = homeblocks::detail::sync_get(trigger(/* force = */ false));
    ASSERT_TRUE(r.has_value());
}

// force=true: untested until now -- this is the value truncate()'s FIXME (craft_repl_dev.hpp) says
// a future correctness-critical call site will need, but the passthrough itself had never been
// exercised against the real cp_mgr() for either value.
TEST_F(CraftHomeStoreBackendTest, CheckpointTriggerHonorsForceFlag) {
    auto trigger = make_homestore_checkpoint_trigger_fn();

    auto r = homeblocks::detail::sync_get(trigger(/* force = */ true));
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(*r) << "a forced checkpoint flushes";
}

// cp_mgr()'s in-flight-flush gate (m_in_flush_phase) is a synchronous check-and-set at entry: the
// first of several concurrent trigger_cp_flush(false) calls holds it for the duration of the real
// flush, so every other concurrent call observes it already set and gets HomeStore's synchronous
// false back -- not a failure. make_homestore_checkpoint_trigger_fn's result must map that to false
// (coalesced) when force=false; every one of these concurrent calls must succeed. None of them has to
// flush: HomeStore's own timer checkpoint may hold the gate for the whole batch.
TEST_F(CraftHomeStoreBackendTest, CheckpointTriggerConcurrentForceFalseNeverFails) {
    auto trigger = make_homestore_checkpoint_trigger_fn();

    constexpr int k_concurrent = 20;
    std::vector< async_result< bool > > futs; // each a sisl::async::task<result<bool>>
    futs.reserve(k_concurrent);
    for (int i = 0; i < k_concurrent; ++i) {
        futs.push_back(trigger(/* force = */ false));
    }

    auto const results = homeblocks::detail::sync_get(sisl::async::when_all(std::move(futs)));
    for (auto const& r : results) {
        EXPECT_TRUE(r.has_value());
    }
}

#ifdef _PRERELEASE
// Every CRAFT state record in the meta service, by volume, read as a boot reads them.
static auto read_state_records() {
    return std::dynamic_pointer_cast< HomeBlocksImpl >(g_helper->inst())->read_craft_state_records();
}

// A partition's durable state in its own meta blk record: each image persisted replaces the last, whatever its size
// (the large one spans several meta blks), and is what a boot reads back; a record belongs to one volume; destroy()
// removes it, and nothing is persisted after it.
TEST_F(CraftHomeStoreBackendTest, StateStoreKeepsTheImageInItsMetaBlk) {
    auto const vol_id = boost::uuids::random_generator()();
    auto store = make_homestore_state_store(vol_id, homestore::superblk< craft_state_sb_t >{craft_state_meta_name});
    auto const none = store->load();
    ASSERT_TRUE(none);
    EXPECT_FALSE(*none) << "nothing persisted yet";

    CraftDurableState small{.term = 7,
                            .client_token = 42,
                            .last_applied_raft_idx = 9,
                            .durable_commit_lsn = 3,
                            .sync_watermark = 5,
                            .empty_lsns = {4, 5}};
    small.pending.push_back(CraftPendingEntry{.raft_idx = 10, .type = CraftEntryType::InternalLogin, .term = 8});
    CraftDurableState large = small;
    for (int64_t i = 0; i < 1000; ++i) {
        large.pending.push_back(CraftPendingEntry{
            .raft_idx = 11 + i, .type = CraftEntryType::SyncRSCommitLSN, .rs_commit_lsn = i, .client_token = 42});
    }
    // The same image twice: the second write is skipped, the record unchanged; a changed one is written again.
    for (auto const& image : {small, small, large, small}) {
        ASSERT_TRUE(homeblocks::detail::sync_get(store->persist(image)));
        auto const back = store->load();
        ASSERT_TRUE(back && *back);
        EXPECT_EQ(**back, image);

        auto records = read_state_records();
        ASSERT_EQ(records.size(), 1u);
        ASSERT_TRUE(records.contains(vol_id));
        auto const reread = make_homestore_state_store(vol_id, std::move(records.at(vol_id)))->load();
        ASSERT_TRUE(reread && *reread);
        EXPECT_EQ(**reread, image) << "the record a boot reads";
    }

    // After a restart, a store made from the record a boot reads counts its image as written; a changed image is still
    // written.
    {
        auto const boot_id = boost::uuids::random_generator()();
        auto first =
            make_homestore_state_store(boot_id, homestore::superblk< craft_state_sb_t >{craft_state_meta_name});
        ASSERT_TRUE(homeblocks::detail::sync_get(first->persist(small)));
        first.reset(); // stopped: the record stays
        auto booted = make_homestore_state_store(boot_id, std::move(read_state_records().at(boot_id)));
        ASSERT_TRUE(homeblocks::detail::sync_get(booted->persist(small)));
        ASSERT_TRUE(homeblocks::detail::sync_get(booted->persist(large)));
        auto const reread = make_homestore_state_store(boot_id, std::move(read_state_records().at(boot_id)))->load();
        ASSERT_TRUE(reread && *reread);
        EXPECT_EQ(**reread, large);
        booted->destroy();
    }

    auto records = read_state_records();
    ASSERT_TRUE(records.contains(vol_id));
    EXPECT_FALSE(make_homestore_state_store(boost::uuids::random_generator()(), std::move(records.at(vol_id)))->load())
        << "another volume's record";

    // Marked faulted: nothing loads or persists any more, also from the record a boot reads.
    store->mark_faulted();
    EXPECT_EQ(store->load().error(), std::make_error_condition(std::errc::state_not_recoverable));
    EXPECT_FALSE(homeblocks::detail::sync_get(store->persist(small)));
    records = read_state_records();
    ASSERT_TRUE(records.contains(vol_id));
    EXPECT_EQ(make_homestore_state_store(vol_id, std::move(records.at(vol_id)))->load().error(),
              std::make_error_condition(std::errc::state_not_recoverable));

    // A partition faulted before it ever persisted gets a record of its own, saying so.
    auto const other_id = boost::uuids::random_generator()();
    auto other = make_homestore_state_store(other_id, homestore::superblk< craft_state_sb_t >{craft_state_meta_name});
    other->mark_faulted();
    records = read_state_records();
    ASSERT_TRUE(records.contains(other_id));
    EXPECT_EQ(make_homestore_state_store(other_id, std::move(records.at(other_id)))->load().error(),
              std::make_error_condition(std::errc::state_not_recoverable));
    other->destroy();

    store->destroy();
    EXPECT_TRUE(read_state_records().empty());
    auto const gone = store->load();
    ASSERT_TRUE(gone);
    EXPECT_FALSE(*gone);
    EXPECT_FALSE(homeblocks::detail::sync_get(store->persist(small)));
    EXPECT_TRUE(read_state_records().empty());
}
#endif

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    char** orig_argv = argv;
    std::vector< std::string > args;
    for (int i = 0; i < argc; ++i) {
        args.emplace_back(argv[i]);
    }

    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_common_setup);
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");

    g_helper = std::make_unique< test_common::HBTestHelper >("test_craft_homestore_backend", args, orig_argv);
    g_helper->setup();
    auto ret = RUN_ALL_TESTS();
    g_helper->teardown();
    return ret;
}
