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
// Also exercises HomeStoreCraftCheckpointTrigger::trigger_cp_flush (SDSTOR-22888) against the REAL
// homestore::cp_mgr() -- same rationale: MockCraftCheckpointTrigger (test_craft_raft_entries.cpp)
// covers CraftReplDev's own gating logic, but the wrapper's factory -> cp_mgr().trigger_cp_flush()
// -> async_status conversion chain had never been compiled and run against a live CPManager.
//
// Links the full homeblocks library (unlike the other craft tests, which compile
// craft_repl_dev.cpp directly to avoid HomeStore bring-up) because a real home_log_store requires
// a running HomeStore instance.

#include <condition_variable>
#include <cstring>
#include <mutex>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/options/options.h>
#include <homestore/logstore_service.hpp>

#include "hb_internal.hpp"
#include "craft/craft_repl_dev.hpp"
#include "coro_helpers.hpp"
#include "test_common.hpp"

SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTIONS_ENABLE(logging, test_common_setup)
SISL_LOGGING_DECL(test_craft_homestore_backend)

std::unique_ptr< test_common::HBTestHelper > g_helper;

using namespace homeblocks;

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
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0);

    auto r =
        homeblocks::detail::sync_get(backend->write_slot(/* lsn = */ 0, /* term = */ 1, /* lba = */ 0, /* len = */ 4096,
                                                         homestore::multi_blk_id{}, /* all_zeros = */ true));
    ASSERT_TRUE(r.has_value());
}

// truncate_to(lsn) must drop journal entries above lsn from the real logdev's tail -- verified via
// the log_store's own tail_lsn() rather than read_slot (not yet implemented; returns not_supported).
TEST_F(CraftHomeStoreBackendTest, TruncateToRollsBackRealLogStore) {
    auto logstore = make_logstore();
    ASSERT_TRUE(logstore != nullptr);
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0);

    for (int64_t lsn = 0; lsn <= 4; ++lsn) {
        auto r = homeblocks::detail::sync_get(backend->write_slot(lsn, /* term = */ 1, /* lba = */ 0, /* len = */ 4096,
                                                                  homestore::multi_blk_id{}, /* all_zeros = */ true));
        ASSERT_TRUE(r.has_value());
    }
    ASSERT_EQ(logstore->tail_lsn(), 4);

    auto r = homeblocks::detail::sync_get(backend->truncate_to(2));
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(logstore->tail_lsn(), 2);
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
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0);

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
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0);

    auto w = homeblocks::detail::sync_get(backend->write_slot(/* lsn = */ 0, /* term = */ 1, /* lba = */ 0,
                                                              /* len = */ 4096, homestore::multi_blk_id{},
                                                              /* all_zeros = */ true));
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
    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0);

    std::vector< uint8_t > garbage(64, 0xEE); // larger than sizeof(CraftJournalEntry); not its magic/version
    sisl::io_blob raw_blob{garbage.data(), static_cast< uint32_t >(garbage.size()), /* is_aligned = */ false};

    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    logstore->write_async(/* seq_num = */ 0, raw_blob, nullptr,
                          [&](homestore::logstore_seq_num_t, sisl::io_blob&, homestore::logdev_key, void*) {
                              std::lock_guard< std::mutex > lk{mu};
                              done = true;
                              cv.notify_one();
                          });
    std::unique_lock< std::mutex > lk{mu};
    cv.wait(lk, [&] { return done; });
    lk.unlock();

    auto r = homeblocks::detail::sync_get(backend->free_slot(0));
    ASSERT_FALSE(r.has_value());
}

// force=false: the value apply_sync_rs_commit_lsn's periodic trigger actually passes today.
TEST_F(CraftHomeStoreBackendTest, CheckpointTriggerFlushesRealCPManager) {
    auto trigger = make_homestore_checkpoint_trigger();
    ASSERT_TRUE(trigger != nullptr);

    auto r = homeblocks::detail::sync_get(trigger->trigger_cp_flush(/* force = */ false));
    ASSERT_TRUE(r.has_value());
}

// force=true: untested until now -- this is the value truncate()'s FIXME (craft_repl_dev.hpp) says
// a future correctness-critical call site will need, but the passthrough itself had never been
// exercised against the real cp_mgr() for either value.
TEST_F(CraftHomeStoreBackendTest, CheckpointTriggerHonorsForceFlag) {
    auto trigger = make_homestore_checkpoint_trigger();
    ASSERT_TRUE(trigger != nullptr);

    auto r = homeblocks::detail::sync_get(trigger->trigger_cp_flush(/* force = */ true));
    ASSERT_TRUE(r.has_value());
}

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
