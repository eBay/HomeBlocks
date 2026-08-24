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
//
// szmyd (PR #171 review 4900497567): the value_awaitable/run_on_forget/INLINE-safety bridge in
// write_slot is the subtlest code in the PR and was previously verified only by comments, never
// executed. make_homestore_journal_backend has no production call site yet (S8/SDSTOR-22745 is
// first), so this deliberately drives the backend directly rather than through CraftReplDev or a
// volume -- narrowest test that still runs the real completion path.
//
// Links the full homeblocks library (unlike the other craft tests, which compile
// craft_repl_dev.cpp directly to avoid HomeStore bring-up) because a real home_log_store requires
// a running HomeStore instance.

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

class CraftHomeStoreBackendTest : public ::testing::Test {};

// TIMER | INLINE matches solo_repl_dev's configuration: the mode where write_async's completion
// can fire before await_suspend returns, the INLINE-safety hazard write_slot's comment documents.
// A hang here (rather than a pass) is exactly the deadlock/UB this test exists to catch.
TEST_F(CraftHomeStoreBackendTest, WriteSlotCompletesInlineWithoutHanging) {
    auto flush_mode = static_cast< homestore::flush_mode_t >(
        static_cast< uint32_t >(homestore::flush_mode_t::TIMER) | static_cast< uint32_t >(homestore::flush_mode_t::INLINE));
    auto logdev_id = homestore::logstore_service().create_new_logdev(flush_mode);
    auto logstore = homestore::logstore_service().create_new_log_store(logdev_id, /* append_mode = */ false);
    ASSERT_TRUE(logstore != nullptr);

    auto backend = make_homestore_journal_backend(logstore, /* vol_ordinal = */ 0);

    auto r = homeblocks::detail::sync_get(
        backend->write_slot(/* lsn = */ 0, /* term = */ 1, /* lba = */ 0, /* len = */ 4096,
                            homestore::multi_blk_id{}, /* all_zeros = */ true));
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
