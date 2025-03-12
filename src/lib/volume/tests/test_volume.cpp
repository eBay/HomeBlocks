/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
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

#include <string>

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <sisl/options/options.h>

#include <homeblks/home_blks.hpp>
#include <homeblks/volume_mgr.hpp>
#include "test_common.hpp"

SISL_LOGGING_DEF(test_volume)
SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTION_GROUP(test_volume_setup,
                  (num_vols, "", "num_vols", "number of volumes", ::cxxopts::value< uint32_t >()->default_value("2"),
                   "number"));
SISL_OPTIONS_ENABLE(logging, config, test_common_setup, test_volume_setup, homeblocks)

std::unique_ptr< test_common::HBTestHelper > g_helper;

using namespace homeblocks;

class VolumeTest : public ::testing::Test {
public:
    void SetUp() override {}

    VolumeInfo gen_vol_info(uint32_t vol_idx) {
        VolumeInfo vol_info;
        vol_info.name = "vol_" + std::to_string(vol_idx);
        vol_info.size_bytes = 1024 * 1024 * 1024;
        vol_info.page_size = 4096;
        vol_info.id = hb_utils::gen_random_uuid();
        return vol_info;
    }
};

TEST_F(VolumeTest, CreateVolume) {
    auto hb = g_helper->inst();
    auto vol_mgr = hb->volume_manager();

    auto num_vols = SISL_OPTIONS["num_vols"].as< uint32_t >();

    for (uint32_t i = 0; i < num_vols; ++i) {
        auto vinfo = gen_vol_info(i);
        auto ret = vol_mgr->create_volume(vinfo).get();
        ASSERT_TRUE(ret);

        auto vinfo_ptr = vol_mgr->lookup_volume(vinfo.id);
    }

    g_helper->restart();

    // verify the volumes are still there
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, config, test_common_setup, test_volume_setup, homeblocks);
    sisl::logging::SetLogger(std::string(argv[0]));
    sisl::logging::SetLogPattern("[%D %T%z] [%^%L%$] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);

    std::vector< std::string > args;
    for (int i = 0; i < argc; ++i) {
        args.emplace_back(argv[i]);
    }

    g_helper = std::make_unique< test_common::HBTestHelper >("test_homeblocks", args, argv);
    g_helper->setup();
    auto ret = RUN_ALL_TESTS();
    g_helper->teardown();

    return ret;
}
