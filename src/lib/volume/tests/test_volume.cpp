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

SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTION_GROUP(test_volume_setup,
                  (num_vols, "", "num_vols", "number of volumes", ::cxxopts::value< uint32_t >()->default_value("2"),
                   "number"));
SISL_OPTIONS_ENABLE(logging, test_common_setup, test_volume_setup, homeblocks)
SISL_LOGGING_DECL(test_volume)

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

TEST_F(VolumeTest, CreateDestroyVolume) {
    std::vector< volume_id_t > vol_ids;
    {
        auto hb = g_helper->inst();
        auto vol_mgr = hb->volume_manager();

        auto num_vols = SISL_OPTIONS["num_vols"].as< uint32_t >();

        for (uint32_t i = 0; i < num_vols; ++i) {
            auto vinfo = gen_vol_info(i);
            auto id = vinfo.id;
            vol_ids.emplace_back(id);
            auto ret = vol_mgr->create_volume(std::move(vinfo)).get();
            ASSERT_TRUE(ret);

            auto vinfo_ptr = vol_mgr->lookup_volume(id);
            // verify the volume is there
            ASSERT_TRUE(vinfo_ptr != nullptr);
        }

        for (uint32_t i = 0; i < num_vols; ++i) {
            auto id = vol_ids[i];
            auto ret = vol_mgr->remove_volume(id).get();
            ASSERT_TRUE(ret);

            auto vinfo_ptr = vol_mgr->lookup_volume(id);
            // verify the volume is not there
            ASSERT_TRUE(vinfo_ptr == nullptr);
        }
    }
}

TEST_F(VolumeTest, CreateVolumeThenRecover) {
    std::vector< volume_id_t > vol_ids;
    {
        auto hb = g_helper->inst();
        auto vol_mgr = hb->volume_manager();

        auto num_vols = SISL_OPTIONS["num_vols"].as< uint32_t >();

        for (uint32_t i = 0; i < num_vols; ++i) {
            auto vinfo = gen_vol_info(i);
            auto id = vinfo.id;
            vol_ids.emplace_back(id);
            auto ret = vol_mgr->create_volume(std::move(vinfo)).get();
            ASSERT_TRUE(ret);

            auto vinfo_ptr = vol_mgr->lookup_volume(id);
            // verify the volume is there
            ASSERT_TRUE(vinfo_ptr != nullptr);
        }
    }

    g_helper->restart(5);

    // verify the volumes are still there
    {
        auto hb = g_helper->inst();
        auto vol_mgr = hb->volume_manager();

        for (const auto& id : vol_ids) {
            auto vinfo_ptr = vol_mgr->lookup_volume(id);
            // verify the volume is there
            ASSERT_TRUE(vinfo_ptr != nullptr);
        }
    }
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_common_setup, test_volume_setup, homeblocks);
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);

    std::vector< std::string > args;
    for (int i = 0; i < argc; ++i) {
        args.emplace_back(argv[i]);
    }

    g_helper = std::make_unique< test_common::HBTestHelper >("test_volume", args, argv);
    g_helper->setup();
    auto ret = RUN_ALL_TESTS();
    g_helper->teardown();

    return ret;
}
