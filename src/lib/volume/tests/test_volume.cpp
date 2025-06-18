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
#include <sisl/flip/flip_client.hpp>
#include <iomgr/iomgr_flip.hpp>
#include <homeblks/home_blks.hpp>
#include <homeblks/volume_mgr.hpp>
#include "test_common.hpp"

SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTION_GROUP(test_volume_setup,
                  (num_vols, "", "num_vols", "number of volumes", ::cxxopts::value< uint32_t >()->default_value("2"),
                   "number"),
                  (gc_timer_nsecs, "", "gc_timer_nsecs", "gc timer in seconds",
                   ::cxxopts::value< uint32_t >()->default_value("5"), "seconds"),
                  (shutdown_timer_nsecs, "", "shutdown_timer_nsecs", "shutdown timer in seconds",
                   ::cxxopts::value< uint32_t >()->default_value("2"), "seconds"));

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

#ifdef _PRERELEASE
TEST_F(VolumeTest, ShutdownWithOutstandingRemoveVol) {
    std::vector< volume_id_t > vol_ids;
    {
        auto hb = g_helper->inst();
        auto vol_mgr = hb->volume_manager();

        uint32_t delay_sec = 6;
        g_helper->set_delay_flip("vol_fake_io_delay_simulation", delay_sec * 1000 * 1000 /*delay_usec*/, 2, 100);

        auto num_vols = 1ul;

        for (uint32_t i = 0; i < num_vols; ++i) {
            auto vinfo = gen_vol_info(i);
            auto id = vinfo.id;
            vol_ids.emplace_back(id);
            auto ret = vol_mgr->create_volume(std::move(vinfo)).get();
            ASSERT_TRUE(ret);

            auto vol_ptr = vol_mgr->lookup_volume(id);
            // verify the volume is there
            ASSERT_TRUE(vol_ptr != nullptr);

            // fake a write that will be delayed;
            vol_interface_req_ptr req1(new vol_interface_req{nullptr, 0, 0, vol_ptr});
            vol_mgr->write(vol_ptr, req1);

            // fake a read that will be delayed;
            vol_interface_req_ptr req2(new vol_interface_req{nullptr, 0, 0, vol_ptr});
            vol_mgr->read(vol_ptr, req2);
        }

        auto const s = hb->get_stats();
        auto const dtype = hb->data_drive_type();
        LOGINFO("Stats: {}, drive_type: {}", s.to_string(), dtype);

        for (uint32_t i = 0; i < num_vols; ++i) {
            auto id = vol_ids[i];
            auto ret = vol_mgr->remove_volume(id).get();
            ASSERT_TRUE(ret);
        }
    }

    g_helper->remove_flip("vol_fake_io_delay_simulation");

    g_helper->restart(2);
}

TEST_F(VolumeTest, ShutdownWithOutstandingIO) {
    std::vector< volume_id_t > vol_ids;
    {
        auto hb = g_helper->inst();
        auto vol_mgr = hb->volume_manager();

        uint32_t delay_sec = 6;
        g_helper->set_delay_flip("vol_fake_io_delay_simulation", delay_sec * 1000 * 1000 /*delay_usec*/, 2, 100);

        auto num_vols = 1ul;
        for (uint32_t i = 0; i < num_vols; ++i) {
            auto vinfo = gen_vol_info(i);
            auto id = vinfo.id;
            vol_ids.emplace_back(id);
            auto ret = vol_mgr->create_volume(std::move(vinfo)).get();
            ASSERT_TRUE(ret);

            auto vol_ptr = vol_mgr->lookup_volume(id);
            // verify the volume is there
            ASSERT_TRUE(vol_ptr != nullptr);

            // fake a write that will be delayed;
            vol_interface_req_ptr req1(new vol_interface_req{nullptr, 0, 0, vol_ptr});
            vol_mgr->write(vol_ptr, req1);

            // fake a read that will be delayed;
            vol_interface_req_ptr req2(new vol_interface_req{nullptr, 0, 0, vol_ptr});
            vol_mgr->read(vol_ptr, req2);
        }
    }

    g_helper->remove_flip("vol_fake_io_delay_simulation");

    // trigger graceful shutdown
    g_helper->restart(2);
}

TEST_F(VolumeTest, CreateDestroyVolumeWithOutstandingIO) {
    std::vector< volume_id_t > vol_ids;
    {
        auto hb = g_helper->inst();
        auto vol_mgr = hb->volume_manager();

        uint32_t delay_sec = 6;
        g_helper->set_delay_flip("vol_fake_io_delay_simulation", delay_sec * 1000 * 1000 /*delay_usec*/, 2, 100);

        auto num_vols = 1ul;

        for (uint32_t i = 0; i < num_vols; ++i) {
            auto vinfo = gen_vol_info(i);
            auto id = vinfo.id;
            vol_ids.emplace_back(id);
            auto ret = vol_mgr->create_volume(std::move(vinfo)).get();
            ASSERT_TRUE(ret);

            auto vol_ptr = vol_mgr->lookup_volume(id);
            // verify the volume is there
            ASSERT_TRUE(vol_ptr != nullptr);

            // fake a write that will be delayed;
            vol_interface_req_ptr req1(new vol_interface_req{nullptr, 0, 0, vol_ptr});
            vol_mgr->write(vol_ptr, req1);

            // fake a read that will be delayed;
            vol_interface_req_ptr req2(new vol_interface_req{nullptr, 0, 0, vol_ptr});
            vol_mgr->read(vol_ptr, req2);
        }

        auto const s = hb->get_stats();
        auto const dtype = hb->data_drive_type();
        LOGINFO("Stats: {}, drive_type: {}", s.to_string(), dtype);

        for (uint32_t i = 0; i < num_vols; ++i) {
            auto id = vol_ids[i];
            auto ret = vol_mgr->remove_volume(id).get();
            ASSERT_TRUE(ret);
            while (true) {
                auto delay_secs = 1;
                LOGINFO("Remove Volume {} triggered, waiting for {} seconds for IO to complete",
                        boost::uuids::to_string(id), delay_secs);
                // sleep for a while
                std::this_thread::sleep_for(std::chrono::milliseconds(delay_secs * 1000));
                auto vol_ptr = vol_mgr->lookup_volume(id);
                if (!vol_ptr) { break; }
            }
        }
    }

    g_helper->remove_flip("vol_fake_io_delay_simulation");

    g_helper->restart(2);
}
#endif

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

        auto const s = hb->get_stats();
        auto const dtype = hb->data_drive_type();
        LOGINFO("Stats: {}, drive_type: {}", s.to_string(), dtype);

        for (uint32_t i = 0; i < num_vols; ++i) {
            auto id = vol_ids[i];
            auto ret = vol_mgr->remove_volume(id).get();
            ASSERT_TRUE(ret);
            // sleep for a while
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            auto vinfo_ptr = vol_mgr->lookup_volume(id);
            // verify the volume is not there
            ASSERT_TRUE(vinfo_ptr == nullptr);
        }
    }

    g_helper->restart(2);
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

    g_helper->restart(2);

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

    {
        auto hb = g_helper->inst();
        auto vol_mgr = hb->volume_manager();
        for (const auto& id : vol_ids) {
            auto ret = vol_mgr->remove_volume(id).get();
            ASSERT_TRUE(ret);
            // sleep for a while
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            auto vinfo_ptr = vol_mgr->lookup_volume(id);
            // verify the volume is not there
            ASSERT_TRUE(vinfo_ptr == nullptr);
        }
    }
}

TEST_F(VolumeTest, DestroyVolumeCrashRecovery) {
#ifdef _PRERELEASE
    g_helper->set_flip_point("vol_destroy_crash_simulation");
#endif
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
        }
    }

    g_helper->restart(2);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    char** orig_argv = argv;

    std::vector< std::string > args;
    for (int i = 0; i < argc; ++i) {
        args.emplace_back(argv[i]);
    }

    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_common_setup, test_volume_setup, homeblocks);
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);

    g_helper = std::make_unique< test_common::HBTestHelper >("test_volume", args, orig_argv);
    g_helper->setup();
    auto ret = RUN_ALL_TESTS();
    g_helper->teardown();

    return ret;
}
