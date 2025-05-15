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
#include <volume/volume.hpp>
#include "test_common.hpp"

SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTION_GROUP(test_volume_io_setup,
                  (num_vols, "", "num_vols", "number of volumes", ::cxxopts::value< uint32_t >()->default_value("2"),
                   "number"),
                  (num_blks, "", "num_blks", "number of volumes", ::cxxopts::value< uint32_t >()->default_value("1"),
                   "number"));

SISL_OPTIONS_ENABLE(logging, test_common_setup, test_volume_io_setup, homeblocks)
SISL_LOGGING_DECL(test_volume_io)

std::unique_ptr< test_common::HBTestHelper > g_helper;

using namespace homeblocks;

class VolumeIOImpl {
public:
    VolumeIOImpl() { create_volume(); }

private:
    VolumeInfo gen_vol_info(uint32_t vol_idx) {
        VolumeInfo vol_info;
        vol_info.name = "vol_" + std::to_string(vol_idx);
        vol_info.size_bytes = 1024 * 1024 * 1024;
        vol_info.page_size = 4096;
        vol_info.id = hb_utils::gen_random_uuid();
        return vol_info;
    }

public:
    void create_volume() {
        auto vinfo = gen_vol_info(m_volume_id_++);
        m_vol_id = vinfo.id;

        auto vol_mgr = g_helper->inst()->volume_manager();
        auto ret = vol_mgr->create_volume(std::move(vinfo)).get();
        ASSERT_TRUE(ret);

        m_vol_ptr = vol_mgr->lookup_volume(m_vol_id);
        ASSERT_TRUE(m_vol_ptr != nullptr);
    }

    void remove_volume() {
        auto vol_mgr = g_helper->inst()->volume_manager();
        auto ret = vol_mgr->remove_volume(m_vol_id).get();
        ASSERT_TRUE(ret);
    }

    void reset() {
        auto vol_mgr = g_helper->inst()->volume_manager();
        m_vol_ptr = vol_mgr->lookup_volume(m_vol_id);
        ASSERT_TRUE(m_vol_ptr != nullptr);
    }

    auto build_random_data(lba_t& start_lba, uint32_t& nblks) {
        // Write upto 1-64 nblks * 4k = 256k size.
        auto info = m_vol_ptr->info();
        uint64_t page_size = info->page_size;
        uint64_t max_blks = info->size_bytes / page_size;
        start_lba = start_lba == 0 ? rand() % max_blks : start_lba;
        nblks = nblks == 0 ? std::max(1, rand() % 64) : nblks;
        nblks = std::min(static_cast< uint64_t >(nblks), max_blks - static_cast< uint64_t >(start_lba));

        auto data_size = nblks * page_size;
        auto data = sisl::make_byte_array(data_size, 512);
        auto data_bytes = data->bytes();
        for (uint64_t i = 0, lba = start_lba; i < nblks; i++) {
            uint64_t data_pattern = ((long long)rand() << 32) | rand();
            test_common::HBTestHelper::fill_data_buf(data_bytes, page_size, data_pattern);
            data_bytes += page_size;
            // Store the lba to pattern mapping
            m_lba_data[lba] = data_pattern;
            LOGINFO("Generate data lba={} pattern={}", lba, data_pattern);
            lba++;
        }

        return data;
    }

    void generate_io_single(shared< VolumeIOImpl > vol, lba_t start_lba = 0, uint32_t nblks = 0, bool wait = true) {
        // Generate a single io with start lba and nblks.
        test_common::Waiter waiter(1);
        auto fut = waiter.start([this, vol, start_lba, nblks, &waiter]() mutable {
            auto data = build_random_data(start_lba, nblks);
            vol_interface_req_ptr req(new vol_interface_req{data->bytes(), start_lba, nblks});
            auto vol_mgr = g_helper->inst()->volume_manager();
            vol_mgr->write(m_vol_ptr, req)
                .via(&folly::InlineExecutor::instance())
                .thenValue([this, data, &waiter](auto&& result) {
                    ASSERT_FALSE(result.hasError());
                    waiter.one_complete();
                });
        });

        if (wait) { std::move(fut).get(); }
    }

    auto generate_io(lba_t start_lba = 0, uint32_t nblks = 0) {
        auto data = build_random_data(start_lba, nblks);
        vol_interface_req_ptr req(new vol_interface_req{data->bytes(), start_lba, nblks});
        auto vol_mgr = g_helper->inst()->volume_manager();
        vol_mgr->write(m_vol_ptr, req).via(&folly::InlineExecutor::instance()).thenValue([this, data](auto&& result) {
            ASSERT_FALSE(result.hasError());
            g_helper->runner().next_task();
        });
    }

    void verify_data() {
        for (auto& [lba, data_pattern] : m_lba_data) {
            auto buffer = iomanager.iobuf_alloc(512, 4096);
            vol_interface_req_ptr req(new vol_interface_req{buffer, lba, 1});

            auto vol_mgr = g_helper->inst()->volume_manager();
            vol_mgr->read(m_vol_ptr, req).get();
            // LOGINFO("Data read={}", fmt::format("{}", spdlog::to_hex((buffer), (buffer) + (128))));
            test_common::HBTestHelper::validate_data_buf(buffer, 4096, data_pattern);
            LOGINFO("Verify data lba={} pattern={} {}", lba, data_pattern, *r_cast< uint64_t* >(buffer));
            iomanager.iobuf_free(buffer);
        }
    }

#ifdef _PRERELEASE
    void set_flip_point(const std::string flip_name) {
        flip::FlipCondition null_cond;
        flip::FlipFrequency freq;
        freq.set_count(1);
        freq.set_percent(100);
        m_fc.inject_noreturn_flip(flip_name, {null_cond}, freq);
        LOGI("Flip {} set", flip_name);
    }
#endif

private:
#ifdef _PRERELEASE
    flip::FlipClient m_fc{iomgr_flip::instance()};
#endif
    VolumePtr m_vol_ptr;
    volume_id_t m_vol_id;
    static inline uint32_t m_volume_id_{1};
    std::map< lba_t, uint64_t > m_lba_data;
};

class VolumeIOTest : public ::testing::Test {
public:
    void SetUp() override {
        for (uint32_t i = 0; i < SISL_OPTIONS["num_vols"].as< uint32_t >(); i++) {
            m_vols_impl.emplace_back(std::make_shared< VolumeIOImpl >());
        }
    }

    void TearDown() override {
        for (auto& vol : m_vols_impl) {
            vol->remove_volume();
        }
    }

    void generate_io_single(shared< VolumeIOImpl > vol, lba_t start_lba = 0, uint32_t nblks = 0, bool wait = true) {
        vol->generate_io_single(vol, start_lba, nblks, wait);
    }

    void generate_io(shared< VolumeIOImpl > vol = nullptr, lba_t start_lba = 0, uint32_t nblks = 0, bool wait = true) {
        // Generate a io based on num_io and qdepth with start lba and nblks.
        g_helper->runner().set_task([this, vol, start_lba, nblks]() mutable {
            if (vol == nullptr) {
                // Get a random volume.
                vol = m_vols_impl[rand() % m_vols_impl.size()];
            }

            vol->generate_io(start_lba, nblks);
        });

        if (wait) { g_helper->runner().execute().get(); }
        LOGINFO("IO completed");
    }

    void verify_data(shared< VolumeIOImpl > vol_impl = nullptr) {
        if (vol_impl) {
            vol_impl->verify_data();
            return;
        }

        for (auto& vol_impl : m_vols_impl) {
            vol_impl->verify_data();
        }
    }

    std::vector< shared< VolumeIOImpl > >& volume_list() { return m_vols_impl; }

private:
    std::vector< shared< VolumeIOImpl > > m_vols_impl;
};

TEST_F(VolumeIOTest, SingleVolumeWriteData) {
    // Write and verify fixed LBA range to single volume multiple times.
    auto vol = volume_list().back();
    uint32_t nblks = 100;
    lba_t start_lba = 1;
    uint32_t num_iter = 1;
    LOGINFO("Write and verify data with num_iter={} start={} nblks={}", num_iter, start_lba, nblks);
    for (uint32_t i = 0; i < num_iter; i++) {
        generate_io_single(vol, start_lba, nblks);
        verify_data(vol);
    }

    // Verify data after restart.
    g_helper->restart(10);
    vol->reset();

    LOGINFO("Verify data");
    verify_data(vol);

    // Write and verify again on same LBA range to single volume multiple times.
    LOGINFO("Write and verify data with num_iter={} start={} nblks={}", num_iter, start_lba, nblks);
    for (uint32_t i = 0; i < num_iter; i++) {
        generate_io_single(vol, start_lba, nblks);
    }

    verify_data(vol);
    LOGINFO("SingleVolumeWriteData test done.");
}

TEST_F(VolumeIOTest, MultipleVolumeWriteData) {
    LOGINFO("Write data randomly on num_vols={} num_io={}", SISL_OPTIONS["num_vols"].as< uint32_t >(),
            SISL_OPTIONS["num_io"].as< uint64_t >());
    generate_io();

    LOGINFO("Verify data");
    verify_data();
    LOGINFO("MultipleVolumeWriteData test done.");
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_common_setup, test_volume_io_setup, homeblocks);
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);

    std::vector< std::string > args;
    for (int i = 0; i < argc; ++i) {
        args.emplace_back(argv[i]);
    }

    g_helper = std::make_unique< test_common::HBTestHelper >("test_volume_io", args, argv);
    g_helper->setup();
    auto ret = RUN_ALL_TESTS();
    g_helper->teardown();

    return ret;
}
