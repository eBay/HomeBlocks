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
#include <latch>
#include <boost/icl/interval_set.hpp>
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
SISL_OPTION_GROUP(
    test_volume_io_setup,
    (num_vols, "", "num_vols", "number of volumes", ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
    (num_blks, "", "num_blks", "number of volumes", ::cxxopts::value< uint32_t >()->default_value("1"), "number"),
    (vol_size, "", "vol_size_gb", "volume size", ::cxxopts::value< uint32_t >()->default_value("1"), "number"),
    (write_num_io, "", "write_num_io", "number of IO operations", ::cxxopts::value< uint64_t >()->default_value("300"),
     "number"),
    (write_qdepth, "", "write_qdepth", "Max outstanding operations", ::cxxopts::value< uint32_t >()->default_value("8"),
     "number"),
    (read_num_io, "", "read_num_io", "number of IO operations", ::cxxopts::value< uint64_t >()->default_value("300"),
     "number"),
    (read_qdepth, "", "read_qdepth", "Max outstanding operations", ::cxxopts::value< uint32_t >()->default_value("8"),
     "number"),
    (run_time, "", "run_time", "running time in seconds", ::cxxopts::value< uint64_t >()->default_value("0"), "number"),
    (cp_timer_ms, "", "cp_timer_ms", "cp timer in milliseconds", ::cxxopts::value< uint64_t >()->default_value("60000"),
     "number"),
    (io_size_kb, "", "io_size_kb", "Write size in kb", ::cxxopts::value< uint32_t >(), "number"),
    (io_size_kb_range, "", "io_size_kb_range", "Write size range in kb", ::cxxopts::value< std::vector< uint32_t > >(),
     "number"),
    (read_verify, "", "read_verify", "Read and verify all data in long running tests", ::cxxopts::value< bool >()->default_value("false"), "true or false"));

SISL_OPTIONS_ENABLE(logging, test_common_setup, test_volume_io_setup, homeblocks)
SISL_LOGGING_DECL(test_volume_io)

std::unique_ptr< test_common::HBTestHelper > g_helper;
std::unique_ptr< test_http_server > g_http_server;
std::shared_ptr< test_common::io_fiber_pool > g_io_fiber_pool;
static constexpr uint64_t g_page_size = 4096;

using namespace homeblocks;

class VolumeIOImpl {
public:
    VolumeIOImpl() {
        auto write_num_io = SISL_OPTIONS["write_num_io"].as< uint64_t >();
        auto write_qdepth = SISL_OPTIONS["write_qdepth"].as< uint32_t >();
        auto read_num_io = SISL_OPTIONS["read_num_io"].as< uint64_t >();
        auto read_qdepth = SISL_OPTIONS["read_qdepth"].as< uint32_t >();
        m_write_runner = std::make_shared< test_common::Runner >(write_num_io, write_qdepth, g_io_fiber_pool);
        m_read_runner = std::make_shared< test_common::Runner >(read_num_io, read_qdepth, g_io_fiber_pool);

        create_volume();
    }

private:
    VolumeInfo gen_vol_info(uint32_t vol_idx) {
        VolumeInfo vol_info;
        vol_info.name = "vol_" + std::to_string(vol_idx);
        vol_info.size_bytes = SISL_OPTIONS["vol_size_gb"].as< uint32_t >() * Gi;
        vol_info.page_size = g_page_size;
        vol_info.id = hb_utils::gen_random_uuid();
        return vol_info;
    }

public:
    void create_volume() {
        auto vinfo = gen_vol_info(m_volume_id_++);
        m_vol_name = vinfo.name;
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

    void get_random_non_overlapping_lba(lba_t& start_lba, uint32_t& nblks, uint64_t max_blks) {
        if (start_lba != 0 && nblks != 0) {
            lba_t end_lba = start_lba + nblks - 1;
            auto new_range = boost::icl::interval< int >::closed(start_lba, end_lba);
            // For user provided lba and nblks, check if they are already in flight.
            std::lock_guard lock(m_mutex);
            // ASSERT_TRUE(m_inflight_ios.find(new_range) == m_inflight_ios.end());
            m_inflight_ios.insert(new_range);
            return;
        }

        do {
            // Generate lba which are not overlapped with the inflight ios, otherwise
            // we cant decide which io completed last and cant verify the data.
            start_lba = rand() % max_blks;
            if (SISL_OPTIONS.count("io_size_kb")) {
                auto io_size_kb_opt = SISL_OPTIONS["io_size_kb"].as< uint32_t >();
                ASSERT_EQ(io_size_kb_opt % 4, 0);
                nlbas = io_size_kb_opt / 4;
            } else if (SISL_OPTIONS.count("io_size_kb_range")) {
                auto lb = SISL_OPTIONS["io_size_kb_range"].as< std::vector< uint32_t > >()[0];
                auto ub = SISL_OPTIONS["io_size_kb_range"].as< std::vector< uint32_t > >()[1];
                ASSERT_TRUE(ub > lb);
                ASSERT_EQ(lb % 4, 0);
                ASSERT_EQ(ub % 4, 0);
                auto lb_blks = lb / 4, ub_blks = ub / 4;
                nblks = (rand() % (ub_blks - lb_blks + 1)) + lb_blks;
            } else {
                nblks = rand() % 64 + 1; // 1-64 blocks
            }
            lba_t end_lba = start_lba + nblks - 1;
            auto new_range = boost::icl::interval< int >::closed(start_lba, end_lba);
            std::lock_guard lock(m_mutex);
            if (m_inflight_ios.find(new_range) == m_inflight_ios.end()) {
                m_inflight_ios.insert(new_range);
                break;
            }

        } while (true);
    }

    auto build_random_data(lba_t& start_lba, uint32_t& nblks, bool store_data = true) {
        // Write upto 1-64 nblks * 4k = 256k size.
        auto info = m_vol_ptr->info();
        uint64_t page_size = info->page_size;
        uint64_t max_blks = static_cast< uint64_t >(info->size_bytes) / page_size;
        get_random_non_overlapping_lba(start_lba, nblks, max_blks);
        nblks = std::min(static_cast< uint64_t >(nblks), max_blks - static_cast< uint64_t >(start_lba));

        auto data_size = nblks * page_size;
        auto data = sisl::make_byte_array(data_size, 512);
        auto data_bytes = data->bytes();
        for (uint64_t i = 0, lba = start_lba; i < nblks; i++) {
            uint64_t data_pattern = ((long long)rand() << 32) | rand();
            test_common::HBTestHelper::fill_data_buf(data_bytes, page_size, data_pattern);
            data_bytes += page_size;
            {
                // Store the lba to pattern mapping
                if (store_data) {
                    std::lock_guard lock(m_mutex);
                    m_lba_data[lba] = data_pattern;
                }
            }

            LOGDEBUG("Generate data vol={} lba={} pattern={}", m_vol_name, lba, data_pattern);
            lba++;
        }

        return data;
    }

    void generate_write_io_single(lba_t start_lba = 0, uint32_t nblks = 0, bool wait = true,
                                  bool expect_failure = false) {
        // Generate a single io with start lba and nblks.
        auto latch = std::make_shared< std::latch >(1);
        auto data = build_random_data(start_lba, nblks, !expect_failure /*store the generated data*/);
        vol_interface_req_ptr req(new vol_interface_req{data->bytes(), start_lba, nblks, m_vol_ptr});
        auto vol_mgr = g_helper->inst()->volume_manager();
        vol_mgr->write(m_vol_ptr, req)
            .via(&folly::InlineExecutor::instance())
            .thenValue([this, data, req, latch, expect_failure](auto&& result) {
                ASSERT_EQ(result.hasError(), expect_failure);
                {
                    std::lock_guard lock(m_mutex);
                    m_inflight_ios.erase(boost::icl::interval< int >::closed(req->lba, req->lba + req->nlbas - 1));
                }

                latch->count_down();
            });

        if (wait) { latch->wait(); }
    }

    auto generate_write_io_task(lba_t start_lba = 0, uint32_t nblks = 0) {
        m_write_count = 0;
        m_write_runner->set_task([this, start_lba, nblks]() mutable {
            // Generate write IO's.
            generate_write_io(start_lba, nblks);
        });
        return m_write_runner->execute();
    }

    auto generate_read_io_task(lba_t start_lba = 0, uint32_t nblks = 0) {
        m_read_count = 0;
        m_read_runner->set_task([this, start_lba, nblks]() mutable {
            // Generate write IO's.
            generate_read_io(start_lba, nblks);
        });
        return m_read_runner->execute();
    }

    void generate_write_io(lba_t start_lba = 0, uint32_t nblks = 0) {
        auto data = build_random_data(start_lba, nblks);
        vol_interface_req_ptr req(new vol_interface_req{data->bytes(), start_lba, nblks, m_vol_ptr});
        auto vol_mgr = g_helper->inst()->volume_manager();
        LOGDEBUG("begin write io start={} end={}", req->lba, req->lba + req->nlbas - 1);
        vol_mgr->write(m_vol_ptr, req)
            .via(&folly::InlineExecutor::instance())
            .thenValue([this, req, data](auto&& result) {
                RELEASE_ASSERT(!result.hasError(), "Write failed with error={}", result.error());
                {
                    std::lock_guard lock(m_mutex);
                    m_inflight_ios.erase(boost::icl::interval< int >::closed(req->lba, req->lba + req->nlbas - 1));
                    LOGDEBUG("end write io start={} end={}", req->lba, req->lba + req->nlbas - 1);
                }
                m_write_count++;
                m_write_runner->next_task();
            });
    }

    void sync_read(lba_t start_lba, uint32_t nlbas) {
        auto sz = nlbas * m_vol_ptr->info()->page_size;
        sisl::io_blob_safe read_blob(sz, 512);
        auto buf = read_blob.bytes();
        vol_interface_req_ptr req(new vol_interface_req{buf, start_lba, nlbas, m_vol_ptr});
        auto read_resp = g_helper->inst()->volume_manager()->read(m_vol_ptr, req).get();
        if (read_resp.hasError()) { LOGERROR("Read failed with error={}", read_resp.error()); }
        RELEASE_ASSERT(!read_resp.hasError(), "Read failed with error={}", read_resp.error());
    }

    void read_and_verify(lba_t start_lba, uint32_t nlbas) {
        LOGTRACE("Reading and verifying data for volume {} from lba={} with nlbas={}", m_vol_name, start_lba, nlbas);
        auto sz = nlbas * m_vol_ptr->info()->page_size;
        sisl::io_blob_safe read_blob(sz, 512);
        auto buf = read_blob.bytes();
        vol_interface_req_ptr req(new vol_interface_req{buf, start_lba, nlbas, m_vol_ptr});
        auto read_resp = g_helper->inst()->volume_manager()->read(m_vol_ptr, req).get();
        if (read_resp.hasError()) { LOGERROR("Read failed with error={}", read_resp.error()); }
        RELEASE_ASSERT(!read_resp.hasError(), "Read failed with error={}", read_resp.error());
        auto read_sz = m_vol_ptr->info()->page_size;
        for (auto lba = start_lba; lba < start_lba + nlbas; lba++, buf += read_sz) {
            uint64_t data_pattern = 0;
            if (auto it = m_lba_data.find(lba); it != m_lba_data.end()) {
                data_pattern = it->second;
                test_common::HBTestHelper::validate_data_buf(buf, m_vol_ptr->info()->page_size, data_pattern);
            } else {
                test_common::HBTestHelper::validate_zeros(buf, m_vol_ptr->info()->page_size);
            }

            LOGDEBUG("Verify data lba={} pattern expected={} actual={}", lba, data_pattern,
                     *r_cast< uint64_t* >(read_blob.bytes()));
        }
    }

    void generate_read_io(lba_t start_lba = 0, uint32_t nlbas = 0) {
        auto info = m_vol_ptr->info();
        uint64_t page_size = info->page_size;
        uint64_t max_blks = static_cast< uint64_t >(info->size_bytes) / page_size;
        get_random_non_overlapping_lba(start_lba, nlbas, max_blks);
        sisl::io_blob_safe read_blob(nlbas * page_size, 512);
        auto buf = read_blob.bytes();
        vol_interface_req_ptr req(new vol_interface_req{buf, start_lba, nlbas, m_vol_ptr});
        LOGDEBUG("begin read io start={} end={}", req->lba, req->lba + req->nlbas - 1);
        auto read_resp =
            g_helper->inst()
                ->volume_manager()
                ->read(m_vol_ptr, req)
                .via(&folly::InlineExecutor::instance())
                .thenValue([this, read_blob = std::move(read_blob), req](auto&& result) {
                    RELEASE_ASSERT(!result.hasError(), "Read failed with error={}", result.error());
                    {
                        std::lock_guard lock(m_mutex);
                        m_inflight_ios.erase(boost::icl::interval< int >::closed(req->lba, req->lba + req->nlbas - 1));
                        LOGDEBUG("end read io start={} end={}", req->lba, req->lba + req->nlbas - 1);
                    }
                    m_read_count++;
                    m_read_runner->next_task();
                });
    }

    void verify_all_data(uint64_t nlbas_per_io = 1) {
        auto start_lba = m_lba_data.begin()->first;
        auto end_lba = m_lba_data.rbegin()->first;
        LOGTRACE("Verifying data for volume {} from lba={} to lba={} with nlbas_per_io={}", m_vol_name, start_lba,
                 end_lba, nlbas_per_io);
        for (auto& [lba, _] : m_lba_data) {
            read_and_verify(lba, nlbas_per_io);
        }
    }

    void verify_data(lba_t start_lba, lba_t max_lba, uint64_t nlbas_per_io) {
        uint64_t num_lbas_verified = 0;
        for (auto lba = start_lba; lba < max_lba; lba += nlbas_per_io) {
            auto num_lbas_this_round = std::min(nlbas_per_io, max_lba - lba);
            read_and_verify(lba, num_lbas_this_round);
            num_lbas_verified += num_lbas_this_round;
        }
        LOGINFO("Verified {} lbas for volume {}", num_lbas_verified, m_vol_ptr->info()->name);
    }

    uint64_t read_count() { return m_read_count.load(); }
    uint64_t write_count() { return m_write_count.load(); }

private:
    std::mutex m_mutex;
    std::string m_vol_name;
    VolumePtr m_vol_ptr;
    volume_id_t m_vol_id;
    static inline uint32_t m_volume_id_{1};
    // Mapping from lba to data patttern.
    std::map< lba_t, uint64_t > m_lba_data;
    boost::icl::interval_set< int > m_inflight_ios;
    std::shared_ptr< test_common::Runner > m_write_runner;
    std::shared_ptr< test_common::Runner > m_read_runner;
    std::atomic< uint64_t > m_read_count{0};
    std::atomic< uint64_t > m_write_count{0};
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
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    void generate_write_io_single(shared< VolumeIOImpl > vol, lba_t start_lba = 0, uint32_t nblks = 0, bool wait = true,
                                  bool expect_failure = false) {
        vol->generate_write_io_single(start_lba, nblks, wait, expect_failure);
    }

    uint64_t get_total_reads() {
        uint64_t count = 0;
        for (auto vol : m_vols_impl) {
            count += vol->read_count();
        }
        return count;
    }

    uint64_t get_total_writes() {
        uint64_t count = 0;
        for (auto vol : m_vols_impl) {
            count += vol->write_count();
        }
        return count;
    }

    auto generate_write_io(shared< VolumeIOImpl > vol = nullptr, lba_t start_lba = 0, uint32_t nblks = 0,
                           bool wait = true) {
        // Generate write io based on num_io and qdepth with start lba and nblks.
        std::vector< folly::Future< folly::Unit > > futs;
        if (vol != nullptr) {
            futs.emplace_back(vol->generate_write_io_task(start_lba, nblks));
        } else {
            for (auto vol : m_vols_impl) {
                futs.emplace_back(vol->generate_write_io_task(start_lba, nblks));
            }
        }

        if (wait) {
            folly::collectAll(futs).get();
            LOGINFO("Write IO completed count={}", get_total_writes());
        }

        return futs;
    }

    auto generate_read_io(shared< VolumeIOImpl > vol = nullptr, lba_t start_lba = 0, uint32_t nblks = 0,
                          bool wait = true) {
        // Generate read io based on num_io and qdepth with start lba and nblks.
        std::vector< folly::Future< folly::Unit > > futs;
        if (vol != nullptr) {
            futs.emplace_back(vol->generate_read_io_task(start_lba, nblks));
        } else {
            for (auto vol : m_vols_impl) {
                futs.emplace_back(vol->generate_read_io_task(start_lba, nblks));
            }
        }

        if (wait) {
            folly::collectAll(futs).get();
            LOGINFO("Read IO completed count={}", get_total_reads());
        }

        return futs;
    }

    void verify_all_data(shared< VolumeIOImpl > vol_impl = nullptr, uint64_t nlbas_per_io = 1) {
        if (vol_impl) {
            vol_impl->verify_all_data(nlbas_per_io);
            return;
        }

        for (auto& vol_impl : m_vols_impl) {
            vol_impl->verify_all_data(nlbas_per_io);
        }
    }

    void restart(int shutdown_delay) {
        g_helper->restart(shutdown_delay);
        for (auto& vol_impl : m_vols_impl) {
            vol_impl->reset();
        }
    }

    std::vector< shared< VolumeIOImpl > >& volume_list() { return m_vols_impl; }

    template < typename T >
    T get_random_number(T min, T max) {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        std::uniform_int_distribution< T > dis(min, max);
        return dis(gen);
    }

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
        generate_write_io_single(vol, start_lba, nblks);
        verify_all_data(vol);
    }

    // Verify data after restart.
    restart(5);

    LOGINFO("Verify data");
    verify_all_data(vol);
    // verify_data(vol, 30 /* nlbas_per_io */);

    // Write and verify again on same LBA range to single volume multiple times.
    LOGINFO("Write and verify data with num_iter={} start={} nblks={}", num_iter, start_lba, nblks);
    for (uint32_t i = 0; i < num_iter; i++) {
        generate_write_io_single(vol, start_lba, nblks);
    }

    verify_all_data(vol, 30 /* nlbas_per_io */);

    LOGINFO("SingleVolumeWriteData test done.");
}

TEST_F(VolumeIOTest, SingleVolumeReadData) {
    // Write and verify fixed LBA range to single volume multiple times.
    auto vol = volume_list().back();
    uint32_t nblks = 5000;
    lba_t start_lba = 500;
    uint32_t num_iter = 1;
    LOGINFO("Write and verify data with num_iter={} start={} nblks={}", num_iter, start_lba, nblks);
    for (uint32_t i = 0; i < num_iter; i++) {
        generate_write_io_single(vol, start_lba, nblks);
    }

    vol->verify_data(300, 800, 40);
    vol->verify_data(2000, 3000, 40);
    vol->verify_data(800, 1800, 40);

    // random reads
    num_iter = 100;
    for (uint32_t i = 0; i < num_iter; i++) {
        auto start_lba = get_random_number< lba_t >(0, 10000);
        auto nblks = get_random_number< uint32_t >(1, 64);
        auto no_lbas_per_io = get_random_number< uint64_t >(1, 50);
        LOGINFO("iter {}: Read data start={} nblks={} no_lbas_per_io {}", i, start_lba, nblks, no_lbas_per_io);
        vol->verify_data(start_lba, start_lba + nblks, no_lbas_per_io);
    }

    LOGINFO("SingleVolumeRead test done.");
}

TEST_F(VolumeIOTest, SingleVolumeReadHoles) {
    auto vol = volume_list().back();
    uint32_t nblks = 5000;
    lba_t start_lba = 500;
    generate_write_io_single(vol, start_lba, nblks);

    // Verify with no holes in the range
    vol->verify_data(1000, 2000, 40);

    start_lba = 10000;
    nblks = 50;
    for (uint32_t i = 0; i / 2 < nblks; i += 2) {
        generate_write_io_single(vol, start_lba + i, 1);
    }

    // Verfy with hole after each lba
    vol->verify_data(10000, 10100, 50);
    LOGINFO("Verified data with hole after each lba");

    start_lba = 20000;
    for (uint32_t i = 0; i < 100; i++) {
        if (i % 7 > 2) { generate_write_io_single(vol, start_lba + i, 1); }
    }
    // Verify with mixed holes in the range
    vol->verify_data(20000, 20100, 50);
}

TEST_F(VolumeIOTest, MultipleVolumeWriteData) {
    LOGINFO("Write data randomly on num_vols={} num_io={}", SISL_OPTIONS["num_vols"].as< uint32_t >(),
            SISL_OPTIONS["num_io"].as< uint64_t >());
    generate_write_io();

    LOGINFO("Verify data");
    verify_all_data();

    restart(5);

    LOGINFO("Verify data again");
    verify_all_data();

    LOGINFO("Write data randomly");
    generate_write_io();

    LOGINFO("Verify data");
    verify_all_data();

    LOGINFO("MultipleVolumeWriteData test done.");
}

TEST_F(VolumeIOTest, LongRunningRandomIO) {
    auto run_time = SISL_OPTIONS["run_time"].as< uint64_t >();
    LOGINFO("Long running random read and write on num_vols={} run_time={}", SISL_OPTIONS["num_vols"].as< uint32_t >(),
            run_time);

    uint64_t total_reads{0}, total_writes{0};
    auto start_time = std::chrono::high_resolution_clock::now();
    do {
        std::vector< folly::Future< folly::Unit > > futs;

        // Generate write's on all volumes with random lba and nblks on all volumes.
        auto writes = generate_write_io(nullptr /* vol */, 0 /* start_lba */, 0 /* nblks */, false /* wait */);

        // In parallel, generate reads on all volumes with random lba and nblks on all volumes.
        auto reads = generate_read_io(nullptr /* vol */, 0 /* start_lba */, 0 /* nblks */, false /* wait */);

        futs.insert(futs.end(), std::make_move_iterator(writes.begin()), std::make_move_iterator(writes.end()));
        futs.insert(futs.end(), std::make_move_iterator(reads.begin()), std::make_move_iterator(reads.end()));
        folly::collectAll(futs).get();

        total_reads += get_total_reads();
        total_writes += get_total_writes();
        std::chrono::duration< double > elapsed = std::chrono::high_resolution_clock::now() - start_time;
        auto elapsed_seconds = static_cast< uint64_t >(elapsed.count());
        static uint64_t log_pct = 0;
        if (auto done_pct = (run_time > 0) ? (elapsed_seconds * 100) / run_time : 100; done_pct > log_pct) {
            LOGINFO("total_read={} total_write={} elapsed={}, done pct={}", total_reads, total_writes, elapsed_seconds,
                    done_pct);
            log_pct += 5;
        }

        if (elapsed_seconds >= run_time) {
            LOGINFO("total_read={} total_write={} elapsed={}, done pct=100", total_reads, total_writes, elapsed_seconds);
            if (SISL_OPTIONS["read_verify"].as< bool >()) {
                LOGINFO("Verifying all data written so far");
                verify_all_data();
                LOGINFO("Read verification done");
            }
            break;
        }

        if (elapsed_seconds >= run_time) { break; }
    } while (true);
}

TEST_F(VolumeIOTest, LongRunningSequentialIO) {
    auto run_time = SISL_OPTIONS["run_time"].as< uint64_t >();
    LOGINFO("Long running sequential read and write on num_vols={} run_time={}",
            SISL_OPTIONS["num_vols"].as< uint32_t >(), run_time);

    uint64_t volume_size = SISL_OPTIONS["vol_size_gb"].as< uint32_t >() * Gi;
    auto start_time = std::chrono::high_resolution_clock::now();
    lba_t cur_lba = 0;
    lba_count_t nblks = 100;
    uint64_t total_reads{0}, total_writes{0};
    do {
        std::vector< folly::Future< folly::Unit > > futs;

        // Generate write's on all volumes with sequential lba and nblks on all volumes.
        auto writes = generate_write_io(nullptr /* vol */, cur_lba /* start_lba */, nblks, false /* wait */);
        folly::collectAll(writes).get();

        // Generate reads on all volumes with sequential lba and nblks on all volumes.
        auto reads = generate_read_io(nullptr /* vol */, cur_lba /* start_lba */, nblks, false /* wait */);
        folly::collectAll(reads).get();

        total_reads += get_total_reads();
        total_writes += get_total_writes();
        std::chrono::duration< double > elapsed = std::chrono::high_resolution_clock::now() - start_time;
        auto elapsed_seconds = static_cast< uint64_t >(elapsed.count());
        static uint64_t log_pct = 0;
        if (auto done_pct = (run_time > 0) ? (elapsed_seconds * 100) / run_time : 100; done_pct > log_pct) {
            LOGINFO("total_read={} total_write={} elapsed={}, done pct={}", total_reads, total_writes, elapsed_seconds,
                    done_pct);
            log_pct += 5;
        }

        if (((cur_lba + nblks) * g_page_size) >= volume_size) {
            cur_lba = 0;
        } else {
            cur_lba += nblks;
        }

        if (elapsed_seconds >= run_time) { 
            LOGINFO("total_read={} total_write={} elapsed={}, done pct=100", total_reads, total_writes, elapsed_seconds);
            if (SISL_OPTIONS["read_verify"].as< bool >()) {
                LOGINFO("Verifying all data written so far");
                verify_all_data();
                LOGINFO("Read verification done");
            }
            break;
        }
    } while (true);
}

#ifdef _PRERELEASE
TEST_F(VolumeIOTest, WriteCrash) {
    LOGINFO("WriteCrash test started");
    // define all the flips to be set
    std::vector< std::string > flip_points = {"vol_write_crash_after_data_write",
                                              "vol_write_crash_after_journal_write"};

    // Crash after writing to disk but before writing to journal.
    // Read should return no data as there is no index.
    uint32_t flip_idx{0};
    g_helper->set_flip_point(flip_points[flip_idx++]);

    auto vol = volume_list().back();
    generate_write_io_single(vol, 1000 /* start_lba */, 100 /* nblks*/);
    restart(2);
    // TODO read and verify zeros for no data.

    // Crash after journal write. After crash, index should be recovered.
    // Verify data after doing read.
    g_helper->set_flip_point(flip_points[flip_idx++]);
    vol = volume_list().back();
    generate_write_io_single(vol, 2000 /* start_lba */, 100 /* nblks*/);
    restart(2);

    vol->verify_data(2000 /* start_lba */, 2100 /* max_lba */, 25 /* nlbas_per_io */);

    // remove the flip points
    for (auto& flip : flip_points) {
        g_helper->remove_flip(flip);
    }

    LOGINFO("WriteCrash test done");
}

TEST_F(VolumeIOTest, IndexPutFailure) {
    LOGINFO("IndexPutFailure test Started");

    auto vol = volume_list().back();
    generate_write_io_single(vol, 1000 /* start_lba */, 100 /* nblks*/);
    verify_all_data(vol, 30 /* nlbas_per_io */);

    // Fail after partial index put.
    g_helper->set_flip_point("vol_index_partial_put_failure", 1000 /*count*/);
    // put failure during rewrite existing lbas
    generate_write_io_single(vol, 1000 /* start_lba */, 100 /* nblks*/, true /* wait */, true /* expect_failure */);
    verify_all_data(vol, 30 /* nlbas_per_io */);

    // put failure during put for new lbas
    generate_write_io_single(vol, 2000 /* start_lba */, 100 /* nblks*/, true /* wait */, true /* expect_failure */);
    verify_all_data(vol, 30 /* nlbas_per_io */);

    // remove the flip points
    g_helper->remove_flip("vol_index_partial_put_failure");
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
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_common_setup, test_volume_io_setup, homeblocks);
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);

    g_helper = std::make_unique< test_common::HBTestHelper >("test_volume_io", args, orig_argv);
    g_helper->setup();
    g_http_server = std::make_unique< test_http_server >();
    g_http_server->start();
    if (SISL_OPTIONS["num_io_reactors"].as< uint32_t >() > 0) {
        g_io_fiber_pool = std::make_shared< test_common::io_fiber_pool >(SISL_OPTIONS["num_io_reactors"].as< uint32_t >());
    }
    auto ret = RUN_ALL_TESTS();
    g_helper->teardown();

    return ret;
}
