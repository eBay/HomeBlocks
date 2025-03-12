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
/*
 * HomeBlocks Testing Binaries shared common define, apis and data structure;
 * */
#pragma once
#include <string>
#include <vector>
#include <map>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/settings/settings.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/nil_generator.hpp>

#include <homeblks/home_blks.hpp>

SISL_OPTION_GROUP(
    test_common_setup,
    (num_threads, "", "num_threads", "number of threads", ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
    (num_devs, "", "num_devs", "number of devices to create", ::cxxopts::value< uint32_t >()->default_value("3"),
     "number"),
    (dev_size_mb, "", "dev_size_mb", "size of each device in MB", ::cxxopts::value< uint64_t >()->default_value("2048"),
     "number"),
    (device_list, "", "device_list", "Device List instead of default created",
     ::cxxopts::value< std::vector< std::string > >(), "path [...]"),
    (http_port, "", "http_port", "http port (0 for no http, -1 for random, rest specific value)",
     ::cxxopts::value< int >()->default_value("-1"), "number"),
    (num_io, "", "num_io", "number of IO operations", ::cxxopts::value< uint64_t >()->default_value("300"), "number"),
    (qdepth, "", "qdepth", "Max outstanding operations", ::cxxopts::value< uint32_t >()->default_value("8"), "number"),
    (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"),
    (flip_list, "", "flip_list", "btree flip list", ::cxxopts::value< std::vector< std::string > >(), "flips [...]"),
    (use_file, "", "use_file", "use file instead of real drive", ::cxxopts::value< bool >()->default_value("false"),
     "true or false"),
    (enable_crash, "", "enable_crash", "enable crash", ::cxxopts::value< bool >()->default_value("0"), ""),
    (app_mem_size, "", "app_mem_size", "app memory size", ::cxxopts::value< uint64_t >()->default_value("20"),
     "number"));

using namespace homeblocks;

namespace test_common {

class HBTestHelper {
    class HBTestApplication : public homeblocks::HomeBlocksApplication {
    private:
        HBTestHelper& helper_;

    public:
        HBTestApplication(HBTestHelper& h) : helper_{h} {}
        virtual ~HBTestApplication() = default;

        // implement all the virtual functions in HomeObjectApplication
        bool spdk_mode() const override {
            // return SISL_OPTIONS["spdk"].as< bool >();
            return false;
        }
        uint32_t threads() const override {
            // return SISL_OPTIONS["num_threads"].as< uint32_t >();
            return 2;
        }

        std::list< device_info_t > devices() const override {
            std::list< device_info_t > devs;
            for (const auto& dev : helper_.dev_list()) {
                devs.emplace_back(dev, DevType::HDD);
            }
            return devs;
        }

        peer_id_t discover_svcid(std::optional< peer_id_t > const&) const override { return helper_.svc_id(); }

        uint64_t app_mem_size() const override {
            // return SISL_OPTIONS["app_mem_size"].as< uint64_t >();
            return 20;
        }
    };

public:
    HBTestHelper(std::string const& name, std::vector< std::string > const& args, char** argv) :
            test_name_{name}, args_{args}, argv_{argv} {}

    void setup() {
        sisl::logging::SetLogger(test_name_);
        sisl::logging::SetLogPattern("[%D %T%z] [%^%L%$] [%n] [%t] %v");

        // init svc_id_
        svc_id_ = boost::uuids::nil_uuid();

        // init device list
        init_dev_list(true /*init_device*/);

        LOGINFO("Starting HomeObject");
        app_ = std::make_shared< HBTestApplication >(*this);
        hb_ = init_homeblocks(std::weak_ptr< HBTestApplication >(app_));
    }

    void restart(uint64_t delay_secs = 0) {
        LOGI("Stoping homeblocks after {} secs, replica={}", delay_secs);
        hb_.reset();
        if (delay_secs > 0) { std::this_thread::sleep_for(std::chrono::seconds(delay_secs)); }
        hb_ = init_homeblocks(std::weak_ptr< HBTestApplication >(app_));
    }

    shared< homeblocks::HomeBlocks > inst() { return hb_; }

    void teardown() {
        LOGI("tearing down test.");
        hb_.reset();
    }

    peer_id_t svc_id() { return svc_id_; }
    std::vector< std::string > const& dev_list() const { return dev_list_; }

private:
    void init_devices(bool is_file, uint64_t dev_size = 0) {
        if (is_file) {
            for (const auto& fpath : dev_list_) {
                if (std::filesystem::exists(fpath)) { std::filesystem::remove(fpath); }
                std::ofstream ofs{fpath, std::ios::binary | std::ios::out | std::ios::trunc};
                std::filesystem::resize_file(fpath, dev_size);
            }
        } else {
            // raw device init
            auto const zero_size = 4096 * 1024; // initialize the first 4MB;
                                                // homestore::hs_super_blk::first_block_size() is 4KB
            std::vector< int > zeros(zero_size, 0);
            for (auto const& path : dev_list_) {
                if (!std::filesystem::exists(path)) { RELEASE_ASSERT(false, "Device {} does not exist", path); }
                auto fd = ::open(path.c_str(), O_RDWR, 0640);
                RELEASE_ASSERT(fd != -1, "Failed to open device");
                auto const write_sz =
                    pwrite(fd, zeros.data(), zero_size, 0 /*homestore::hs_super_blk::first_block_offset())*/);
                RELEASE_ASSERT(write_sz == zero_size, "Failed to write to device");
                LOGINFO("Successfully zeroed the 1st {} bytes of device {}", zero_size, path);
                ::close(fd);
            }
        }
    }

    void init_dev_list(bool init_device) {
        auto const ndevices = SISL_OPTIONS["num_devs"].as< uint32_t >();
        auto const dev_size = SISL_OPTIONS["dev_size_mb"].as< uint64_t >() * 1024 * 1024;
        if (SISL_OPTIONS.count("device_list")) {
            dev_list_ = SISL_OPTIONS["device_list"].as< std::vector< std::string > >();
            init_devices(false /*is_file*/);
        } else {
            // generate devices
            for (uint32_t i = 0; i < ndevices; i++) {
                std::string dev = test_name_ + std::to_string(i);
                dev_list_.push_back(dev);
            }
            LOGINFO("creating {} device files with each of size {} ", ndevices, homestore::in_bytes(dev_size));
            init_devices(true /*is_file*/, dev_size);
        }
    }

private:
    std::string test_name_;
    std::vector< std::string > args_;
    char** argv_;
    std::vector< std::string > dev_list_;
    shared< homeblocks::HomeBlocks > hb_;
    shared< HBTestApplication > app_;
    peer_id_t svc_id_;
};

} // namespace test_common
