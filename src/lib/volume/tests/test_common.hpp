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
#include <fcntl.h>
#include <string>
#include <vector>
#include <map>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/settings/settings.hpp>
#include <iomgr/io_environment.hpp>
#include <iomgr/http_server.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/nil_generator.hpp>
#include <boost/uuid/random_generator.hpp>
#include <chrono>
#include <iostream>
#include <thread>
#include <homeblks/home_blks.hpp>
#include "lib/homeblks_impl.hpp"

SISL_OPTION_GROUP(
    test_common_setup,
    (num_threads, "", "num_threads", "number of threads", ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
    (num_devs, "", "num_devs", "number of devices to create", ::cxxopts::value< uint32_t >()->default_value("3"),
     "number"),
    (dev_size_mb, "", "dev_size_mb", "size of each device in MB", ::cxxopts::value< uint64_t >()->default_value("4096"),
     "number"),
    (device_list, "", "device_list", "Device List instead of default created",
     ::cxxopts::value< std::vector< std::string > >(), "path [...]"),
    (http_port, "", "http_port", "http port (0 for no http, -1 for random, rest specific value)",
     ::cxxopts::value< int >()->default_value("-1"), "number"),
    (num_io, "", "num_io", "number of IO operations", ::cxxopts::value< uint64_t >()->default_value("300"), "number"),
    (qdepth, "", "qdepth", "Max outstanding operations", ::cxxopts::value< uint32_t >()->default_value("8"), "number"),
    (num_io_reactors, "", "num_io_reactors", "number of IO reactors", ::cxxopts::value< uint32_t >()->default_value("0"), "number"),
    (spdk, "", "spdk", "spdk", ::cxxopts::value< bool >()->default_value("false"), "true or false"),
    (flip_list, "", "flip_list", "btree flip list", ::cxxopts::value< std::vector< std::string > >(), "flips [...]"),
    (use_file, "", "use_file", "use file instead of real drive", ::cxxopts::value< bool >()->default_value("false"),
     "true or false"),
    (enable_crash, "", "enable_crash", "enable crash", ::cxxopts::value< bool >()->default_value("0"), ""),
    (app_mem_size, "", "app_mem_size", "app memory size", ::cxxopts::value< uint64_t >()->default_value("20"),
     "number"),
    (hs_chunk_size_mb, "", "hs_chunk_size_mb", "hs_chunk_size_mb", ::cxxopts::value< uint64_t >()->default_value("128"),
     "number"));

using namespace homeblocks;

class test_http_server {
public:
    void get_prometheus_metrics(const Pistache::Rest::Request&, Pistache::Http::ResponseWriter response) {
        response.send(Pistache::Http::Code::Ok,
                      sisl::MetricsFarm::getInstance().report(sisl::ReportFormat::kTextFormat));
    }

    void start() {
        auto http_server_ptr = ioenvironment.get_http_server();

        std::vector< iomgr::http_route > routes = {
            {Pistache::Http::Method::Get, "/metrics",
             Pistache::Rest::Routes::bind(&test_http_server::get_prometheus_metrics, this), iomgr::url_t::safe}};
        try {
            http_server_ptr->setup_routes(routes);
            LOGINFO("Started http server ");
        } catch (std::runtime_error const& e) { LOGERROR("setup routes failed, {}", e.what()) }

        // start the server
        http_server_ptr->start();
    }
};

namespace test_common {

struct io_fiber_pool {
    std::vector< iomgr::io_fiber_t > io_fibers_;
    io_fiber_pool(uint32_t num_io_reactors) {
        struct Context {
            std::condition_variable cv;
            std::mutex mtx;
            uint32_t thread_cnt{0};
        };
        auto ctx = std::make_shared< Context >();
        for (uint32_t i{0}; i < num_io_reactors; ++i) {
        iomanager.create_reactor("homeblks_long_running_io" + std::to_string(i), iomgr::INTERRUPT_LOOP, 1u,
                                 [this, ctx](bool is_started) {
                                     if (is_started) {
                                         {
                                             std::unique_lock< std::mutex > lk{ctx->mtx};
                                             io_fibers_.push_back(iomanager.iofiber_self());
                                             ++(ctx->thread_cnt);
                                         }
                                         ctx->cv.notify_one();
                                     }
                                 });
        }
        {
            std::unique_lock< std::mutex > lk{ctx->mtx};
            ctx->cv.wait(lk, [&ctx, num_io_reactors]() { return ctx->thread_cnt == num_io_reactors; });
        }
        LOGINFO("Created {} IO reactors", io_fibers_.size());
    }

    void run_task(uint32_t idx, std::function< void(void) > const& task) {
        iomanager.run_on_forget(io_fibers_[idx], task);
    }
};

struct Runner {
    uint64_t total_tasks_{0};
    uint32_t qdepth_{8};
    std::atomic< uint64_t > issued_tasks_{0};
    std::atomic< uint64_t > completed_tasks_{0};
    std::function< void(void) > task_;
    folly::Promise< folly::Unit > comp_promise_;
    std::shared_ptr< io_fiber_pool > io_fiber_pool_;

    Runner(uint64_t num_tasks, uint32_t qd = 8, std::shared_ptr< io_fiber_pool > const& g_io_fiber_pool = nullptr) : total_tasks_{num_tasks}, qdepth_{qd}, io_fiber_pool_{g_io_fiber_pool} {
        if (total_tasks_ < (uint64_t)qdepth_) { total_tasks_ = qdepth_; }
    }
    Runner() : Runner{SISL_OPTIONS["num_io"].as< uint64_t >(), SISL_OPTIONS["qdepth"].as< uint32_t >(), nullptr} {}
    Runner(const Runner&) = delete;
    Runner& operator=(const Runner&) = delete;

    void set_num_tasks(uint64_t num_tasks) { total_tasks_ = std::max((uint64_t)qdepth_, num_tasks); }
    void set_task(std::function< void(void) > f) {
        issued_tasks_.store(0);
        completed_tasks_.store(0);
        comp_promise_ = folly::Promise< folly::Unit >{};
        task_ = std::move(f);
    }

    folly::Future< folly::Unit > execute() {
        for (uint32_t i{0}; i < qdepth_; ++i) {
            run_task();
        }
        return comp_promise_.getFuture();
    }

    void next_task() {
        auto ctasks = completed_tasks_.fetch_add(1);
        if ((issued_tasks_.load() < total_tasks_)) {
            run_task();
        } else if ((ctasks + 1) == total_tasks_) {
            comp_promise_.setValue();
        }
    }

    void run_task() {
        ++issued_tasks_;
        if (io_fiber_pool_) {
            static std::atomic<uint32_t> idx{0};
            static const uint32_t max_idx{SISL_OPTIONS["num_io_reactors"].as< uint32_t >()};
            uint32_t io_idx = (idx.fetch_add(1) % max_idx);
            // run the task on the next io_fiber
            io_fiber_pool_->run_task(io_idx, task_);
        } else {
            iomanager.run_on_forget(iomgr::reactor_regex::random_worker, task_);
        }
    }
};

struct Waiter {
    std::atomic< uint64_t > expected_comp{0};
    std::atomic< uint64_t > actual_comp{0};
    folly::Promise< folly::Unit > comp_promise;

    Waiter(uint64_t num_op) : expected_comp{num_op} {}
    Waiter() : Waiter{SISL_OPTIONS["num_io"].as< uint64_t >()} {}
    Waiter(const Waiter&) = delete;
    Waiter& operator=(const Waiter&) = delete;

    folly::Future< folly::Unit > start(std::function< void(void) > f) {
        f();
        return comp_promise.getFuture();
    }

    void one_complete() {
        if ((actual_comp.fetch_add(1) + 1) >= expected_comp.load()) { comp_promise.setValue(); }
    }
};

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
            return SISL_OPTIONS["num_threads"].as< uint32_t >();
        }

        std::list< device_info_t > devices() const override {
            std::list< device_info_t > devs;
            for (const auto& dev : helper_.dev_list()) {
                devs.emplace_back(dev);
            }
            return devs;
        }

        std::optional< peer_id_t > discover_svc_id(std::optional< peer_id_t > const&) const override {
            return helper_.svc_id();
        }

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
        spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");

        // init svc_id_
        svc_id_ = boost::uuids::random_generator()();

        // init device list
        init_dev_list(true /*init_device*/);

        LOGINFO("Starting HomeBlocks");
        homeblocks::HomeBlocksImpl::_hs_chunk_size = SISL_OPTIONS["hs_chunk_size_mb"].as< uint64_t >() * Mi;
        // set_min_chunk_size(4 * Mi);
        app_ = std::make_shared< HBTestApplication >(*this);
        hb_ = init_homeblocks(std::weak_ptr< HBTestApplication >(app_));
    }

    void restart(uint64_t delay_secs = 0) {
        LOGINFO("Restart HomeBlocks");
        hb_->shutdown();
        hb_.reset();
        LOGINFO("Start HomeBlocks after {} secs", delay_secs);
        if (delay_secs > 0) { std::this_thread::sleep_for(std::chrono::seconds(delay_secs)); }
        hb_ = init_homeblocks(std::weak_ptr< HBTestApplication >(app_));
    }

    shared< homeblocks::HomeBlocks > inst() { return hb_; }

    void teardown() {
        LOGINFO("Tearing down test.");
        hb_->shutdown();
        hb_.reset();
        remove_files();
    }

    peer_id_t svc_id() { return svc_id_; }
    std::vector< std::string > const& dev_list() const { return dev_list_; }
    Runner& runner() { return io_runner_; }
    Waiter& waiter() { return waiter_; }

    static void fill_data_buf(uint8_t* buf, uint64_t size, uint64_t pattern = 0) {
        uint64_t* ptr = r_cast< uint64_t* >(buf);
        for (uint64_t i = 0ul; i < size / sizeof(uint64_t); ++i) {
            *(ptr + i) = (pattern == 0) ? i : pattern;
        }
    }

    static void validate_data_buf(uint8_t const* buf, uint64_t size, uint64_t pattern = 0) {
        uint64_t const* ptr = r_cast< uint64_t const* >(buf);
        for (uint64_t i = 0ul; i < size / sizeof(uint64_t); ++i) {
            RELEASE_ASSERT_EQ(ptr[i], ((pattern == 0) ? i : pattern), "data_buf mismatch at offset={}", i);
        }
    }

    static void validate_zeros(uint8_t const* buf, uint64_t size) {
        static const uint8_t zeros[1024 * 1024] = {}; // 1 mb static zero buffer
        while (size > 0) {
            size_t chunk = std::min(size, sizeof(zeros));
            RELEASE_ASSERT_EQ(memcmp(buf, zeros, chunk), 0, "Data buffer mismatch");
            buf += chunk;
            size -= chunk;
        }
    }

#ifdef _PRERELEASE
    void set_flip_point(const std::string flip_name, uint32_t count = 2) {
        flip::FlipCondition null_cond;
        flip::FlipFrequency freq;
        freq.set_count(count);
        freq.set_percent(100);
        m_fc.inject_noreturn_flip(flip_name, {null_cond}, freq);
        LOGI("Flip {} set", flip_name);
    }

    void set_delay_flip(const std::string flip_name, uint64_t delay_usec, uint32_t count = 1, uint32_t percent = 100) {
        flip::FlipCondition null_cond;
        flip::FlipFrequency freq;
        freq.set_count(count);
        freq.set_percent(percent);
        m_fc.inject_delay_flip(flip_name, {null_cond}, freq, delay_usec);
        LOGDEBUG("Flip {} set", flip_name);
    }

    void remove_flip(const std::string flip_name) {
        m_fc.remove_flip(flip_name);
        LOGDEBUG("Flip {} removed", flip_name);
    }
#endif
    void set_min_chunk_size(uint64_t chunk_size) {
#ifdef _PRERELEASE
        LOGINFO("Set minimum chunk size {}", chunk_size);
        flip::FlipClient* fc = iomgr_flip::client_instance();

        flip::FlipFrequency freq;
        freq.set_count(2000000);
        freq.set_percent(100);

        flip::FlipCondition dont_care_cond;
        fc->create_condition("", flip::Operator::DONT_CARE, (int)1, &dont_care_cond);
        fc->inject_retval_flip< long >("set_minimum_chunk_size", {dont_care_cond}, freq, chunk_size);
#endif
    }

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

    void remove_files() {
        if (SISL_OPTIONS.count("device_list")) { return; }
        for (const auto& fpath : dev_list_) {
            if (std::filesystem::exists(fpath)) {
                LOGINFO("Removing file {}", fpath);
                std::filesystem::remove(fpath);
            }
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
    Runner io_runner_;
    Waiter waiter_;

#ifdef _PRERELEASE
    flip::FlipClient m_fc{iomgr_flip::instance()};
#endif
};

} // namespace test_common
