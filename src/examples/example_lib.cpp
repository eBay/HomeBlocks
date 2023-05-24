#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include <iomgr/io_environment.hpp>
#include <homestore/homestore.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

SISL_OPTION_GROUP(example_lib,
                  (num_threads, "", "num_threads", "number of threads",
                   ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
                  (num_devs, "", "num_devs", "number of devices to create",
                   ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
                  (dev_size_mb, "", "dev_size_mb", "size of each device in MB",
                   ::cxxopts::value< uint64_t >()->default_value("2048"), "number"),
                  (device_list, "", "device_list", "Device List instead of default created",
                   ::cxxopts::value< std::vector< std::string > >(), "path [...]"));

static const std::string s_fpath_root{"/tmp/example_obj_store"};

void start_homestore(std::string const& svc_id) {
    auto const ndevices = SISL_OPTIONS["num_devs"].as< uint32_t >();
    auto const dev_size = SISL_OPTIONS["dev_size_mb"].as< uint64_t >() * 1024 * 1024;
    auto nthreads = SISL_OPTIONS["num_threads"].as< uint32_t >();

    std::vector< homestore::dev_info > device_info;
    if (SISL_OPTIONS.count("device_list")) {
        /* if user customized file/disk names */
        auto dev_names = SISL_OPTIONS["device_list"].as< std::vector< std::string > >();
        std::string dev_list_str;
        for (const auto& d : dev_names) {
            dev_list_str += d;
        }
        LOGINFO("Taking input dev_list: {}", dev_list_str);

        for (uint32_t i{0}; i < dev_names.size(); ++i) {
            device_info.emplace_back(dev_names[i], homestore::HSDevType::Data);
        }
    } else {
        /* create files */
        LOGINFO("creating {} device files with each of size {} ", ndevices, homestore::in_bytes(dev_size));
        for (uint32_t i{0}; i < ndevices; ++i) {
            auto fpath{s_fpath_root + "_" + svc_id + "_" + std::to_string(i + 1)};
            LOGINFO("creating {} device file", fpath);
            if (std::filesystem::exists(fpath)) { std::filesystem::remove(fpath); }
            std::ofstream ofs{fpath, std::ios::binary | std::ios::out | std::ios::trunc};
            std::filesystem::resize_file(fpath, dev_size);
            device_info.emplace_back(std::filesystem::canonical(fpath).string(), homestore::HSDevType::Data);
        }
    }

    LOGINFO("Starting iomgr with {} threads, spdk: {}", nthreads, false);
    ioenvironment.with_iomgr(nthreads, false);

    const uint64_t app_mem_size = ((ndevices * dev_size) * 15) / 100;
    LOGINFO("Initialize and start HomeStore with app_mem_size = {}", homestore::in_bytes(app_mem_size));

    homestore::hs_input_params params;
    params.app_mem_size = app_mem_size;
    params.data_devices = device_info;
    homestore::HomeStore::instance()
        ->with_params(params)
        .with_meta_service(5.0)
        .with_log_service(20.0, 5.0)
        .with_data_service(30.0)
        // .before_init_devices([this]() { })
        .init(true /* wait_for_init */);
}

void stop_homestore(std::string const& svc_id) {
    homestore::HomeStore::instance()->shutdown();
    homestore::HomeStore::reset_instance();
    iomanager.stop();
    auto const ndevices = SISL_OPTIONS["num_devs"].as< uint32_t >();
    for (uint32_t i{0}; i < ndevices; ++i) {
        auto fpath{s_fpath_root + "_" + svc_id + "_" + std::to_string(i + 1)};
        LOGINFO("removing {} device file", fpath);
        if (std::filesystem::exists(fpath)) { std::filesystem::remove(fpath); }
    }
}
