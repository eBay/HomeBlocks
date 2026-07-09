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
//
// homeblk_ublk: a ublkpp_disk-style CLI that brings up a homeblocks instance, creates (or recovers) a
// volume, and exposes it as a /dev/ublkbN block device via the HomeBlkDisk adapter. Mirrors ublkpp's
// example/ublkpp_disk.cpp -- run it, point fio/dd/mkfs at the printed device path, ^C to tear down.
//
//   sudo ./homeblk_ublk --device /dev/nvme0n1 --vol_size_mb 4096
//   sudo ./homeblk_ublk --device hb.dev --create_device --dev_size_mb 8192 --data_chunk_size_mb 512
//
#include <cerrno>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <string>
#include <vector>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <ublkpp/target.hpp>
#include <homeblks/home_blocks.hpp>

#include "homeblk_disk.hpp"
#include "coro_helpers.hpp" // homeblocks::detail::sync_get

SISL_OPTION_GROUP(homeblk_ublk,
                  (vol_id, "", "vol_id", "Volume UUID to expose (recovered if it exists, else created)",
                   ::cxxopts::value< std::string >(), "<uuid>"),
                  (vol_size_mb, "", "vol_size_mb", "Volume size in MB (when creating)",
                   ::cxxopts::value< uint64_t >()->default_value("1024"), "<mb>"),
                  (page_size, "", "page_size", "Volume logical block / page size in bytes",
                   ::cxxopts::value< uint32_t >()->default_value("4096"), "<bytes>"),
                  (device, "", "device", "homeblocks backing device(s)",
                   ::cxxopts::value< std::vector< std::string > >(), "<path>[,<path>...]"),
                  (create_device, "", "create_device", "Create the backing device(s) as files of --dev_size_mb",
                   ::cxxopts::value< bool >()->default_value("false"), ""),
                  (dev_size_mb, "", "dev_size_mb", "Size of each created backing file in MB",
                   ::cxxopts::value< uint64_t >()->default_value("8192"), "<mb>"),
                  (num_threads, "", "num_threads", "homeblocks iomgr reactor count",
                   ::cxxopts::value< uint32_t >()->default_value("2"), "<n>"),
                  (app_mem_size_mb, "", "app_mem_size_mb", "homeblocks memory budget in MB",
                   ::cxxopts::value< uint64_t >()->default_value("4096"), "<mb>"),
                  (data_chunk_size_mb, "", "data_chunk_size_mb", "homeblocks data chunk size in MB (for small devices)",
                   ::cxxopts::value< uint32_t >(), "<mb>"),
                  (index_chunk_size_mb, "", "index_chunk_size_mb",
                   "homeblocks index chunk size in MB (for small devices)", ::cxxopts::value< uint32_t >(), "<mb>"),
                  (device_id, "", "device_id", "ublk device id: -1 to assign, >=0 to recover a preserved device",
                   ::cxxopts::value< int32_t >()->default_value("-1"), "<ublkid>"))

// `homeblocks` is a logging module (SISL_LOGGING_INIT below), NOT an options group, so it does not
// appear here. homeblocks reads its tunables (data/index_chunk_size_mb) out of the homeblk_ublk group.
// `ublkpp_tgt` is ublkpp's own target option group (nr_hw_queues, feature_zero_copy, ...) -- ublkpp_tgt::run
// reads it, so it must be enabled and loaded here just as ublkpp's own example/ublkpp_disk.cpp does.
#define ENABLED_OPTIONS logging, ublkpp_tgt, homeblk_ublk, config

SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

SISL_LOGGING_INIT(homeblocks, UBLKPP_LOG_MODS)

using namespace homeblocks;

namespace {
constexpr uint64_t Mi = 1024ULL * 1024ULL;

// Clean shutdown plumbing (same shape as ublkpp_disk.cpp).
std::promise< int > s_stop_code;

void handle_signal(int sig) {
    switch (sig) {
    case SIGINT:
        [[fallthrough]];
    case SIGTERM:
        try {
            LOGWARN("SIGNAL: {}", strsignal(sig));
            s_stop_code.set_value(sig);
        } catch (std::future_error const& e) { LOGERROR("Failed to set stop code: {}", e.what()); }
        break;
    default:
        LOGERROR("Unhandled SIGNAL: {}", strsignal(sig));
        break;
    }
}

std::vector< std::string > resolve_devices() {
    if (0 == SISL_OPTIONS.count("device")) {
        LOGERROR("At least one --device is required");
        return {};
    }
    auto devices = SISL_OPTIONS["device"].as< std::vector< std::string > >();
    if (SISL_OPTIONS["create_device"].as< bool >()) {
        auto const sz = SISL_OPTIONS["dev_size_mb"].as< uint64_t >() * Mi;
        for (auto const& d : devices) {
            if (std::filesystem::exists(d)) std::filesystem::remove(d);
            std::ofstream ofs{d, std::ios::binary | std::ios::out | std::ios::trunc};
            std::filesystem::resize_file(d, sz);
            LOGINFO("Created backing file {} ({} MB)", d, sz / Mi);
        }
    }
    return devices;
}

home_blocks_config make_config(std::vector< std::string > const& devices) {
    home_blocks_config cfg;
    cfg.threads = SISL_OPTIONS["num_threads"].as< uint32_t >();
    cfg.app_mem_size_mb = SISL_OPTIONS["app_mem_size_mb"].as< uint64_t >();
    for (auto const& d : devices) {
        cfg.devices.emplace_back(d);
    }
    auto const id = boost::uuids::random_generator()();
    cfg.on_svc_id = [id]() -> async_result< peer_id_t > { co_return id; };
    return cfg;
}

// Look up the volume if it already exists, otherwise create it with the requested geometry.
volume_handle open_or_create_volume(std::shared_ptr< home_blocks > const& hb, volume_id_t const& vid, uint64_t capacity,
                                    uint32_t page_size) {
    if (auto existing = hb->get_volume(vid); existing) {
        LOGINFO("Recovered existing volume {}", boost::uuids::to_string(vid));
        return existing.value();
    }
    volume_info info{vid, capacity, page_size, fmt::format("ublk_{}", boost::uuids::to_string(vid).substr(0, 8))};
    auto created = detail::sync_get(hb->create_volume(std::move(info)));
    if (!created) {
        LOGERROR("create_volume failed: {}", created.error().message());
        return nullptr;
    }
    LOGINFO("Created volume {} ({} MB, page_size {})", boost::uuids::to_string(vid), capacity / Mi, page_size);
    return created.value();
}
} // namespace

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T] [%^%l%$] [%n] [%t] %v");

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    auto exit_future = s_stop_code.get_future();

    auto const devices = resolve_devices();
    if (devices.empty()) {
        std::cout << SISL_PARSER.help({}) << std::endl;
        return EINVAL;
    }

    // Bring up homeblocks (this also starts iomgr internally).
    auto hb_res = init_homeblocks(make_config(devices));
    if (!hb_res) {
        LOGERROR("init_homeblocks failed: {}", hb_res.error().message());
        return EIO;
    }
    auto hb = hb_res.value();

    auto const vid = (0 < SISL_OPTIONS.count("vol_id"))
        ? boost::uuids::string_generator()(SISL_OPTIONS["vol_id"].as< std::string >())
        : boost::uuids::random_generator()();
    auto const page_size = SISL_OPTIONS["page_size"].as< uint32_t >();
    auto const capacity = SISL_OPTIONS["vol_size_mb"].as< uint64_t >() * Mi;

    int rc = 0;
    if (auto vol = open_or_create_volume(hb, vid, capacity, page_size); vol) {
        auto disk = std::make_shared< ublk::HomeBlkDisk >(hb, vol, vid, capacity, page_size,
                                                          static_cast< uint32_t >(hb->max_vol_io_size()));
        auto run = ublkpp::ublkpp_tgt::run(vid, std::move(disk), SISL_OPTIONS["device_id"].as< int32_t >());
        if (run) {
            auto target = std::move(run.value());
            LOGINFO("Volume {} is live at {}", boost::uuids::to_string(vid), target->device_path().native());
            std::cout << "homeblocks volume exposed at: " << target->device_path().native() << std::endl;

            rc = exit_future.get(); // block until SIGINT/SIGTERM
            LOGINFO("Shutting down ublk target");
            ublkpp::ublkpp_tgt::remove(std::move(target));
        } else {
            LOGERROR("ublkpp_tgt::run failed: {}", run.error().message());
            rc = EIO;
        }
    } else {
        rc = EIO;
    }

    LOGINFO("Shutting down homeblocks");
    hb->shutdown();
    hb.reset();
    return rc;
}
