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
// craft_ublk: a ublkpp_disk-style CLI that stands up an in-memory CRAFT replica set (a single replica
// by default), logs a craft_client into it, and exposes it as a /dev/ublkbN block device via the
// CraftUblkDisk adapter. Unlike homeblk_ublk it does NOT bring up HomeStore -- the CRAFT reference model
// is HomeStore-free, so we only start iomgr (the ublk<->client completion bridge needs reactors).
//
//   sudo ./craft_ublk --vol_size_mb 8192 --page_size 4096
//   sudo ./craft_ublk --vol_size_mb 4096 --replicas 1 --num_threads 4
//
#include <cerrno>
#include <csignal>
#include <cstring>
#include <future>
#include <iostream>
#include <random>
#include <string>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <iomgr/io_environment.hpp> // ioenvironment.with_iomgr
#include <iomgr/iomgr.hpp>          // iomanager.stop
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <ublkpp/target.hpp>
#include <homeblks/home_blocks.hpp>

#include "model/mem_craft_volume.hpp" // make_memory_replica_set
#include "craft_ublk_disk.hpp"
#include "craft_client.hpp"
#include "coro_helpers.hpp" // homeblocks::detail::sync_get

SISL_OPTION_GROUP(craft_ublk,
                  (vol_id, "", "vol_id", "Volume UUID to expose (random if unset)", ::cxxopts::value< std::string >(),
                   "<uuid>"),
                  (vol_size_mb, "", "vol_size_mb", "Volume size in MB",
                   ::cxxopts::value< uint64_t >()->default_value("1024"), "<mb>"),
                  (page_size, "", "page_size", "Volume logical block / page size in bytes",
                   ::cxxopts::value< uint32_t >()->default_value("4096"), "<bytes>"),
                  (replicas, "", "replicas", "Number of in-memory CRAFT replicas in the set",
                   ::cxxopts::value< uint32_t >()->default_value("1"), "<n>"),
                  (max_io_size_mb, "", "max_io_size_mb", "Largest single transfer in MB",
                   ::cxxopts::value< uint32_t >()->default_value("1"), "<mb>"),
                  (num_threads, "", "num_threads", "iomgr reactor count",
                   ::cxxopts::value< uint32_t >()->default_value("2"), "<n>"),
                  (device_id, "", "device_id", "ublk device id: -1 to assign, >=0 to recover a preserved device",
                   ::cxxopts::value< int32_t >()->default_value("-1"), "<ublkid>"))

// `homeblocks` is a logging module. The in-memory model + public API live in libhomeblocks, but we never
// call init_homeblocks (no HomeStore), so the static archive does not pull in the module's definition --
// define it here, exactly as test_craft_e2e does. `ublkpp_tgt` is ublkpp's own target option group
// (nr_hw_queues, feature_zero_copy, ...) -- ublkpp_tgt::run reads it, so it must be enabled and loaded.
#define ENABLED_OPTIONS logging, ublkpp_tgt, craft_ublk, config

SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

SISL_LOGGING_DEF(homeblocks)
SISL_LOGGING_INIT(homeblocks, UBLKPP_LOG_MODS)

using namespace homeblocks;

namespace {
constexpr uint64_t Mi = 1024ULL * 1024ULL;

// Clean shutdown plumbing (same shape as ublkpp_disk.cpp / homeblk_ublk).
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

uint64_t random_token() {
    std::random_device rd;
    uint64_t const t = (static_cast< uint64_t >(rd()) << 32) | static_cast< uint64_t >(rd());
    return t ? t : 0xC0FFEEULL; // never hand out 0
}
} // namespace

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T] [%^%l%$] [%n] [%t] %v");

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    auto exit_future = s_stop_code.get_future();

    // Start iomgr only (no HomeStore): the ublk queue<->client completion bridge runs on worker reactors.
    auto const num_threads = SISL_OPTIONS["num_threads"].as< uint32_t >();
    ioenvironment.with_iomgr(iomgr::iomgr_params{.num_threads = num_threads});

    auto const vid = (0 < SISL_OPTIONS.count("vol_id"))
        ? boost::uuids::string_generator()(SISL_OPTIONS["vol_id"].as< std::string >())
        : boost::uuids::random_generator()();
    auto const page_size = SISL_OPTIONS["page_size"].as< uint32_t >();
    auto const capacity = SISL_OPTIONS["vol_size_mb"].as< uint64_t >() * Mi;
    auto const replicas = SISL_OPTIONS["replicas"].as< uint32_t >();
    auto const max_tx = SISL_OPTIONS["max_io_size_mb"].as< uint32_t >() * static_cast< uint32_t >(Mi);

    int rc = 0;
    {
        // Stand up the in-process replica set and log a client into it. The client owns the replica
        // handles; today these are in-memory, later they become network-shim handles (the only change).
        volume_info info;
        info.id = vid;
        info.size_bytes = capacity;
        info.page_size = page_size;
        info.name = fmt::format("craft_{}", boost::uuids::to_string(vid).substr(0, 8));
        info.repl_mode = replication_mode::CRAFT;

        // ublk's outstanding-IO bound (one tag per in-flight IO) sizes the client's shadow-scan tripwire.
        auto const max_inflight = static_cast< uint32_t >(SISL_OPTIONS["nr_hw_queues"].as< uint16_t >()) *
            static_cast< uint32_t >(SISL_OPTIONS["qdepth"].as< uint16_t >());

        auto set = craft::make_memory_replica_set(std::move(info), replicas);
        auto client = std::make_shared< craft::craft_client >(std::move(set.handles), 0, max_inflight);

        auto login_res = detail::sync_get(client->login(random_token()));
        if (!login_res) {
            LOGERROR("CRAFT login failed: {}", login_res.error().message());
            iomanager.stop();
            return EIO;
        }
        LOGINFO("CRAFT login ok: {} replica(s), term={}, lba_size={}", replicas, client->term(), client->lba_size());

        auto disk = std::make_shared< ublk::CraftUblkDisk >(client, vid, capacity, page_size, max_tx);
        auto run = ublkpp::ublkpp_tgt::run(vid, std::move(disk), SISL_OPTIONS["device_id"].as< int32_t >());
        if (run) {
            auto target = std::move(run.value());
            LOGINFO("Volume {} is live at {}", boost::uuids::to_string(vid), target->device_path().native());
            std::cout << "CRAFT volume exposed at: " << target->device_path().native() << std::endl;

            rc = exit_future.get(); // block until SIGINT/SIGTERM
            LOGINFO("Shutting down ublk target");
            ublkpp::ublkpp_tgt::remove(std::move(target));
        } else {
            LOGERROR("ublkpp_tgt::run failed: {}", run.error().message());
            rc = EIO;
        }
    }

    LOGINFO("Stopping iomgr");
    iomanager.stop();
    return rc;
}
