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
#pragma once

#include <list>
#include <iomgr/iomgr_types.hpp>
#include <sisl/utility/enum.hpp>

#include "common.hpp"

namespace homeblocks {

class VolumeManager;

ENUM(DevType, uint8_t, AUTO_DETECT = 1, HDD, NVME, UNSUPPORTED);
struct device_info_t {
    explicit device_info_t(std::string name, DevType dtype = DevType::AUTO_DETECT) :
            path{std::filesystem::canonical(name)}, type{dtype} {}
    device_info_t() = default;
    bool operator==(device_info_t const& rhs) const { return path == rhs.path && type == rhs.type; }
    friend std::istream& operator>>(std::istream& input, device_info_t& di) {
        std::string i_path, i_type;
        std::getline(input, i_path, ':');
        std::getline(input, i_type);
        di.path = std::filesystem::canonical(i_path);
        if (i_type == "HDD") {
            di.type = DevType::HDD;
        } else if (i_type == "NVME") {
            di.type = DevType::NVME;
        } else {
            di.type = DevType::AUTO_DETECT;
        }
        return input;
    }
    std::filesystem::path path;
    DevType type;
};

class HomeBlocksApplication {
public:
    virtual ~HomeBlocksApplication() = default;

    virtual bool spdk_mode() const = 0;
    virtual uint32_t threads() const = 0;
    virtual std::list< device_info_t > devices() const = 0;
    // in bytes;
    virtual uint64_t app_mem_size() const = 0;

    // Callback made after determining if a SvcId exists or not during initialization, will consume response
    virtual std::optional< peer_id_t > discover_svc_id(std::optional< peer_id_t > const& found) const = 0;
};

struct HomeBlocksStats {
    uint64_t total_capacity_bytes{0};
    uint64_t used_capacity_bytes{0};
    std::string to_string() const {
        return fmt::format("total_capacity_bytes={}, used_capacity_bytes={}", total_capacity_bytes,
                           used_capacity_bytes);
    }
};

class HomeBlocks {
public:
    virtual ~HomeBlocks() = default;
    virtual peer_id_t our_uuid() const = 0;
    virtual std::shared_ptr< VolumeManager > volume_manager() = 0;
    virtual HomeBlocksStats get_stats() const = 0;
    virtual iomgr::drive_type data_drive_type() const = 0;
    virtual uint64_t max_vol_io_size() const = 0;
    virtual void shutdown() = 0;
};

extern std::shared_ptr< HomeBlocks > init_homeblocks(std::weak_ptr< HomeBlocksApplication >&& application);
} // namespace homeblocks
