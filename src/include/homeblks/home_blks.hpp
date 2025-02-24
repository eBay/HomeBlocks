#pragma once

#include <list>

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
    virtual peer_id_t discover_svcid(std::optional< peer_id_t > const& found) const = 0;
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
};

extern std::shared_ptr< HomeBlocks > init_homeobject(std::weak_ptr< HomeBlocksApplication >&& application);
} // namespace homeblocks
