#pragma once
#include <compare>
#include <string>

#include <boost/uuid/uuid_io.hpp>
#include <sisl/utility/enum.hpp>

#include "common.hpp"

namespace homeblocks {

ENUM(VolumeError, uint16_t, UNKNOWN = 1, INVALID_ARG, TIMEOUT, UNKNOWN_VOLUME, UNSUPPORTED_OP, CRC_MISMATCH,
     NO_SPACE_LEFT, DRIVE_WRITE_ERROR);

struct VolumeInfo {
    VolumeInfo(volume_id_t _id, uint64_t _num_bytes) : id(_id), size_bytes(_num_bytes) {}

    volume_id_t id;
    uint64_t size_bytes{0};

    auto operator<=>(VolumeInfo const& rhs) const {
        return boost::uuids::hash_value(id) <=> boost::uuids::hash_value(rhs.id);
    }
    auto operator==(VolumeInfo const& rhs) const { return id == rhs.id; }
};

struct VolumeStats {
    volume_id_t id;

    uint64_t used_bytes;  // total number of bytes used by all shards on this Volume;
    uint64_t avail_bytes; // total number of bytes available on this Volume;

    std::string to_string() {
        return fmt::format("VolumeStats: id={} used_bytes={}, avail_bytes={}", boost::uuids::to_string(id), used_bytes,
                           avail_bytes);
    }
};

class VolumeManager : public Manager< VolumeError > {
public:
    virtual NullAsyncResult create_volume(VolumeInfo&& volume_info) = 0;

    /**
     * Retrieves the statistics for a specific Volume identified by its ID.
     *
     * @param id The ID of the Volume.
     * @param stats The reference to the VolumeStats object where the statistics will be stored.
     * @return True if the statistics were successfully retrieved, false otherwise (e.g. id not found).
     */
    virtual bool get_stats(volume_id_t id, VolumeStats& stats) const = 0;

    /**
     * @brief Retrieves the list of volume_ids.
     *
     * This function retrieves the list of volume_ids and stores them in the provided vector.
     *
     * @param vol_ids The vector to store the volume ids.
     */
    virtual void get_volume_ids(std::vector< volume_id_t >& vol_ids) const = 0;
};

} // namespace homeblocks
