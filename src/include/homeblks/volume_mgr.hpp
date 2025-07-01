#pragma once
#include <compare>
#include <string>

#include <boost/uuid/uuid_io.hpp>
#include <sisl/utility/enum.hpp>
#include <sisl/utility/obj_life_counter.hpp>

#include "common.hpp"

namespace homeblocks {

ENUM(VolumeError, uint16_t, UNKNOWN = 1, INVALID_ARG, TIMEOUT, UNKNOWN_VOLUME, UNSUPPORTED_OP, CRC_MISMATCH,
     NO_SPACE_LEFT, DRIVE_WRITE_ERROR, INTERNAL_ERROR, INDEX_ERROR);

using lba_t = uint64_t;
using lba_count_t = uint32_t;

class Volume;
using VolumePtr = shared< Volume >;

// volume interface request should be freed only after IO is completed.
struct vol_interface_req : public sisl::ObjLifeCounter< vol_interface_req > {
    uint8_t* buffer{nullptr};
    lba_t lba;
    lba_count_t nlbas;
    sisl::atomic_counter< int > refcount;
    bool part_of_batch{false};
    uint64_t request_id;
    VolumePtr vol{nullptr}; // back refto the volume this request is associated with.

    friend void intrusive_ptr_add_ref(vol_interface_req* req) { req->refcount.increment(1); }
    friend void intrusive_ptr_release(vol_interface_req* req);

public:
    vol_interface_req(uint8_t* const buf, const uint64_t lba, const uint32_t nlbas, VolumePtr vol_ptr);
    virtual ~vol_interface_req() = default; // override; sisl::ObjLifeCounter should have virtual destructor
    virtual void free_yourself() { delete this; }
    lba_t end_lba() const { return lba + nlbas - 1; }
};

using vol_interface_req_ptr = boost::intrusive_ptr< vol_interface_req >;

struct VolumeInfo {
    VolumeInfo() = default;
    VolumeInfo(const VolumeInfo&) = delete;
    VolumeInfo(VolumeInfo&& rhs) noexcept :
            id(rhs.id),
            size_bytes(rhs.size_bytes),
            page_size(rhs.page_size),
            name(std::move(rhs.name)),
            ordinal(rhs.ordinal) {}

    VolumeInfo(volume_id_t id_in, uint64_t size, uint64_t psize, std::string in_name, uint64_t ord) :
            id(id_in), size_bytes(size), page_size(psize), name(std::move(in_name)), ordinal(ord) {}

    volume_id_t id;
    uint64_t size_bytes{0};
    uint64_t page_size{0};
    std::string name;
    uint64_t ordinal;

    auto operator<=>(VolumeInfo const& rhs) const {
        return boost::uuids::hash_value(id) <=> boost::uuids::hash_value(rhs.id);
    }

    auto operator==(VolumeInfo const& rhs) const { return id == rhs.id; }

    std::string to_string() {
        return fmt::format("VolumeInfo: id={} size_bytes={}, page_size={}, name={} ordinal={}",
                           boost::uuids::to_string(id), size_bytes, page_size, name, ordinal);
    }
};

using VolumeInfoPtr = std::shared_ptr< VolumeInfo >;
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

    virtual NullAsyncResult remove_volume(const volume_id_t& id) = 0;

    virtual VolumePtr lookup_volume(const volume_id_t& id) = 0;

    /**
     * @brief Write the data to the volume asynchronously, created from the request. After completion the attached
     * callback function will be called with this req ptr.
     *
     * @param vol Pointer to the volume
     * @param req Request created which contains all the write parameters
     * req.part_of_batch field can be used if this request is part of a batch request. If so, implementation can wait
     * for batch_submit call before issuing the writes. IO might already be started or even completed (in case of
     * errors) before batch_sumbit call, so application cannot assume IO will be started only after submit_batch call.
     *
     * @return std::error_condition no_error or error in issuing writes
     */
    virtual NullAsyncResult write(const VolumePtr& vol, const vol_interface_req_ptr& req) = 0;

    /**
     * @brief Read the data from the volume asynchronously, created from the request. After completion the attached
     * callback function will be called with this req ptr.
     *
     * @param vol Pointer to the volume
     * @param req Request created which contains all the read parameters
     * req.part_of_batch field can be used if this request is part of a batch request. If so, implementation can wait
     * for batch_submit call before issuing the reads. IO might already be started or even completed (in case of errors)
     * before batch_sumbit call, so application cannot assume IO will be started only after submit_batch call.
     *
     * @return std::error_condition no_error or error in issuing reads
     */
    virtual NullAsyncResult read(const VolumePtr& vol, const vol_interface_req_ptr& req) = 0;

    /**
     * @brief unmap the given block range
     *
     * @param vol Pointer to the volume
     * @param req Request created which contains all the read parameters
     */
    virtual NullAsyncResult unmap(const VolumePtr& vol, const vol_interface_req_ptr& req) = 0;

    /**
     * @brief Submit the io batch, which is a mandatory method to be called if read/write are issued with part_of_batch
     * is set to true. In those cases, without this method, IOs might not be even issued. No-op if previous io requests
     * are not part of batch.
     */
    virtual void submit_io_batch() = 0;

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
