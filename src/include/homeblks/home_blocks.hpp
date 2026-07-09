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
#pragma once

// The complete homeblocks public API, in one header. Consumers include only this file.

#include <compare>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <istream>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>
#include <iomgr/iomgr_types.hpp>
#include <sisl/async/task.hpp>
#include <sisl/fds/buffer.hpp> // sisl::sg_list
#include <sisl/logging/logging.h>
#include <sisl/utility/enum.hpp>
#include <homestore/error.hpp> // result / async_result / status / ok

#include <homeblks/craft_types.hpp> // client-facing CRAFT types (LoginResult, LSNPair, io_extent, client_hdr, craft_error); HomeStore-free

// Declares the homeblocks logging module so consumers can wire its log level into their own logging init. The
// per-call LOG* shorthand macros are internal (lib/hb_internal.hpp) and intentionally not exposed here.
SISL_LOGGING_DECL(homeblocks)

namespace homeblocks {

using peer_id_t = boost::uuids::uuid;
using volume_id_t = boost::uuids::uuid;

// homeblocks speaks homestore's one error surface:
//   result<T>       == std::expected<T, std::error_condition>   (synchronous)
//   async_result<T> == sisl::async::task<result<T>>             (a coroutine you co_await / sync_get)
//   status / async_status carry no value on success (just ok()/error).
using homestore::async_result;
using homestore::async_status;
using homestore::ok;
using homestore::result;
using homestore::status;

// =============================================== volumes ===============================================

// Opaque volume handle. Produced by home_blocks::create_volume() / get_volume(); never constructed directly.
// All I/O is via the free async_* functions below.
class volume;
using volume_handle = std::shared_ptr< volume >;

// homeblocks-specific failures only -- registered as a std::error_condition enum (bottom of this file) so they
// ride result<T> while staying branchable: if (r.error() == volume_error::CRC_MISMATCH) { ... }. Anything with a
// standard equivalent (invalid arg, no space, io error, unsupported op, ...) is returned as
// std::make_error_condition(std::errc::*) directly rather than duplicated here.
ENUM(volume_error, uint16_t, UNKNOWN_VOLUME = 1, CRC_MISMATCH, INDEX_ERROR, INTERNAL_ERROR, OFFLINE);

ENUM(volume_state, uint32_t,
     INIT,       // created, not yet online
     ONLINE,     // ready for I/O
     OFFLINE,    // not ready
     DESTROYING, // teardown in progress (also used for destroy crash-recovery)
     DESTROYED,  // fully torn down
     READONLY);

// Selects the replication backend for a volume.
// DISABLED  — existing ReplDev (raid1-style, data through RAFT log).
// CRAFT     — new CraftReplDev (data via client broadcast; RAFT carries only sync-LSN and login entries).
ENUM(replication_mode, uint8_t, DISABLED = 1, CRAFT);

struct volume_info {
    volume_id_t id;
    uint64_t size_bytes{0};
    uint64_t page_size{0}; // logical block size for this volume (a per-volume runtime setting)
    std::string name;
    uint64_t ordinal{0}; // internal: chunk-selector ordinal, assigned by homeblocks on create/recover
    replication_mode repl_mode{replication_mode::DISABLED};

    volume_info() = default;
    volume_info(const volume_info&) = delete;
    volume_info(volume_info&& rhs) noexcept :
            id(rhs.id),
            size_bytes(rhs.size_bytes),
            page_size(rhs.page_size),
            name(std::move(rhs.name)),
            ordinal(rhs.ordinal),
            repl_mode(rhs.repl_mode) {}
    volume_info(volume_id_t id_in, uint64_t size, uint64_t psize, std::string in_name) :
            id(id_in), size_bytes(size), page_size(psize), name(std::move(in_name)) {}
    volume_info(volume_id_t id_in, uint64_t size, uint64_t psize, std::string in_name, uint64_t ord) :
            id(id_in), size_bytes(size), page_size(psize), name(std::move(in_name)), ordinal(ord) {}

    auto operator<=>(volume_info const& rhs) const {
        return boost::uuids::hash_value(id) <=> boost::uuids::hash_value(rhs.id);
    }
    auto operator==(volume_info const& rhs) const { return id == rhs.id; }
    std::string to_string() const {
        return fmt::format("volume_info: id={} size_bytes={}, page_size={}, name={} ordinal={} repl_mode={}",
                           boost::uuids::to_string(id), size_bytes, page_size, name, ordinal, enum_name(repl_mode));
    }
};

struct volume_stats {
    volume_id_t id;
    volume_state state;
};

// ---- volume_error <-> std::error_condition registration ----
class volume_error_category : public std::error_category {
public:
    const char* name() const noexcept override { return "homeblocks.volume"; }
    std::string message(int ev) const override { return std::string{enum_name(static_cast< volume_error >(ev))}; }
};
inline std::error_category const& volume_error_category_inst() noexcept {
    static volume_error_category inst;
    return inst;
}
inline std::error_condition make_error_condition(volume_error e) noexcept {
    return std::error_condition{static_cast< int >(e), volume_error_category_inst()};
}

// ---- I/O: free functions over a volume_handle ----
//
// `addr` / `len` are RAW BYTE offsets into the volume, not block indices: the block size is a per-volume
// runtime setting, so bytes keep the contract unambiguous and confine the byte<->lba conversion to one place
// inside homeblocks. They must be block-aligned and in range, else the op resolves to std::errc::invalid_argument.
//
// Each returns a lazy coroutine: co_await it (or batch several with sisl::async::when_all) to run it. The
// result is [[nodiscard]] -- dropping the task means the I/O never issues. read/write resolve to the byte count
// transferred or a volume_error; the sg_list's iovecs point at caller-owned buffers that must outlive the await
// (the descriptor itself is copied into the coroutine frame), and its total length is the transfer size.
[[nodiscard]] [[deprecated("legacy block op; use the CRAFT async_read/async_write overloads below (see docs/craft)")]]
async_result< size_t > async_read(volume_handle const& vol, uint64_t addr, sisl::sg_list sgs);
[[nodiscard]] [[deprecated("legacy block op; use the CRAFT async_read/async_write overloads below (see docs/craft)")]]
async_result< size_t > async_write(volume_handle const& vol, uint64_t addr, sisl::sg_list sgs);
[[nodiscard]] [[deprecated("legacy block op; use CRAFT async_write(..., all_zeros=true) (see docs/craft)")]]
async_status async_unmap(volume_handle const& vol, uint64_t addr, uint64_t len);

// ---- CRAFT data plane: free functions over a volume_handle (one handle == one replica device) ----
//
// The calls a CRAFT client issues against ONE replica. The client owns the CRAFT work: it assigns a
// per-partition dLSN to each write, broadcasts async_write to every member itself, tallies quorum,
// and routes async_read (carrying the horizon `read_lsn` = H) to one eligible member. The handle
// comes from create_volume (production, later) or create_memory_volume (the in-memory reference
// model in src/test/craft/model). Unlike the byte-based legacy API above, these are LBA-based and
// carry the session `term` and client-assigned `dLSN` / `read_lsn` that CRAFT defines. Errors ride
// craft_error (e.g. STALE_TERM, NOT_LEADER). See docs/craft/api.md and the CRAFT-Design wiki.
// Note: login() returns a LoginResult redirect (term==0) on NOT_LEADER, not a craft_error.

// Establishes the session; returns members, starting dLSN, and term. A follower returns a redirect
// LoginResult (term==0, leader_hint set) -- not an error -- so the client can retry the right replica.
// Other failures (NO_QUORUM, REPLICA_DOWN) are returned as errors.
[[nodiscard]] async_result< LoginResult > login(volume_handle const& vol, uint64_t client_token);

// Explicit session teardown. Term-fenced; leader propagates InternalLogout to all live replicas so
// subsequent IOs from the old client fail with STALE_TERM. Returns NOT_LEADER on a follower.
[[nodiscard]] async_status logout(volume_handle const& vol, client_hdr hdr);

// Append one client-assigned write at slot `dlsn`. `addr`/`len` are BYTE offset/length and must be
// aligned to the volume's lba_size (from LoginResult), else std::errc::invalid_argument. `data` is a
// caller-owned (iomgr) buffer: EMPTY data (size 0) is a zero write (WRITE_ZEROES / unmap; reads back as
// a hole); non-empty data is a data write of exactly `len` bytes -- so the empty buffer, not a flag,
// signals a zero write. Not applied to the index directly; `hdr.commit_lsn` rides along and advances the
// frontier best-effort in dLSN order (CRAFT's piggybacked commit). STALE_TERM if hdr.term != session term.
[[nodiscard]] async_status async_write(volume_handle const& vol, client_hdr hdr, int64_t dlsn, uint64_t addr,
                                       uint64_t len, sisl::sg_list data);

// Read the latest version <= `read_lsn` (horizon H) for [addr, addr+len) (BYTE offset/length, aligned to
// lba_size). Fills the caller-owned `dest` buffer in place -- data sub-ranges get their bytes, holes get
// zeros -- and returns the sparse layout (which byte sub-ranges were data vs holes; the thin/hole info).
// Served from the index or the journal-tail overlay; never fetches from a peer. Advances the frontier to
// hdr.commit_lsn (piggybacked commit). std::errc::invalid_argument if addr/len are misaligned.
[[nodiscard]] async_result< std::vector< io_extent > >
async_read(volume_handle const& vol, client_hdr hdr, int64_t read_lsn, uint64_t addr, uint64_t len, sisl::sg_list dest);

// Advance the frontier toward hdr.commit_lsn (best-effort; stalls below the first hole) and reset the
// client-liveness watchdog (hence it is term-fenced). hdr.all_committed_lsn floors journal reclaim.
// Returns the achieved {commit_lsn, last_append_lsn}. The dedicated commit carrier; write/read piggyback
// it too, so there is no standalone commit() verb.
[[nodiscard]] async_result< LSNPair > keep_alive(volume_handle const& vol, client_hdr hdr);

// ============================================= home_blocks =============================================

struct home_blocks_stats {
    uint64_t total_capacity_bytes{0};
    uint64_t used_capacity_bytes{0};
    std::string to_string() const {
        return fmt::format("total_capacity_bytes={}, used_capacity_bytes={}", total_capacity_bytes,
                           used_capacity_bytes);
    }
};

ENUM(dev_type, uint8_t, AUTO_DETECT = 1, HDD, NVME, UNSUPPORTED);

struct device_info {
    std::filesystem::path path;
    dev_type type{dev_type::AUTO_DETECT};

    device_info() = default;
    explicit device_info(std::string name, dev_type t = dev_type::AUTO_DETECT) :
            path{std::filesystem::canonical(name)}, type{t} {}
    bool operator==(device_info const& rhs) const { return path == rhs.path && type == rhs.type; }
    friend std::istream& operator>>(std::istream& input, device_info& di) {
        std::string i_path, i_type;
        std::getline(input, i_path, ':');
        std::getline(input, i_type);
        di.path = std::filesystem::canonical(i_path);
        if (i_type == "HDD") {
            di.type = dev_type::HDD;
        } else if (i_type == "NVME") {
            di.type = dev_type::NVME;
        } else {
            di.type = dev_type::AUTO_DETECT;
        }
        return input;
    }
};

// All configuration for bringing up a homeblocks instance. Passed by value to init_homeblocks(); designated
// initializers + defaults keep the trivial case a one-liner (mirrors homestore's hs_input_params{...}).
struct home_blocks_config {
    std::vector< device_info > devices; // backing devices (required)
    uint32_t threads{2};                // iomgr reactor count
    uint64_t app_mem_size_mb{1024};     // memory budget (caches, etc.)

    // Cold-boot identity fetch. homeblocks invokes this exactly once -- and only when homestore comes up with no
    // persisted svc id -- then sync_gets the returned coroutine OFF-reactor (init is a cold, non-reactor path).
    // Resolve your (possibly rotated) OM client INSIDE the closure so the call uses the live one. The
    // result<peer_id_t> may carry an error (e.g. OM unreachable), which fails init_homeblocks. Empty -> a random
    // svc id is generated on first boot.
    std::function< async_result< peer_id_t >() > on_svc_id{};
};

// Opaque handle to a running homeblocks instance. Obtained only from init_homeblocks().
class home_blocks {
public:
    virtual ~home_blocks() = default;

    // --- instance ---
    virtual peer_id_t our_uuid() const = 0;
    virtual home_blocks_stats get_stats() const = 0;
    virtual iomgr::drive_type data_drive_type() const = 0;
    virtual uint64_t max_vol_io_size() const = 0;
    virtual void shutdown() = 0;

    // --- volume control plane ---
    // create_volume hands back the new volume; the handle is frequently discarded (get_volume retrieves it
    // later, e.g. to bring a volume online on restart). The task is [[nodiscard]] regardless -- you must
    // co_await / sync_get it or no work happens.
    [[nodiscard]] virtual async_result< volume_handle > create_volume(volume_info info) = 0;
    [[nodiscard]] virtual async_status remove_volume(volume_id_t const& id) = 0;

    // get_volume returns a ready-to-use handle for an existing (created or recovered) volume, or
    // volume_error::UNKNOWN_VOLUME.
    [[nodiscard]] virtual result< volume_handle > get_volume(volume_id_t const& id) const = 0;
    [[nodiscard]] virtual result< volume_stats > get_stats(volume_id_t id) const = 0;
    virtual std::vector< volume_id_t > volume_ids() const = 0;
};

// Bring up homeblocks. Operational failures (device open, format, on_svc_id RPC) -> result error; precondition
// bugs (e.g. no devices) assert. The returned handle owns the instance; drop it (or call shutdown()) to stop.
[[nodiscard]] result< std::shared_ptr< home_blocks > > init_homeblocks(home_blocks_config cfg);

} // namespace homeblocks

template <>
struct std::is_error_condition_enum< homeblocks::volume_error > : std::true_type {};
