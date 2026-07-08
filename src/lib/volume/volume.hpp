
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

#include "hb_internal.hpp"
#include <sisl/utility/obj_life_counter.hpp>
#include "sisl/utility/enum.hpp"
#include <homestore/homestore.hpp>
#include <homestore/index/index_table.hpp>
#include <homestore/replication/repl_dev.hpp>
#include <iomgr/io_op.hpp>
#include <sisl/async/task.hpp>
#include <sisl/async/value_awaitable.hpp>
#include <sisl/async/when_all.hpp>
#include <sisl/metrics/metrics.hpp>

#if USE_FIXED_INDEX
#include "index_fixed_table.hpp"
#else
#include "index_prefix_table.hpp"
#endif

#include "volume_chunk_selector.hpp"
#include "io_req.hpp"
#include "sisl/utility/atomic_counter.hpp"

namespace homeblocks {

using VolIdxTablePtr = shared< VolumeIndexTable >;

using ReplDevPtr = shared< homestore::repl_dev >;
using index_cfg_t = homestore::BtreeConfig;

using read_blks_list_t = std::vector< std::pair< lba_t, homestore::multi_blk_id > >;
struct vol_read_ctx {
    const io_req* req;
    uint32_t blk_size;
    index_kv_list_t index_kvs{};
};

struct VolJournalEntry {
    lba_t start_lba;
    lba_count_t nlbas;
    uint16_t num_old_blks;
};

ENUM(MsgType, uint8_t, READ, WRITE, UNMAP);
struct MsgHeader {
    MsgHeader() = default;
    MsgType msg_type;
    volume_id_t volume_id;

    std::string to_string() const {
        return fmt::format(" msg_type={}volume={}\n", enum_name(msg_type), boost::uuids::to_string(volume_id));
    }
};

class VolumeMetrics : public sisl::MetricsGroup {
public:
    explicit VolumeMetrics(const std::string& vol_name) : sisl::MetricsGroup("volume", vol_name) {
        // counters
        REGISTER_COUNTER(volume_read_count, "Total volume read operations", "volume_op_count", {"op", "read"});
        REGISTER_COUNTER(volume_write_count, "Total volume write operations", "volume_op_count", {"op", "write"});
        REGISTER_COUNTER(volume_write_size_total, "Total volume data size written", "volume_data_size",
                         {"op", "write"});
        REGISTER_COUNTER(volume_read_size_total, "Total volume data size read", "volume_data_size", {"op", "read"});
        // gauges
        REGISTER_GAUGE(volume_data_used_size, "Total volume data used size");
        // histograms
        REGISTER_HISTOGRAM(volume_write_size_distribution, "Distribution of volume write sizes",
                           HistogramBucketsType(OpSizeBuckets));
        REGISTER_HISTOGRAM(volume_read_size_distribution, "Distribution of volume read sizes",
                           HistogramBucketsType(OpSizeBuckets));
        REGISTER_HISTOGRAM(volume_read_latency, "volume overall read latency", "volume_op_latency", {"op", "read"},
                           HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(volume_write_latency, "volume overall write latency", "volume_op_latency", {"op", "write"},
                           HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(volume_data_read_latency, "volume data blocks read latency", "volume_data_op_latency",
                           {"op", "read"}, HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(volume_data_write_latency, "volume data blocks write latency", "volume_data_op_latency",
                           {"op", "write"}, HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(volume_map_read_latency, "volume mapping read latency", "volume_map_op_latency",
                           {"op", "read"}, HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(volume_map_write_latency, "volume mapping write latency", "volume_map_op_latency",
                           {"op", "write"}, HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(volume_journal_write_latency, "volume journal write latency", "volume_journal_op_latency",
                           {"op", "write"}, HistogramBucketsType(OpLatecyBuckets));

        register_me_to_farm();
        attach_gather_cb(std::bind(&VolumeMetrics::on_gather, this));
    }

    VolumeMetrics(const VolumeMetrics&) = delete;
    VolumeMetrics(VolumeMetrics&&) noexcept = delete;
    VolumeMetrics& operator=(const VolumeMetrics&) = delete;
    VolumeMetrics& operator=(VolumeMetrics&&) noexcept = delete;
    ~VolumeMetrics() { deregister_me_from_farm(); }

    void on_gather();
};

// The CRAFT per-replica backend a memory-mode volume delegates its data plane to (defined in
// craft/craft_replica.hpp; only src/lib/craft needs the full type, so a forward decl here keeps
// volume.hpp free of that include). HomeStore-free.
class craft_replica;

class volume : public std::enable_shared_from_this< volume > {
public:
    inline static auto const VOL_META_NAME = std::string("Volume2"); // different from old releae;
private:
    static constexpr uint64_t VOL_SB_MAGIC = 0xc01fadeb; // different from old release;
    static constexpr uint64_t VOL_SB_VER = 0x3;          // bump one from old release
    static constexpr uint64_t VOL_NAME_SIZE = 100;
    static constexpr homestore::csum_t init_crc_16 = 0x8005;

    struct vol_sb_t {
        uint64_t magic;
        uint32_t version;
        uint32_t num_streams{0}; // number of streams in the volume; only used in HDD case;
        uint32_t page_size;
        uint64_t size; // privisioned size in bytes of volume;
        volume_id_t id;
        char name[VOL_NAME_SIZE];
        volume_state state{volume_state::INIT};
        uint64_t ordinal; // Id unique to local homeblk instance.
        uint32_t pdev_id; // All chunks for this volume allocated from this physical dev.
        uint32_t num_chunks;
        // List of chunk ids allocated for this volume are stored after this.

        void init(uint32_t page_sz, uint64_t sz_bytes, volume_id_t vid, std::string const& name_str, uint64_t ord,
                  uint32_t pdev, std::vector< homestore::chunk_num_t > const& chunk_ids) {
            magic = VOL_SB_MAGIC;
            version = VOL_SB_VER;
            page_size = page_sz;
            size = sz_bytes;
            id = vid;
            ordinal = ord;
            // name will be truncated if input name is longer than VOL_NAME_SIZE;
            std::strncpy((char*)name, name_str.c_str(), VOL_NAME_SIZE - 1);
            name[VOL_NAME_SIZE - 1] = '\0';

            // Store the pdev, num_chunks and chunk id's.
            pdev_id = pdev;
            num_chunks = chunk_ids.size();
            auto chunk_id_ptr = get_chunk_ids_mutable();
            for (auto& chunk_id : chunk_ids) {
                *chunk_id_ptr = chunk_id;
                chunk_id_ptr++;
            }
        }

        homestore::chunk_num_t* get_chunk_ids_mutable() {
            return reinterpret_cast< homestore::chunk_num_t* >(reinterpret_cast< uint8_t* >(this) + sizeof(vol_sb_t));
        }

        const homestore::chunk_num_t* get_chunk_ids() const {
            return reinterpret_cast< const homestore::chunk_num_t* >(reinterpret_cast< const uint8_t* >(this) +
                                                                     sizeof(vol_sb_t));
        }
    };

public:
    explicit volume(volume_info&& info, shared< VolumeChunkSelector > vol_chunk_sel,
                    shared< VolumeChunkSelector > index_chunk_sel) :
            sb_{VOL_META_NAME}, volume_chunk_selector_{vol_chunk_sel}, index_chunk_selector_{index_chunk_sel} {
        vol_info_ = std::make_shared< volume_info >(info.id, info.size_bytes, info.page_size, info.name, info.ordinal);
        metrics_ = std::make_unique< VolumeMetrics >(vol_info_->name);
    }
    explicit volume(sisl::byte_view const& buf, void* cookie, shared< VolumeChunkSelector > vol_chunk_sel,
                    shared< VolumeChunkSelector > index_chunk_sel);
    volume(volume const& volume) = delete;
    volume(volume&& volume) = default;
    volume& operator=(volume const& volume) = delete;
    volume& operator=(volume&& volume) = default;

    // Tag + constructor for the in-memory CRAFT reference model (src/lib/craft/memory). Builds a
    // HomeStore-free volume whose CRAFT data plane is served by `backend`; skips all HomeStore setup
    // (no repl_dev / index table / superblk write / chunk selectors / metrics). rd() and the index
    // paths must not be used on such a volume -- only the CRAFT free functions, which route to
    // craft_backend().
    struct mem_craft_tag {};
    explicit volume(volume_info&& info, shared< craft_replica > backend, mem_craft_tag) :
            sb_{VOL_META_NAME}, craft_backend_{std::move(backend)} {
        vol_info_ =
            std::make_shared< volume_info >(info.id, info.size_bytes, info.page_size, info.name, info.ordinal);
        m_state_ = volume_state::ONLINE;
    }

    virtual ~volume() = default;

    // static APIs exposed to HomeBlks Implementation Layer;
    static volume_handle make_volume(sisl::byte_view const& buf, void* cookie,
                                     shared< VolumeChunkSelector > volume_chunk_sel,
                                     shared< VolumeChunkSelector > index_chunk_sel) {
        auto vol = std::make_shared< volume >(buf, cookie, volume_chunk_sel, index_chunk_sel);
        auto ret = vol->init(true /*is_recovery*/);
        return ret ? vol : nullptr;
    }
    void get_stats(volume_stats& stats) const {
        stats.id = vol_info_->id;
        stats.state = sb_->state;
    }

    static volume_handle make_volume(volume_info&& info, shared< VolumeChunkSelector > volume_chunk_sel,
                                     shared< VolumeChunkSelector > index_chunk_sel) {
        auto vol = std::make_shared< volume >(std::move(info), volume_chunk_sel, index_chunk_sel);
        auto ret = vol->init(false /* is_recovery */);
        // in failure case, volume shared ptr will be destroyed automatically;
        return ret ? vol : nullptr;
    }

    VolIdxTablePtr indx_table() const { return indx_tbl_; }
    volume_id_t id() const { return vol_info_->id; };
    uint64_t ordinal() const { return vol_info_->ordinal; }
    std::string id_str() const { return boost::uuids::to_string(vol_info_->id); };
    const ReplDevPtr& rd() const { return rd_; }

    // The CRAFT per-replica backend for a memory-mode volume (null for HomeStore-backed volumes).
    // The CRAFT free functions in home_blocks.hpp route here.
    craft_replica* craft_backend() const { return craft_backend_.get(); }

    volume_info_ptr info() const { return vol_info_; }

    std::string to_string() { return vol_info_->to_string(); }

    //
    // Initialize index table for this volume and saves the index handle in the volume object;
    //
    VolIdxTablePtr init_index_table(bool is_recovery, VolIdxTablePtr tbl = nullptr);
    uint64_t get_index_size();

    bool is_online() const { return m_state_.load() == volume_state::ONLINE; }

    sisl::async::task< void > destroy();
    bool is_destroying() const { return m_state_.load() == volume_state::DESTROYING; }
    bool is_destroy_started() const { return destroy_started_.load(); }
    bool is_offline() const { return m_state_.load() == volume_state::OFFLINE; }

    //
    // This API will be called to set the volume state and persist to disk;
    //
    void state_change(volume_state s) {
        if (sb_->state != s) {
            sb_->state = s;
            sb_.write();
            m_state_ = s;
        }
    }

    async_status write(io_req& vol_req);

    async_status read(io_req& req);

    //
    // if destroy_started_ is true, it means volume destroy has started and we should not call remove again;
    // if outstanding_reqs_ is not zero, it means there are still requests outstanding and we should not call remove;
    // destroy_started_ will be set to true when volume destroy starts processing;
    //
    bool can_remove() const { return !destroy_started_ && outstanding_reqs_.test_eq(0); }

    void inc_ref(uint64_t n = 1) { outstanding_reqs_.increment(n); }
    void dec_ref(uint64_t n = 1) { outstanding_reqs_.decrement(n); }
    uint64_t num_outstanding_reqs() const { return outstanding_reqs_.get(); }
    void update_vol_sb_cb(const std::vector< chunk_num_t >& chunk_ids);

private:
    //
    // this API will be called to initialize volume in both volume creation and volume recovery;
    // it also creates repl dev underlying the volume which provides read/write apis to the volume;
    // init is synchronous and will return false in case of failure to create repl dev and volume instance will be
    // destroyed automatically; if success, the repl dev will be stored in the volume object;
    //
    bool init(bool is_recovery);

    status verify_checksum(vol_read_ctx const& read_ctx);

    void submit_read_to_backend(read_blks_list_t const& blks_to_read, const io_req& req,
                                std::vector< sisl::async::task< iomgr::io_result > >& futs,
                                std::vector< std::unique_ptr< sisl::sg_list > >& sgs_keepalive);

    void generate_blkids_to_read(const index_kv_list_t& index_kvs, read_blks_list_t& blks_to_read);

private:
    volume_info_ptr vol_info_; // volume info
    ReplDevPtr rd_;            // replication device for this volume, which provides read/write APIs to the volume;
    VolIdxTablePtr indx_tbl_;  // index table for this volume
    superblk< vol_sb_t > sb_;  // meta data of the volume
    shared< VolumeChunkSelector > volume_chunk_selector_; // volume chunk selector.
    shared< VolumeChunkSelector > index_chunk_selector_;  // index chunk selector.

    sisl::atomic_counter< uint64_t > outstanding_reqs_{0}; // number of outstanding requests
    std::atomic< bool > destroy_started_{
        false}; // indicates if volume destroy has started, avoid destroy to be executed more than once.
    std::atomic< volume_state > m_state_; // in-memory sb state, avoid taking lock in IO path;
    std::unique_ptr< VolumeMetrics > metrics_;
    shared< craft_replica > craft_backend_{nullptr}; // CRAFT per-replica backend (memory-mode volumes only)
};

// RAII: marks an IO in-flight on its volume for the op's duration -- so destroy()/shutdown() wait for it (and
// the volume stays alive). Replaces the refcount the old heap-allocated request carried; lives in the
// async_read/async_write coroutine frame.
struct vol_io_guard {
    volume_handle vol;
    explicit vol_io_guard(volume_handle v) : vol(std::move(v)) { vol->inc_ref(); }
    ~vol_io_guard() { vol->dec_ref(); }
    vol_io_guard(vol_io_guard const&) = delete;
    vol_io_guard& operator=(vol_io_guard const&) = delete;
};

struct vol_repl_ctx : public homestore::repl_req_ctx {
    sisl::io_blob_safe hdr_buf_;
    sisl::io_blob_safe key_buf_;

    vol_repl_ctx(uint32_t hdr_extn_size, uint32_t key_size = 0) : homestore::repl_req_ctx{} {
        hdr_buf_ = std::move(sisl::io_blob_safe{static_cast< uint32_t >(sizeof(MsgHeader) + hdr_extn_size), 0});
        new (hdr_buf_.bytes()) MsgHeader();

        if (key_size) { key_buf_ = std::move(sisl::io_blob_safe{key_size, 0}); }
    }

    ~vol_repl_ctx() {
        if (hdr_buf_.bytes()) { header()->~MsgHeader(); }
    }

    template < typename T >
    T* to() {
        return reinterpret_cast< T* >(this);
    }

    MsgHeader* header() { return reinterpret_cast< MsgHeader* >(hdr_buf_.bytes()); }
    uint8_t* header_extn() { return hdr_buf_.bytes() + sizeof(MsgHeader); }

    sisl::io_blob_safe& header_buf() { return hdr_buf_; }
    sisl::io_blob_safe const& cheader_buf() const { return hdr_buf_; }

    sisl::io_blob_safe& key_buf() { return key_buf_; }
    sisl::io_blob_safe const& ckey_buf() const { return key_buf_; }
};

template < typename T >
struct repl_result_ctx : public vol_repl_ctx {
    // Cross-thread single-shot completion for the journal write. The producer
    // (HomeBlocksImpl::on_write, on the commit thread) calls promise_.complete(value); the consumer
    // (volume::write coroutine) co_awaits promise_ directly. This ctx is heap-allocated and kept alive by an
    // intrusive_ptr through completion, satisfying value_awaitable's stable-address / outlive-the-await rule.
    sisl::async::value_awaitable< T > promise_;
    volume_handle vol_ptr_{nullptr};

    template < typename... Args >
    static intrusive< repl_result_ctx< T > > make(Args&&... args) {
        return intrusive< repl_result_ctx< T > >{new repl_result_ctx< T >(std::forward< Args >(args)...)};
    }

    repl_result_ctx(uint32_t hdr_extn_size, uint32_t key_size = 0) : vol_repl_ctx{hdr_extn_size, key_size} {}
};

} // namespace homeblocks
