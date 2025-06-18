
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
#include "index.hpp"
#include "volume_chunk_selector.hpp"
#include "sisl/utility/enum.hpp"
#include "sisl/utility/atomic_counter.hpp"
#include <homeblks/volume_mgr.hpp>
#include <homestore/homestore.hpp>
#include <homestore/index/index_table.hpp>
#include <homestore/replication/repl_dev.h>
#include <homeblks/common.hpp>

namespace homeblocks {
class VolumeIndexKey;
class VolumeIndexValue;
using VolumeIndexTable = homestore::IndexTable< VolumeIndexKey, VolumeIndexValue >;
using VolIdxTablePtr = shared< VolumeIndexTable >;

using ReplDevPtr = shared< homestore::ReplDev >;
using index_cfg_t = homestore::BtreeConfig;

using index_kv_list_t = std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >;
using read_blks_list_t = std::vector< std::pair< lba_t, homestore::MultiBlkId > >;
struct vol_read_ctx {
    uint8_t* buf;
    lba_t start_lba;
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

ENUM(vol_state, uint32_t,
     INIT,       // initialized, but not ready online yet;
     ONLINE,     // online and ready to be used;
     OFFLINE,    // offline and not ready to be used;
     DESTROYING, // being destroyed, this state will be used for vol-destroy crash recovery;
     DESTROYED,  // fully destroyed, currently not used,
                 // for future use of lazy-destroy, e.g. set destroyed and move forward, let the volume be destroyed in
                 // background;
     READONLY    // in read only mode;
);

class Volume : public std::enable_shared_from_this< Volume > {
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
        vol_state state{vol_state::INIT};
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
            return r_cast< homestore::chunk_num_t* >(uintptr_cast(this) + sizeof(vol_sb_t));
        }

        const homestore::chunk_num_t* get_chunk_ids() const {
            return r_cast< const homestore::chunk_num_t* >(reinterpret_cast< const uint8_t* >(this) + sizeof(vol_sb_t));
        }
    };

public:
    explicit Volume(VolumeInfo&& info, shared< VolumeChunkSelector > chunk_sel) :
            sb_{VOL_META_NAME}, chunk_selector_{chunk_sel} {
        vol_info_ = std::make_shared< VolumeInfo >(info.id, info.size_bytes, info.page_size, info.name, info.ordinal);
    }
    explicit Volume(sisl::byte_view const& buf, void* cookie, shared< VolumeChunkSelector > chunk_sel);
    Volume(Volume const& volume) = delete;
    Volume(Volume&& volume) = default;
    Volume& operator=(Volume const& volume) = delete;
    Volume& operator=(Volume&& volume) = default;

    virtual ~Volume() = default;

    // static APIs exposed to HomeBlks Implementation Layer;
    static VolumePtr make_volume(sisl::byte_view const& buf, void* cookie, shared< VolumeChunkSelector > chunk_sel) {
        auto vol = std::make_shared< Volume >(buf, cookie, chunk_sel);
        auto ret = vol->init(true /*is_recovery*/);
        return ret ? vol : nullptr;
    }

    static VolumePtr make_volume(VolumeInfo&& info, shared< VolumeChunkSelector > chunk_sel) {
        auto vol = std::make_shared< Volume >(std::move(info), chunk_sel);
        auto ret = vol->init(false /* is_recovery */);
        // in failure case, volume shared ptr will be destroyed automatically;
        return ret ? vol : nullptr;
    }

    VolIdxTablePtr indx_table() const { return indx_tbl_; }
    volume_id_t id() const { return vol_info_->id; };
    uint64_t ordinal() const { return vol_info_->ordinal; }
    std::string id_str() const { return boost::uuids::to_string(vol_info_->id); };
    ReplDevPtr rd() const { return rd_; }

    VolumeInfoPtr info() const { return vol_info_; }

    std::string to_string() { return vol_info_->to_string(); }

    //
    // Initialize index table for this volume and saves the index handle in the volume object;
    //
    shared< VolumeIndexTable > init_index_table(bool is_recovery, shared< VolumeIndexTable > tbl = nullptr);

    void destroy();

    bool is_destroying() const { return m_state_.load() == vol_state::DESTROYING; }
    bool is_destroy_started() const { return destroy_started_.load(); }

    //
    // This API will be called to set the volume state and persist to disk;
    //
    void state_change(vol_state s) {
        if (sb_->state != s) {
            sb_->state = s;
            sb_.write();
            m_state_ = s;
        }
    }

    VolumeManager::NullAsyncResult write(const vol_interface_req_ptr& vol_req);

    VolumeManager::Result< folly::Unit > write_to_index(lba_t start_lba, lba_t end_lba,
                                                        std::unordered_map< lba_t, BlockInfo >& blocks_info);

    VolumeManager::NullAsyncResult read(const vol_interface_req_ptr& req);

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

    VolumeManager::Result< folly::Unit > verify_checksum(vol_read_ctx const& read_ctx);

    void submit_read_to_backend(read_blks_list_t const& blks_to_read, const vol_interface_req_ptr& req,
                                std::vector< folly::Future< std::error_code > >& futs);

    void generate_blkids_to_read(const index_kv_list_t& index_kvs, read_blks_list_t& blks_to_read);

    VolumeManager::Result< folly::Unit > read_from_index(const vol_interface_req_ptr& req, index_kv_list_t& index_kvs);

private:
    VolumeInfoPtr vol_info_;  // volume info
    ReplDevPtr rd_;           // replication device for this volume, which provides read/write APIs to the volume;
    VolIdxTablePtr indx_tbl_; // index table for this volume
    superblk< vol_sb_t > sb_; // meta data of the volume
    shared< VolumeChunkSelector > chunk_selector_; // volume chunk selector.

    sisl::atomic_counter< uint64_t > outstanding_reqs_{0}; // number of outstanding requests
    std::atomic< bool > destroy_started_{
        false}; // indicates if volume destroy has started, avoid destroy to be executed more than once.
    std::atomic< vol_state > m_state_; // in-memory sb state;
};

struct vol_repl_ctx : public homestore::repl_req_ctx {
    sisl::io_blob_safe hdr_buf_;
    sisl::io_blob_safe key_buf_;

    vol_repl_ctx(uint32_t hdr_extn_size, uint32_t key_size = 0) : homestore::repl_req_ctx{} {
        hdr_buf_ = std::move(sisl::io_blob_safe{uint32_cast(sizeof(MsgHeader) + hdr_extn_size), 0});
        new (hdr_buf_.bytes()) MsgHeader();

        if (key_size) { key_buf_ = std::move(sisl::io_blob_safe{key_size, 0}); }
    }

    ~vol_repl_ctx() {
        if (hdr_buf_.bytes()) { header()->~MsgHeader(); }
    }

    template < typename T >
    T* to() {
        return r_cast< T* >(this);
    }

    MsgHeader* header() { return r_cast< MsgHeader* >(hdr_buf_.bytes()); }
    uint8_t* header_extn() { return hdr_buf_.bytes() + sizeof(MsgHeader); }

    sisl::io_blob_safe& header_buf() { return hdr_buf_; }
    sisl::io_blob_safe const& cheader_buf() const { return hdr_buf_; }

    sisl::io_blob_safe& key_buf() { return key_buf_; }
    sisl::io_blob_safe const& ckey_buf() const { return key_buf_; }
};

template < typename T >
struct repl_result_ctx : public vol_repl_ctx {
    folly::Promise< T > promise_;
    VolumePtr vol_ptr_{nullptr};

    template < typename... Args >
    static intrusive< repl_result_ctx< T > > make(Args&&... args) {
        return intrusive< repl_result_ctx< T > >{new repl_result_ctx< T >(std::forward< Args >(args)...)};
    }

    repl_result_ctx(uint32_t hdr_extn_size, uint32_t key_size = 0) : vol_repl_ctx{hdr_extn_size, key_size} {}
    folly::SemiFuture< T > result() { return promise_.getSemiFuture(); }
};

} // namespace homeblocks
