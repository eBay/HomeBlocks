
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
#include <map>
#include <string>
#include <sisl/logging/logging.h>
#include <sisl/utility/obj_life_counter.hpp>
#include <homestore/crc.h>
#include <homestore/homestore.hpp>
#include <homestore/index/index_table.hpp>
#include <homestore/superblk_handler.hpp>
#include <homeblks/home_blks.hpp>
#include <homeblks/volume_mgr.hpp>
#include <homeblks/common.hpp>
#include "volume/volume.hpp"

namespace homeblocks {

class Volume;

ENUM(HomeBlksMsgType, uint8_t, READ, WRITE, UNMAP);

struct VolJournalEntry {
    lba_t start_lba;
    lba_count_t nlbas;
    uint16_t num_old_blks;
};

class HomeBlocksImpl : public HomeBlocks, public VolumeManager, public std::enable_shared_from_this< HomeBlocksImpl > {
    struct homeblks_sb_t {
        uint64_t magic;
        uint32_t version;
        uint32_t flag;
        uint64_t boot_cnt;
        peer_id_t svc_id;

        void init_flag(uint32_t f) { flag = f; }
        void set_flag(uint32_t bit) { flag |= bit; }
        void clear_flag(uint32_t bit) { flag &= ~bit; }
        bool test_flag(uint32_t bit) { return flag & bit; }
    };

private:
    inline static auto const HB_META_NAME = std::string("HomeBlks2");
    static constexpr uint64_t HB_SB_MAGIC{0xCEEDDEEB};
    static constexpr uint32_t HB_SB_VER{0x1};
    static constexpr uint64_t HS_CHUNK_SIZE = 2 * Gi;
    static constexpr uint32_t DATA_BLK_SIZE = 4096;
    static constexpr uint32_t SB_FLAGS_GRACEFUL_SHUTDOWN{0x00000001};
    static constexpr uint32_t SB_FLAGS_RESTRICTED{0x00000002};

private:
    /// Our SvcId retrieval and SvcId->IP mapping
    std::weak_ptr< HomeBlocksApplication > _application;
    folly::Executor::KeepAlive<> executor_;

    ///
    mutable std::shared_mutex vol_lock_;
    std::map< volume_id_t, VolumePtr > vol_map_;

    // index table map which only used during recovery;
    mutable std::shared_mutex index_lock_;
    std::unordered_map< std::string, shared< VolumeIndexTable > > idx_tbl_map_;

    bool recovery_done_{false};
    superblk< homeblks_sb_t > sb_;
    peer_id_t our_uuid_;

public:
    explicit HomeBlocksImpl(std::weak_ptr< HomeBlocksApplication >&& application);

    ~HomeBlocksImpl() override;
    HomeBlocksImpl(const HomeBlocksImpl&) = delete;
    HomeBlocksImpl(HomeBlocksImpl&&) noexcept = delete;
    HomeBlocksImpl& operator=(const HomeBlocksImpl&) = delete;
    HomeBlocksImpl& operator=(HomeBlocksImpl&&) noexcept = delete;

    shared< VolumeManager > volume_manager() final;

    /// HomeBlocks
    /// Returns the UUID of this HomeBlocks.
    HomeBlocksStats get_stats() const final;
    iomgr::drive_type data_drive_type() const final;

    peer_id_t our_uuid() const final { return our_uuid_; }

    /// VolumeManager
    NullAsyncResult create_volume(VolumeInfo&& vol_info) final;

    NullAsyncResult remove_volume(const volume_id_t& id) final;

    VolumePtr lookup_volume(const volume_id_t& id) final;

    NullAsyncResult write(const VolumePtr& vol, const vol_interface_req_ptr& req) final;

    NullAsyncResult read(const VolumePtr& vol, const vol_interface_req_ptr& req) final;

    NullAsyncResult unmap(const VolumePtr& vol, const vol_interface_req_ptr& req) final;

    // Submit the io batch, which is a mandatory method to be called if read/write are issued
    // with part_of_batchis set to true.
    void submit_io_batch() final;

    // see api comments in base class;
    bool get_stats(volume_id_t id, VolumeStats& stats) const final;
    void get_volume_ids(std::vector< volume_id_t >& vol_ids) const final;

    // Index
    shared< VolumeIndexTable > recover_index_table(homestore::superblk< homestore::index_table_sb >&& sb);

    // HomeStore
    void init_homestore();
    void init_cp();

    uint64_t get_current_timestamp();

    void on_init_complete();

    void on_write(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                  const std::vector< homestore::MultiBlkId >& blkids, cintrusive< homestore::repl_req_ctx >& ctx);

    VolumeManager::Result< folly::Unit > verify_checksum(std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >const& index_kvs, 
                                                            uint8_t* buf, lba_t start_lba, uint32_t blk_size);

private:
    // Should only be called for first-time-boot
    void superblk_init();
    void register_metablk_cb();

    void get_dev_info(shared< HomeBlocksApplication > app, std::vector< homestore::dev_info >& device_info,
                      bool& has_data_dev, bool& has_fast_dev);

    DevType get_device_type(std::string const& devname);
    auto defer() const { return folly::makeSemiFuture().via(executor_); }

    // recovery apis
    void on_hb_meta_blk_found(sisl::byte_view const& buf, void* cookie);
    void on_vol_meta_blk_found(sisl::byte_view const& buf, void* cookie);

    VolumeManager::Result< folly::Unit > write_to_index(const VolumePtr& vol_ptr, lba_t start_lba, lba_t end_lba,
                                                        std::unordered_map< lba_t, BlockInfo >& blocks_info);
    VolumeManager::Result< folly::Unit > read_from_index(const VolumePtr& vol_ptr, const vol_interface_req_ptr& req,
                        std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >& out_vector);
};

class HBIndexSvcCB : public homestore::IndexServiceCallbacks {
public:
    HBIndexSvcCB(HomeBlocksImpl* hb) : hb_(hb) {}
    shared< homestore::IndexTableBase >
    on_index_table_found(homestore::superblk< homestore::index_table_sb >&& sb) override {
        LOGI("Recovered index table to index service");
        return hb_->recover_index_table(std::move(sb));
    }

private:
    HomeBlocksImpl* hb_;
};

const homestore::csum_t init_crc_16 = 0x8005;

struct HomeBlksMessageHeader {
    HomeBlksMessageHeader() = default;
    HomeBlksMsgType msg_type;
    volume_id_t volume_id;

    std::string to_string() const {
        return fmt::format(" msg_type={}volume={}\n", enum_name(msg_type), boost::uuids::to_string(volume_id));
    }
};

struct homeblks_repl_ctx : public homestore::repl_req_ctx {
    sisl::io_blob_safe hdr_buf_;
    sisl::io_blob_safe key_buf_;

    homeblks_repl_ctx(uint32_t hdr_extn_size, uint32_t key_size = 0) : homestore::repl_req_ctx{} {
        hdr_buf_ = std::move(sisl::io_blob_safe{uint32_cast(sizeof(HomeBlksMessageHeader) + hdr_extn_size), 0});
        new (hdr_buf_.bytes()) HomeBlksMessageHeader();

        if (key_size) { key_buf_ = std::move(sisl::io_blob_safe{key_size, 0}); }
    }

    ~homeblks_repl_ctx() {
        if (hdr_buf_.bytes()) { header()->~HomeBlksMessageHeader(); }
    }

    template < typename T >
    T* to() {
        return r_cast< T* >(this);
    }

    HomeBlksMessageHeader* header() { return r_cast< HomeBlksMessageHeader* >(hdr_buf_.bytes()); }
    uint8_t* header_extn() { return hdr_buf_.bytes() + sizeof(HomeBlksMessageHeader); }

    sisl::io_blob_safe& header_buf() { return hdr_buf_; }
    sisl::io_blob_safe const& cheader_buf() const { return hdr_buf_; }

    sisl::io_blob_safe& key_buf() { return key_buf_; }
    sisl::io_blob_safe const& ckey_buf() const { return key_buf_; }
};

template < typename T >
struct repl_result_ctx : public homeblks_repl_ctx {
    folly::Promise< T > promise_;
    VolumePtr vol_ptr_{nullptr};

    template < typename... Args >
    static intrusive< repl_result_ctx< T > > make(Args&&... args) {
        return intrusive< repl_result_ctx< T > >{new repl_result_ctx< T >(std::forward< Args >(args)...)};
    }

    repl_result_ctx(uint32_t hdr_extn_size, uint32_t key_size = 0) : homeblks_repl_ctx{hdr_extn_size, key_size} {}
    folly::SemiFuture< T > result() { return promise_.getSemiFuture(); }
};

} // namespace homeblocks
