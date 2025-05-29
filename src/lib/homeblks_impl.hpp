
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
#include <sisl/fds/id_reserver.hpp>
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
#include "volume/volume_chunk_selector.hpp"

namespace homeblocks {

class Volume;

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
    static constexpr uint64_t MAX_VOL_IO_SIZE = 1 * Mi; // 1 MiB

private:
    /// Our SvcId retrieval and SvcId->IP mapping
    std::weak_ptr< HomeBlocksApplication > _application;
    folly::Executor::KeepAlive<> executor_;

    /// Volume management
    mutable std::shared_mutex vol_lock_;
    std::map< volume_id_t, VolumePtr > vol_map_;

    // index table map which only used during recovery;
    mutable std::shared_mutex index_lock_;
    std::unordered_map< std::string, shared< VolumeIndexTable > > idx_tbl_map_;

    bool recovery_done_{false};
    superblk< homeblks_sb_t > sb_;
    peer_id_t our_uuid_;
    shared< VolumeChunkSelector > chunk_selector_;
    std::unique_ptr< sisl::IDReserver > ordinal_reserver_;

public:
    static uint64_t _hs_chunk_size;

    sisl::atomic_counter< uint64_t > outstanding_reqs_{0};
    bool shutdown_started_{false};
    folly::Promise< folly::Unit > shutdown_promise_;
    iomgr::io_fiber_t reaper_fiber_;
    iomgr::timer_handle_t vol_gc_timer_hdl_{iomgr::null_timer_handle};
    iomgr::timer_handle_t shutdown_timer_hdl_{iomgr::null_timer_handle};

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

    uint64_t max_vol_io_size() const final { return MAX_VOL_IO_SIZE; }

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

    void start_reaper_thread();

private:
    // Should only be called for first-time-boot
    void superblk_init();
    void register_metablk_cb();

    void get_dev_info(shared< HomeBlocksApplication > app, std::vector< homestore::dev_info >& device_info,
                      bool& has_data_dev, bool& has_fast_dev);

    DevType get_device_type(std::string const& devname);
    auto defer() const { return folly::makeSemiFuture().via(executor_); }

    void update_vol_sb_cb(uint64_t volume_ordinal, const std::vector< chunk_num_t >& chunk_ids);

    // recovery apis
    void on_hb_meta_blk_found(sisl::byte_view const& buf, void* cookie);
    void on_vol_meta_blk_found(sisl::byte_view const& buf, void* cookie);

    void vol_gc();

    uint64_t gc_timer_nsecs() const;

    void inc_ref(uint64_t n = 1) { outstanding_reqs_.increment(n); }
    void dec_ref(uint64_t n = 1) { outstanding_reqs_.decrement(n); }
    bool is_shutting_down() const { return shutdown_started_; }
    bool can_shutdown() const;

    bool no_outstanding_vols() const;

    folly::Future< folly::Unit > shutdown_start();
    void do_shutdown();
    uint64_t shutdown_timer_nsecs() const;

#ifdef _PRERELEASE
    // For testing purpose only
    // If delay flip is not set, false will be returned;
    // If delay flip is set, it will delay the IOs for a given VolumePtr
    bool delay_fake_io(VolumePtr vol);
    bool crash_simulated_{false};
#endif
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

} // namespace homeblocks
