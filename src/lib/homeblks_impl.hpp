
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
#include <atomic>
#include <future>
#include <map>
#include <set>
#include <string>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <boost/uuid/uuid_hash.hpp> // std::hash of volume ids
#include <sisl/fds/id_reserver.hpp>
#include <sisl/logging/logging.h>
#include <sisl/utility/obj_life_counter.hpp>
#include <homestore/crc.hpp>
#include <homestore/homestore.hpp>
#include <homestore/index/index_table.hpp>
#include <homestore/superblk_handler.hpp>
#include <homestore/fault_cmt_service.hpp>
#include "hb_internal.hpp"
#include "volume/volume.hpp"
#include "volume/volume_chunk_selector.hpp"

namespace homeblocks {

class volume;

struct hs_chunk_size_cfg_t {
    uint64_t index;
    uint64_t data;
};

// The meta blk record holding a CRAFT RAFT group's last snapshot as HomeStore serializes it: this header, then ctx_len
// bytes. One per group, removed with it.
#pragma pack(push, 1)
struct craft_snapshot_sb_t {
    static constexpr uint64_t k_magic = 0x50414E53'54465243ull; // "CRFTSNAP" little-endian
    static constexpr uint32_t k_version = 1;

    uint64_t magic{k_magic};
    uint32_t version{k_version};
    volume_id_t group_id{};
    int64_t lsn{-1}; // the last RAFT entry it covers
    uint32_t ctx_len{0};
};
#pragma pack(pop)
inline const std::string craft_snapshot_meta_name{"CraftSnapshot"};

// This node's metrics; each volume has its own (VolumeMetrics).
class HomeBlocksMetrics : public sisl::MetricsGroup {
public:
    HomeBlocksMetrics() : sisl::MetricsGroup("homeblocks", "node") {
        REGISTER_COUNTER(craft_snapshot_transfers_refused,
                         "RAFT snapshot transfers this node refused to send: CRAFT has no snapshots yet");
        REGISTER_GAUGE(craft_groups_without_replica,
                       "CRAFT groups this node is a member of without a replica here, until a partition move "
                       "replaces this member");
        REGISTER_COUNTER(craft_leaderships_handed_off,
                         "RAFT leaderships this node handed to another member, having no replica of the volume");
        REGISTER_COUNTER(craft_recoveries_failed,
                         "CRAFT partitions whose recovery at restart failed: faulted, they refuse I/O until replaced");
        register_me_to_farm();
    }
    ~HomeBlocksMetrics() { deregister_me_from_farm(); }
};

class HomeBlocksImpl : public home_blocks, public std::enable_shared_from_this< HomeBlocksImpl > {
    // The free data-plane functions need the system-level guards (restricted / shutting-down / fake-io delay).
    friend async_result< size_t > async_read(volume_handle const&, uint64_t, sisl::sg_list);
    friend async_result< size_t > async_write(volume_handle const&, uint64_t, sisl::sg_list);
    friend async_status async_unmap(volume_handle const&, uint64_t, uint64_t);

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
    /// bring-up configuration (devices, threads, app_mem_size, on_svc_id hook)
    home_blocks_config config_;

    /// volume management
    mutable std::shared_mutex vol_lock_;
    std::unordered_map< volume_id_t, volume_handle > vol_map_;
    std::unordered_set< volume_id_t > creating_vols_; // CRAFT creates in flight on this node (guarded by vol_lock_)

    // Index tables recovered and not yet installed in a volume: volume id -> {table, its ordinal}. A volume takes its
    // table at load; one left by a create that crashed stays, holding its ordinal, until that volume's next create
    // takes it.
    mutable std::shared_mutex index_lock_;
    std::unordered_map< std::string, std::pair< shared< VolumeIndexTable >, uint64_t > > idx_tbl_map_;

    // CRAFT durable state records read at boot, by volume id, until their volumes take them. Only on_init_complete()
    // uses it, reading the records and then the volumes on one thread.
    std::unordered_map< volume_id_t, superblk< craft_state_sb_t > > craft_state_sbs_;

    // CRAFT RAFT groups' last snapshots, by group: read at boot, kept up to date by their listeners.
    std::mutex craft_snapshot_mu_;
    std::unordered_map< volume_id_t, superblk< craft_snapshot_sb_t > > craft_snapshot_sbs_; // guarded by the same

    // CRAFT reattaches started and not done yet, and what their waiters await: completed when the count drops to 0.
    std::mutex craft_reattach_mu_;
    uint32_t craft_reattaching_{0};                                             // guarded by craft_reattach_mu_
    shared< sisl::async::shared_awaitable< bool > > craft_reattached_{nullptr}; // guarded by craft_reattach_mu_
    std::unordered_set< volume_id_t > craft_unreplayed_; // counted, journal not replayed yet; guarded by the same

    bool recovery_done_{false};
    bool gracefully_shutdown_{false};
    std::mutex sb_lock_; // this lock is only used when FC is triggered;
    superblk< homeblks_sb_t > sb_;
    peer_id_t our_uuid_;
    shared< VolumeChunkSelector > volume_chunk_selector_;
    shared< VolumeChunkSelector > index_chunk_selector_;
    std::unique_ptr< sisl::IDReserver > ordinal_reserver_;

    sisl::atomic_counter< uint64_t > outstanding_reqs_{0};
    // Removals accepted by remove_volume whose do_remove_volume has not finished yet; shutdown waits for them.
    std::atomic< uint32_t > pending_vol_removals_{0};
    std::atomic_flag shutdown_started_{};
    std::atomic< bool > is_restricted_{false}; // avoid taking lock in IO path;

    std::promise< void > shutdown_promise_;
    iomgr::timer_handle_t vol_gc_timer_hdl_{iomgr::null_timer_handle};
    iomgr::timer_handle_t shutdown_timer_hdl_{iomgr::null_timer_handle};
    iomgr::timer_handle_t craft_sweep_timer_hdl_{iomgr::null_timer_handle}; // CRAFT: expire_overdue_craft_waits()
    // CRAFT: when this member last handed off each group's leadership, while it leads it without a replica.
    std::mutex craft_handoff_mu_;
    std::unordered_map< volume_id_t, std::chrono::steady_clock::time_point > craft_handed_off_at_;

    HomeBlocksMetrics metrics_;

public:
    // static uint64_t _hs_chunk_size;
    static shared< HomeBlocksImpl > s_instance_;

public:
    explicit HomeBlocksImpl(home_blocks_config&& cfg);

    ~HomeBlocksImpl() override = default;
    HomeBlocksImpl(const HomeBlocksImpl&) = delete;
    HomeBlocksImpl(HomeBlocksImpl&&) noexcept = delete;
    HomeBlocksImpl& operator=(const HomeBlocksImpl&) = delete;
    HomeBlocksImpl& operator=(HomeBlocksImpl&&) noexcept = delete;

    /// home_blocks
    home_blocks_stats get_stats() const final;
    iomgr::drive_type data_drive_type() const final;

    peer_id_t our_uuid() const final { return our_uuid_; }

    uint64_t max_vol_io_size() const final { return MAX_VOL_IO_SIZE; }

    void shutdown() final;

    /// volume control plane
    [[nodiscard]] async_result< volume_handle > create_volume(volume_info info) final;

    [[nodiscard]] async_status remove_volume(const volume_id_t& id) final;

    [[nodiscard]] result< volume_handle > get_volume(const volume_id_t& id) const final;

    [[nodiscard]] result< volume_stats > get_stats(volume_id_t id) const final;
    std::vector< volume_id_t > volume_ids() const final;

    [[nodiscard]] async_status replace_member(volume_id_t const& id, peer_id_t const& out, peer_id_t const& in) final;

    // Index
    shared< hs_index_table_t > recover_index_table(homestore::superblk< homestore::index_table_sb >&& sb);

    // HomeStore
    void init_homestore();
    void init_cp();

    uint64_t get_current_timestamp();

    void on_init_complete();

    void on_write(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                  const std::vector< homestore::multi_blk_id >& blkids, cintrusive< homestore::repl_req_ctx >& ctx);

    // CRAFT: applies a committed CreateVolume entry on this member, creating its local replica of the volume. Runs on
    // the RAFT commit thread. `replaying`: the commit is a log replay at restart.
    void on_create_volume_commit(int64_t lsn, sisl::blob const& key, cintrusive< homestore::repl_req_ctx > const& ctx,
                                 bool replaying);
    // CRAFT: a volume's RAFT group was destroyed (rollback of its create, or the partition's removal): removes this
    // node's replica, if it has one. Called from HomeStore's reaper reactor: the teardown runs on a worker.
    void on_craft_group_destroyed(volume_id_t const& id);
    // CRAFT: a RAFT group's last snapshot, in its own record: nuraft takes one every snapshot_freq_distance entries and
    // compacts the log on it, and needs it back after a restart. Empty if the group has none.
    sisl::io_blob_safe craft_snapshot(volume_id_t const& group_id);
    void save_craft_snapshot(volume_id_t const& group_id, homestore::snapshot_context& ctx);
    void remove_craft_snapshot(volume_id_t const& group_id);
    // CRAFT: this member was elected leader of a volume's RAFT group. Without a replica here that serves, it would fail
    // every CRAFT op of the volume, so it hands leadership to another member. The hand-off runs on a worker, inline if
    // called on one: never with a lock held.
    void on_craft_group_leader(volume_id_t const& id);
    // CRAFT: the groups this node is a member of without a replica of their volume here (a local create that failed
    // leaves one), other than those of a create in flight here and those being destroyed.
    int64_t craft_groups_without_replica() const;
    // CRAFT: the checkpoint step. Persists every CRAFT partition's durable state, with the RAFT entries it has not
    // applied yet; stops the process if one can't be.
    sisl::async::task< void > save_craft_states();
#ifdef _PRERELEASE
    // Reads every CRAFT state record in the meta service again, as a boot does, and hands them over by volume.
    std::unordered_map< volume_id_t, superblk< craft_state_sb_t > > read_craft_state_records() {
        homestore::hs()->meta_service().read_sub_sb(craft_state_meta_name);
        return std::exchange(craft_state_sbs_, {});
    }
#endif
    // CRAFT: HomeStore asks for a new RAFT group's listener on the thread that instantiated the group.
    static void on_craft_group_made(volume_id_t const& id) {
        if (forming_ && forming_->id == id) { forming_->made = true; }
    }

    void start_reaper_thread();
    // CRAFT: a volume loaded at boot reopens its journal: its reattach is counted from now on, so a checkpoint, even
    // one a partition recovered earlier triggers, waits for the entries held for it.
    void expect_craft_reattach(volume_id_t const& id);
    // CRAFT: that journal replayed: the volume's device is made and recovered on a worker (reattach_craft_volume),
    // counted until published. HomeStore calls this from the replay, with the log device locked. `vol`: null if gone.
    void on_craft_journal_replayed(volume_id_t const& id, volume_handle vol,
                                   shared< homestore::home_log_store > journal);
    // CRAFT: one counted reattach is over.
    void end_craft_reattach();
    // CRAFT: once no reattach is pending. A checkpoint's flush waits for this, so it never covers entries a volume
    // holds for its device.
    sisl::async::task< void > craft_reattaches_done();
    // CRAFT, end of init: faults for good the partitions whose journal never replayed, then waits for every reattach.
    void finish_craft_reattach();
    // CRAFT, at shutdown: stops every CRAFT device (CraftReplDev::stop), keeping their queued entries.
    void stop_craft_devices();
    // CRAFT, at shutdown, before it waits for the requests in flight: ends every device's proposals
    // (CraftReplDev::stop_proposing()).
    void stop_craft_proposals();
    // CRAFT, every second from start_reaper_thread()'s timer: fails each device's proposals and journal writes past
    // their deadline (CraftReplDev::expire_overdue()), those of a volume being destroyed too.
    void expire_overdue_craft_waits();
    // CRAFT, on a worker: hands the group's RAFT leadership off if this member leads it without a replica here that
    // serves, unless it did within craft_leadership_handoff_backoff_ms (it keeps leadership; retry_craft_handoffs()
    // tries again).
    void hand_off_craft_leadership(volume_id_t const& id);
    // CRAFT, every second from the same timer: hands off again the groups whose backoff is over, and checks those whose
    // replica here doesn't serve: it may have stopped after the election.
    void retry_craft_handoffs();
    std::vector< volume_handle > craft_volumes() const;

    void fault_containment(const volume_handle vol, const std::string& reason = "");
    bool fc_on() const;
    void exit_fc(volume_handle& vol);
    bool is_restricted() const { return is_restricted_.load(); }
    bool is_shutting_down() const { return shutdown_started_.test(); }
    hs_chunk_size_cfg_t get_chunk_size() const;
    bool dynamic_chunk_allocation() const;
    bool is_graceful_shutdown() const { return gracefully_shutdown_; }
    bool has_pending_chunk_releases() const;
    home_blocks_config const& config() const { return config_; }
    HomeBlocksMetrics& metrics() { return metrics_; }
    // CRAFT process mode: every volume is a CRAFT partition replicated by its own RAFT group.
    bool is_craft() const { return config_.repl_mode == replication_mode::CRAFT; }

public:
    // public static APIs;
    static shared< HomeBlocksImpl > instance() { return s_instance_; }
    static void reset_instance() {
        if (s_instance_) {
            s_instance_.reset();
            s_instance_ = nullptr;
        }
    }

private:
    // Should only be called for first-time-boot
    void superblk_init();
    void register_metablk_cb();

    void get_dev_info(std::vector< homestore::dev_info >& device_info, bool& has_data_dev, bool& has_fast_dev);

    dev_type get_device_type(std::string const& devname);

    void update_vol_sb_cb(uint64_t volume_ordinal, const std::vector< chunk_num_t >& chunk_ids);

    // CRAFT: creates this member's replica of a volume a committed CreateVolume entry defines.
    status create_craft_volume_local(volume_info&& info);

    // CRAFT create_volume on the member that receives it. form_craft_volume blocks (group formation, then the wait
    // for CreateVolume), so it runs on a thread of its own, never on a reactor.
    async_result< volume_handle > create_craft_volume(volume_info info);
    status validate_craft_volume(volume_info const& info) const;
    result< volume_handle > form_craft_volume(volume_info info);
    void remove_craft_group(volume_id_t const& id);
    // The group this thread's create_repl_dev forms, and whether that call made it rather than was handed one a peer's
    // join installed: nuraft_mesg instantiates a group once, on the thread whose create or join won.
    struct craft_group_forming {
        volume_id_t id;
        bool made{false};
    };
    static inline thread_local craft_group_forming* forming_{nullptr};

    // recovery apis
    void on_hb_meta_blk_found(sisl::byte_view const& buf, void* cookie);
    void on_vol_meta_blk_found(sisl::byte_view const& buf, void* cookie);
    void on_craft_state_meta_blk_found(sisl::byte_view const& buf, void* cookie);
    void on_craft_snapshot_meta_blk_found(sisl::byte_view const& buf, void* cookie);

    void vol_gc();

    // Coroutine driving the actual volume teardown (co_awaits volume::destroy); launched via detach on a
    // worker reactor by remove_volume so the reactor yields during the destroy's CP flush instead of parking.
    sisl::async::task< void > do_remove_volume(volume_id_t id);
    // CRAFT: one volume's reattach (volume::reattach_craft_dev), counted in craft_reattaching_.
    sisl::async::task< void > reattach_craft_volume(volume_handle vol, shared< homestore::home_log_store > journal);
    // Starts a volume's teardown (do_remove_volume) on a worker. False if the system is restricted or shutting down.
    bool start_volume_removal(volume_id_t const& id);

    uint64_t gc_timer_nsecs() const;

    void inc_ref(uint64_t n = 1) { outstanding_reqs_.increment(n); }
    void dec_ref(uint64_t n = 1) { outstanding_reqs_.decrement(n); }
    bool can_shutdown() const;

    bool no_outstanding_vols() const;

    std::future< void > shutdown_start();
    void do_shutdown();
    uint64_t shutdown_timer_nsecs() const;

#ifdef _PRERELEASE
    // For testing purpose only
    // If delay flip is not set, false will be returned;
    // If delay flip is set, it will delay the IOs for a given volume_handle
    bool delay_fake_io(volume_handle vol);
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

class HBFCSvcCB : public homestore::FaultContainmentCallback {
public:
    HBFCSvcCB(HomeBlocksImpl* hb) : hb_(hb) {}

    void on_fault_containment(const homestore::FaultContainmentEvent event, void* cookie,
                              const std::string& reason) override {
        if (event == homestore::FaultContainmentEvent::ENTER_GLOBAL) {
            hb_->fault_containment(nullptr);
            LOGI("Global fault containment event received, reason: {}", reason);
            DEBUG_ASSERT(cookie == nullptr, "Global fault containment event should not have a cookie");
            return;
        }

        if (cookie == nullptr) {
            LOGW("Fault containment event received with null cookie, ignoring");
            return;
        }

        auto vol_id = static_cast< volume_id_t* >(cookie);
        auto vol = hb_->get_volume(*vol_id);
        if (!vol) {
            LOGW("Fault containment event for unknown volume {}, ignoring", boost::uuids::to_string(*vol_id));
            return;
        }
        if (event == homestore::FaultContainmentEvent::ENTER) {
            hb_->fault_containment(*vol, reason);
        } else if (event == homestore::FaultContainmentEvent::EXIT) {
            hb_->exit_fc(*vol);
        }
    }

private:
    HomeBlocksImpl* hb_;
};

} // namespace homeblocks
