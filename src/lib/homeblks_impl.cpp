
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
#include <algorithm>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/nil_generator.hpp>
#include <thread>
#include <iomgr/io_environment.hpp>
#include <iomgr/drive.hpp>
#include <iomgr/iomgr_flip.hpp>
#include <homestore/homestore.hpp>
#include <homestore/checkpoint/cp_mgr.hpp>
#include <homestore/replication_service.hpp>
#include <homestore/index_service.hpp>
#include <sisl/options/options.h>
#include "homeblks_impl.hpp"
#include "listener.hpp"
#include "coro_helpers.hpp"
#include "home_blks_config.hpp"

SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)

namespace homeblocks {

result< shared< home_blocks > > init_homeblocks(home_blocks_config cfg) {
    LOGI("Initializing home_blocks with reaper thread timer: {} seconds", HB_DYNAMIC_CONFIG(reaper_thread_timer_secs));
    auto inst = std::make_shared< HomeBlocksImpl >(std::move(cfg));
    // TODO: surface operational init failures (on_svc_id RPC, device open, format) as result<> errors rather than
    // aborting inside init_homestore(); precondition bugs (e.g. no devices) stay asserts.
    inst->init_homestore();
    inst->finish_craft_reattach();
    inst->start_reaper_thread();
    HomeBlocksImpl::s_instance_ = inst;
    return inst;
}

iomgr::drive_type HomeBlocksImpl::data_drive_type() const {
    auto const dtype = homestore::hs()->data_service().get_dev_type();
    if (dtype == homestore::HSDevType::Data) {
        return iomgr::drive_type::block_hdd;
    } else if (dtype == homestore::HSDevType::Fast) {
        return iomgr::drive_type::block_nvme;
    } else {
        return iomgr::drive_type::unknown;
    }
}

home_blocks_stats HomeBlocksImpl::get_stats() const {
    auto const stats = homestore::hs()->repl_service().get_cap_stats();
    return {stats.total_capacity, stats.used_capacity};
}

std::future< void > HomeBlocksImpl::shutdown_start() {
    LOGI("Setting shutdown start flag");
    shutdown_started_.test_and_set();
    // A scrape no longer reads HomeStore, which goes down next; this waits out one in progress.
    metrics_.detach_gather_cb();

    // Pending chunk releases skip write-back cache eviction from now on; can_shutdown() waits for them.
    if (volume_chunk_selector_) { volume_chunk_selector_->start_shutdown(); }
    if (index_chunk_selector_) { index_chunk_selector_->start_shutdown(); }

    auto f = shutdown_promise_.get_future();

    auto const nsecs = shutdown_timer_nsecs();
    LOGI("Setting shutdown timer with {} seconds", nsecs);
    // start timer thread if there are still outstanding jobs;
    shutdown_timer_hdl_ = iomanager.schedule_global_timer(
        nsecs * 1000 * 1000 * 1000, true /* recurring */, nullptr /* cookie */, iomgr::reactor_regex::all_user,
        [this](void*) { this->do_shutdown(); }, true /* wait_to_schedule */);

    return f;
}

uint64_t HomeBlocksImpl::shutdown_timer_nsecs() const {
    if (SISL_OPTIONS.count("shutdown_timer_nsecs")) {
        auto const n = SISL_OPTIONS["shutdown_timer_nsecs"].as< uint32_t >();
        LOGINFO("Using shutdown_timer_nsecs option value: {}", n);
        return n;
    } else {
        return HB_DYNAMIC_CONFIG(shutdown_thread_timer_secs);
    }
}

bool HomeBlocksImpl::no_outstanding_vols() const {
    // 1. check if a volume removal is still running. A DESTROYING volume without one (its remove backed off and was
    // left to the reaper, which no longer runs removes once shutdown started) is not waited for: its DESTROYING state
    // is persisted and the destroy resumes on the next boot.
    if (auto const n = pending_vol_removals_.load(); n > 0) {
        LOGI("Found {} volume removals in progress", n);
        return false;
    }

    // check if there are no volumes with outstanding requests;
    std::shared_lock lg(vol_lock_);
    for (auto const& vol_pair : vol_map_) {
        auto const& vol = vol_pair.second;
        // 2. check if volume has outstanding requests;
        if (vol->num_outstanding_reqs() > 0) {
            LOGI("Found outstanding volume {} that has outstanding requests: {}", vol->id_str(),
                 vol->num_outstanding_reqs());
            return false; // found a volume with outstanding requests
        }
    }

    LOGI("No volumes with outstanding requests");
    return true; // no volumes with outstanding requests
}

bool HomeBlocksImpl::can_shutdown() const {
    // check if shutdown has started and no outstanding requests;
    // A destroyed volume may still be releasing its chunks: that work touches homestore and its allocator resets must
    // be persisted by homestore's final checkpoint, so homestore must not shut down before it is done.
    if (is_shutting_down() && no_outstanding_vols() && outstanding_reqs_.test_eq(0) && !has_pending_chunk_releases()) {
        LOGI("Shutdown can proceed, outstanding requests: {}", outstanding_reqs_.get());
        return true;
    }

    LOGI("Shutdown cannot proceed, outstanding requests: {}", outstanding_reqs_.get());
    return false;
}

bool HomeBlocksImpl::has_pending_chunk_releases() const {
    if ((volume_chunk_selector_ && volume_chunk_selector_->has_pending_releases()) ||
        (index_chunk_selector_ && index_chunk_selector_->has_pending_releases())) {
        LOGI("Chunk releases of destroyed volumes are still pending");
        return true;
    }
    return false;
}

void HomeBlocksImpl::do_shutdown() {
    LOGI("Shutdown timer triggered, checking for outstanding requests");
    if (can_shutdown()) {
        LOGI("No outstanding requests, proceeding with shutdown");

        shutdown_promise_.set_value();
    } else {
        LOGI("Outstanding requests exist, will retry shutdown in {} seconds", shutdown_timer_nsecs());
    }
}

void HomeBlocksImpl::shutdown() {
    LOGI("Shutting down HomeBlocksImpl Received");
    DEBUG_ASSERT(!is_shutting_down(), "Shutdown already started, cannot destruct HomeBlocksImpl again");
    // set the shutdown flag so that no new requests are accepted;
    // start timer thread if there are still outstanding jobs;
    auto f = shutdown_start();
    // After the flag, which refuses new CRAFT ops: a request in flight may be waiting on a proposal that only a stop
    // would end (no deadline, no quorum), and the wait below waits for it.
    stop_craft_proposals();

    f.get();

    // stop the timer thread
    if (shutdown_timer_hdl_ != iomgr::null_timer_handle) {
        iomanager.cancel_timer(shutdown_timer_hdl_);
        shutdown_timer_hdl_ = iomgr::null_timer_handle;
    }

    // The sweep runs on while the devices stop: a stop waits out the apply in progress, which a journal write that
    // never completes would hold forever without its deadline.
    stop_craft_devices();
    if (craft_sweep_timer_hdl_ != iomgr::null_timer_handle) {
        iomanager.cancel_timer(craft_sweep_timer_hdl_, true /* wait: a sweep in progress finishes */);
        craft_sweep_timer_hdl_ = iomgr::null_timer_handle;
    }

    // set the shutdown flag so that no new requests are accepted;
    sb_->set_flag(SB_FLAGS_GRACEFUL_SHUTDOWN);
    sb_.write();

    homestore::hs()->shutdown();
    homestore::home_store::reset_instance();
    iomanager.stop();

    reset_instance();
}

HomeBlocksImpl::HomeBlocksImpl(home_blocks_config&& cfg) : config_(std::move(cfg)), sb_{HB_META_NAME} {
    ordinal_reserver_ = std::make_unique< sisl::IDReserver >(MAX_NUM_VOLUMES);
}

dev_type HomeBlocksImpl::get_device_type(std::string const& devname) {
    const iomgr::drive_type dtype = iomgr::type_of(devname);
    if (dtype == iomgr::drive_type::block_hdd || dtype == iomgr::drive_type::file_on_hdd) { return dev_type::HDD; }
    if (dtype == iomgr::drive_type::file_on_nvme || dtype == iomgr::drive_type::block_nvme) { return dev_type::NVME; }
    return dev_type::UNSUPPORTED;
}

// repl application to init homestore
class HBReplApp : public homestore::repl_application {
public:
    HBReplApp(homestore::repl_impl_type impl_type, bool tl_consistency, HomeBlocksImpl* hb) :
            impl_type_(impl_type), tl_consistency_(tl_consistency), hb_(hb) {}

    // TODO: make this override after the base class in homestore adds a virtual destructor
    virtual ~HBReplApp() = default;

    // overrides
    homestore::repl_impl_type get_impl_type() const override { return impl_type_; }

    bool need_timeline_consistency() const override { return tl_consistency_; }

    // this will be called by homestore when create_repl_dev is called;
    std::shared_ptr< homestore::repl_dev_listener > create_repl_dev_listener(homestore::group_id_t group_id) override {
        if (hb_->is_craft()) {
            // HomeStore loads a recovered group's repl dev before asking for its listener, and creates a new group's
            // (on create or join) only after.
            bool const recovered = homestore::hs()->repl_service().get_repl_dev(group_id).has_value();
            if (!recovered) { HomeBlocksImpl::on_craft_group_made(group_id); }
            return std::make_shared< CraftGroupListener >(hb_, group_id, recovered);
        }
        return std::make_shared< HBListener >(hb_);
#if 0
        std::scoped_lock lock_guard(_repl_sm_map_lock);
        auto [it, inserted] = _repl_sm_map.emplace(group_id, nullptr);
        if (inserted) { it->second = std::make_shared< ReplicationStateMachine >(hb_); }
        return it->second;
#endif
    }

    void on_repl_devs_init_completed() override { hb_->on_init_complete(); }

    // Only the RAFT (CRAFT) service looks peers up; init_homestore() checks the hook is set in that mode.
    std::pair< std::string, uint16_t > lookup_peer(homestore::replica_id_t uuid) const override {
        return hb_->config().lookup_peer(uuid);
    }

    homestore::replica_id_t get_my_repl_id() const override { return hb_->our_uuid(); }

    uint32_t get_my_repl_svc_port() const override { return hb_->config().repl_svc_port; }

    void destroy_repl_dev_listener(homestore::group_id_t gid) override {
        LOGI("Destroying repl dev listener for group_id {}", boost::uuids::to_string(gid));
    }

private:
    homestore::repl_impl_type impl_type_;
    bool tl_consistency_; // indicates whether this application needs timeline consistency;
    HomeBlocksImpl* hb_;
#if 0
    std::map< homestore::group_id_t, std::shared_ptr< HBListener> > _repl_sm_map;
    std::mutex _repl_sm_map_lock;
#endif
};

void HomeBlocksImpl::get_dev_info(std::vector< homestore::dev_info >& dev_info, bool& has_data_dev,
                                  bool& has_fast_dev) {
    for (auto const& dev : config_.devices) {
        auto input_dev_type = dev.type;
        auto detected_type = get_device_type(dev.path.string());
        LOGD("Device {} detected as {}", dev.path.string(), detected_type);
        auto final_type = (dev.type == dev_type::AUTO_DETECT) ? detected_type : input_dev_type;
        if (final_type == dev_type::UNSUPPORTED) {
            LOGW("Device {} is not supported, skipping", dev.path.string());
            continue;
        }
        if (input_dev_type != dev_type::AUTO_DETECT && detected_type != final_type) {
            LOGW("Device {} detected as {}, but input type is {}, using input type", dev.path.string(), detected_type,
                 input_dev_type);
        }
        auto hs_type = (final_type == dev_type::HDD) ? homestore::HSDevType::Data : homestore::HSDevType::Fast;
        if (hs_type == homestore::HSDevType::Data) { has_data_dev = true; }
        if (hs_type == homestore::HSDevType::Fast) { has_fast_dev = true; }
        dev_info.emplace_back(std::filesystem::canonical(dev.path).string(), hs_type);
    }
}

// uint64_t HomeBlocksImpl::_hs_chunk_size = HS_CHUNK_SIZE;

hs_chunk_size_cfg_t HomeBlocksImpl::get_chunk_size() const {
    hs_chunk_size_cfg_t hs_chunk_sz;
    uint32_t n = 0;
    if (SISL_OPTIONS.count("index_chunk_size_mb")) {
        n = SISL_OPTIONS["index_chunk_size_mb"].as< uint32_t >();
        LOGI("Using index_chunk_size_mb option value: {}", n);
        hs_chunk_sz.index = n * 1024 * 1024;
    } else {
        n = HB_DYNAMIC_CONFIG(hs_index_chunk_size_mb);
        LOGI("Using dynamic config value in MB: {}", n);
        hs_chunk_sz.index = n * Mi;
    }

    if (SISL_OPTIONS.count("data_chunk_size_mb")) {
        n = SISL_OPTIONS["data_chunk_size_mb"].as< uint32_t >();
        LOGI("Using data_chunk_size_mb option value: {}", n);
        hs_chunk_sz.data = n * 1024 * 1024;
    } else {
        n = HB_DYNAMIC_CONFIG(hs_data_chunk_size_mb);
        LOGI("Using dynamic config value in MB: {}", n);
        hs_chunk_sz.data = n * Mi;
    }

    LOGI("Using index_chunk_size_mb: {} MB, data_chunk_size_mb: {} MB", hs_chunk_sz.index / (1024 * 1024),
         hs_chunk_sz.data / (1024 * 1024));
    return hs_chunk_sz;
}

bool HomeBlocksImpl::dynamic_chunk_allocation() const {
    if (SISL_OPTIONS.count("dynamic_chunk_allocation")) {
        auto const on = SISL_OPTIONS["dynamic_chunk_allocation"].as< bool >();
        LOGI("Using dynamic_chunk_allocation option value: {}", on);
        return on;
    }

    return HB_DYNAMIC_CONFIG(dynamic_chunk_allocation);
}

void HomeBlocksImpl::init_homestore() {
    LOGI("Starting iomgr with {} threads", config_.threads);
    ioenvironment.with_iomgr(iomgr::iomgr_params{.num_threads = config_.threads}).with_http_server();

    const uint64_t app_mem_size = config_.app_mem_size_mb * 1024 * 1024;
    LOGI("Initialize and start HomeStore with app_mem_size = {}", app_mem_size);

    std::vector< homestore::dev_info > device_info;
    bool has_data_dev{false}, has_fast_dev{false};
    get_dev_info(device_info, has_data_dev, has_fast_dev);

    RELEASE_ASSERT(device_info.size() != 0, "No supported devices found!");
    RELEASE_ASSERT(!is_craft() || (config_.lookup_peer && config_.repl_svc_port != 0),
                   "CRAFT mode needs repl_svc_port and lookup_peer");

    volume_chunk_selector_ = std::make_shared< VolumeChunkSelector >(
        "volume", [this](uint64_t volume_ordinal, const std::vector< chunk_num_t >& chunk_ids) {
            update_vol_sb_cb(volume_ordinal, chunk_ids);
        });
    LOGI("Initialize index chunk selector");
    index_chunk_selector_ = std::make_shared< VolumeChunkSelector >(
        "index", [this](uint64_t volume_ordinal, const std::vector< chunk_num_t >& chunk_ids) {
            // Todo: decide it later whether needed for index
        });
    // Only index chunks hold btree nodes in the index write-back cache; data chunk blkids can never be in it.
    index_chunk_selector_->set_evict_chunk_fn([](const auto& chunk) {
        homestore::hs()->index_service().wb_cache().evict_chunk_blkids(*chunk->get_internal_chunk());
    });

    using namespace homestore;
    // CRAFT runs HomeStore's RAFT service (one group per volume); legacy volumes keep the solo repl dev. The type is
    // process-wide in HomeStore. Timeline consistency is not supported by either.
    auto repl_app = std::make_shared< HBReplApp >(is_craft() ? repl_impl_type::server_side : repl_impl_type::solo,
                                                  false /*timeline_consistency*/, this);
    bool need_format = false;
    if (fc_on()) {
        need_format = homestore::hs()
                          ->with_index_service(std::make_unique< HBIndexSvcCB >(this),
                                               index_chunk_selector_) // custom index chunk selector
                          .with_fault_containment(std::make_unique< HBFCSvcCB >(this))
                          .with_repl_data_service(repl_app, volume_chunk_selector_) // custom volume chunk selector
                          .start(hs_input_params{.devices = device_info, .app_mem_size = app_mem_size},
                                 [this]() { register_metablk_cb(); });
    } else {
        need_format = homestore::hs()
                          ->with_index_service(std::make_unique< HBIndexSvcCB >(this),
                                               index_chunk_selector_)               // custom index chunk selector
                          .with_repl_data_service(repl_app, volume_chunk_selector_) // custom volume chunk selector
                          .start(hs_input_params{.devices = device_info, .app_mem_size = app_mem_size},
                                 [this]() { register_metablk_cb(); });
    }

    auto const hs_chunk_sz = get_chunk_size();
    if (need_format) {
        // Cold boot: ask the consumer's hook for our svc id (e.g. a gRPC to the OM), driven off-reactor here on
        // the init thread. Empty hook -> generate one.
        if (config_.on_svc_id) {
            auto ret = detail::sync_get(config_.on_svc_id());
            RELEASE_ASSERT(ret.has_value(), "on_svc_id hook failed to provide a svc id");
            our_uuid_ = ret.value();
        } else {
            our_uuid_ = boost::uuids::random_generator()();
        }
        LOGINFO("We are starting for the first time on svc_id: [{}]. Formatting HomeStore. ",
                boost::uuids::to_string(our_uuid()));
        if (has_data_dev && has_fast_dev) {
            // NOTE: chunk_size, num_chunks only has to specify one, can be deduced from each other.
            homestore::hs()->format_and_start({
                {HS_SERVICE::META, hs_format_params{.dev_type = HSDevType::Fast, .size_pct = 9.0}},
                {HS_SERVICE::LOG,
                 hs_format_params{
                     .dev_type = HSDevType::Fast, .size_pct = 45.0, .num_chunks = 0, .chunk_size = 32 * Mi}},
                {HS_SERVICE::INDEX,
                 hs_format_params{.dev_type = HSDevType::Fast,
                                  .size_pct = 45.0,
                                  .num_chunks = 0,
                                  .chunk_size = hs_chunk_sz.index,
                                  .chunk_sel_type = chunk_selector_type_t::CUSTOM}},
                {HS_SERVICE::REPLICATION,
                 hs_format_params{.dev_type = HSDevType::Data,
                                  .size_pct = 95.0,
                                  .num_chunks = 0, // num_chunks will be deduced from chunk_size
                                  .chunk_size = hs_chunk_sz.data,
                                  .block_size = DATA_BLK_SIZE,
                                  .chunk_sel_type = chunk_selector_type_t::CUSTOM}},
            });
        } else {
            auto run_on_type = has_fast_dev ? homestore::HSDevType::Fast : homestore::HSDevType::Data;
            LOGD("Running with Single mode, all service on {}", run_on_type);
            homestore::hs()->format_and_start({
                {HS_SERVICE::META, hs_format_params{.dev_type = run_on_type, .size_pct = 5.0}},
                {HS_SERVICE::LOG,
                 hs_format_params{.dev_type = run_on_type, .size_pct = 10.0, .num_chunks = 0, .chunk_size = 32 * Mi}},
                {HS_SERVICE::INDEX,
                 hs_format_params{.dev_type = run_on_type,
                                  .size_pct = 5.0,
                                  .num_chunks = 0,
                                  .chunk_size = hs_chunk_sz.index,
                                  .chunk_sel_type = chunk_selector_type_t::CUSTOM}},
                {HS_SERVICE::REPLICATION,
                 hs_format_params{.dev_type = run_on_type,
                                  .size_pct = 75.0,
                                  .num_chunks = 0, // num_chunks will be deduced from chunk_size;
                                  .chunk_size = hs_chunk_sz.data,
                                  .block_size = DATA_BLK_SIZE,
                                  .chunk_sel_type = chunk_selector_type_t::CUSTOM}},
            });
        }
        // repl_app->on_repl_devs_init_completed();
        superblk_init();
    } else {
        // we are starting on an existing system;
        DEBUG_ASSERT(our_uuid() != boost::uuids::nil_uuid(), "UUID should be recovered from HB superblock!");
        // Reboot: svc id recovered from the superblock; on_svc_id is cold-boot-only, so nothing to call here. A
        // consumer that needs to re-announce on restart reads home_blocks::our_uuid() after init.
        LOGINFO("We are starting on [{}].", boost::uuids::to_string(our_uuid_));
    }

    recovery_done_ = true;
    // Counted at each scrape from HomeStore's groups and this node's replicas, now that both are recovered; until
    // shutdown starts.
    if (is_craft()) {
        metrics_.attach_gather_cb(
            [this] { GAUGE_UPDATE(metrics_, craft_groups_without_replica, craft_groups_without_replica()); });
    }
    LOGI("Initialize and start HomeStore is successfully");
}

void HomeBlocksImpl::superblk_init() {
    sb_.create(sizeof(homeblks_sb_t));
    sb_->magic = HB_SB_MAGIC;
    sb_->version = HB_SB_VER;
    sb_->boot_cnt = 0;
    sb_->init_flag(0);
    sb_->svc_id = our_uuid_;
    sb_.write();
}

void HomeBlocksImpl::on_hb_meta_blk_found(sisl::byte_view const& buf, void* cookie) {
    sb_.load(buf, static_cast< homestore::meta_blk* >(cookie));
    // sb verification
    RELEASE_ASSERT_EQ(sb_->version, HB_SB_VER);
    RELEASE_ASSERT_EQ(sb_->magic, HB_SB_MAGIC);

    if (sb_->test_flag(SB_FLAGS_GRACEFUL_SHUTDOWN)) {
        // if it is a gracefuln shutdown, this flag should be set again in shutdown routine;
        sb_->clear_flag(SB_FLAGS_GRACEFUL_SHUTDOWN);
        gracefully_shutdown_ = true;
        LOGI("System was shutdown gracefully");
    } else if (sb_->test_flag(SB_FLAGS_RESTRICTED)) {
        is_restricted_.store(true);
        LOGI("System is booted into restricted mode, reason: {}", sb_->flag);
    } else {
        LOGI("System experienced sudden crash since last boot");
    }

    ++sb_->boot_cnt;
    our_uuid_ = sb_->svc_id;
    LOGI("HomeBlks superblock loaded, boot_cnt: {}, svc_id: {}", sb_->boot_cnt, boost::uuids::to_string(our_uuid_));

    // avoid doing sb meta blk write in callback which will cause deadlock;
    // the 1st CP should flush all dirty SB before taking traffic;
}

void HomeBlocksImpl::register_metablk_cb() {
    // register some callbacks for metadata recovery;
    using namespace homestore;

    // HomeBlks SB
    homestore::hs()->meta_service().register_handler(
        HB_META_NAME,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_hb_meta_blk_found(std::move(buf), reinterpret_cast< void* >(mblk));
        },
        nullptr /*recovery_comp_cb*/, true /* do_crc */);

    // CRAFT snapshot records: before HomeStore recovers the RAFT groups, whose listeners nuraft asks for their last
    // snapshot during that recovery.
    homestore::hs()->meta_service().register_handler(
        craft_snapshot::snapshot_record_meta_name,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_craft_snapshot_meta_blk_found(std::move(buf), reinterpret_cast< void* >(mblk));
        },
        nullptr /*recovery_comp_cb*/, true /* do_crc */);
    homestore::hs()->meta_service().register_handler(
        craft_snapshot::snapshot_receive_meta_name,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_craft_snapshot_receive_meta_blk_found(std::move(buf), reinterpret_cast< void* >(mblk));
        },
        nullptr /*recovery_comp_cb*/, true /* do_crc */);
}

void HomeBlocksImpl::on_init_complete() {
    // this is called after HomeStore all recovery completed.
    // Add anything that needs to be done here.
    using namespace homestore;

    // While HomeStore starts: the checkpoint that ends its start waits, through this consumer, for the CRAFT partitions
    // to be reattached, so it can't cover the entries they hold meanwhile.
    init_cp();

    // CRAFT state records, ahead of the volumes that take them
    homestore::hs()->meta_service().register_handler(
        craft_state_meta_name,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_craft_state_meta_blk_found(std::move(buf), reinterpret_cast< void* >(mblk));
        },
        nullptr /*recovery_comp_cb*/, true /* do_crc */);
    homestore::hs()->meta_service().read_sub_sb(craft_state_meta_name);

    // volume SB
    homestore::hs()->meta_service().register_handler(
        volume::VOL_META_NAME,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            on_vol_meta_blk_found(std::move(buf), reinterpret_cast< void* >(mblk));
        },
        nullptr /*recovery_comp_cb*/, true /* do_crc */,
        std::optional< meta_subtype_vec_t >({homestore::hs()->repl_service().get_meta_blk_name()}));

    homestore::hs()->meta_service().read_sub_sb(volume::VOL_META_NAME);

    // A volume removes its record before its superblock, so none should be left; one that is, stays on disk.
    for (auto const& [id, sb] : craft_state_sbs_) {
        LOGW("CRAFT state record of volume {} has no volume here: left in place", boost::uuids::to_string(id));
    }
    craft_state_sbs_.clear();

    // Every index (recovered before repl devs) and volume has claimed its chunks by now, so chunks left in the free
    // pools are unowned. Clear the ones a crash during volume destroy left with allocated blks, before they are reused.
    // No CP flushes the data allocators concurrently: the data service registers for CPs only after this. The CP the
    // index service forces after its recovery can still be flushing index allocators, which is harmless: a free chunk's
    // allocator was only loaded from disk and has nothing dirty to flush.
    volume_chunk_selector_->reset_dirty_free_chunks();
    index_chunk_selector_->reset_dirty_free_chunks();
}

namespace {
// HomeStore records a checkpoint (checkpoint_lsn) in the replication service's flush, after this consumer's
// (HS_CLIENT), and replays the RAFT log after a restart only above it. Every RAFT entry a checkpoint covers has been
// committed to its CRAFT partition (HomeStore calls on_commit before advancing the LSN the checkpoint takes), not
// always applied: this flush saves each partition's durable state with its entries not yet applied, which recovery
// applies again.
class CraftCheckpointConsumer : public homestore::CPCallbacks {
public:
    explicit CraftCheckpointConsumer(HomeBlocksImpl* hb) : hb_{hb} {}

    std::unique_ptr< homestore::CPContext > on_switchover_cp(homestore::CP*, homestore::CP* new_cp) override {
        return std::make_unique< homestore::CPContext >(new_cp); // non-null: flushed with every checkpoint
    }
    sisl::async::task< bool > cp_flush(homestore::CP*) override {
        co_await hb_->save_craft_states();
        co_return true;
    }
    void cp_cleanup(homestore::CP*) override {}
    int cp_progress_percent() override { return 100; }

private:
    HomeBlocksImpl* hb_;
};
} // namespace

void HomeBlocksImpl::init_cp() {
    if (is_craft()) {
        homestore::cp_mgr().register_consumer(homestore::cp_consumer_t::HS_CLIENT,
                                              std::make_unique< CraftCheckpointConsumer >(this));
    }
}

// ponytail: one meta blk write per CRAFT volume per checkpoint, in turn (the meta service serializes them anyway); skip
// the unchanged ones if checkpoints get slow with many partitions.
sisl::async::task< void > HomeBlocksImpl::save_craft_states() {
    // A volume being reattached holds the entries committed meanwhile, which this checkpoint may cover: they reach its
    // device, whose state is saved below with them, first.
    co_await craft_reattaches_done();
    for (auto const& vol : craft_volumes()) {
        // Counted in flight while saved: its destroy, which removes the state record, waits for it or is seen here.
        vol_io_guard const guard{vol};
        if (vol->is_destroying()) { continue; }
        auto const dev = vol->craft_dev_ref();
        if (!dev) { continue; } // no device yet: nothing to save
        auto const saved = co_await dev->save_state();
        // The replication service records the checkpoint whatever this flush returns: stop rather than let it cover
        // entries whose state isn't durable.
        RELEASE_ASSERT(saved, "CRAFT volume {}: saving its durable state for a checkpoint failed: {}", vol->id_str(),
                       saved.error().message());
    }
}

uint64_t HomeBlocksImpl::gc_timer_nsecs() const {
    if (SISL_OPTIONS.count("gc_timer_nsecs")) {
        auto const n = SISL_OPTIONS["gc_timer_nsecs"].as< uint32_t >();
        LOGINFO("Using gc_timer_nsecs option value: {}", n);
        return n;
    } else {
        return HB_DYNAMIC_CONFIG(reaper_thread_timer_secs);
    }
}

void HomeBlocksImpl::expect_craft_reattach(volume_id_t const& id) {
    std::lock_guard lk{craft_reattach_mu_};
    if (craft_reattaching_++ == 0) craft_reattached_ = std::make_shared< sisl::async::shared_awaitable< bool > >();
    craft_unreplayed_.insert(id);
}

void HomeBlocksImpl::on_craft_journal_replayed(volume_id_t const& id, volume_handle vol,
                                               shared< homestore::home_log_store > journal) {
    {
        std::lock_guard lk{craft_reattach_mu_};
        if (craft_unreplayed_.erase(id) == 0) return; // given up already (finish_craft_reattach)
    }
    if (!vol) {
        end_craft_reattach();
        return;
    }
    // A worker, not the replay's thread: HomeStore holds the log device locked, and its start goes on meanwhile.
    iomanager.run_on_forget(iomgr::reactor_regex::random_worker, [this, vol = std::move(vol), journal]() mutable {
        detail::detach(reattach_craft_volume(std::move(vol), std::move(journal)));
    });
}

sisl::async::task< void > HomeBlocksImpl::reattach_craft_volume(volume_handle vol,
                                                                shared< homestore::home_log_store > journal) {
#ifdef _PRERELEASE
    // Holds the reattach for the flip's milliseconds, for a test to commit entries meanwhile, and resumes from a
    // message on a worker, as it started. A thread of its own waits them out, not an iomgr delay flip: each file's
    // first use of iomgr's flips replaces their timer, leaving one still pending to fire on a destroyed object.
    if (auto const ms = iomgr_flip::instance()->get_test_flip< long >("craft_reattach_delay_ms")) {
        auto held = std::make_shared< sisl::async::value_awaitable< bool > >();
        std::thread([held, ms = *ms]() {
            std::this_thread::sleep_for(std::chrono::milliseconds{ms});
            iomanager.run_on_forget(iomgr::reactor_regex::random_worker, [held]() { held->complete(true); });
        }).detach();
        co_await *held;
    }
#endif
    if (auto const r = co_await vol->reattach_craft_dev(std::move(journal), our_uuid()); !r) {
        COUNTER_INCREMENT(metrics_, craft_recoveries_failed, 1);
    }
    end_craft_reattach();
}

void HomeBlocksImpl::end_craft_reattach() {
    shared< sisl::async::shared_awaitable< bool > > done;
    {
        std::lock_guard lk{craft_reattach_mu_};
        if (--craft_reattaching_ == 0) done = std::exchange(craft_reattached_, nullptr);
    }
    if (done) done->complete(true); // outside the lock: resumes the waiters
}

sisl::async::task< void > HomeBlocksImpl::craft_reattaches_done() {
    shared< sisl::async::shared_awaitable< bool > > reattached;
    {
        std::lock_guard lk{craft_reattach_mu_};
        reattached = craft_reattached_;
    }
    if (reattached) co_await *reattached;
}

std::vector< volume_handle > HomeBlocksImpl::craft_volumes() const {
    std::vector< volume_handle > vols;
    auto lg = std::shared_lock(vol_lock_);
    for (auto const& [id, vol] : vol_map_) {
        if (vol->is_craft()) { vols.push_back(vol); }
    }
    return vols;
}

void HomeBlocksImpl::expire_overdue_craft_waits() {
    for (auto const& vol : craft_volumes()) {
        if (auto const dev = vol->craft_dev_ref()) { dev->expire_overdue(); }
    }
}

void HomeBlocksImpl::stop_craft_proposals() {
    for (auto const& vol : craft_volumes()) {
        if (auto const dev = vol->craft_dev_ref()) { dev->stop_proposing(); }
    }
}

// Before HomeStore shuts down: nothing a CRAFT device runs on its own may touch HomeStore after it. The entries still
// queued stay queued, for HomeStore's last checkpoint to save with each partition's state.
void HomeBlocksImpl::stop_craft_devices() {
    for (auto const& vol : craft_volumes()) {
        if (auto const dev = vol->craft_dev_ref()) { (void)detail::sync_get(dev->stop(true /* keep_queued */)); }
    }
}

// HomeStore has started: every journal reopened at load that replays has replayed by now. One that hasn't won't (its
// log store is missing), and the entries held for it can't be saved: its partition is faulted for good, its record
// marked before the count lets a checkpoint cover them, so a later boot doesn't bring it back from that record.
void HomeBlocksImpl::finish_craft_reattach() {
    std::unordered_set< volume_id_t > unreplayed;
    {
        std::lock_guard lk{craft_reattach_mu_};
        unreplayed.swap(craft_unreplayed_);
    }
    for (auto const& id : unreplayed) {
        if (auto const vol = get_volume(id)) {
            COUNTER_INCREMENT(metrics_, craft_recoveries_failed, 1);
            (*vol)->fault_unreplayed_craft();
        }
        end_craft_reattach();
    }
    detail::sync_get(craft_reattaches_done());
}

void HomeBlocksImpl::start_reaper_thread() {
    auto const nsecs = gc_timer_nsecs();
    LOGI("Starting volume garbage collection timer with interval: {} seconds", nsecs);
    vol_gc_timer_hdl_ = iomanager.schedule_global_timer(
        nsecs * 1000 * 1000 * 1000, true /* recurring */, nullptr /* cookie */, iomgr::reactor_regex::all_user,
        [this](void*) { this->vol_gc(); }, true /* wait_to_schedule */);
    if (is_craft()) {
        // One timer for every partition, armed off the reactors: a recurring timer per proposer is armed and canceled
        // on every worker, a blocking round that hangs when two workers do it at once.
        craft_sweep_timer_hdl_ = iomanager.schedule_global_timer(
            1'000'000'000 /* 1s: a proposal times out up to this late */, true /* recurring */, nullptr /* cookie */,
            iomgr::reactor_regex::all_user,
            [this](void*) {
                expire_overdue_craft_waits();
                retry_craft_handoffs();
            },
            true /* wait_to_schedule */);
    }
}

void HomeBlocksImpl::vol_gc() {
    LOGI("Running volume garbage collection");
    // loop through every volume and call remove volume if volume's ref_cnt is zero;
    std::vector< volume_handle > vols_to_remove;
    {
        auto lg = std::shared_lock(vol_lock_);
        for (auto& vol_pair : vol_map_) {
            auto& vol = vol_pair.second;
            LOGI("Checking volume with id: {}, is_destroying: {}, can_remove: {}, num_outstanding_reqs: {}",
                 vol->id_str(), vol->is_destroying(), vol->can_remove(), vol->num_outstanding_reqs());

            if (vol->is_destroying() && vol->can_remove()) {
                // 1. volume has been issued with removed command before
                // 2. no one has already started removing it
                // 3. volume is not in use anymore (ref_cnt == 0)
                vols_to_remove.push_back(vol);
            }
        }
    }

    for (auto& vol : vols_to_remove) {
        LOGI("Garbage Collecting removed volume with id: {}", vol->id_str());
        start_volume_removal(vol->id());
    }
}

bool HomeBlocksImpl::fc_on() const {
#ifdef _PRERELEASE
    // for prerelease mode, fault containment is disabled;
    return false;
#endif
    // for release mode, Fault containment is enabled by default, but can be disabled via dynamic config;
    return HB_DYNAMIC_CONFIG(fault_containment_on);
}

void HomeBlocksImpl::exit_fc(volume_handle& vol) { vol->state_change(volume_state::ONLINE); }

void HomeBlocksImpl::fault_containment(const volume_handle vol, const std::string& reason) {
    if (vol == nullptr) {
        // Put entire HB into offline;
        std::scoped_lock lg(sb_lock_);
        sb_->set_flag(SB_FLAGS_RESTRICTED);
        is_restricted_.store(true);
        sb_.write();
        LOGE("Putting HB into restricted mode, reason: {}", reason);
        return;
    }

    LOGI("volume {} is in fault containment due to: {}", vol->id_str(), reason);
    // if volume is in fault containment, we should not allow any new requests to be issued on it;
    vol->state_change(volume_state::OFFLINE);
}

shared< HomeBlocksImpl > HomeBlocksImpl::s_instance_ = nullptr;
} // namespace homeblocks
