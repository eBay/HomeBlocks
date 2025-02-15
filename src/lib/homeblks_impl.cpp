#include <algorithm>

#include <iomgr/io_environment.hpp>
#include <homestore/homestore.hpp>
#include <homestore/replication_service.hpp>
#include <sisl/options/options.h>
#include "homeblks_impl.hpp"
#include "listener.hpp"

SISL_OPTION_GROUP(homeblocks,
                  (executor_type, "", "executor", "Executor to use for Future deferal",
                   ::cxxopts::value< std::string >()->default_value("immediate"), "immediate|cpu|io"));

SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)

namespace homeblocks {

extern std::shared_ptr< HomeBlocks > init_homeobject(std::weak_ptr< HomeBlocksApplication >&& application) {
    LOGI("Initializing HomeBlocks");
    auto inst = std::make_shared< HomeBlocksImpl >(std::move(application));
    inst->init_homestore();
    inst->init_cp();
    return inst;
}

HomeBlocksStats HomeBlocksImpl::get_stats() const {
    HomeBlocksStats s;
    return s;
}

HomeBlocksImpl::HomeBlocksImpl(std::weak_ptr< HomeBlocksApplication >&& application) :
        _application(std::move(application)) {
    auto exe_type = SISL_OPTIONS["executor"].as< std::string >();
    std::transform(exe_type.begin(), exe_type.end(), exe_type.begin(), ::tolower);

    if ("immediate" == exe_type) [[likely]]
        executor_ = &folly::QueuedImmediateExecutor::instance();
    else if ("io" == exe_type)
        executor_ = folly::getGlobalIOExecutor();
    else if ("cpu" == exe_type)
        executor_ = folly::getGlobalCPUExecutor();
    else
        RELEASE_ASSERT(false, "Unknown Folly Executor type: [{}]", exe_type);
    LOGI("initialized with [executor={}]", exe_type);
}

DevType HomeBlocksImpl::get_device_type(std::string const& devname) {
    const iomgr::drive_type dtype = iomgr::DriveInterface::get_drive_type(devname);
    if (dtype == iomgr::drive_type::block_hdd || dtype == iomgr::drive_type::file_on_hdd) { return DevType::HDD; }
    if (dtype == iomgr::drive_type::file_on_nvme || dtype == iomgr::drive_type::block_nvme) { return DevType::NVME; }
    return DevType::UNSUPPORTED;
}

// repl application to init homestore
class HBReplApp : public homestore::ReplApplication {
public:
    HBReplApp(homestore::repl_impl_type impl_type, bool tl_consistency, HomeBlocksImpl* hb,
              std::weak_ptr< HomeBlocksApplication > ho_app) :
            impl_type_(impl_type), tl_consistency_(tl_consistency), hb_(hb), ho_app_(ho_app) {}

    // TODO: make this override after the base class in homestore adds a virtual destructor
    virtual ~HBReplApp() = default;

    // overrides
    homestore::repl_impl_type get_impl_type() const override { return impl_type_; }

    bool need_timeline_consistency() const override { return tl_consistency_; }

    // this will be called by homestore when create_repl_dev is called;
    std::shared_ptr< homestore::ReplDevListener > create_repl_dev_listener(homestore::group_id_t group_id) override {
        return std::make_shared< HBListener >(hb_);
#if 0
        std::scoped_lock lock_guard(_repl_sm_map_lock);
        auto [it, inserted] = _repl_sm_map.emplace(group_id, nullptr);
        if (inserted) { it->second = std::make_shared< ReplicationStateMachine >(hb_); }
        return it->second;
#endif
    }

    void on_repl_devs_init_completed() override { hb_->on_init_complete(); }

    std::pair< std::string, uint16_t > lookup_peer(homestore::replica_id_t uuid) const override {
        // r1: should never come here;
        RELEASE_ASSERT(false, "Unexpected to be called.");
        return std::make_pair("", 0);
    }

    homestore::replica_id_t get_my_repl_id() const override { return hb_->our_uuid(); }

private:
    homestore::repl_impl_type impl_type_;
    bool tl_consistency_; // indicates whether this application needs timeline consistency;
    HomeBlocksImpl* hb_;
    std::weak_ptr< HomeBlocksApplication > ho_app_;
#if 0
    std::map< homestore::group_id_t, std::shared_ptr< HBListener> > _repl_sm_map; 
    std::mutex _repl_sm_map_lock;
#endif
};

void HomeBlocksImpl::init_homestore() {
    auto app = _application.lock();
    RELEASE_ASSERT(app, "HomeObjectApplication lifetime unexpected!");

    LOGI("Starting iomgr with {} threads, spdk: {}", app->threads(), false);
    ioenvironment.with_iomgr(iomgr::iomgr_params{.num_threads = app->threads(), .is_spdk = app->spdk_mode()})
        .with_http_server();

    const uint64_t app_mem_size = app->app_mem_size();
    LOGI("Initialize and start HomeStore with app_mem_size = {}", app_mem_size);

    std::vector< homestore::dev_info > device_info;
    bool has_data_dev = false;
    bool has_fast_dev = false;
    for (auto const& dev : app->devices()) {
        // TODO: Simplify this logic;
        auto input_dev_type = dev.type;
        auto detected_type = get_device_type(dev.path.string());
        LOGD("Device {} detected as {}", dev.path.string(), detected_type);
        auto final_type = (dev.type == DevType::AUTO_DETECT) ? detected_type : input_dev_type;
        if (final_type == DevType::UNSUPPORTED) {
            LOGW("Device {} is not supported, skipping", dev.path.string());
            continue;
        }
        if (input_dev_type != DevType::AUTO_DETECT && detected_type != final_type) {
            LOGW("Device {} detected as {}, but input type is {}, using input type", dev.path.string(), detected_type,
                 input_dev_type);
        }
        auto hs_type = (final_type == DevType::HDD) ? homestore::HSDevType::Data : homestore::HSDevType::Fast;
        if (hs_type == homestore::HSDevType::Data) { has_data_dev = true; }
        if (hs_type == homestore::HSDevType::Fast) { has_fast_dev = true; }
        device_info.emplace_back(std::filesystem::canonical(dev.path).string(), hs_type);
    }

    RELEASE_ASSERT(device_info.size() != 0, "No supported devices found!");
    using namespace homestore;
    // Note: timeline_consistency doesn't matter as we are using solo repl dev;
    auto repl_app =
        std::make_shared< HBReplApp >(repl_impl_type::solo, false /*timeline_consistency*/, this, _application);
    bool need_format = HomeStore::instance()
                           ->with_index_service(std::make_unique< HBIndexSvcCB >(this))
                           .with_repl_data_service(repl_app) // chunk selector defaulted to round_robine
                           .start(hs_input_params{.devices = device_info, .app_mem_size = app_mem_size},
                                  [this]() { register_metablk_cb(); });

    if (need_format) {
        LOGI("We are starting for the first time. Formatting HomeStore. ");
        if (has_data_dev && has_fast_dev) {
            HomeStore::instance()->format_and_start({
                {HS_SERVICE::META, hs_format_params{.dev_type = HSDevType::Fast, .size_pct = 9.0, .num_chunks = 64}},
                {HS_SERVICE::LOG,
                 hs_format_params{.dev_type = HSDevType::Fast, .size_pct = 45.0, .chunk_size = 32 * Mi}},
                {HS_SERVICE::INDEX, hs_format_params{.dev_type = HSDevType::Fast, .size_pct = 45.0, .num_chunks = 128}},
                {HS_SERVICE::REPLICATION,
                 hs_format_params{.dev_type = HSDevType::Data,
                                  .size_pct = 95.0,
                                  .num_chunks = 0,
                                  .chunk_size = HS_CHUNK_SIZE,
                                  .block_size = DATA_BLK_SIZE}},
            });
        } else {
            auto run_on_type = has_fast_dev ? homestore::HSDevType::Fast : homestore::HSDevType::Data;
            LOGD("Running with Single mode, all service on {}", run_on_type);
            HomeStore::instance()->format_and_start({
                {HS_SERVICE::META, hs_format_params{.dev_type = run_on_type, .size_pct = 5.0, .num_chunks = 1}},
                {HS_SERVICE::LOG, hs_format_params{.dev_type = run_on_type, .size_pct = 10.0, .chunk_size = 32 * Mi}},
                {HS_SERVICE::INDEX, hs_format_params{.dev_type = run_on_type, .size_pct = 5.0, .num_chunks = 1}},
                {HS_SERVICE::REPLICATION,
                 hs_format_params{.dev_type = run_on_type,
                                  .size_pct = 75.0,
                                  .num_chunks = 0,
                                  .chunk_size = HS_CHUNK_SIZE,
                                  .block_size = DATA_BLK_SIZE}},
            });
        }
        repl_app->on_repl_devs_init_completed();
        superblk_init();
    }

    recovery_done_ = true;
    LOGI("Initialize and start HomeStore is successfully");
}

void HomeBlocksImpl::superblk_init() {
    auto sb = homestore::superblk< homeblks_sb_t >(HB_META_NAME);
    sb.create(sizeof(homeblks_sb_t));
    sb->magic = HB_SB_MAGIC;
    sb->version = HB_SB_VER;
    sb->boot_cnt = 0;
    sb->init_flag(0);
    sb.write();
}

void HomeBlocksImpl::register_metablk_cb() {
    // register some callbacks for metadata recovery;
    using namespace homestore;
    HomeStore::instance()->meta_service().register_handler(
        HB_META_NAME,
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            auto sb = homestore::superblk< homeblks_sb_t >(HB_META_NAME);
            sb.load(buf, mblk);
        },
        nullptr /*recovery_comp_cb*/, true /* do_crc */);
}

void HomeBlocksImpl::on_init_complete() {
    // TODO: register to meta service for HomeBlks meta block handler;
}

void HomeBlocksImpl::init_cp() {}
} // namespace homeblocks
