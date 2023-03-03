///
// This test application validates the usability of the public interface home_replication exports along with
// providing a working demonstration for implementers with an S3-like engine.
//
// Brief:
//   - Startup initialzes homestore on a given device/file.
//   - Application will form a single raft group with peers explicitly listed as CLI parameters.
//   - REST service allows PUT/GET of simple objects to bucket-less endpoint (No-multipart, hierchy, iteration etc...)
///

#include <condition_variable>
#include <filesystem>
#include <mutex>

#include <boost/uuid/random_generator.hpp>
#include <iomgr/io_environment.hpp>
#include <homestore/homestore.hpp>
#include <nuraft_mesg/messaging.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <home_replication/repl_service.h>

SISL_LOGGING_INIT(HOMEREPL_LOG_MODS)

SISL_OPTION_GROUP(obj_store,
                  (tcp_port, "", "tcp_port", "TCP port to listen for incomming gRPC connections on",
                   cxxopts::value< uint32_t >()->default_value("22222"), "port"),
                  (num_threads, "", "num_threads", "number of threads",
                   ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
                  (num_devs, "", "num_devs", "number of devices to create",
                   ::cxxopts::value< uint32_t >()->default_value("2"), "number"),
                  (dev_size_mb, "", "dev_size_mb", "size of each device in MB",
                   ::cxxopts::value< uint64_t >()->default_value("1024"), "number"),
                  (device_list, "", "device_list", "Device List instead of default created",
                   ::cxxopts::value< std::vector< std::string > >(), "path [...]"));

SISL_OPTIONS_ENABLE(logging, obj_store)

static bool s_stop_signal;
static std::condition_variable s_stop_signal_condition;
static std::mutex s_stop_signal_condition_lck;

static const std::string s_fpath_root{"/tmp/example_obj_store"};

static void handle(int signal);
static void start_homestore(std::string const& svc_id);
static void stop_homestore(std::string const& svc_id);
static std::unique_ptr< home_replication::ReplicaSetListener > on_set_init(home_replication::rs_ptr_t const&);

int main(int argc, char** argv) {
    SISL_OPTIONS_LOAD(argc, argv, logging, obj_store);
    sisl::logging::SetLogger(std::string(argv[0]));
    sisl::logging::install_crash_handler();

    // Configure the gRPC service parameters for this instance (TCP port, svc UUID etc.)
    auto const listen_port = SISL_OPTIONS["tcp_port"].as< uint32_t >();
    if (UINT16_MAX < listen_port) {
        LOGCRITICAL("Invalid TCP port: {}", listen_port);
        exit(-1);
    }

    s_stop_signal = false;
    signal(SIGINT, handle);
    signal(SIGTERM, handle);

    // Start the Homestore service on some devices configured via the CLI parameters
    auto const svc_id = to_string(boost::uuids::random_generator()());
    LOGINFO("[{}] starting homestore service...", svc_id);
    start_homestore(svc_id);

    LOGINFO("[{}] starting messaging service...", svc_id);
    auto consensus_params = nuraft_mesg::consensus_component::params{
        svc_id, listen_port, [](std::string const& client) -> std::string { return client; }, "home_replication"};
    consensus_params.enable_data_service = true;

    // Start the NuRaft gRPC service.
    auto consensus_instance = std::make_shared< nuraft_mesg::service >();
    consensus_instance->start(consensus_params);

    LOGINFO("Initializing replication backend...");
    auto repl_svc = home_replication::ReplicationService(home_replication::backend_impl_t::homestore,
                                                         consensus_instance, &on_set_init);

    // Now we wait until we are asked to terminate
    {
        auto lk = std::unique_lock< std::mutex >(s_stop_signal_condition_lck);
        s_stop_signal_condition.wait(lk, [] { return s_stop_signal; });
    }

    LOGWARN("Shutting down!");
    stop_homestore(svc_id);
    LOGINFO("Exiting.");
    return 0;
}

void handle(int signal) {
    switch (signal) {
    case SIGINT:
        [[fallthrough]];
    case SIGTERM: {
        LOGWARN("SIGNAL: {}", strsignal(signal));
        {
            auto lk = std::lock_guard< std::mutex >(s_stop_signal_condition_lck);
            s_stop_signal = true;
        }
        s_stop_signal_condition.notify_all();
    } break;
        ;
    default:
        LOGERROR("Unhandled SIGNAL: {}", strsignal(signal));
        break;
    }
}

void start_homestore(std::string const& svc_id) {
    auto const ndevices = SISL_OPTIONS["num_devs"].as< uint32_t >();
    auto const dev_size = SISL_OPTIONS["dev_size_mb"].as< uint64_t >() * 1024 * 1024;
    auto nthreads = SISL_OPTIONS["num_threads"].as< uint32_t >();

    std::vector< homestore::dev_info > device_info;
    if (SISL_OPTIONS.count("device_list")) {
        /* if user customized file/disk names */
        auto dev_names = SISL_OPTIONS["device_list"].as< std::vector< std::string > >();
        std::string dev_list_str;
        for (const auto& d : dev_names) {
            dev_list_str += d;
        }
        LOGINFO("Taking input dev_list: {}", dev_list_str);

        for (uint32_t i{0}; i < dev_names.size(); ++i) {
            device_info.emplace_back(dev_names[i], homestore::HSDevType::Data);
        }
    } else {
        /* create files */
        LOGINFO("creating {} device files with each of size {} ", ndevices, homestore::in_bytes(dev_size));
        for (uint32_t i{0}; i < ndevices; ++i) {
            auto fpath{s_fpath_root + "_" + svc_id + "_" + std::to_string(i + 1)};
            LOGINFO("creating {} device file", fpath);
            if (std::filesystem::exists(fpath)) { std::filesystem::remove(fpath); }
            std::ofstream ofs{fpath, std::ios::binary | std::ios::out | std::ios::trunc};
            std::filesystem::resize_file(fpath, dev_size);
            device_info.emplace_back(std::filesystem::canonical(fpath).string(), homestore::HSDevType::Data);
        }
    }

    LOGINFO("Starting iomgr with {} threads, spdk: {}", nthreads, false);
    ioenvironment.with_iomgr(nthreads, false);

    const uint64_t app_mem_size = ((ndevices * dev_size) * 15) / 100;
    LOGINFO("Initialize and start HomeStore with app_mem_size = {}", homestore::in_bytes(app_mem_size));

    homestore::hs_input_params params;
    params.app_mem_size = app_mem_size;
    params.data_devices = device_info;
    homestore::HomeStore::instance()
        ->with_params(params)
        .with_meta_service(5.0)
        .with_log_service(80.0, 5.0)
        // .before_init_devices([this]() { })
        .init(true /* wait_for_init */);
}

void stop_homestore(std::string const& svc_id) {
    homestore::HomeStore::instance()->shutdown();
    homestore::HomeStore::reset_instance();
    iomanager.stop();
    auto const ndevices = SISL_OPTIONS["num_devs"].as< uint32_t >();
    for (uint32_t i{0}; i < ndevices; ++i) {
        auto fpath{s_fpath_root + "_" + svc_id + "_" + std::to_string(i + 1)};
        LOGINFO("removing {} device file", fpath);
        if (std::filesystem::exists(fpath)) { std::filesystem::remove(fpath); }
    }
}

class SetListener : public home_replication::ReplicaSetListener {
public:
    using home_replication::ReplicaSetListener::ReplicaSetListener;
    ~SetListener() override = default;

    void on_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                   const home_replication::pba_list_t& pbas, void* ctx) override {}

    void on_pre_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key, void* ctx) override {}

    void on_rollback(int64_t lsn, const sisl::blob& header, const sisl::blob& key, void* ctx) override {}

    void on_replica_stop() override {}
};

std::unique_ptr< home_replication::ReplicaSetListener > on_set_init(home_replication::rs_ptr_t const&) {
    return std::make_unique< SetListener >();
}
