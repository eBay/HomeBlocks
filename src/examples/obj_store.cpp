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
#include <iomgr/http_server.hpp>
#include <nuraft_mesg/messaging.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/version.hpp>

#include "home_replication/repl_service.h"

///
// From example_lib.cpp
void start_homestore(std::string const& svc_id);
void stop_homestore(std::string const& svc_id);
///

SISL_LOGGING_INIT(HOMEREPL_LOG_MODS)

SISL_OPTION_GROUP(obj_store,
                  (tcp_port, "", "tcp_port", "TCP port to listen for incomming gRPC connections on",
                   cxxopts::value< uint32_t >()->default_value("22222"), "port"));

SISL_OPTIONS_ENABLE(logging, obj_store, example_lib)

///
// Clean shutdown
static bool s_stop_signal;
static std::condition_variable s_stop_signal_condition;
static std::mutex s_stop_signal_condition_lck;
static void handle(int signal);
///

static std::unique_ptr< home_replication::ReplicaSetListener > on_set_init(home_replication::rs_ptr_t const&);

static auto read_object([[maybe_unused]] auto& svc, auto const request, auto response) {
    LOGINFO("Read Object: [{}]", request.resource());
    auto vers{sisl::VersionMgr::getVersions()};
    std::string ver_str{""};
    for (auto v : vers) {
        ver_str += fmt::format("{0}: {1}; ", v.first, v.second);
    }
    response.send(Pistache::Http::Code::Ok, ver_str);
    return Pistache::Rest::Route::Result::Ok;
}

static auto write_object([[maybe_unused]] auto& svc, auto const request, [[maybe_unused]] auto response) {
    LOGINFO("Read Object: [{}]", request.resource());
    return Pistache::Rest::Route::Result::Ok;
}

int main(int argc, char** argv) {
    SISL_OPTIONS_LOAD(argc, argv, logging, obj_store, example_lib);
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

    auto http_server = ioenvironment.with_http_server().get_http_server();
    http_server->setup_route(Pistache::Http::Method::Get, "/api/v1/objects/*",
                             [&repl_svc](const auto& request, auto response) {
                                 return read_object(repl_svc, request, std::move(response));
                             });
    http_server->setup_route(Pistache::Http::Method::Put, "/api/v1/objects/*",
                             [&repl_svc](const auto& request, auto response) {
                                 return write_object(repl_svc, request, std::move(response));
                             });

    // start the server
    http_server->start();

    // Now we wait until we are asked to terminate
    {
        auto lk = std::unique_lock< std::mutex >(s_stop_signal_condition_lck);
        s_stop_signal_condition.wait(lk, [] { return s_stop_signal; });
    }
    http_server->stop();

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
