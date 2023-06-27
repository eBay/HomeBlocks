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
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <iomgr/io_environment.hpp>
#include <iomgr/http_server.hpp>
#include <nuraft_mesg/messaging.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/version.hpp>

#include <home_replication/repl_service.hpp>

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

static auto ver_str() {
    auto vers{sisl::VersionMgr::getVersions()};
    auto ver_str = std::string();
    for (auto v : vers) {
        ver_str += fmt::format("{0}: {1}; ", v.first, v.second);
    }
    return ver_str;
}

static auto get_object([[maybe_unused]] auto& set, auto const request, auto response) {
    LOGINFO("Get Object: [{}]", request.resource());
    response.send(Pistache::Http::Code::Ok, ver_str());
    return Pistache::Rest::Route::Result::Ok;
}

static auto put_object([[maybe_unused]] auto& set, auto const request, auto response) {
    auto const& resource = request.resource();
    auto const sz = request.body().size();

    if (4096u >= sz) {
        LOGINFO("Put Object: [{}]:[{}]B", resource, sz);
        iomanager.run_on(
            iomgr::thread_regex::random_worker,
            [&set, &request, &resource](iomgr::io_thread_addr_t a) {
                auto iovs = sisl::sg_iovs_t();
                iovs.push_back(iovec{const_cast< char* >(request.body().data()), 4096});
                auto sg = sisl::sg_list{4096, iovs};
                auto blob_header = sisl::blob{reinterpret_cast< uint8_t* >(const_cast< char* >(resource.data())),
                                              static_cast< uint32_t >(resource.size())};
                auto blob_key = sisl::blob{reinterpret_cast< uint8_t* >(const_cast< char* >(resource.data())),
                                           static_cast< uint32_t >(resource.size())};
                set->write(blob_header, blob_key, sg, nullptr);
            },
            iomgr::wait_type_t::spin);
        response.send(Pistache::Http::Code::Ok);
    } else {
        LOGWARN("Put Object too big!: [{}]:[{}]", resource, sz);
        response.send(Pistache::Http::Code::Request_Entity_Too_Large);
    }
    return Pistache::Rest::Route::Result::Ok;
}

static auto delete_object([[maybe_unused]] auto& set, auto const request, auto response) {
    LOGINFO("Delete Object: [{}]", request.resource());
    response.send(Pistache::Http::Code::Ok, ver_str());
    return Pistache::Rest::Route::Result::Ok;
}

class Server : public home_replication::ReplicatedServer {
    ~Server() override = default;

    folly::SemiFuture< endpoint > member_address(uuid const&) override { return folly::makeSemiFuture(endpoint()); }
};

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
    // auto consensus_instance = std::make_shared< nuraft_mesg::service >();
    // consensus_instance->start(consensus_params);
    auto server = Server();

    LOGINFO("Initializing replication backend...");
    auto repl_svc = home_replication::create_repl_service(server);

    // Create a replication group
    auto repl_set = repl_svc->create_replica_set("ExampleObjStore:Group1");

    auto http_server = ioenvironment.with_http_server().get_http_server();
    http_server->setup_route(
        Pistache::Http::Method::Get, "/api/v1/objects/*",
        [&repl_set](const auto& request, auto response) { return get_object(repl_set, request, std::move(response)); });
    http_server->setup_route(
        Pistache::Http::Method::Put, "/api/v1/objects/*",
        [&repl_set](const auto& request, auto response) { return put_object(repl_set, request, std::move(response)); });
    http_server->setup_route(Pistache::Http::Method::Delete, "/api/v1/objects/*",
                             [&repl_set](const auto& request, auto response) {
                                 return delete_object(repl_set, request, std::move(response));
                             });

    // start the server
    http_server->start();

    // Now we wait until we are asked to terminate
    {
        auto lk = std::unique_lock< std::mutex >(s_stop_signal_condition_lck);
        s_stop_signal_condition.wait(lk, [] { return s_stop_signal; });
    }
    repl_set.reset();
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
