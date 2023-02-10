///
// This test application validates the usability of the public interface home_replication exports along with
// providing a working demonstration for implementers with an S3-like engine.
//
// Brief:
//   - Startup initialzes homestore on a given device/file.
//   - Application will form a single raft group with peers explicitly listed as CLI parameters.
//   - REST service allows PUT/GET of simple objects to bucket-less endpoint (No-multipart, hierchy, iteration etc...)
///

#include <boost/uuid/random_generator.hpp>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <home_replication/repl_service.h>
#include <nuraft_mesg/messaging.hpp>

SISL_LOGGING_INIT(HOMEREPL_LOG_MODS)

SISL_OPTION_GROUP(obj_store,
                  (tcp_port, "", "tcp_port", "TCP port to listen for incomming gRPC connections on",
                   cxxopts::value< uint32_t >()->default_value("22222"), "port"))

SISL_OPTIONS_ENABLE(logging, obj_store)

std::unique_ptr< home_replication::ReplicaSetListener > on_set_init(home_replication::rs_ptr_t const&) {
    return nullptr;
}

int main(int argc, char** argv) {
    SISL_OPTIONS_LOAD(argc, argv, logging, obj_store);
    sisl::logging::SetLogger(std::string(argv[0]));

    // Configure the gRPC service parameters for this instance (TCP port, svc UUID etc.)
    auto const svc_id = to_string(boost::uuids::random_generator()());
    LOGINFO("[{}] starting messaging service...", svc_id);

    auto const listen_port = SISL_OPTIONS["tcp_port"].as< uint32_t >();
    if (UINT16_MAX < listen_port) {
        LOGCRITICAL("Invalid TCP port: {}", listen_port);
        exit(-1);
    }

    auto consensus_params = nuraft_mesg::consensus_component::params{
        svc_id, listen_port, [](std::string const& client) -> std::string { return client; }, "home_replication"};
    consensus_params.enable_data_service = true;

    auto consensus_instance = std::make_shared< nuraft_mesg::service >();
    consensus_instance->start(consensus_params);

    LOGINFO("Initializing replication backend...");
    auto repl_svc = home_replication::ReplicationService(home_replication::backend_impl_t::jungle, consensus_instance,
                                                         &on_set_init);

    LOGINFO("Exiting.");
    return 0;
}
