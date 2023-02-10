///
// This test application validates the usability of the public interface home_replication exports along with
// providing a working demonstration for implementers with an S3-like engine.
//
// Brief:
//   - Startup initialzes homestore on a given device/file.
//   - Application will form a single raft group with peers explicitly listed as CLI parameters.
//   - REST service allows PUT/GET of simple objects to bucket-less endpoint (No-multipart, hierchy, iteration etc...)
///

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <home_replication/repl_service.h>

SISL_LOGGING_INIT(HOMEREPL_LOG_MODS)

SISL_OPTIONS_ENABLE(logging)

std::unique_ptr< home_replication::ReplicaSetListener > on_set_init(home_replication::rs_ptr_t const&) {
    return nullptr;
}

int main(int argc, char** argv) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");

    auto repl_svc = home_replication::ReplicationService(home_replication::backend_impl_t::jungle, &on_set_init);

    return 0;
}
