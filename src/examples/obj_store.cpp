///
// This test application validates the usability of the public interface home_replication exports along with
// providing a working demonstration for implementers with an S3-like engine.
//
// Brief:
//   - Startup initialzes homestore on a given device/file.
//   - Application will form a single raft group with peers explicitly listed as CLI parameters.
//   - REST service allows PUT/GET of simple objects to bucket-less endpoint (No-multipart, hierchy, iteration etc...)
///

#include <sisl/options/options.h>
#include "home_replication.h"

SISL_LOGGING_INIT(HOMEREPL_LOG_MODS)

SISL_OPTIONS_ENABLE(logging)

int main(int argc, char** argv) {
   SISL_OPTIONS_LOAD(argc, argv, logging)
   sisl::logging::SetLogger(std::string(argv[0]));
   spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
   sisl::logging::SetModuleLogLevel("home_repl", spdlog::level::level_enum::info);
   return 0;
}
