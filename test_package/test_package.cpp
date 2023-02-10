#include <sisl/options/options.h>
#include <home_replication/repl_service.h>

SISL_LOGGING_INIT(HOMEREPL_LOG_MODS)

SISL_OPTIONS_ENABLE(logging)

int main(int argc, char** argv) {
    SISL_OPTIONS_LOAD(argc, argv, logging)
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    sisl::logging::SetModuleLogLevel("home_replication", spdlog::level::level_enum::trace);
    return 0;
}
