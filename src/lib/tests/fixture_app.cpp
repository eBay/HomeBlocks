#include <string>

#include <gtest/gtest.h>
#include <sisl/options/options.h>

#include "hb_internal.hpp"

SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    sisl::logging::SetLogPattern("[%D %T%z] [%^%L%$] [%t] %v");
    return RUN_ALL_TESTS();
}
