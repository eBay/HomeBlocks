#include <string>

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <sisl/options/options.h>

#include <homeblocks/homeblocks.hpp>

SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTIONS_ENABLE(logging, homeblocks)

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, homeblocks);
    sisl::logging::SetLogger(std::string(argv[0]));
    sisl::logging::SetLogPattern("[%D %T%z] [%^%L%$] [%t] %v");
    parsed_argc = 1;
    auto f = ::folly::Init(&parsed_argc, &argv, true);
    return RUN_ALL_TESTS();
}
