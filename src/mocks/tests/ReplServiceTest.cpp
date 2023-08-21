#include <chrono>
#include <string>

#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>
#include <folly/executors/QueuedImmediateExecutor.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <variant>

#include "home_replication/repl_service.h"

using namespace std::chrono_literals;
using home_replication::ReplServiceError;

SISL_LOGGING_INIT(logging, HOMEREPL_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)

class PgManagerFixture : public ::testing::Test {
public:
    void SetUp() override {
        m_mock_svc = home_replication::create_repl_service([](auto&) { return nullptr; });
    }

protected:
    std::shared_ptr< home_replication::ReplicationService > m_mock_svc;
};

TEST_F(PgManagerFixture, CreatePgEmpty) {
    auto v = m_mock_svc->create_replica_set("0", std::set< std::string, std::less<> >())
                 .via(&folly::QueuedImmediateExecutor::instance())
                 .get();
    ASSERT_TRUE(std::holds_alternative< ReplServiceError >(v));
    EXPECT_EQ(std::get< ReplServiceError >(v), ReplServiceError::BAD_REQUEST);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    return RUN_ALL_TESTS();
}
