#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <random>
#include "state_machine/rpc_data_channel.h"
#include "mock_storage_engine.h"
#include <boost/uuid/random_generator.hpp>

SISL_LOGGING_INIT(HOMEREPL_LOG_MODS)
SISL_OPTIONS_ENABLE(logging);
using namespace home_replication;
using namespace testing;

static uint64_t get_random_num() {
    static std::random_device dev;
    static std::mt19937 rng(dev());
    std::uniform_int_distribution< std::mt19937::result_type > dist(std::numeric_limits< std::uint64_t >::min(),
                                                                    std::numeric_limits< std::uint64_t >::max());
    return dist(rng);
}

TEST(RpcDataChannel, SerializeAndDeserialize) {
    using npbas_type = uint16_t;
    uint16_t test_max_pbas =
        (data_channel_rpc_hdr::max_hdr_size - sizeof(data_channel_rpc_hdr) - sizeof(pbas_serialized::n_pbas)) /
        sizeof(pbas_serialized::_pba_info);
    EXPECT_EQ(data_rpc::max_pbas(), test_max_pbas);

    pba_list_t pbas = {get_random_num(), get_random_num(), get_random_num(), get_random_num()};
    sisl::sg_list sgl{0, {}};
    std::vector< uint64_t > data_vec;
    for (uint16_t i = 0; i < pbas.size(); ++i) {
        data_vec.emplace_back(get_random_num());
        sgl.iovs.emplace_back(iovec{new uint64_t(data_vec[i]), sizeof(uint64_t)});
        sgl.size += sizeof(uint64_t);
    }

    MockStorageEngine mock_engine;
    EXPECT_CALL(mock_engine, pba_to_size(_)).Times(pbas.size()).WillRepeatedly([](pba_t const&) {
        return sizeof(uint64_t);
    });

    data_channel_rpc_hdr hdr = {boost::uuids::random_generator()(), 1};
    auto ioblob_list = data_rpc::serialize(hdr, pbas, &mock_engine, sgl);
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_home_local_journal");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%t] %v");
    return RUN_ALL_TESTS();
}