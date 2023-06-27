#include "mock_storage_engine.h"
#include "test_common.h"

SISL_LOGGING_INIT(HOMEREPL_LOG_MODS)
SISL_OPTIONS_ENABLE(logging);
using namespace home_replication;
using namespace testing;

TEST(RpcDataChannel, SerializeAndDeserialize) {
    using npbas_type = uint16_t;
    uint16_t test_max_pbas =
        (data_channel_rpc_hdr::max_hdr_size - sizeof(data_channel_rpc_hdr) - sizeof(pbas_serialized::n_pbas)) /
        sizeof(pbas_serialized::_pba_info);
    EXPECT_EQ(data_rpc::max_pbas(), test_max_pbas);

    // create a list of 4 random pbas and a data vector of 4 random values which represent the data the pbas hold
    pba_list_t pbas;
    sisl::sg_list sgl;
    std::vector< uint64_t > data_vec;
    generate_data(4, pbas, data_vec, sgl);

    static constexpr uint32_t mock_engine_size = 1000;
    static constexpr int32_t svr_id = 5;

    // mock the size storage engine reports for a pba during data_rpc serialize. This is a junk value just for testing
    MockStorageEngine mock_engine;
    EXPECT_CALL(mock_engine, pba_to_size(_)).Times(pbas.size()).WillRepeatedly([](pba_t const&) {
        return mock_engine_size;
    });

    // create a hdr with random group id and replica id and serialize hdr, pbas and value into a list of io_blobs
    auto hdr = data_channel_rpc_hdr{"RpcDataChannelTest", svr_id};
    auto serialized_blob = serialize_to_ioblob(hdr, &mock_engine, pbas, sgl);

    // verify the equality of deserialized and serialized values
    data_channel_rpc_hdr d_hdr;
    fq_pba_list_t fq_pbas;
    sisl::sg_list value;
    data_rpc::deserialize(serialized_blob, d_hdr, fq_pbas, value);

    // common header
    EXPECT_EQ(hdr.group_id, d_hdr.group_id);
    EXPECT_EQ(hdr.issuer_replica_id, d_hdr.issuer_replica_id);
    EXPECT_EQ(hdr.major_version, d_hdr.major_version);
    EXPECT_EQ(hdr.minor_version, d_hdr.minor_version);

    // pbas
    EXPECT_EQ(pbas.size(), fq_pbas.size());
    for (uint16_t i = 0; i < pbas.size(); i++) {
        EXPECT_EQ(pbas[i], fq_pbas[i].pba);
        EXPECT_EQ(fq_pbas[i].size, mock_engine_size);
        EXPECT_EQ(fq_pbas[i].server_id, svr_id);
    }

    // value
    verify_data(value, data_vec, true);

    free_resources(sgl, serialized_blob);
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_home_local_journal");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%t] %v");
    return RUN_ALL_TESTS();
}
