#include <gtest/gtest.h>
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

    // create a list of 4 random pbas and a data vector of 4 random values which represent the data the pbas hold
    pba_list_t pbas = {get_random_num(), get_random_num(), get_random_num(), get_random_num()};
    sisl::sg_list sgl{0, {}};
    std::vector< uint64_t > data_vec;
    for (uint16_t i = 0; i < pbas.size(); ++i) {
        data_vec.emplace_back(get_random_num());
        sgl.iovs.emplace_back(iovec{new uint64_t(data_vec[i]), sizeof(uint64_t)});
        sgl.size += sizeof(uint64_t);
    }

    static constexpr uint32_t mock_engine_size = 1000;
    static constexpr int32_t svr_id = 5;

    // mock the size storage engine reports for a pba during data_rpc serialize. This is a junk value just for testing
    MockStorageEngine mock_engine;
    EXPECT_CALL(mock_engine, pba_to_size(_)).Times(pbas.size()).WillRepeatedly([](pba_t const&) {
        return mock_engine_size;
    });

    // create a hdr with random group id and replica id and serialize hdr, pbas and value into a list of io_blobs
    data_channel_rpc_hdr hdr = {boost::uuids::random_generator()(), svr_id};
    auto ioblob_list = data_rpc::serialize(hdr, pbas, &mock_engine, sgl);

    // allocate a single io_blob which can hold the entire data list above and use this as the rpc incoming buf to
    // deserialize.
    sisl::io_blob serialized_blob(sgl.size + data_channel_rpc_hdr::max_hdr_size);
    auto offset = serialized_blob.bytes;
    for (uint16_t i = 0; i < ioblob_list.size(); i++) {
        std::memcpy(offset, ioblob_list[i].bytes, ioblob_list[i].size);
        offset += ioblob_list[i].size;
    }

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
    EXPECT_EQ(value.size, sizeof(uint64_t) * pbas.size());
    // We always dump the incoming buf value into a single iovec
    EXPECT_EQ(value.iovs.size(), 1);
    auto iovec_offset = r_cast< uint8_t* >(value.iovs[0].iov_base);
    for (auto const& rn : data_vec) {
        auto rand_num = r_cast< uint64_t* >(iovec_offset);
        EXPECT_EQ(*rand_num, rn);
        iovec_offset += sizeof(uint64_t);
    }

    // free resources
    for (auto& iov : sgl.iovs) {
        auto data_ptr = r_cast< uint64_t* >(iov.iov_base);
        delete data_ptr;
    }
    serialized_blob.buf_free();
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_home_local_journal");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%t] %v");
    return RUN_ALL_TESTS();
}