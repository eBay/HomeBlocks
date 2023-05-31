#pragma once

#include <gtest/gtest.h>
#include <random>
#include "state_machine/rpc_data_channel.h"

static uint64_t get_random_num() {
    static std::random_device dev;
    static std::mt19937 rng(dev());
    std::uniform_int_distribution< std::mt19937::result_type > dist(std::numeric_limits< std::uint64_t >::min(),
                                                                    std::numeric_limits< std::uint64_t >::max());
    return dist(rng);
}

static void generate_data(uint32_t const size, pba_list_t& pbas, std::vector< uint64_t >& data_vec, sisl::sg_list& sgl,
                          bool const populate_sgl = true) {
    sgl.size = 0;
    for (uint32_t i = 0; i < size; ++i) {
        pbas.emplace_back(get_random_num());
        data_vec.emplace_back(get_random_num());
        if (populate_sgl) {
            sgl.iovs.emplace_back(iovec{new uint64_t(data_vec[i]), sizeof(uint64_t)});
            sgl.size += sizeof(uint64_t);
        }
    }
}

static sisl::io_blob serialize_to_ioblob(sisl::io_blob_list_t const& ioblob_list) {
    // allocate a single io_blob which can hold the entire data list above and use this as the rpc incoming buf to
    // deserialize.
    uint64_t size = 0;
    for (auto const& blob : ioblob_list) {
        size += blob.size;
    }
    sisl::io_blob serialized_blob(size);
    auto offset = serialized_blob.bytes;
    for (uint16_t i = 0; i < ioblob_list.size(); i++) {
        std::memcpy(offset, ioblob_list[i].bytes, ioblob_list[i].size);
        offset += ioblob_list[i].size;
    }

    return serialized_blob;
}

static sisl::io_blob serialize_to_ioblob(data_channel_rpc_hdr const& hdr, StateMachineStore* store_ptr,
                                         pba_list_t const& pbas, sisl::sg_list const& sgl) {
    auto ioblob_list = data_rpc::serialize(hdr, pbas, store_ptr, sgl);
    auto ret = serialize_to_ioblob(ioblob_list);
    // delete the header allocated in data_rpc::serialize above
    delete[] ioblob_list[0].bytes;
    return ret;
}

static void verify_data(sisl::sg_list const& value, std::vector< uint64_t > const& data_vec, bool single_iov) {
    EXPECT_EQ(value.size, sizeof(uint64_t) * data_vec.size());
    auto iovec_offset = r_cast< uint8_t* >(value.iovs[0].iov_base);
    if (single_iov) { EXPECT_EQ(value.iovs.size(), 1); }
    for (uint32_t i = 0; i < data_vec.size(); i++) {
        auto rand_num = r_cast< uint64_t* >(iovec_offset);
        EXPECT_EQ(*rand_num, data_vec[i]);
        if (i + 1 < data_vec.size()) {
            iovec_offset =
                single_iov ? iovec_offset + sizeof(uint64_t) : r_cast< uint8_t* >(value.iovs[i + 1].iov_base);
        }
    }
}

static void free_resources(sisl::sg_list& sgl, sisl::io_blob& serialized_blob) {
    for (auto& iov : sgl.iovs) {
        auto data_ptr = r_cast< uint64_t* >(iov.iov_base);
        delete data_ptr;
    }
    serialized_blob.buf_free();
}