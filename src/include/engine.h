#pragma once
#include <memory>
#include <string>
#include <vector>
#include <functional>

#include <sisl/fds/buffer.hpp>

namespace home_replication {

using io_completion_cb_t = std::function< void(int status, void* cookie) >;
using pba_t = uint64_t;
using pba_list_t = folly::small_vector< pba_t, 4 >;

class StorageEngine {
    virtual pba_list_t alloc_pbas(uint32_t size) = 0;
    virtual void async_write(const sg_list& sgs, const pba_list_t& in_pbas, const io_completion_cb_t& cb) = 0;
    virtual void async_read(pba_t pba, sg_list& sgs, uint32_t size, const io_completion_cb_t& cb) = 0;
    virtual void persist_state() = 0;
};

} // namespace home_replication
