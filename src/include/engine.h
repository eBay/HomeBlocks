#pragma once
#include <memory>
#include <string>
#include <vector>
#include <functional>

#include <sisl/fds/buffer.hpp>

namespace home_replication {

typedef std::function< void(int status, void* cookie) > io_completion_cb_t;
typedef uint64_t pba_t;
typedef folly::small_vector< pba_t, 4 > pba_list_t;

class StorageEngine {
    virtual pba_list_t alloc_pbas(uint32_t size) = 0;
    virtual void async_write(const sg_list& sgs, const pba_list_t& in_pbas, const io_completion_cb_t& cb) = 0;
    virtual void async_read(pba_t pba, sg_list& sgs, uint32_t size, const io_completion_cb_t& cb) = 0;
    virtual void persist_state() = 0;
};

} // namespace home_replication