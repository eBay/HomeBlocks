#pragma once
#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <sisl/fds/buffer.hpp>
#include <home_replication/repl_service.h>

namespace home_replication {

typedef std::function< void(int status, void* cookie) > io_completion_cb_t;
typedef boost::uuids::uuid uuid_t;

class StateMachineStore {
public:
    struct DirtySession {};

    virtual pba_list_t alloc_pbas(uint32_t size) = 0;
    virtual void async_write(const sg_list& sgs, const pba_list_t& in_pbas, const io_completion_cb_t& cb) = 0;
    virtual void async_read(pba_t pba, sg_list& sgs, uint32_t size, const io_completion_cb_t& cb) = 0;

    virtual void destroy() = 0;
    virtual std::unique_ptr< DirtySession > create_dirty_session() = 0;
    virtual void add_free_pba_record(DirtySession* ds, int64_t lsn, const pba_list_t& pbas) = 0;
    virtual void get_free_pba_records(int64_t from_lsn, int64_t to_lsn,
                                      const std::function< void(int64_t lsn, const pba_list_t& pba) >& cb) = 0;
    virtual void remove_free_pba_records_upto(DirtySession* ds, int64_t lsn) = 0;
    virtual void flush_dirty_session(DirtySession* ds) = 0;
}

} // namespace home_replication