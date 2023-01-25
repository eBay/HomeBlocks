#pragma once

#include <functional>
#include <sisl/fds/buffer.hpp>

namespace home_replication {
typedef std::function< void(int64_t, const sisl::byte_view&) > entry_found_cb_t;

class Journal {
public:
    Journal(entry_found_cb_t cb) : m_entry_found_cb{std::move(cb)} {}
    virtual void insert_async(int64_t lsn, const sisl::io_blob& blob) = 0;
    virtual sisl::byte_view read_sync(int64_t lsn) = 0;
    virtual void truncate(int64_t lsn) = 0;
    virtual void flush_sync(int64_t upto_lsn) = 0;

protected:
    entry_found_cb_t m_entry_found_cb;
};
} // namespace home_replication