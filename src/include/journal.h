#pragma once

#include <funtional>
#include <sisl/fds/buffer.hpp>>

namespace home_replication {
typedef std::function< void(uint64_t, const sisl::io_blob&) > journal_comp_cb_t;

class Journal {
public:
    virtual void write_at(uint64_t index, const sisl::io_blob& blob, journal_comp_cb_t comp_cb) = 0;
};
} // namespace home_replication