#pragma once
#include <boost/uuid/uuid.hpp>
#include <folly/small_vector.h>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif
#include <libnuraft/nuraft.hxx>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif
#undef auto_lock

namespace home_replication {
using pba_t = uint64_t;
using pba_list_t = folly::small_vector< pba_t, 4 >;
using uuid_t = boost::uuids::uuid;
using raft_buf_ptr_t = nuraft::ptr< nuraft::buffer >;
} // namespace home_replication