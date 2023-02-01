#pragma once
#include <boost/uuid/uuid.hpp>
#include <folly/small_vector.h>

namespace home_replication {
using pba_t = uint64_t;
using pba_list_t = folly::small_vector< pba_t, 4 >;
using uuid_t = boost::uuids::uuid;

} // namespace home_replication