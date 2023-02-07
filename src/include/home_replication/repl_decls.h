#pragma once
#include <boost/uuid/uuid.hpp>
#include <folly/small_vector.h>

namespace home_replication {
using pba_t = uint64_t;
using pba_list_t = folly::small_vector< pba_t, 4 >;
using uuid_t = boost::uuids::uuid;

// Fully qualified domain pba, unique pba id across replica set
struct fully_qualified_pba {
    fully_qualified_pba(uint32_t s, pba_t p) : server_id{s}, pba{p} {}
    uint32_t server_id;
    pba_t pba;
};
using fq_pba_list_t = folly::small_vector< fully_qualified_pba, 4 >;
} // namespace home_replication