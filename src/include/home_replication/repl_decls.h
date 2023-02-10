#pragma once
#include <boost/uuid/uuid.hpp>
#include <folly/small_vector.h>

#include <sisl/logging/logging.h>
#include <iomgr/reactor.hpp>
#include <homestore/homestore_decl.hpp>

SISL_LOGGING_DECL(home_replication)

#define HOMEREPL_LOG_MODS grpc_server, HOMESTORE_LOG_MODS, nuraft_mesg, nuraft, home_replication

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
