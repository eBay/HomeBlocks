#pragma once
#include <boost/uuid/uuid.hpp>
#include <folly/small_vector.h>

#include <sisl/logging/logging.h>
#include <iomgr/reactor.hpp>
#include <homestore/homestore_decl.hpp>

SISL_LOGGING_DECL(home_replication)

#define HOMEREPL_LOG_MODS HOMESTORE_LOG_MODS, home_replication

namespace home_replication {
using pba_t = uint64_t;
using pba_list_t = folly::small_vector< pba_t, 4 >;
using uuid_t = boost::uuids::uuid;

} // namespace home_replication
