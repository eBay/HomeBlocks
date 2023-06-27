#pragma once

#include <string>

#include <boost/uuid/uuid.hpp>
#include <folly/small_vector.h>
#include <homestore/homestore_decl.hpp>
#include <sisl/logging/logging.h>

SISL_LOGGING_DECL(home_replication)

#define HOMEREPL_LOG_MODS grpc_server, nuraft_mesg, nuraft, home_replication, HOMESTORE_LOG_MODS

namespace home_replication {

using endpoint = std::string;
using boost::uuids::uuid;

using pba_t = uint64_t;
using pba_list_t = folly::small_vector< pba_t, 4 >;

} // namespace home_replication
