
/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#pragma once

#include <boost/uuid/uuid.hpp>
#include <boost/intrusive_ptr.hpp>
#include <folly/Expected.h>
#include <folly/Unit.h>
#include <folly/futures/Future.h>
#include <sisl/logging/logging.h>
#include <homestore/homestore_decl.hpp>
#include <homestore/superblk_handler.hpp>

SISL_LOGGING_DECL(homeblocks);

#define HOMEBLOCKS_LOG_MODS homeblocks
#define LOGT(...) LOGTRACEMOD(homeblocks, ##__VA_ARGS__)
#define LOGD(...) LOGDEBUGMOD(homeblocks, ##__VA_ARGS__)
#define LOGI(...) LOGINFOMOD(homeblocks, ##__VA_ARGS__)
#define LOGW(...) LOGWARNMOD(homeblocks, ##__VA_ARGS__)
#define LOGE(...) LOGERRORMOD(homeblocks, ##__VA_ARGS__)
#define LOGC(...) LOGCRITICALMOD(homeblocks, ##__VA_ARGS__)

#ifndef Ki
constexpr uint64_t Ki = 1024ul;
#endif
#ifndef Mi
constexpr uint64_t Mi = Ki * Ki;
#endif
#ifndef Gi
constexpr uint64_t Gi = Ki * Mi;
#endif

namespace homeblocks {
using peer_id_t = boost::uuids::uuid;
using volume_id_t = boost::uuids::uuid;

template < typename T >
using shared = std::shared_ptr< T >;

template < typename T >
using cshared = const std::shared_ptr< T >;

template < typename T >
using unique = std::unique_ptr< T >;

template < typename T >
using intrusive = boost::intrusive_ptr< T >;

template < typename T >
using cintrusive = const boost::intrusive_ptr< T >;

template < typename T >
using superblk = homestore::superblk< T >;

template < class E >
class Manager {
public:
    template < typename T >
    using Result = folly::Expected< T, E >;
    template < typename T >
    using AsyncResult = folly::SemiFuture< Result< T > >;

    using NullResult = Result< folly::Unit >;
    using NullAsyncResult = AsyncResult< folly::Unit >;

    virtual ~Manager() = default;
};

class hb_utils {
public:
    static homestore::uuid_t gen_random_uuid();
};

} // namespace homeblocks
