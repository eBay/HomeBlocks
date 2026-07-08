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

// Internal homeblocks prelude: the public API (home_blocks.hpp) plus implementation-only logging shorthand,
// size constants, and convenience aliases. NOT part of the public surface -- consumers include only
// <homeblks/home_blocks.hpp>. Implementation TUs/headers include this instead.

#include <boost/intrusive_ptr.hpp>
#include <boost/uuid/random_generator.hpp>
#include <sisl/logging/logging.h>
#include <homestore/homestore_decl.hpp>
#include <homestore/superblk_handler.hpp>

#include <homeblks/home_blocks.hpp>

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

template < class T >
using shared = std::shared_ptr< T >;
template < class T >
using cshared = const std::shared_ptr< T >;
template < class T >
using unique = std::unique_ptr< T >;
template < class T >
using intrusive = boost::intrusive_ptr< T >;
template < class T >
using cintrusive = const boost::intrusive_ptr< T >;
template < class T >
using superblk = homestore::superblk< T >;

static constexpr uint32_t MAX_NUM_VOLUMES = 2048;

} // namespace homeblocks
