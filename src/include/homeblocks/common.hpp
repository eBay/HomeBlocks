#pragma once

#include <sisl/logging/logging.h>

SISL_LOGGING_DECL(homeblocks);

#define HOMEBLOCKS_LOG_MODS homeblocks

#ifndef Ki
constexpr uint64_t Ki = 1024ul;
#endif
#ifndef Mi
constexpr uint64_t Mi = Ki * Ki;
#endif
#ifndef Gi
constexpr uint64_t Gi = Ki * Mi;
#endif

namespace homeblocks {} // namespace homeblocks
