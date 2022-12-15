#include <algorithm>

#include <sisl/options/options.h>
#include <homeblocks/homeblocks.hpp>

SISL_OPTION_GROUP(homeblocks,
                  (executor_type, "", "executor", "Executor to use for Future deferal",
                   ::cxxopts::value< std::string >()->default_value("immediate"), "immediate|cpu|io"));

SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)

namespace homeobject {} // namespace homeobject
