#include <sisl/logging/logging.h>

#include "home_replication.hpp"

SISL_LOGGING_DECL(home_repl)

namespace home_replication {

[[maybe_unused]] void foo() {
   LOGTRACEMOD(home_repl, "Foo");
}

}
