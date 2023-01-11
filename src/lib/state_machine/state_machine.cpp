#include "state_machine.h"

#include <sisl/logging/logging.h>

SISL_LOGGING_DECL(home_repl)

namespace home_replication {

uint64_t hr_state_machine::last_commit_index() { return 0ul; }

nuraft::ptr< nuraft::buffer > hr_state_machine::commit(const uint64_t log_idx, nuraft::buffer& data) {
    LOGTRACEMOD(home_repl, "apply_commit: {}, size: {}", log_idx, data.size());
    return nullptr;
}

void hr_state_machine::create_snapshot(nuraft::snapshot& s, nuraft::async_result< bool >::handler_type& when_done) {
    LOGDEBUGMOD(home_repl, "create_snapshot {}/{}", s.get_last_log_idx(), s.get_last_log_term());
    auto null_except = std::shared_ptr< std::exception >();
    auto ret_val{false};
    if (when_done) when_done(ret_val, null_except);
}

} // namespace home_replication
