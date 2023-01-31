#include <sisl/logging/logging.h>
#include <sisl/fds/utils.hpp>
#include <home_replication/repl_service.h>
#include "state_machine.h"
#include "storage/storage_engine.h"

SISL_LOGGING_DECL(home_replication)

namespace home_replication {

ReplicaStateMachine::ReplicaStateMachine(const std::shared_ptr< StateMachineStore >& state_store, ReplicaSet* rs) :
        m_state_store{state_store}, m_rs{rs} {}

uint64_t ReplicaStateMachine::last_commit_index() { return uint64_cast(m_state_store->get_last_commit_lsn()); }

nuraft::ptr< nuraft::buffer > ReplicaStateMachine::commit(uint64_t log_idx, nuraft::buffer& data) {
    SM_LOG(DEBUG, "apply_commit: {}, size: {}", log_idx, data.size());
    // m_rs->m_listener->on_commit();
    return nullptr;
}

void ReplicaStateMachine::create_snapshot(nuraft::snapshot& s, nuraft::async_result< bool >::handler_type& when_done) {
    SM_LOG(DEBUG, "create_snapshot {}/{}", s.get_last_log_idx(), s.get_last_log_term());
    auto null_except = std::shared_ptr< std::exception >();
    auto ret_val{false};
    if (when_done) when_done(ret_val, null_except);
}

} // namespace home_replication
