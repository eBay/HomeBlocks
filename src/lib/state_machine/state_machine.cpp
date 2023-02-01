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

raft_buf_ptr_t ReplicaStateMachine::commit_ext(const nuraft::ext_op_params& params) {
    int64_t lsn = s_cast< int64_t >(params.log_idx);
    raft_buf_ptr_t data = params.data;

    SM_LOG(DEBUG, "apply_commit: {}, size: {}", lsn, data->size());

    // Pull the req from the lsn
    auto const it = m_lsn_req_map(log_idx);
    HS_DBG_ASSERT(it != m_lsn_req_map.cend(), "lsn req map missing lsn={}", log_idx);

    const repl_req* req = it->second;
    HS_DBG_ASSERT_EQ(log_idx, uint64_cast(req - lsn), "lsn req map mismatch");

    req->is_data_replicated = true;
    m_rs->handle_completion_activity(req);

    return nullptr;
}

void ReplicaStateMachine::create_snapshot(nuraft::snapshot& s, nuraft::async_result< bool >::handler_type& when_done) {
    SM_LOG(DEBUG, "create_snapshot {}/{}", s.get_last_log_idx(), s.get_last_log_term());
    auto null_except = std::shared_ptr< std::exception >();
    auto ret_val{false};
    if (when_done) when_done(ret_val, null_except);
}

} // namespace home_replication
