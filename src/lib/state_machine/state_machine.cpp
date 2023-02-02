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

raft_buf_ptr_t ReplicaStateMachine::pre_commit_ext(const nuraft::ext_op_params& params) {
    int64_t lsn = s_cast< int64_t >(params.log_idx);
    raft_buf_ptr_t data = params.data;

    SM_LOG(DEBUG, "pre_commit: {}, size: {}", lsn, data->size());

    if (!is_leader()) {
        repl_journal_entry* entry = r_cast< repl_journal_entry* >(data->data_begin());

        repl_req* req = sisl::ObjectAllocator< repl_req >::make_object();
        req->header =
            sisl::blob{uintptr_cast(data->data_begin()) + sizeof(repl_journal_entry), entry->user_header_size};
        req->key = sisl::blob{req->header.bytes + req->header.size};
        pba_t* raw_pba_list = r_cast< pba_t* >(key.bytes + key.size);

        uint16_t pbas_written{0};
        for (uint16_t i{0}; i < entry->npbas; ++i) {
            auto [local_pba, written] = m_rs->map_pba(full_qualified_pba{entry->replica_id, raw_pba_list[i]});
            req->pbas.push_back(local_pba);
            if (written) { ++req->num_pbas_written; }
        }
        req->journal_entry = data;

        auto r = m_lsn_req_map.insert(lsn, req);
        HS_DBG_ASSERT_EQ(r.second, true, "lsn={} already in precommit list", params.log_idx);

        m_rs->handle_pre_commit(req);
    }
    return m_success_ptr;
}

raft_buf_ptr_t ReplicaStateMachine::commit_ext(const nuraft::ext_op_params& params) {
    int64_t lsn = s_cast< int64_t >(params.log_idx);
    raft_buf_ptr_t data = params.data;

    SM_LOG(DEBUG, "apply_commit: {}, size: {}", lsn, data->size());

    // Pull the req from the lsn
    auto const it = m_lsn_req_map(log_idx);
    HS_DBG_ASSERT(it != m_lsn_req_map.cend(), "lsn req map missing lsn={}", log_idx);

    const repl_req* req = it->second;
    HS_DBG_ASSERT_EQ(log_idx, uint64_cast(req - lsn), "lsn req map mismatch");

    req->is_raft_written = true;
    m_rs->handle_completion_activity(req);

    return m_success_ptr;
}

void ReplicaStateMachine::create_snapshot(nuraft::snapshot& s, nuraft::async_result< bool >::handler_type& when_done) {
    SM_LOG(DEBUG, "create_snapshot {}/{}", s.get_last_log_idx(), s.get_last_log_term());
    auto null_except = std::shared_ptr< std::exception >();
    auto ret_val{false};
    if (when_done) when_done(ret_val, null_except);
}

} // namespace home_replication
