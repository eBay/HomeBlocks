#include <sisl/fds/obj_allocator.hpp>
#include <sisl/fds/vector_pool.hpp>
#include <home_replication/repl_service.h>
#include "state_machine/state_machine.h"
#include "log_store/repl_log_store.hpp"
#include "log_store/journal_entry.h"
#include "storage/storage_engine.h"

namespace home_replication {
ReplicaSet::ReplicaSet(const std::string& group_id, const std::shared_ptr< StateMachineStore >& sm_store,
                       const std::shared_ptr< nuraft::log_store >& log_store) :
        m_state_machine{std::make_shared< ReplicaStateMachine >(sm_store, this)},
        m_state_store{sm_store},
        m_data_journal{log_store},
        m_group_id{group_id} {}

void ReplicaSet::write(const sisl::blob& header, const sisl::blob& key, const sisl::sg_list& value, void* user_ctx) {
    m_state_machine->propose(header, key, value, user_ctx);
}

void ReplicaSet::transfer_pba_ownership(int64_t lsn, const pba_list_t& pbas) {
    m_state_store->add_free_pba_record(lsn, pbas);
}

// void ReplicaSet::on_data_received() {}

std::shared_ptr< nuraft::state_machine > ReplicaSet::get_state_machine() {
    return std::dynamic_pointer_cast< nuraft::state_machine >(m_state_machine);
}

bool ReplicaSet::is_leader() {
    // TODO: Need to implement after setting up RAFT replica set
    return true;
}
} // namespace home_replication