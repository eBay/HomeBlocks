#pragma once

#include <home_replication/repl_service.h>
#include <libnuraft/nuraft.hxx>
#include "storage/storage_engine.h"

namespace home_replication {

typedef std::shared_ptr< ReplicaStateMachine > rs_sm_ptr_t;

class ReplicaStateMachine : public nuraft::state_machine {
public:
    ReplicaStateMachine(const rs_sm_ptr_t& state_store);
    ~ReplicaStateMachine() override = default;
    ReplicaStateMachine(hr_state_machine const&) = delete;
    ReplicaStateMachine& operator=(hr_state_machine const&) = delete;

    /// NuRaft overrides
    uint64_t last_commit_index() override;
    nuraft::ptr< nuraft::buffer > commit(uint64_t lsn, nuraft::buffer& data) override;
    nuraft::ptr< nuraft::buffer > pre_commit(uint64_t lsn, nuraft::buffer& data) override;
    void rollback(uint64_t lsn, nuraft::buffer& data) override;

    bool apply_snapshot(nuraft::snapshot&) override { return false; }
    void create_snapshot(nuraft::snapshot& s, nuraft::async_result< bool >::handler_type& when_done) override;
    nuraft::ptr< nuraft::snapshot > last_snapshot() override { return nullptr; }

private:
    std::shared_ptr< StateMachineStore > m_state_store;
};

} // namespace home_replication
