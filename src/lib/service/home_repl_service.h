#pragma once

#include <home_replication/repl_service.h>
#include "storage/home_storage_engine.h"
#include "state_machine/state_machine.h"

namespace home_replication {
class HomeReplicationServiceImpl : public ReplicationServiceImpl {
public:
    HomeReplicationServiceImpl();
    virtual ~HomeReplicationServiceImpl() = default;

    std::shared_ptr< StateMachineStore > create_state_store(uuid_t uuid) override;
    std::shared_ptr< nuraft::log_store > create_log_store() override;

private:
    void rs_super_blk_found(const sisl::byte_view& buf, void* meta_cookie);

private:
    std::mutex m_rs_map_mtx;
    std::unordered_map< uuid_t, std::shared_ptr< ReplicaSet > > m_rs_map;
};
} // namespace home_replication