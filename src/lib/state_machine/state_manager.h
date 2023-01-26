#pragma once

#include <nuraft_grpc/messaging_if.hpp>

namespace home_replication {
class ReplicaStateManager : public nuraft_msg::mesg_state_mgr {
public:
    uint32_t get_logstore_id() const override;
    std::shared_ptr< nuraft::state_machine > get_state_machine() override;
    void permanent_destroy() override;
    void leave() override;

    nuraft::ptr< nuraft::cluster_config > load_config() override;
    void save_config(const nuraft::cluster_config& config) override;
    void save_state(const nuraft::srv_state& state) override;
    nuraft::ptr< nuraft::srv_state > read_state() override;
    nuraft::ptr< nuraft::log_store > load_log_store() override;
    int32 server_id() override;
    void system_exit(const int exit_code) override;

    std::shared_ptr< ReplicaStateMachine > state_machine();
};
} // namespace home_replication