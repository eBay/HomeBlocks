#pragma once

#include <home_replication/repl_set.h>

#include <string>

#include <folly/concurrency/ConcurrentHashMap.h>
#include <nuraft_mesg/messaging_if.hpp>
#include <sisl/fds/buffer.hpp>

namespace home_replication {

class ReplicaStateMachine;
class StateMachineStore;

class ReplicaSetImpl : public ReplicaSet {
public:
    friend class ReplicaStateMachine;

    ReplicaSetImpl(std::string const& group_id, std::shared_ptr< StateMachineStore > const& sm_store,
                   std::shared_ptr< nuraft::log_store > const& log_store);

    virtual ~ReplicaSetImpl() = default;

    void write(const sisl::blob& header, const sisl::blob& key, const sisl::sg_list& value, void* user_ctx) override;

    void transfer_pba_ownership(int64_t lsn, const pba_list_t& pbas) override;

    void send_data_service_response(sisl::io_blob_list_t const& outgoing_buf,
                                    boost::intrusive_ptr< sisl::GenericRpcData >& rpc_data) override;

    bool is_leader() const override;

    /// @brief Register server side implimentation callbacks to data service apis
    /// @param messaging - messaging service pointer
    /// @return false indicates error in the data service registration
    bool register_data_service_apis(std::shared_ptr< nuraft_mesg::consensus_component >& messaging);

    /// @brief Send data to followers
    /// @param pbas - PBAs to be sent
    /// @param value - data to be sent
    void send_in_data_channel(const pba_list_t& pbas, const sisl::sg_list& value);

    /// @brief Fetch pba data from the leader
    /// @param remote_pbas - list of remote pbas for which data is needed from the leader
    void fetch_pba_data_from_leader(const pba_list_t& remote_pbas);

    std::shared_ptr< nuraft::state_machine > get_state_machine() override;

    void append_entry(nuraft::buffer const&) override {}

    std::string group_id() const override { return m_group_id; }

    void attach_listener(std::unique_ptr< ReplicaSetListener > listener) { m_listener = std::move(listener); }

protected:
    uint32_t get_logstore_id() const override { return 0; }

    std::shared_ptr< nuraft::log_store > data_journal() { return m_data_journal; }

    void permanent_destroy() override {}

    void leave() override {}

private:
    nuraft::ptr< nuraft::cluster_config > load_config() override { return nullptr; }
    void save_config(const nuraft::cluster_config&) override {}
    void save_state(const nuraft::srv_state&) override {}
    nuraft::ptr< nuraft::srv_state > read_state() override { return nullptr; }
    nuraft::ptr< nuraft::log_store > load_log_store() override { return nullptr; }
    int32_t server_id() override { return 0; }
    void system_exit(const int) override {}

    void after_precommit_in_leader(const nuraft::raft_server::req_ext_cb_params& cb_params);

private:
    std::shared_ptr< ReplicaStateMachine > m_state_machine;
    std::shared_ptr< StateMachineStore > m_state_store;
    std::unique_ptr< ReplicaSetListener > m_listener;
    std::shared_ptr< nuraft::log_store > m_data_journal;
    std::string m_group_id;
};

} // namespace home_replication
