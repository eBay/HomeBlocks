#pragma once

#include <string>

#include <home_replication/repl_set.hpp>

namespace home_replication {

struct repl_req;

class ReplicaSetListener;
class ReplicaStateMachine;
class StateMachineStore;

class ReplicaSetImpl : public ReplicaSet {
public:
    ReplicaSetImpl(const std::string& group_id, const std::shared_ptr< StateMachineStore >& sm_store,
                   const std::shared_ptr< nuraft::log_store >& log_store);

    ~ReplicaSetImpl() override = default;

    void attach_state_machine(std::unique_ptr< nuraft::state_machine >) override {}
    void append_entry(nuraft::buffer const&) override {}

    /// @brief Replicate the data to the replica set. This method goes through the following steps
    /// Step 1: Allocates pba from the storage engine to write the value into. Storage engine returns a pba_list in
    /// cases where single contiguous blocks are not available. For convenience, the comment will continue to refer
    /// pba_list as pba.
    /// Step 2: Uses data channel to send the <pba, value> to all replicas
    /// Step 3: Creates a log/journal entry with <header, key, pba> and calls nuraft to append the entry and replicate
    /// using nuraft channel (also called header_channel).
    ///
    /// @param header - Blob representing the header (it is opaque and will be copied as-is to the journal entry)
    /// @param key - Blob representing the key (it is opaque and will be copied as-is to the journal entry). We are
    /// tracking this seperately to support consistent read use cases
    /// @param value - vector of io buffers that contain value for the key
    /// @param user_ctx - User supplied opaque context which will be passed to listener callbacks
    void write(const sisl::blob& header, const sisl::blob& key, const sisl::sg_list& value, void* user_ctx) override;

    /// @brief After data is replicated and on_commit to the listener is called. the pbas are implicityly transferred to
    /// listener. This call will transfer the ownership of pba back to the replication service. This listener should
    /// never free the pbas on its own and should always transfer the ownership after it is no longer useful.
    /// @param lsn - LSN of the old pba that is being transferred
    /// @param pbas - PBAs to be transferred.
    void transfer_pba_ownership(int64_t lsn, const pba_list_t& pbas) override;

    /// @brief Checks if this replica is the leader in this replica set
    /// @return true or false
    bool is_leader() const override;

    /// @brief Register server side implimentation callbacks to data service apis
    /// @param messaging - messaging service pointer
    /// @return false indicates error in the data service registration
    bool register_data_service_apis(std::shared_ptr< nuraft_mesg::consensus_component >& messaging);

    /// @brief Send data to followers
    /// @param pbas - PBAs to be sent
    /// @param value - data to be sent
    void send_in_data_channel(const pba_list_t& pbas, const sisl::sg_list& value);

    /// @brief Send the final responce to the rpc client. This method is defined virtual for mocking in the gtest
    /// @param outgoing_buf - response buf to client
    /// @param rpc_data - context provided by the rpc server
    void send_data_service_response(sisl::io_blob_list_t const& outgoing_buf,
                                    boost::intrusive_ptr< sisl::GenericRpcData >& rpc_data) override;

    /// @brief Fetch pba data from the leader
    /// @param remote_pbas - list of remote pbas for which data is needed from the leader
    void fetch_pba_data_from_leader(const pba_list_t& remote_pbas);

    std::shared_ptr< nuraft::state_machine > get_state_machine() override;

    uint32_t get_logstore_id() const override { return 0; }

    std::shared_ptr< nuraft::log_store > data_journal() const { return m_data_journal; }

    void permanent_destroy() override {}

    void leave() override {}

    std::string group_id() const override { return m_group_id; }

private:
    nuraft::ptr< nuraft::cluster_config > load_config() override { return nullptr; }
    void save_config(const nuraft::cluster_config&) override {}
    void save_state(const nuraft::srv_state&) override {}
    nuraft::ptr< nuraft::srv_state > read_state() override { return nullptr; }
    nuraft::ptr< nuraft::log_store > load_log_store() override { return nullptr; }
    int32_t server_id() override { return 0; }
    void system_exit(const int) override {}

    void after_precommit_in_leader(const nuraft::raft_server::req_ext_cb_params& cb_params);

    std::shared_ptr< ReplicaStateMachine > m_state_machine;
    std::shared_ptr< StateMachineStore > m_state_store;
    std::shared_ptr< nuraft::log_store > m_data_journal;
    std::string m_group_id;
};

} // namespace home_replication
