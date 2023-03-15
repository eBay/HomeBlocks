#pragma once

#include <home_replication/repl_decls.h>

#include <string>

#include <folly/concurrency/ConcurrentHashMap.h>
#include <nuraft_mesg/messaging_if.hpp>
#include <sisl/fds/buffer.hpp>

namespace home_replication {

struct repl_req;
class ReplicaSetListener;
class ReplicaStateMachine;
class StateMachineStore;

class ReplicaStateMachine;
class StateMachineStore;
struct repl_req;

class ReplicaSet : public nuraft_mesg::mesg_state_mgr {
public:
    friend class ReplicaStateMachine;
    friend class ReplicationService;

    ReplicaSet(const std::string& group_id, const std::shared_ptr< StateMachineStore >& sm_store,
               const std::shared_ptr< nuraft::log_store >& log_store);

    virtual ~ReplicaSet() = default;

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
    virtual void write(const sisl::blob& header, const sisl::blob& key, const sisl::sg_list& value, void* user_ctx);

    /// @brief After data is replicated and on_commit to the listener is called. the pbas are implicityly transferred to
    /// listener. This call will transfer the ownership of pba back to the replication service. This listener should
    /// never free the pbas on its own and should always transfer the ownership after it is no longer useful.
    /// @param lsn - LSN of the old pba that is being transferred
    /// @param pbas - PBAs to be transferred.
    virtual void transfer_pba_ownership(int64_t lsn, const pba_list_t& pbas);

    /// @brief Checks if this replica is the leader in this replica set
    /// @return true or false
    bool is_leader();

    std::shared_ptr< nuraft::state_machine > get_state_machine() override;

protected:
    uint32_t get_logstore_id() const override { return 0; }

    void attach_listener(std::unique_ptr< ReplicaSetListener > listener) { m_listener = std::move(listener); }

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
