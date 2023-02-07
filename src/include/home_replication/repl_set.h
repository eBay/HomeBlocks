#pragma once

#include <home_replication/repl_decls.h>

#include <string>

#include <folly/concurrency/ConcurrentHashMap.h>
#include <nuraft_mesg/messaging_if.hpp>
#include <sisl/fds/buffer.hpp>

namespace home_replication {

// Fully qualified domain pba, unique pba id across replica set
struct fully_qualified_pba {
    uint32_t server_id;
    pba_t pba;
};

struct repl_req;
class ReplicaSetListener;
class ReplicaStateMachine;
class StateMachineStore;

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

protected:
    /// @brief Map the fully qualified pba (possibly remote pba) and get the local pba if available. If its not
    /// immediately available, it reaches out to the remote replica and then fetch the data, write to the local storage
    /// engine and updates the map and then returns the local pba.
    ///
    /// @param fq_pba Fully qualified pba to be fetched and mapped to local pba
    /// @return Returns Returns the local_pba
    virtual pba_t map_pba(fully_qualified_pba fq_pba);

    std::shared_ptr< nuraft::state_machine > get_state_machine() override;

    uint32_t get_logstore_id() const override;

    void attach_listener(std::unique_ptr< ReplicaSetListener > listener) { m_listener = std::move(listener); }

    std::shared_ptr< nuraft::log_store > data_journal() { return m_data_journal; }

    void permanent_destroy() override;

    void leave() override;

private:
    nuraft::ptr< nuraft::cluster_config > load_config() override;
    void save_config(const nuraft::cluster_config& config) override;
    void save_state(const nuraft::srv_state& state) override;
    nuraft::ptr< nuraft::srv_state > read_state() override;
    nuraft::ptr< nuraft::log_store > load_log_store() override;
    int32_t server_id() override;
    void system_exit(const int exit_code) override;

    void after_precommit_in_leader(const nuraft::raft_server::req_ext_cb_params& cb_params);

private:
    std::shared_ptr< ReplicaStateMachine > m_state_machine;
    std::shared_ptr< StateMachineStore > m_state_store;
    std::unique_ptr< ReplicaSetListener > m_listener;
    std::shared_ptr< nuraft::log_store > m_data_journal;
    folly::ConcurrentHashMap< fully_qualified_pba, pba_t > m_pba_map;
    folly::ConcurrentHashMap< int64_t, repl_req* > m_lsn_req_map;
    std::string m_group_id;
};

} // namespace home_replication
