#pragma once

#include <string>
#include <map>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <sisl/logging/logging.h>
#include <sisl/fds/buffer.hpp>
#include <nuraft_mesg/messaging_if.hpp>
#include <home_replication/repl_decls.h>

#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif
#include <libnuraft/nuraft.hxx>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif
#undef auto_lock

SISL_LOGGING_DECL(home_replication)

namespace home_replication {

enum class backend_impl_t : uint8_t { homestore, jungle };

//
// Callbacks to be implemented by ReplicaSet users.
//
class ReplicaSetListener {
public:
    virtual ~ReplicaSetListener() = default;

    /// @brief Called when the log entry has been committed in the replica set.
    ///
    /// This function is called from a dedicated commit thread which is different from the original thread calling
    /// replica_set::write(). There is only one commit thread, and lsn is guaranteed to be monotonically increasing.
    ///
    /// @param lsn - The log sequence number
    /// @param header - Header originally passed with replica_set::write() api
    /// @param key - Key originally passed with replica_set::write() api
    /// @param pbas - List of pbas where data is written to the storage engine.
    /// @param ctx - User contenxt passed as part of the replica_set::write() api
    ///
    virtual void on_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key, const pba_list_t& pbas,
                           void* ctx) = 0;

    /// @brief Called when the log entry has been received by the replica set.
    ///
    /// On recovery, this is called from a random worker thread before the raft server is started. It is
    /// guaranteed to be serialized in log index order.
    ///
    /// On the leader, this is called from the same thread that replica_set::write() was called.
    ///
    /// On the follower, this is called when the follower has received the log entry. It is guaranteed to be serialized
    /// in log sequence order.
    ///
    /// NOTE: Listener can choose to ignore this pre commit, however, typical use case of maintaining this is in-case
    /// replica set needs to support strong consistent reads and follower needs to ignore any keys which are not being
    /// currently in pre-commit, but yet to be committed.
    ///
    /// @param lsn - The log sequence number
    /// @param header - Header originally passed with replica_set::write() api
    /// @param key - Key originally passed with replica_set::write() api
    /// @param ctx - User contenxt passed as part of the replica_set::write() api
    virtual void on_pre_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key, void* ctx) = 0;

    /// @brief Called when the log entry has been rolled back by the replica set.
    ///
    /// This function is called on followers only when the log entry is going to be overwritten. This function is called
    /// from a random worker thread, but is guaranteed to be serialized.
    ///
    /// For each log index, it is guaranteed that either on_commit() or on_rollback() is called but not both.
    ///
    /// NOTE: Listener should do the free any resources created as part of pre-commit.
    ///
    /// @param lsn - The log sequence number getting rolled back
    /// @param header - Header originally passed with replica_set::write() api
    /// @param key - Key originally passed with replica_set::write() api
    /// @param ctx - User contenxt passed as part of the replica_set::write() api
    virtual void on_rollback(int64_t lsn, const sisl::blob& header, const sisl::blob& key, void* ctx) = 0;

    /// @brief Called when the replica set is being stopped
    virtual void on_replica_stop() = 0;
};

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

    bool is_leader();

    std::shared_ptr< nuraft::state_machine > get_state_machine() override;

protected:
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
    std::string m_group_id;
};
typedef std::shared_ptr< ReplicaSet > rs_ptr_t;

class ReplicationServiceBackend;
typedef std::function< std::unique_ptr< ReplicaSetListener >(const rs_ptr_t& rs) > on_replica_set_init_t;

class ReplicationService {
    friend class HomeReplicationBackend;

public:
    ReplicationService(backend_impl_t engine_impl, on_replica_set_init_t cb);
    ~ReplicationService();

    rs_ptr_t create_replica_set(uuid_t uuid);
    rs_ptr_t lookup_replica_set(uuid_t uuid);
    void iterate_replica_sets(const std::function< void(const rs_ptr_t&) >& cb);

private:
    void on_replica_store_found(uuid_t uuid, const std::shared_ptr< StateMachineStore >& sm_store,
                                const std::shared_ptr< nuraft::log_store >& log_store);

private:
    std::unique_ptr< ReplicationServiceBackend > m_backend;
    std::mutex m_rs_map_mtx;
    std::map< uuid_t, std::shared_ptr< ReplicaSet > > m_rs_map;
    on_replica_set_init_t m_on_rs_init_cb;
};
} // namespace home_replication
