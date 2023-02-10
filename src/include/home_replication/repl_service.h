#pragma once

#include <map>
#include <sisl/fds/buffer.hpp>

#include <home_replication/repl_set.h>

namespace nuraft {
struct log_store;
}

namespace nuraft_mesg {
class consensus_component;
}

namespace home_replication {

class ReplicationServiceBackend;

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

typedef std::shared_ptr< ReplicaSet > rs_ptr_t;
typedef std::function< std::unique_ptr< ReplicaSetListener >(const rs_ptr_t& rs) > on_replica_set_init_t;

class ReplicationService {
    friend class HomeReplicationBackend;

public:
    ReplicationService(backend_impl_t engine_impl, std::shared_ptr< nuraft_mesg::consensus_component > messaging,
                       on_replica_set_init_t cb);
    ~ReplicationService();

    rs_ptr_t lookup_replica_set(uuid_t uuid);
    void iterate_replica_sets(const std::function< void(const rs_ptr_t&) >& cb);

private:
    rs_ptr_t create_replica_set(uuid_t const uuid);
    void on_replica_store_found(uuid_t const uuid, const std::shared_ptr< StateMachineStore >& sm_store,
                                const std::shared_ptr< nuraft::log_store >& log_store);

private:
    std::unique_ptr< ReplicationServiceBackend > m_backend;
    std::mutex m_rs_map_mtx;
    std::map< uuid_t, rs_ptr_t > m_rs_map;
    on_replica_set_init_t m_on_rs_init_cb;

    std::shared_ptr< nuraft_mesg::consensus_component > m_messaging;
};
} // namespace home_replication
