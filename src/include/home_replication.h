#pragma once

#include <sisl/logging/logging.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <string>
#include <home_replication/engine.h>

#define HOMEREPL_LOG_MODS home_repl

namespace home_replication {

// Fully qualified domain pba, unique pba id across replica set
struct fully_qualified_pba {
    std::string& srv_id;
    pba_t pba;
};

typedef folly::small_vector< std::pair< pba_t, int64_t >, 4 > pba_lsn_list_t;

enum class log_store_impl_t : uint8_t { homestore, jungle };
enum class engine_impl_t : uint8_t { homestore, jungle, file };

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
    /// @return The implementation needs to return the current list of <pba, lsn> pair that are being released as part
    /// of the commit of the key. The life cycle of these pbas are controlled by the replica set and no longer should be
    /// owned by the consumer/listener.
    virtual pba_lsn_list_t on_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                                     const pba_list_t& pbas, void* ctx) = 0;

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
class ReplicaSet : public sds::messaging::mesg_state_mgr {
public:
    ReplicaSet(const std::string& group_id, const std::vector< std::string >& peer_ids, log_store_impl_t log_store_impl,
               std::unique_ptr< ReplicaSetListener > listener);

    virtual ~ReplicaSet() = default;

    /// @brief Add new member to the replica set, by adding to the raft group
    /// @param to_dst_srv_id
    virtual void add_new_member(const std::string& to_dst_srv_id);

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
    virtual void write(const sisl::blob& header, const sisl::blob& key, const sg_list& value, void* user_ctx);

    /// @brief Map the fully qualified pba (possibly remote pba) and get the local pba if available. If its not
    /// immediately available, it reaches out to the remote replica and then fetch the data, write to the local storage
    /// engine and updates the map and then returns the local pba.
    ///
    /// @param fq_pba Fully qualified pba to be fetched and mapped to local pba
    /// @return Returns Returns the local_pba
    virtual pba_t map_pba(fully_qualified_pba fq_pba);

    nuraft::ptr< nuraft::cluster_config > load_config() override;
    void save_config(const nuraft::cluster_config& config) override;
    void save_state(const nuraft::srv_state& state) override;
    nuraft::ptr< nuraft::srv_state > read_state() override;
    nuraft::ptr< nuraft::log_store > load_log_store() override;
    int32 server_id() override;
    void system_exit(const int exit_code) override;

    std::shared_ptr< ReplicaStateMachine > state_machine();

private:
    std::shared_ptr< ReplicaStateMachine > m_state_machine;
    std::unique_ptr< ReplicaSetListener > m_listener;
    std::shared_ptr< nuraft::log_store > m_data_journal;
    std::shared_ptr< Journal > m_free_pba_journal;
    log_store_impl_t m_log_store_impl;
    folly::ConcurrentHashMap< fully_qualified_pba, pba_t > m_pba_map;
};

class ReplicaStateMachine : public nuraft::state_machine {
public:
    nuraft::ptr< nuraft::buffer > commit(uint64_t lsn, nuraft::buffer& data) override;
    nuraft::ptr< nuraft::buffer > pre_commit(uint64_t lsn, nuraft::buffer& data) override;
    void rollback(uint64_t lsn, nuraft::buffer& data) override;

    bool apply_snapshot(nuraft::snapshot&) override { return false; }
    void create_snapshot(nuraft::snapshot& s, nuraft::async_result< bool >::handler_type& when_done) override;
    nuraft::ptr< nuraft::snapshot > last_snapshot() override { return nullptr; }

    uint64_t last_commit_index() override;

private:
};

class ReplicationService {
public:
    ReplicationService(engine_impl_t engine_impl);

private:
};
} // namespace home_replication
