#include <home_replication/repl_service.h>

namespace home_replication {
ReplicationService::ReplicationService(engine_impl_t engine_impl, log_store_impl_t log_store_impl,
                                       on_replica_set_identified_t cb) :
        m_engine_impl{engine_impl}, m_log_store_impl{log_store_imp}, m_rs_found_cb{std::move(cb)} {}

rs_ptr_t ReplicationService::lookup_replica_set(uuid_t uuid) {
    std::unique_lock lg(m_sm_store_map_mtx);
    auto it = m_rs_map.find(uuid);
    return (it == m_rs_map.end() ? nullptr : it->second);
}

rs_ptr_t ReplicationService::create_replica_set(uuid_t uuid) {
    if (lookup_replica_set(uuid) != nullptr) {
        assert(0);
        LOGDEBUGMOD(home_replication, "Attempting to create replica set instance with already existing uuid={}", uuid);
    }

    auto smachine = std::make_shared< ReplicaStateMachine >(m_impl->create_state_store(uuid));
    auto smgr = std::make_shared< ReplicaStateManager >(smachine);
    auto log_store = m_impl->create_log_store();

    auto rs = std::make_shared< ReplicaSet >(uuid, smachine, smgr, log_store);
    {
        std::unique_lock lg(m_rs_map_mtx);
        m_rs_map.insert(std::make_pair(uuid, rs));
    }
    return rs;
}

void HomeReplicationServiceImpl::iterate_replica_sets(const std::function< void(const rs_ptr_t&) >& cb) {
    std::unique_lock lg(m_rs_map_mtx);
    for (const auto& [uuid, rs] : m_rs_map) {
        cb(rs);
    }
}
} // namespace home_replication