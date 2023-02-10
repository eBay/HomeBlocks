#include <home_replication/repl_service.h>

#include <boost/uuid/uuid_io.hpp>
#include <sisl/logging/logging.h>

#include <home_replication/repl_set.h>
#include "service/repl_backend.h"
#include "service/home_repl_backend.h"

namespace home_replication {
ReplicationService::ReplicationService(backend_impl_t backend, on_replica_set_init_t cb) :
        m_on_rs_init_cb{std::move(cb)} {
    switch (backend) {
    case backend_impl_t::homestore:
        m_backend = std::make_unique< HomeReplicationBackend >(this);
        break;
    case backend_impl_t::jungle:
    default:
        LOGERROR("We do not support jungleDB backend for repl services yet");
        throw std::runtime_error("Repl Services with jungleDB backend is unsupported yet");
    }
}

ReplicationService::~ReplicationService() = default;

rs_ptr_t ReplicationService::lookup_replica_set(uuid_t uuid) {
    std::unique_lock lg(m_rs_map_mtx);
    auto it = m_rs_map.find(uuid);
    return (it == m_rs_map.end() ? nullptr : it->second);
}

rs_ptr_t ReplicationService::create_replica_set(uuid_t uuid) {
    if (lookup_replica_set(uuid) != nullptr) {
        assert(0);
        LOGDEBUGMOD(home_replication, "Attempting to create replica set instance with already existing uuid={}",
                    boost::uuids::to_string(uuid));
    }

    auto log_store = m_backend->create_log_store();
    auto rs =
        std::make_shared< ReplicaSet >(boost::uuids::to_string(uuid), m_backend->create_state_store(uuid), log_store);
    {
        std::unique_lock lg(m_rs_map_mtx);
        m_rs_map.insert(std::make_pair(uuid, rs));
    }
    rs->attach_listener(std::move(m_on_rs_init_cb(rs)));
    m_backend->link_log_store_to_replica_set(log_store.get(), rs.get());
    return rs;
}

void ReplicationService::on_replica_store_found(uuid_t uuid, const std::shared_ptr< StateMachineStore >& sm_store,
                                                const std::shared_ptr< nuraft::log_store >& log_store) {
    if (lookup_replica_set(uuid) != nullptr) {
        assert(0);
        LOGDEBUGMOD(home_replication, "Attempting to create replica set instance with already existing uuid={}",
                    boost::uuids::to_string(uuid));
    }

    auto rs = std::make_shared< ReplicaSet >(boost::uuids::to_string(uuid), sm_store, log_store);
    {
        std::unique_lock lg(m_rs_map_mtx);
        m_rs_map.insert(std::make_pair(uuid, rs));
    }
    rs->attach_listener(std::move(m_on_rs_init_cb(rs)));
    m_backend->link_log_store_to_replica_set(log_store.get(), rs.get());
}

void ReplicationService::iterate_replica_sets(const std::function< void(const rs_ptr_t&) >& cb) {
    std::unique_lock lg(m_rs_map_mtx);
    for (const auto& [uuid, rs] : m_rs_map) {
        cb(rs);
    }
}
} // namespace home_replication
