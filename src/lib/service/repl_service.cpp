#include <home_replication/repl_service.h>

#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <nuraft_mesg/messaging_if.hpp>
#include <sisl/logging/logging.h>

#include <home_replication/repl_set.h>
#include "service/repl_backend.h"
#include "service/home_repl_backend.h"

namespace home_replication {
ReplicationService::ReplicationService(backend_impl_t backend,
                                       std::shared_ptr< nuraft_mesg::consensus_component > messaging,
                                       on_replica_set_init_t cb) :
        m_on_rs_init_cb{std::move(cb)}, m_messaging(messaging) {
    switch (backend) {
    case backend_impl_t::homestore:
        m_backend = std::make_unique< HomeReplicationBackend >(this);
        break;
    case backend_impl_t::jungle:
    default:
        LOGERROR("We do not support jungleDB backend for repl services yet");
        throw std::runtime_error("Repl Services with jungleDB backend is unsupported yet");
    }

    // FIXME: RAFT server parameters, should be a config and reviewed!!!
    nuraft::raft_params r_params;
    r_params.with_election_timeout_lower(900)
        .with_election_timeout_upper(1400)
        .with_hb_interval(250)
        .with_max_append_size(10)
        .with_rpc_failure_backoff(250)
        .with_auto_forwarding(true)
        .with_snapshot_enabled(1);

    // This closure is where we initialize new ReplicaSet instances. When NuRaft Messging is asked to join a new group
    // either through direct creation or gRPC request it will use this callback to initialize a new state_manager and
    // state_machine for the raft_server it constructs.
    auto group_type_params = nuraft_mesg::consensus_component::register_params{
        r_params,
        [this](int32_t const, std::string const& group_id) mutable -> std::shared_ptr< nuraft_mesg::mesg_state_mgr > {
            return create_replica_set(boost::uuids::string_generator()(group_id));
        }};
    m_messaging->register_mgr_type("home_replication", group_type_params);
}

ReplicationService::~ReplicationService() = default;

rs_ptr_t ReplicationService::lookup_replica_set(uuid_t uuid) {
    std::unique_lock lg(m_rs_map_mtx);
    auto it = m_rs_map.find(uuid);
    return (it == m_rs_map.end() ? nullptr : it->second);
}

rs_ptr_t ReplicationService::create_replica_set(uuid_t const uuid) {
    auto it = m_rs_map.end();
    bool happened = false;

    {
        std::unique_lock lg(m_rs_map_mtx);
        std::tie(it, happened) = m_rs_map.emplace(std::make_pair(uuid, nullptr));
    }
    DEBUG_ASSERT(m_rs_map.end() != it, "Could not insert into map!");
    if (!happened) return it->second;

    auto log_store = m_backend->create_log_store();
    it->second =
        std::make_shared< ReplicaSet >(boost::uuids::to_string(uuid), m_backend->create_state_store(uuid), log_store);
    it->second->attach_listener(std::move(m_on_rs_init_cb(it->second)));
    m_backend->link_log_store_to_replica_set(log_store.get(), it->second.get());
    return it->second;
}

void ReplicationService::on_replica_store_found(uuid_t const uuid, const std::shared_ptr< StateMachineStore >& sm_store,
                                                const std::shared_ptr< nuraft::log_store >& log_store) {
    auto it = m_rs_map.end();
    bool happened = false;

    {
        std::unique_lock lg(m_rs_map_mtx);
        std::tie(it, happened) = m_rs_map.emplace(std::make_pair(uuid, nullptr));
    }
    DEBUG_ASSERT(m_rs_map.end() != it, "Could not insert into map!");
    if (!happened) return;

    it->second = std::make_shared< ReplicaSet >(boost::uuids::to_string(uuid), sm_store, log_store);
    it->second->attach_listener(std::move(m_on_rs_init_cb(it->second)));
    m_backend->link_log_store_to_replica_set(log_store.get(), it->second.get());
}

void ReplicationService::iterate_replica_sets(const std::function< void(const rs_ptr_t&) >& cb) {
    std::unique_lock lg(m_rs_map_mtx);
    for (const auto& [uuid, rs] : m_rs_map) {
        cb(rs);
    }
}
} // namespace home_replication
