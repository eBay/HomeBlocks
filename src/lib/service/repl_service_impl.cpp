#include "repl_service_impl.hpp"

#include <nuraft_mesg/messaging_if.hpp>
#include <sisl/logging/logging.h>
#include <folly/executors/QueuedImmediateExecutor.h>

#include "state_machine/replica_set_impl.hpp"
#include "service/repl_backend.h"
#include "service/home_repl_backend.h"

namespace home_replication {
ReplicationServiceImpl::ReplicationServiceImpl(backend_impl_t backend) : m_messaging(nullptr) {
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
            return std::get< rs_ptr_t >(create_replica_set(group_id, std::set< endpoint, std::less<> >())
                                            .via(&folly::QueuedImmediateExecutor::instance())
                                            .get());
        }};
    m_messaging->register_mgr_type("home_replication", group_type_params);
}

ReplicationServiceImpl::~ReplicationServiceImpl() = default;

ReplicationService::set_var ReplicationServiceImpl::get_replica_set(std::string const& group_id) const {
    std::unique_lock lg(m_rs_map_mtx);
    if (auto it = m_rs_map.find(group_id); it != m_rs_map.end()) return it->second;
    return ReplServiceError::FAILED;
}

folly::SemiFuture< ReplicationService::set_var >
ReplicationServiceImpl::create_replica_set(std::string const& group_id, std::set< endpoint, std::less<> >&&) {
    auto log_store = m_backend->create_log_store();
    auto sm_store = m_backend->create_state_store(group_id);
    return folly::makeSemiFuture(on_replica_store_found(group_id, sm_store, log_store));
}

ReplicationService::set_var
ReplicationServiceImpl::on_replica_store_found(std::string const group_id,
                                               const std::shared_ptr< StateMachineStore >& sm_store,
                                               const std::shared_ptr< nuraft::log_store >& log_store) {
    auto it = m_rs_map.end();
    bool happened = false;
    {
        std::unique_lock lg(m_rs_map_mtx);
        std::tie(it, happened) = m_rs_map.emplace(std::make_pair(group_id, nullptr));
    }
    if (m_rs_map.end() == it) return ReplServiceError::FAILED;
    if (happened) {
        auto repl_set = std::make_shared< ReplicaSetImpl >(group_id, sm_store, log_store);
        it->second = repl_set;
        m_backend->link_log_store_to_replica_set(log_store.get(), repl_set.get());
        if (!repl_set->register_data_service_apis(m_messaging)) {
            // TODO: log error message
        }
    }
    return it->second;
}

void ReplicationServiceImpl::iterate_replica_sets(std::function< void(const rs_ptr_t&) >&& cb) const {
    std::unique_lock lg(m_rs_map_mtx);
    for (const auto& [group_id, rs] : m_rs_map) {
        cb(rs);
    }
}

folly::SemiFuture< ReplServiceError > ReplicationServiceImpl::replace_member(std::string const&, std::string const&,
                                                                             std::string const&) const {
    return folly::makeSemiFuture(ReplServiceError::CANCELLED);
}

std::shared_ptr< ReplicationService > create_repl_service(ReplicatedServer&) {
    return std::make_shared< ReplicationServiceImpl >(ReplicationServiceImpl::backend_impl_t::homestore);
}
} // namespace home_replication
