#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <sisl/fds/buffer.hpp>

#include <home_replication/repl_service.h>

namespace nuraft {
struct log_store;
}

namespace nuraft_mesg {
class consensus_component;
}

namespace home_replication {

class ReplicationServiceBackend;
class ReplicaSetListener;
class ReplicaSetImpl;
class StateMachineStore;

enum class backend_impl_t : uint8_t { homestore, jungle };

using rsi_ptr_t = std::shared_ptr< ReplicaSetImpl >;

class ReplicationServiceImpl : public ReplicationService {
    friend class HomeReplicationBackend;

    std::unique_ptr< ReplicationServiceBackend > m_backend;
    mutable std::mutex m_rs_map_mtx;
    std::map< std::string, rsi_ptr_t > m_rs_map;
    on_replica_set_init_t m_on_rs_init_cb;

    std::shared_ptr< nuraft_mesg::consensus_component > m_messaging;

    rsi_ptr_t on_replica_store_found(std::string const& gid, const std::shared_ptr< StateMachineStore >& sm_store,
                                     const std::shared_ptr< nuraft::log_store >& log_store);

public:
    ReplicationServiceImpl(backend_impl_t engine_impl, std::shared_ptr< nuraft_mesg::consensus_component > messaging,
                           on_replica_set_init_t cb);
    ~ReplicationServiceImpl() override;

    /// Sync APIs
    set_var get_replica_set(std::string const& group_id) const override;
    void iterate_replica_sets(std::function< void(const rs_ptr_t&) > cb) const override;

    /// Async APIs
    folly::SemiFuture< set_var > create_replica_set(std::string const& group_id,
                                                    std::set< std::string, std::less<> >&& members) override;
    folly::SemiFuture< ReplServiceError > replace_member(std::string const& group_id, std::string const& member_out,
                                                         std::string const& member_in) const override;
};

} // namespace home_replication
