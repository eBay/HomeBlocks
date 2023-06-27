#pragma once

#include <map>
#include <mutex>
#include <sisl/fds/buffer.hpp>

#include <home_replication/repl_service.hpp>

namespace nuraft {
struct log_store;
}

namespace nuraft_mesg {
class consensus_component;
}

namespace home_replication {

class ReplicationServiceBackend;
class ReplicaSetListener;
class StateMachineStore;

class ReplicationServiceImpl : public ReplicationService {
    std::unique_ptr< ReplicationServiceBackend > m_backend;
    mutable std::mutex m_rs_map_mtx;
    std::map< std::string, rs_ptr_t > m_rs_map;

    std::shared_ptr< nuraft_mesg::consensus_component > m_messaging;

public:
    enum class backend_impl_t : uint8_t { homestore, jungle };

    ReplicationServiceImpl(backend_impl_t backend);
    ~ReplicationServiceImpl() override;

    /// Sync APIs
    set_var get_replica_set(std::string const& group_id) const override;
    void iterate_replica_sets(std::function< void(const rs_ptr_t&) >&& cb) const override;

    /// Async APIs
    folly::SemiFuture< set_var > create_replica_set(std::string const& group_id,
                                                    std::set< endpoint, std::less<> >&& members) override;
    folly::SemiFuture< ReplServiceError > replace_member(std::string const& group_id, std::string const& member_out,
                                                         std::string const& member_in) const override;

    set_var on_replica_store_found(std::string const group_id, const std::shared_ptr< StateMachineStore >& sm_store,
                                   const std::shared_ptr< nuraft::log_store >& log_store);
};

} // namespace home_replication
