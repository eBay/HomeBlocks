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
    std::map< uuid_t, rs_ptr_t > m_rs_map;
    on_replica_set_init_t m_on_rs_init_cb;

    std::shared_ptr< nuraft_mesg::consensus_component > m_messaging;

public:
    enum class backend_impl_t : uint8_t { homestore, jungle };

    ReplicationServiceImpl(backend_impl_t backend, ReplicationService::on_replica_set_init_t cb,
                           ReplicationService::lookup_member_cb);
    ~ReplicationServiceImpl() override;

    rs_ptr_t on_replica_store_found(uuid_t const uuid, const std::shared_ptr< StateMachineStore >& sm_store,
                                    const std::shared_ptr< nuraft::log_store >& log_store);

    rs_ptr_t create_replica_set(uuid_t const& uuid) override;
    rs_ptr_t lookup_replica_set(uuid_t const& uuid) const override;
    void iterate_replica_sets(each_set_cb cb) const override;
};

} // namespace home_replication
