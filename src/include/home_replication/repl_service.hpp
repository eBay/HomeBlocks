#pragma once
#include <functional>
#include <memory>
#include <string>

#include "common.hpp"

#include "repl_set.hpp"
#include "repl_set_listener.hpp"

namespace home_replication {

using rs_ptr_t = std::shared_ptr< ReplicaSet >;

class ReplicationService {
public:
    using lookup_member_cb = std::function< std::string(uuid const&) >;
    using each_set_cb = std::function< void(const rs_ptr_t&) >;
    using on_replica_set_init_t = std::function< std::unique_ptr< ReplicaSetListener >(const rs_ptr_t& rs) >;

    virtual ~ReplicationService() = default;

    virtual rs_ptr_t create_replica_set(std::string const& group_id) = 0;
    virtual rs_ptr_t lookup_replica_set(std::string const& group_id) const = 0;
    virtual void iterate_replica_sets(each_set_cb cb) const = 0;
};

extern std::shared_ptr< ReplicationService > create_repl_service(ReplicationService::on_replica_set_init_t cb,
                                                                 ReplicationService::lookup_member_cb);

} // namespace home_replication
