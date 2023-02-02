#pragma once

#include <home_replication/repl_decls.h>

namespace home_replication {
class ReplicaSet;
class StateMachineStore;

#define RS_LOG(level, msg, ...) _RS_LOG(level, m_group_id, msg, ##__VA_ARGS__)
#define SM_LOG(level, msg, ...) _RS_LOG(level, m_rs->m_group_id, msg, ##__VA_ARGS__)
#define _RS_LOG(level, group_id, msg, ...)                                                                             \
    LOG##level##MOD_FMT(home_replication, ([&](fmt::memory_buffer& buf, const char* msgcb, auto&&... args) -> bool {   \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}:{}] "},                          \
                                            fmt::make_format_args(file_name(__FILE__), __LINE__));                     \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}={}] "},                          \
                                            fmt::make_format_args("rs", group_id));                                    \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{msgcb},                               \
                                            fmt::make_format_args(std::forward< decltype(args) >(args)...));           \
                            return true;                                                                               \
                        }),                                                                                            \
                        msg, ##__VA_ARGS__);

class ReplicaStateMachine : public nuraft::state_machine {
public:
    ReplicaStateMachine(const std::shared_ptr< StateMachineStore >& state_store, ReplicaSet* rs);
    ~ReplicaStateMachine() override = default;
    ReplicaStateMachine(ReplicaStateMachine const&) = delete;
    ReplicaStateMachine& operator=(ReplicaStateMachine const&) = delete;

    /// NuRaft overrides
    uint64_t last_commit_index() override;
<<<<<<< HEAD
    raft_buf_ptr_t commit_ext(const nuraft::ext_op_params& params) override;
    raft_buf_ptr_t pre_commit_ext(const nuraft::ext_op_params& params) override;
=======
    nuraft::ptr< nuraft::buffer > commit(uint64_t lsn, nuraft::buffer& data) override;
    nuraft::ptr< nuraft::buffer > pre_commit(uint64_t lsn, nuraft::buffer& data) override;
>>>>>>> 0deca066d3fee8b074f5a58de265b893a3f523c1
    void rollback(uint64_t lsn, nuraft::buffer& data) override;

    bool apply_snapshot(nuraft::snapshot&) override { return false; }
    void create_snapshot(nuraft::snapshot& s, nuraft::async_result< bool >::handler_type& when_done) override;
    nuraft::ptr< nuraft::snapshot > last_snapshot() override { return nullptr; }

private:
    std::shared_ptr< StateMachineStore > m_state_store;
    ReplicaSet* m_rs;
};

} // namespace home_replication
