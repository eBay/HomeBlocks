#pragma once

#include <vector>
#include <functional>
#include <home_replication/repl_decls.h>

#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif
#include <libnuraft/nuraft.hxx>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif
#undef auto_lock

namespace home_replication {
class ReplicaSet;
class StateMachineStore;

#define RS_LOG(level, msg, ...)                                                                                        \
    LOG##level##MOD_FMT(home_replication, ([&](fmt::memory_buffer& buf, const char* msgcb, auto&&... args) -> bool {   \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}:{}] "},                          \
                                            fmt::make_format_args(file_name(__FILE__), __LINE__));                     \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}={}] "},                          \
                                            fmt::make_format_args("rs", m_group_id));                                  \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{msgcb},                               \
                                            fmt::make_format_args(std::forward< decltype(args) >(args)...));           \
                            return true;                                                                               \
                        }),                                                                                            \
                        msg, ##__VA_ARGS__);

#define RS_ASSERT_CMP(assert_type, val1, cmp, val2, ...)                                                               \
    {                                                                                                                  \
        assert_type##_ASSERT_CMP(                                                                                      \
            val1, cmp, val2,                                                                                           \
            [&](fmt::memory_buffer& buf, const char* const msgcb, auto&&... args) -> bool {                            \
                fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}:{}] "},                                      \
                                fmt::make_format_args(file_name(__FILE__), __LINE__));                                 \
                sisl::logging::default_cmp_assert_formatter(buf, msgcb, std::forward< decltype(args) >(args)...);      \
                fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}={}] "},                                      \
                                fmt::make_format_args("rs", m_group_id));                                              \
                return true;                                                                                           \
            },                                                                                                         \
            ##__VA_ARGS__);                                                                                            \
    }
#define RS_ASSERT(assert_type, cond, ...)                                                                              \
    {                                                                                                                  \
        assert_type##_ASSERT_FMT(cond,                                                                                 \
                                 ([&](fmt::memory_buffer& buf, const char* const msgcb, auto&&... args) -> bool {      \
                                     fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}={}] "},                 \
                                                     fmt::make_format_args("rs", m_group_id));                         \
                                     fmt::vformat_to(fmt::appender{buf}, fmt::string_view{msgcb},                      \
                                                     fmt::make_format_args(std::forward< decltype(args) >(args)...));  \
                                     return true;                                                                      \
                                 }),                                                                                   \
                                 ##__VA_ARGS__);                                                                       \
    }

#define RS_DBG_ASSERT(cond, ...) RS_ASSERT(DEBUG, cond, ##__VA_ARGS__)
#define RS_DBG_ASSERT_EQ(val1, val2, ...) RS_ASSERT_CMP(DEBUG, val1, ==, val2, ##__VA_ARGS__)
#define RS_DBG_ASSERT_NE(val1, val2, ...) RS_ASSERT_CMP(DEBUG, val1, !=, val2, ##__VA_ARGS__)
#define RS_DBG_ASSERT_LT(val1, val2, ...) RS_ASSERT_CMP(DEBUG, val1, <, val2, ##__VA_ARGS__)
#define RS_DBG_ASSERT_LE(val1, val2, ...) RS_ASSERT_CMP(DEBUG, val1, <=, val2, ##__VA_ARGS__)
#define RS_DBG_ASSERT_GT(val1, val2, ...) RS_ASSERT_CMP(DEBUG, val1, >, val2, ##__VA_ARGS__)
#define RS_DBG_ASSERT_GE(val1, val2, ...) RS_ASSERT_CMP(DEBUG, val1, >=, val2, ##__VA_ARGS__)

#define RS_REL_ASSERT(cond, ...) RS_ASSERT(RELEASE, cond, ##__VA_ARGS__)
#define RS_REL_ASSERT_EQ(val1, val2, ...) RS_ASSERT_CMP(RELEASE, val1, ==, val2, ##__VA_ARGS__)
#define RS_REL_ASSERT_NE(val1, val2, ...) RS_ASSERT_CMP(RELEASE, val1, !=, val2, ##__VA_ARGS__)
#define RS_REL_ASSERT_LT(val1, val2, ...) RS_ASSERT_CMP(RELEASE, val1, <, val2, ##__VA_ARGS__)
#define RS_REL_ASSERT_LE(val1, val2, ...) RS_ASSERT_CMP(RELEASE, val1, <=, val2, ##__VA_ARGS__)
#define RS_REL_ASSERT_GT(val1, val2, ...) RS_ASSERT_CMP(RELEASE, val1, >, val2, ##__VA_ARGS__)
#define RS_REL_ASSERT_GE(val1, val2, ...) RS_ASSERT_CMP(RELEASE, val1, >=, val2, ##__VA_ARGS__)

struct repl_req;
using raft_buf_ptr_t = nuraft::ptr< nuraft::buffer >;

class ReplicaStateMachine : public nuraft::state_machine {
public:
    using batch_completion_cb_t = std::function< void(void) >;

    ReplicaStateMachine(const std::shared_ptr< StateMachineStore >& state_store, ReplicaSet* rs);
    ~ReplicaStateMachine() override = default;
    ReplicaStateMachine(ReplicaStateMachine const&) = delete;
    ReplicaStateMachine& operator=(ReplicaStateMachine const&) = delete;

    /// NuRaft overrides
    uint64_t last_commit_index() override;
    raft_buf_ptr_t pre_commit_ext(const nuraft::state_machine::ext_op_params& params) override;
    raft_buf_ptr_t commit_ext(const nuraft::state_machine::ext_op_params& params) override;
    void rollback(uint64_t lsn, nuraft::buffer& ) override { LOGCRITICAL("Unimplemented rollback on: [{}]", lsn); }

    bool apply_snapshot(nuraft::snapshot&) override { return false; }
    void create_snapshot(nuraft::snapshot& s, nuraft::async_result< bool >::handler_type& when_done) override;
    nuraft::ptr< nuraft::snapshot > last_snapshot() override { return nullptr; }

    ////////// APIs outside of nuraft::state_machine requirements ////////////////////
    void propose(const sisl::blob& header, const sisl::blob& key, const sisl::sg_list& value, void* user_ctx);

    repl_req* transform_journal_entry(const raft_buf_ptr_t& raft_buf);

    /// @brief Map the fully qualified pba (possibly remote pba) and get the local pba if available. If its not
    /// available it will allocate a local pba and create a map entry for remote_pba to local_pba and its associated
    /// repl_req instance. The local_pba will be immediately returned.
    ///
    /// @param fq_pba Fully qualified pba to be fetched and mapped to local pba
    /// @return Returns Returns the local_pba
    std::pair< pba_t, bool > try_map_pba(const fully_qualified_pba& fq_pba);

    /// @brief First try to map the pbas if available. If not available in local map, wait for some time (based on if
    /// it is in resync mode or not) and then reach out to remote replica and fetch the actual data, write to the local
    /// storage engine and update the map. It then calls callback after all pbas in the list are fetched.
    ///
    /// @param fq_pbas Vector of fq_pbas that needs to mapped
    /// @param completion callback called after all pbas are fetched if that is needed.
    /// @return Returns if all fq_pbas have their corresponding map is readily available. If the return is true, the
    /// completion callback will not be called.
    bool async_fetch_write_pbas(const std::vector< fully_qualified_pba >& fq_pbas, batch_completion_cb_t cb);

    void link_lsn_to_req(repl_req* req, int64_t lsn);
    repl_req* lsn_to_req(int64_t lsn);

private:
    void after_precommit_in_leader(const nuraft::raft_server::req_ext_cb_params& params);
    void check_and_commit(repl_req* req);

private:
    std::shared_ptr< StateMachineStore > m_state_store;
    folly::ConcurrentHashMap< fully_qualified_pba, pba_t > m_pba_map;
    folly::ConcurrentHashMap< int64_t, repl_req* > m_lsn_req_map;
    ReplicaSet* m_rs;
    std::string m_group_id;
    uint32_t m_server_id;                        // TODO: Populate the value from replica set/RAFT
    nuraft::ptr< nuraft::buffer > m_success_ptr; // Preallocate the success return to raft
};

} // namespace home_replication
