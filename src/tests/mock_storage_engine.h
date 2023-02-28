#include "storage/storage_engine.h"

using namespace home_replication;

class MockStorageEngine : public StateMachineStore {
public:
    MOCK_METHOD(pba_list_t, alloc_pbas, (uint32_t), (override));
    MOCK_METHOD(void, async_write, (const sisl::sg_list&, const pba_list_t&, const io_completion_cb_t&), (override));
    MOCK_METHOD(void, async_read, (pba_t, sisl::sg_list&, uint32_t, const io_completion_cb_t&), (override));
    MOCK_METHOD(void, free_pba, (pba_t), (override));
    MOCK_METHOD(uint32_t, pba_to_size, (pba_t), (override, const));
    MOCK_METHOD(void, destroy, (), (override));
    MOCK_METHOD(void, commit_lsn, (repl_lsn_t), (override));
    MOCK_METHOD(repl_lsn_t, get_last_commit_lsn, (), (override, const));
    MOCK_METHOD(void, add_free_pba_record, (repl_lsn_t, const pba_list_t&), (override));
    using get_free_pba_cb = const std::function< void(repl_lsn_t lsn, const pba_list_t& pba) >;
    MOCK_METHOD(void, get_free_pba_records, (repl_lsn_t, repl_lsn_t, get_free_pba_cb&), (override));
    MOCK_METHOD(void, remove_free_pba_records_upto, (repl_lsn_t), (override));
    MOCK_METHOD(void, flush_free_pba_records, (), (override));
};