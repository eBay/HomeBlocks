#pragma once

#include <homestore/logstore_service.hpp>
#include <homestore/superblk_handler.hpp>
#include "storage_engine.h"

namespace home_replication {
using store_lsn_t = int64_t;

#pragma pack(1)
struct home_rs_superblk {
    static constexpr uint64_t REPLICA_SET_SB_MAGIC = 0xABCDF00D;
    static constexpr uint32_t REPLICA_SET_SB_VERSION = 1;

    uint64_t magic{REPLICA_SET_SB_MAGIC};
    uint32_t version{REPLICA_SET_SB_VERSION};
    uuid_t uuid;                                // uuid of this replica set
    homestore::logstore_id_t free_pba_store_id; // Logstore id for storing free pba records
    homestore::logstore_id_t m_data_journal_id; // Logstore id for the data journal
    repl_lsn_t commit_lsn;                      // LSN upto which this replica has committed
    repl_lsn_t m_checkpoint_lsn;                // LSN upto which this replica have checkpointed the data

    uint64_t get_magic() const { return magic; }
    uint32_t get_version() const { return version; }
};
#pragma pack()

class HomeStateMachineStore : public StateMachineStore {
public:
    HomeStateMachineStore(uuid_t rs_uuid);
    HomeStateMachineStore(const homestore::superblk< home_rs_superblk >& rs_sb);
    virtual ~HomeStateMachineStore() = default;

    ////////////// Storage Writes of Data Blocks ///////////////////////
    pba_list_t alloc_pbas(uint32_t size) override;
    void async_write(const sisl::sg_list& sgs, pba_list_t& in_pbas, const io_completion_cb_t& cb) override;
    void async_read(pba_t pba, sisl::sg_list& sgs, uint32_t size, const io_completion_cb_t& cb) override;
    void free_pba(pba_t pba, const io_completion_cb_t& cb) override;

    //////////////////// Control operations ///////////////////////////////
    void destroy() override;

    ////////////////// State machine and free pba persistence ///////////////////
    void commit_lsn(repl_lsn_t lsn) override;
    repl_lsn_t get_last_commit_lsn() const override;
    void add_free_pba_record(repl_lsn_t lsn, const pba_list_t& pbas) override;
    void get_free_pba_records(repl_lsn_t start_lsn, repl_lsn_t end_lsn,
                              const std::function< void(repl_lsn_t, const pba_list_t&) >& cb) override;
    void remove_free_pba_records_upto(repl_lsn_t lsn) override;
    void flush_free_pba_records() override;

private:
    void on_store_created(std::shared_ptr< homestore::HomeLogStore > log_store);

private:
    std::shared_ptr< homestore::HomeLogStore > m_free_pba_store; // Logstore for storing free pba records
    homestore::superblk< home_rs_superblk > m_sb;                // Superblk where we store the state machine etc
    mutable folly::SharedMutexWritePriority m_sb_lock;           // Lock to protect staged sb and persisting sb
    home_rs_superblk m_sb_in_mem;                                // Cached version which is used to read and for staging
    std::atomic< repl_lsn_t > m_last_write_lsn{0};               // LSN which was lastly written, to track flushes
};

} // namespace home_replication
