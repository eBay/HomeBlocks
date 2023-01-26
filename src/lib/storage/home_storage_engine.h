#pragma once

#include <homestore/logstore_service.hpp>
#include "storage_engine.h"

namespace home_replication {
#pragma pack(1)
struct home_rs_superblk {
    uuid_t uuid;                                // uuid of this replica set
    homestore::logstore_id_t free_pba_store_id; // Logstore id for storing free pba records
    rs_superblk m_base;                         // Base class superblk information for this replica set
};
#pragma pack()

typedef std::shared_ptr< StateMachineStore > sm_store_ptr_t;

class HomeStateMachineStore;
class HomeReplicationServiceImpl : public ReplicationServiceImpl {
public:
    HomeReplicationServiceImpl();
    virtual ~HomeReplicationServiceImpl() = default;

    sm_store_ptr_t lookup_state_store(uuid_t uuid) override;
    sm_store_ptr_t create_state_store(uuid_t uuid) override;
    void iterate_state_stores(const std::function< void(const sm_store_ptr_t&) >& cb) override;

private:
    void rs_super_blk_found(const sisl::byte_view& buf, void* meta_cookie);

private:
    std::mutex m_sm_store_map_mtx;
    std::unordered_map< uuid_t, std::shared_ptr< HomeStateMachineStore > > m_sm_store_map;
};

class HomeStateMachineStore : public StateMachineStore {
public:
    HomeStateMachineStore(uuid_t rs_uuid);
    HomeStateMachineStore(const home_rs_superblk& rs_sb);
    virtual ~HomeStateMachineStore() = default;

    //////////////// All overridden impl methods ///////////////////////
    // Storage Writes of Data Blocks
    virtual pba_list_t alloc_pbas(uint32_t size) = 0;
    virtual void async_write(const sg_list& sgs, const pba_list_t& in_pbas, const io_completion_cb_t& cb) = 0;
    virtual void async_read(pba_t pba, sg_list& sgs, uint32_t size, const io_completion_cb_t& cb) = 0;

    // State machine and free pba persistence
    std::unique_ptr< DirtySession > create_dirty_session() override;
    DirtySession* get_current_dirty_session() override;
    void add_free_pba_record(DirtySession* ds, int64_t lsn, const pba_list_t& pbas) override;
    void get_free_pba_records(int64_t from_lsn, int64_t to_lsn,
                              const std::function< void(int64_t lsn, const pba_list_t& pba) >& cb) override;
    void remove_free_pba_records_upto(DirtySession* ds, int64_t lsn) override;
    void flush_dirty_session(DirtySession* ds) override;

    // Control operations
    void destroy() override;

private:
    void on_store_created(std::shared_ptr< homestore::HomeLogStore > log_store);

private:
    std::shared_ptr< homestore::HomeLogStore > m_free_pba_store; // Logstore for storing free pba records
    homestore::superblk< home_rs_superblk > m_sb;                // Superblk where we store the state machine etc
};

} // namespace home_replication
