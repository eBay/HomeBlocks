#include "home_storage_engine.h"

#define STATE_MACHINE_LOG(level, msg, ...)                                                                             \
    LOG##level##MOD_FMT(home_replication, ([&](fmt::memory_buffer& buf, const char* msgcb, auto&&... args) -> bool {   \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}:{}] "},                          \
                                            fmt::make_format_args(file_name(__FILE__), __LINE__));                     \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}={}] "},                          \
                                            fmt::make_format_args("rsengine", m_logstore_id));                         \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{msgcb},                               \
                                            fmt::make_format_args(std::forward< decltype(args) >(args)...));           \
                            return true;                                                                               \
                        }),                                                                                            \
                        msg, ##__VA_ARGS__);

SISL_LOGGING_DECL(home_replication)

namespace home_replication {
///////////////////////////// HomeReplicationServiceImpl Section ////////////////////////////
HomeReplicationServiceImpl::HomeReplicationServiceImpl() : {
    homestore::meta_service().register_handler(
        "replica_set",
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            rs_super_blk_found(std::move(buf), voidptr_cast(mblk));
        },
        nullptr);
}

void HomeReplicationServiceImpl::rs_super_blk_found(const sisl::byte_view& buf, void* meta_cookie) {
    homestore::superblk< home_rs_superblk > rs_sb;
    rs_sb.load(buf, meta_cookie);
    HS_REL_ASSERT_EQ(m_sb->get_magic(), rs_superblk::REPLICA_SET_SB_MAGIC, "Invalid rs metablk, magic mismatch");
    HS_REL_ASSERT_EQ(m_sb->get_version(), rs_superblk::REPLICA_SET_SB_MAGIC, "Invalid version of rs metablk");

    auto sms = std::make_shared< HomeStateMachineStore >(rs_sb);
    {
        std::unique_lock lg(m_sm_store_map_mtx);
        m_sm_store_map.insert(std::make_pair(rs_sb->uuid, sms));
    }
}

sm_store_ptr_t HomeReplicationServiceImpl::lookup_state_store(uuid_t uuid) {
    std::unique_lock lg(m_sm_store_map_mtx);
    auto it = m_sm_store_map.find(uuid);
    return (it == m_sm_store_map.end() ? nullptr : it->second);
}

sm_store_ptr_t HomeReplicationServiceImpl::create_state_store(uuid_t uuid) {
    if (look_state_store(uuid) != nullptr) {
        assert(0);
        LOGDEBUGMOD(home_replication, "Attempting to create a state machine with already existing uuid={}", uuid);
    }

    auto sms = std::make_shared< HomeStateMachineStore >(uuid);
    std::unique_lock lg(m_sm_store_map_mtx);
    m_sm_store_map.insert(std::make_pair(rs_sb->uuid, sms));
    return sms;
}

void HomeReplicationServiceImpl::iterate_state_stores(const std::function< void(const sm_store_ptr_t&) >& cb) {
    std::unique_lock lg(m_sm_store_map_mtx);
    for (const auto& [uuid, sms] : m_sm_store_map) {
        cb(sms);
    }
}

///////////////////////////// HomeStateMachineStore Section ////////////////////////////
void HomeStateMachineStore::HomeStateMachineStore(uuid_t rs_uuid) {
    LOGDEBUGMOD(home_replication, "Creating new instance of replica state machine store for uuid={}", rs_uuid);

    // Create a superblk for the replica set.
    m_sb.create();
    m_sb->uuid = rs_uuid;

    // Create logstore to store the free pba records
    m_free_pba_store =
        homestore::logstore_service().create_new_log_store(homestore::LogStoreService::CTRL_LOG_FAMILY_IDX, true);
    if (!m_free_pba_store) { throw std::runtime_error("Failed to create log store"); }
    m_sb->free_pba_store_id = m_free_pba_store->get_store_id();
    m_sb.write();
    STATE_MACHINE_LOG(DEBUG, "New free pba record logstore={} created", m_sb->free_pba_store_id);
}

HomeStateMachineStore::HomeStateMachineStore(const home_rs_superblk& rs_sb) {
    LOGDEBUGMOD(home_replication, "Opening existing replica state machine store for uuid={}", rs_sb->uuid);
    m_sb = rs_sb;

    STATE_MACHINE_LOG(DEBUG, "Opening free pba record logstore={}", m_sb->free_pba_store_id);
    logstore_service().open_log_store(homestore::LogStoreService::DATA_LOG_FAMILY_IDX, m_sb->free_pba_store_id, true,
                                      bind_this(HomeStateMachineStore::on_store_created, 1));
}

void HomeStateMachineStore::on_store_created(std::shared_ptr< homestore::HomeLogStore > free_pba_store) {
    assert(m_sb->free_pba_store_id == free_pba_store->get_store_id());
    m_free_pba_store = free_pba_store;
    // m_free_pba_store->register_log_found_cb(
    //     [this](int64_t lsn, homestore::log_buffer buf, [[maybe_unused]] void* ctx) { m_entry_found_cb(lsn, buf); });
    STATE_MACHINE_LOG(DEBUG, "Successfully opened free pba record logstore={}", m_sb->free_pba_store_id);
}

void HomeStateMachineStore::destroy() {
    STATE_MACHINE_LOG(DEBUG, "Free pba record logstore={} is being physically removed", m_sb->free_pba_store_id);
    homestore::logstore_service().remove_log_store(homestore::LogStoreService::CTRL_LOG_FAMILY_IDX,
                                                   m_sb->free_pba_store_id);
    m_free_pba_store.reset();
}

void HomeStateMachineStore::add_free_pba_record(DirtySession* ds, int64_t lsn, const pba_list_t& pbas) {
    // Serialize it as
    // # num pbas (N)       4 bytes
    // +---
    // | PBA                8 bytes
    // +--- repeat N
    uint32_t size_needed = sizeof(uint32_t) + (pba.size() * sizeof(pba_t));
    sisl::io_blob b{size_needed, 0 /* unaligned */};
    *(r_cast< uint32_t* > b.bytes) = uint32_cast(pbas.size());

    pba_t* raw_ptr = r_cast< pba_t* >(b.bytes + sizeof(uint32_t));
    for (const auto pba : pbas) {
        *raw_ptr = pba;
        ++raw_ptr;
    }
    m_free_pba_store->write_async(lsn - 1, b, nullptr,
                                  [](int64_t, sisl::io_blob& b, logdev_key, void*) { b.buf_free(); });
}

pba_list_t HomeStateMachineStore::get_free_pba_record(int64_t lsn) {
    pba_list_t plist;

    auto bview = m_free_pba_store->read_sync(lsn - 1);
    uint32_t num_pbas = *(r_cast< uint32_t* >(bview.bytes()));
    pba_t* raw_ptr = r_cast< pba_t* >(bview.bytes() + sizeof(uint32_t));

    for (uint32_t i{0}; i < num_pbas; ++i) {
        plist.push_back(*raw_ptr);
        ++raw_ptr;
    }

    return plist;
}

void HomeStateMachineStore::remove_free_pba_records_upto(DirtySession* ds, int64_t lsn) {
    m_free_pba_store->truncate(lsn - 1);
}

void HomeLocalJournal::flush_sync(int64_t upto_lsn) { m_log_store->flush_sync(upto_lsn - 1); }

} // namespace home_replication