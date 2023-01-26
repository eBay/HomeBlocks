#include "home_journal.h"

#define LOCAL_JOURNAL_LOG(level, msg, ...)                                                                             \
    LOG##level##MOD_FMT(home_replication, ([&](fmt::memory_buffer& buf, const char* msgcb, auto&&... args) -> bool {   \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}:{}] "},                          \
                                            fmt::make_format_args(file_name(__FILE__), __LINE__));                     \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}={}] "},                          \
                                            fmt::make_format_args("localjournal", m_logstore_id));                     \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{msgcb},                               \
                                            fmt::make_format_args(std::forward< decltype(args) >(args)...));           \
                            return true;                                                                               \
                        }),                                                                                            \
                        msg, ##__VA_ARGS__);

SISL_LOGGING_DECL(home_replication)

namespace home_replication {
HomeLocalJournal::HomeLocalJournal(entry_found_cb_t cb, homestore::logstore_id_t logstore_id) : Journal{std::move(cb)} {
    if (logstore_id != UINT32_MAX) {
        LOGDEBUGMOD(home_replication, "Opening existing home log store id={}", logstore_id);
        homestore::logstore_service().open_log_store(homestore::LogStoreService::CTRL_LOG_FAMILY_IDX, logstore_id, true,
                                                     bind_this(HomeLocalJournal::on_store_created, 1));
    }
}

void HomeLocalJournal::create_store() {
    m_log_store =
        homestore::logstore_service().create_new_log_store(homestore::LogStoreService::CTRL_LOG_FAMILY_IDX, true);
    if (!m_log_store) { throw std::runtime_error("Failed to create log store"); }
    on_store_created(m_log_store);
    LOCAL_JOURNAL_LOG(DEBUG, "New logstore created");
}

void HomeLocalJournal::remove_store() {
    LOCAL_JOURNAL_LOG(DEBUG, "Logstore is being physically removed");
    homestore::logstore_service().remove_log_store(homestore::LogStoreService::CTRL_LOG_FAMILY_IDX, m_logstore_id);
    m_log_store.reset();
}

void HomeLocalJournal::on_store_created(std::shared_ptr< homestore::HomeLogStore > log_store) {
    m_log_store = log_store;
    m_logstore_id = m_log_store->get_store_id();
    m_log_store->register_log_found_cb(
        [this](int64_t lsn, homestore::log_buffer buf, [[maybe_unused]] void* ctx) { m_entry_found_cb(lsn, buf); });
    LOCAL_JOURNAL_LOG(DEBUG, "Home Log store created/opened successfully");
}

void HomeLocalJournal::insert_async(int64_t lsn, const sisl::io_blob& blob) {
    m_log_store->write_async(lsn - 1, blob, nullptr, nullptr);
}

sisl::byte_view HomeLocalJournal::read_sync(int64_t lsn) { return m_log_store->read_sync(lsn - 1); }

void HomeLocalJournal::truncate(int64_t lsn) { m_log_store->truncate(lsn - 1); }

void HomeLocalJournal::flush_sync(int64_t upto_lsn) { m_log_store->flush_sync(upto_lsn - 1); }

} // namespace home_replication