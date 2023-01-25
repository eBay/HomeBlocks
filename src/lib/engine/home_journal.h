#pragma once

#include <homestore/logstore_service.hpp>
#include "journal.h"

namespace home_replication {
class HomeLocalJournal : public Journal {
public:
    explicit HomeLocalJournal(entry_found_cb_t cb, homestore::logstore_id_t logstore_id = UINT32_MAX);
    virtual ~HomeLocalJournal() = default;

    void create_store();
    void remove_store();

    // All overridden impl methods
    void insert_async(int64_t lsn, const sisl::io_blob& blob) override;
    sisl::byte_view read_sync(int64_t lsn) override;
    void truncate(int64_t lsn) override;
    void flush_sync(int64_t upto_lsn) override;
    homestore::logstore_id_t logstore_id() const { return m_logstore_id; }

private:
    void on_store_created(std::shared_ptr< homestore::HomeLogStore > log_store);

private:
    homestore::logstore_id_t m_logstore_id;
    std::shared_ptr< homestore::HomeLogStore > m_log_store;
};

} // namespace home_replication