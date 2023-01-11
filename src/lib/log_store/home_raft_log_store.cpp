/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/

#include "home_raft_log_store.h"

using namespace homestore;

#define REPL_STORE_LOG(level, msg, ...)                                                                                \
    LOG##level##MOD_FMT(home_replication, ([&](fmt::memory_buffer& buf, const char* msgcb, auto&&... args) -> bool {   \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}:{}] "},                          \
                                            fmt::make_format_args(file_name(__FILE__), __LINE__));                     \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}={}] "},                          \
                                            fmt::make_format_args(replstore, m_logstore_id));                          \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{msgcb},                               \
                                            fmt::make_format_args(std::forward< decltype(args) >(args)...));           \
                            return true;                                                                               \
                        }),                                                                                            \
                        msg, ##__VA_ARGS__);

namespace home_replication {
static nuraft::ptr< nuraft::log_entry > to_nuraft_log_entry(const logbuffer& log_bytes) {
    nukv::SEBufSerializer ss(nukv::SEBuf{log_bytes.bytes(), log_bytes.size()});

    // Read the raw log entry
    uint64_t term = ss.getU64();
    nuraft::log_val_type type = static_cast< log_val_type >(ss.getU8());
    nukv::SEBuf data = ss.getSEBuf();

    // Alloc a buffer and then copy to nuraft::log_entry
    nuraft::buffer nb = nuraft::buffer::alloc(data.len);
    nb->pos(0);
    memcpy(nb->data(), data.buf, data.len);

    return nuraft::cs_new< nuraft::log_entry >(term, nb, type);
}

HomeRaftLogStore::HomeRaftLogStore(homestore::logstore_id_t logstore_id) {
    if (logstore_id == UINT32_MAX) {
        m_home_log_store = logstore_service().create_new_log_store(LogStoreService::DATA_LOG_FAMILY_IDX, true);
        if (!m_home_log_store) throw std::runtime_error("Failed to create log store");
        m_logstore_id = m_home_log_store->get_store_id();
        REPL_STORE_LOG(DEBUG, "New logstore created");
    } else {
        LOGDEBUGMOD(home_replication, "Opening existing home log store id={}", logstore_id);
        logstore_service().open_log_store(LogStoreService::DATA_LOG_FAMILY_IDX, logstore_id, true,
                                          [this](std::shared_ptr< HomeLogStore > log_store) {
                                              m_home_log_store = log_store;
                                              m_logstore_id = m_home_log_store->get_store_id();
                                              REPL_STORE_LOG(DEBUG, "Home Log store opened successfully");
                                          });
    }
}

HomeRaftLogStore::~HomeRaftLogStore() {}

ulong HomeRaftLogStore::next_slot() const {
    uint64_t next_slot = m_home_log_store->get_contiguous_issued_seq_num(0) + 1;
    REPL_STORE_LOG(DEBUG, "next_slot()={}", next_slot);
    return next_slot;
}

ulong HomeRaftLogStore::start_index() const {
    // start_index starts from 1.
    ulong start_index = std::max((int64_t)1, m_home_log_store->truncated_upto() + 1);
    REPL_STORE_LOG(DEBUG, "start_index()={}", start_index);
    return start_index;
}

ptr< nuraft::log_entry > HomeRaftLogStore::last_entry() const {
    uint64_t max_seq = m_home_log_store->get_contiguous_completed_seq_num(0);
    REPL_STORE_LOG(DEBUG, "last_entry() seqnum={}", max_seq);
    if (max_seq == 0) return dummyLogEntry;

    nuraft::ptr< nuraft::log_entry > nle;
    try {
        auto log_bytes = m_log_store->read_sync(max_seq);
        nle = to_nuraft_log_entry(log_bytes);
    } catch (const std::exception& e) {
        REPL_STORE_LOG(ERROR, "last_entry() out_of_range={}", max_seq);
        throw e;
    }

    return nle;
}

ulong HomeRaftLogStore::append(nuraft::ptr< nuraft::log_entry >& entry) {
    REPL_STORE_LOG(TRACE, "append entry term={}, log_val_type={} size={}", entry->get_term(), entry->get_val_type(),
                   entry->get_buf().size());

    auto next_seq = m_log_store->append_async(
        sisl::io_blob{entry->get_buf_ptr()->data_begin(), entry->get_buf_ptr()->size(), false /* is_aligned */},
        nullptr /* cookie */, nullptr /* comp_cb */);
    return next_seq;
}

void HomeRaftLogStore::write_at(ulong index, ptr< nuraft::log_entry >& entry) {
    m_log_store->rollback_async(index - 1, nullptr);
    append(entry);
}

void HomeRaftLogStore::end_of_append_batch(ulong start, ulong cnt) {}

} // namespace home_replication