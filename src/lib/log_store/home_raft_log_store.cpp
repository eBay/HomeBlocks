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
#include "storage_engine_buffer.h"
#include <sisl/fds/utils.hpp>

using namespace homestore;

SISL_LOGGING_DECL(home_replication)

#define REPL_STORE_LOG(level, msg, ...)                                                                                \
    LOG##level##MOD_FMT(home_replication, ([&](fmt::memory_buffer& buf, const char* msgcb, auto&&... args) -> bool {   \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}:{}] "},                          \
                                            fmt::make_format_args(file_name(__FILE__), __LINE__));                     \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}={}] "},                          \
                                            fmt::make_format_args("replstore", m_logstore_id));                        \
                            fmt::vformat_to(fmt::appender{buf}, fmt::string_view{msgcb},                               \
                                            fmt::make_format_args(std::forward< decltype(args) >(args)...));           \
                            return true;                                                                               \
                        }),                                                                                            \
                        msg, ##__VA_ARGS__);

namespace home_replication {
static nuraft::ptr< nuraft::log_entry > to_nuraft_log_entry(const homestore::log_buffer& log_bytes) {
    uint8_t* raw_ptr = log_bytes.bytes();
    uint64_t term = *r_cast< uint64_t* >(raw_ptr);
    raw_ptr += sizeof(uint64_t);
    nuraft::log_val_type type = static_cast< nuraft::log_val_type >(*raw_ptr);
    raw_ptr += sizeof(uint8_t);

    size_t data_len = log_bytes.size() - sizeof(uint64_t) - sizeof(uint8_t);
    auto nb = nuraft::buffer::alloc(data_len);
    nb->put_raw(raw_ptr, data_len);
    return nuraft::cs_new< nuraft::log_entry >(term, nb, type);
}

static uint64_t extract_term(const homestore::log_buffer& log_bytes) {
    SEBufSerializer ss(SEBuf{log_bytes.size(), log_bytes.bytes()});
    return ss.getU64();
}

HomeRaftLogStore::HomeRaftLogStore(homestore::logstore_id_t logstore_id) {
    m_dummy_log_entry = nuraft::cs_new< nuraft::log_entry >(0, nuraft::buffer::alloc(0), nuraft::log_val_type::app_log);

    if (logstore_id != UINT32_MAX) {
        LOGDEBUGMOD(home_replication, "Opening existing home log store id={}", logstore_id);
        logstore_service().open_log_store(homestore::LogStoreService::DATA_LOG_FAMILY_IDX, logstore_id, true,
                                          bind_this(HomeRaftLogStore::on_store_created, 1));
    }
}

void HomeRaftLogStore::create_store() {
    m_log_store = logstore_service().create_new_log_store(homestore::LogStoreService::DATA_LOG_FAMILY_IDX, true);
    if (!m_log_store) { throw std::runtime_error("Failed to create log store"); }
    on_store_created(m_log_store);
    REPL_STORE_LOG(DEBUG, "New logstore created");
}

void HomeRaftLogStore::remove_store() {
    REPL_STORE_LOG(DEBUG, "Logstore is being physically removed");
    logstore_service().remove_log_store(homestore::LogStoreService::DATA_LOG_FAMILY_IDX, m_logstore_id);
    m_log_store.reset();
}

void HomeRaftLogStore::on_store_created(std::shared_ptr< HomeLogStore > log_store) {
    m_log_store = log_store;
    m_logstore_id = m_log_store->get_store_id();
    REPL_STORE_LOG(DEBUG, "Home Log store created/opened successfully");
}

ulong HomeRaftLogStore::next_slot() const {
    // Since logstore tracks from 0, raft logstore tracks from 1, we need to increment by 2
    uint64_t next_slot = m_log_store->get_contiguous_issued_seq_num(-1) + 2;
    REPL_STORE_LOG(DEBUG, "next_slot()={}", next_slot);
    return next_slot;
}

ulong HomeRaftLogStore::start_index() const {
    // start_index starts from 1.
    ulong start_index = std::max((int64_t)1, m_log_store->truncated_upto() + 2);
    REPL_STORE_LOG(DEBUG, "start_index()={}", start_index);
    return start_index;
}

nuraft::ptr< nuraft::log_entry > HomeRaftLogStore::last_entry() const {
    int64_t max_seq = m_log_store->get_contiguous_issued_seq_num(-1);
    REPL_STORE_LOG(DEBUG, "last_entry() seqnum={}", max_seq);
    if (max_seq == -1) { return m_dummy_log_entry; }

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

    nuraft::ptr< nuraft::buffer > entry_buf = entry->serialize();
    auto next_seq = m_log_store->append_async(
        sisl::io_blob{entry_buf->data_begin(), uint32_cast(entry_buf->size()), false /* is_aligned */},
        nullptr /* cookie */, [entry_buf](int64_t, sisl::io_blob&, homestore::logdev_key, void*) {});
    return next_seq + 1;
}

void HomeRaftLogStore::write_at(ulong index, nuraft::ptr< nuraft::log_entry >& entry) {
    m_log_store->rollback_async(index - 2, nullptr);
    append(entry);
}

void HomeRaftLogStore::end_of_append_batch([[maybe_unused]] ulong start, [[maybe_unused]] ulong cnt) {
    // TODO: Check if we need to trigger a device flush
}

nuraft::ptr< std::vector< nuraft::ptr< nuraft::log_entry > > > HomeRaftLogStore::log_entries(ulong start, ulong end) {
    auto out_vec = std::make_shared< std::vector< nuraft::ptr< nuraft::log_entry > > >();
    m_log_store->foreach (start - 1, [end, &out_vec](int64_t cur, const homestore::log_buffer& entry) -> bool {
        bool ret = (cur < int64_cast(end - 2));
        if (cur < int64_cast(end - 1)) { out_vec->emplace_back(to_nuraft_log_entry(entry)); }
        return ret;
    });
    return out_vec;
}

nuraft::ptr< nuraft::log_entry > HomeRaftLogStore::entry_at(ulong index) {
    nuraft::ptr< nuraft::log_entry > nle;
    try {
        auto log_bytes = m_log_store->read_sync(index - 1);
        nle = to_nuraft_log_entry(log_bytes);
    } catch (const std::exception& e) {
        REPL_STORE_LOG(ERROR, "entry_at({}) index out_of_range", index);
        throw e;
    }
    return nle;
}

ulong HomeRaftLogStore::term_at(ulong index) {
    ulong term;
    try {
        auto log_bytes = m_log_store->read_sync(index - 1);
        term = extract_term(log_bytes);
    } catch (const std::exception& e) {
        REPL_STORE_LOG(ERROR, "term_at({}) index out_of_range", index);
        throw e;
    }
    return term;
}

nuraft::ptr< nuraft::buffer > HomeRaftLogStore::pack(ulong index, int32_t cnt) {
    static constexpr size_t estimated_record_size = 128;
    size_t estimated_size = cnt * estimated_record_size + sizeof(uint32_t);

    //   << Format >>
    // # records (N)        4 bytes
    // +---
    // | log length (X)     4 bytes
    // | log data           X bytes
    // +--- repeat N
    nuraft::ptr< nuraft::buffer > out_buf = nuraft::buffer::alloc(estimated_size);
    out_buf->put(cnt);

    int32_t remain_cnt = cnt;
    m_log_store->foreach (int64_cast(index - 1),
                          [this, &out_buf, &remain_cnt]([[maybe_unused]] int64_t cur,
                                                        const homestore::log_buffer& entry) mutable -> bool {
                              if (remain_cnt-- > 0) {
                                  size_t avail_size = out_buf->size() - out_buf->pos();
                                  if (avail_size < entry.size()) {
                                      avail_size += std::max(out_buf->size() * 2, (size_t)entry.size());
                                      out_buf = nuraft::buffer::expand(*out_buf, avail_size);
                                  }
                                  REPL_STORE_LOG(TRACE, "packing lsn={} of size={}, avail_size in buffer={}", cur + 1,
                                                 entry.size(), avail_size);
                                  out_buf->put(entry.bytes(), entry.size());
                              }
                              return (remain_cnt > 0);
                          });
    return out_buf;
}

void HomeRaftLogStore::apply_pack(ulong index, nuraft::buffer& pack) {
    pack.pos(0);
    auto num_entries = pack.get_int();

    auto slot = next_slot();
    if (index < slot) {
        // We are asked to apply/insert data behind next slot, so we must rollback before index and then append
        m_log_store->rollback_async(index - 2, nullptr);
    } else if (index > slot) {
        // We are asked to apply/insert data after next slot, so we need to fill in with dummy entries upto the slot
        // before append the entries
        REPL_STORE_LOG(WARN,
                       "RaftLogStore is asked to apply pack on lsn={}, but current lsn={} is behind, will be filling "
                       "with dummy data to make it functional, however, this could result in inconsistent data",
                       index, slot - 1);
        while (index++ < slot) {
            append(m_dummy_log_entry);
        }
    }

    for (int i{0}; i < num_entries; ++i) {
        size_t entry_len;
        auto* entry = const_cast< nuraft::byte* >(pack.get_bytes(entry_len));
        [[maybe_unused]] auto store_sn =
            m_log_store->append_async(sisl::io_blob{entry, uint32_cast(entry_len), false}, nullptr, nullptr);
        REPL_STORE_LOG(TRACE, "unpacking nth_entry={} of size={}, lsn={}", i + 1, entry_len, store_sn + 1);
    }
    m_log_store->flush_sync(index + num_entries - 2);
}

bool HomeRaftLogStore::compact(ulong compact_lsn) {
    auto cur_max_lsn = m_log_store->get_contiguous_issued_seq_num(-1);
    if (cur_max_lsn < int64_cast(compact_lsn - 1)) {
        // We need to fill the remaining entries with dummy data.
        for (auto lsn{cur_max_lsn + 1}; lsn <= int64_cast(compact_lsn - 1); ++lsn) {
            append(m_dummy_log_entry);
        }
    }
    m_log_store->flush_sync(compact_lsn - 1);
    m_log_store->truncate(compact_lsn - 1);
    return true;
}

bool HomeRaftLogStore::flush() {
    m_log_store->flush_sync();
    return true;
}

} // namespace home_replication