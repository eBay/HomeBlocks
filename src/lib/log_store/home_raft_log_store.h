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
#pragma once

#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif
#include <libnuraft/nuraft.hxx>
#if defined __clang__ or defined __GNUC__
#pragma GCC diagnostic pop
#endif
#undef auto_lock

#include <homestore/logstore_service.hpp>

namespace home_replication {

class HomeRaftLogStore : public nuraft::log_store {
public:
    static void removeLogStore(homestore::logstore_id_t logstore_id);

    explicit HomeRaftLogStore(homestore::logstore_id_t logstore_id = UINT32_MAX);
    virtual ~HomeRaftLogStore();

    /**
     * The first available slot of the store, starts with 1.
     *
     * @return Last log index number + 1
     */
    virtual ulong next_slot() const override;

    /**
     * The start index of the log store, at the very beginning, it must be 1.
     * However, after some compact actions, this could be anything
     * greater or equals to one.
     *
     * @return Starting log index number.
     */
    virtual ulong start_index() const override;

    /**
     * The last log entry in store.
     *
     * @return If no log entry exists: a dummy constant entry with
     *         value set to null and term set to zero.
     */
    virtual ptr< nuraft::log_entry > last_entry() const override;

    /**
     * Append a log entry to store
     *
     * @param entry Log entry
     * @return Log index number.
     */
    virtual ulong append(ptr< nuraft::log_entry >& entry) override;

    /**
     * Overwrite a log entry at the given `index`.
     *
     * @param index Log index number to overwrite.
     * @param entry New log entry to overwrite.
     */
    virtual void write_at(ulong index, ptr< nuraft::log_entry >& entry) override;

    /**
     * Invoked after a batch of logs is written as a part of
     * a single append_entries request.
     *
     * @param start The start log index number (inclusive)
     * @param cnt The number of log entries written.
     */
    virtual void end_of_append_batch(ulong start, ulong cnt) override;

    /**
     * Get log entries with index [start, end).
     *
     * @param start The start log index number (inclusive).
     * @param end The end log index number (exclusive).
     * @return The log entries between [start, end).
     */
    virtual ptr< std::vector< ptr< nuraft::log_entry > > > log_entries(ulong start, ulong end) override;

    /**
     * Get the log entry at the specified log index number.
     *
     * @param index Should be equal to or greater than 1.
     * @return The log entry or null if index >= this->next_slot().
     */
    virtual ptr< nuraft::log_entry > entry_at(ulong index) override;

    /**
     * Get the term for the log entry at the specified index
     * Suggest to stop the system if the index >= this->next_slot()
     *
     * @param index Should be equal to or greater than 1.
     * @return The term for the specified log entry, or
     *         0 if index < this->start_index().
     */
    virtual ulong term_at(ulong index) override;

    /**
     * Pack cnt log items starts from index
     *
     * @param index The start log index number (inclusive).
     * @param cnt The number of logs to pack.
     * @return log pack
     */
    virtual ptr< buffer > pack(ulong index, int32 cnt) override;

    /**
     * Apply the log pack to current log store, starting from index.
     *
     * @param index The start log index number (inclusive).
     * @param pack
     */
    virtual void apply_pack(ulong index, buffer& pack);

    /**
     * Compact the log store by purging all log entries,
     * including the log at the last_log_index.
     *
     * If current max log idx is smaller than given `last_log_index`,
     * set start log idx to `last_log_index + 1`.
     *
     * @param last_log_index Log index number that will be purged up to (inclusive).
     * @return True on success.
     */
    virtual bool compact(ulong last_log_index) override;

    /**
     * Synchronously flush all log entries in this log store to the backing storage
     * so that all log entries are guaranteed to be durable upon process crash.
     *
     * @return `true` on success.
     */
    virtual bool flush() override;

private:
    homestore::logstore_id_t m_logstore_id;
    std::shared_ptr< homestore::HomeLogStore > m_home_log_store;
};
} // namespace home_replication