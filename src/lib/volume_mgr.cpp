
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
#include <boost/uuid/uuid_io.hpp>
#include <iomgr/iomgr.hpp>
#include <homestore/crc.hpp>
#include "volume/volume.hpp"
#include "homeblks_impl.hpp"
#include "coro_helpers.hpp"

namespace homeblocks {

void HomeBlocksImpl::on_vol_meta_blk_found(sisl::byte_view const& buf, void* cookie) {
    auto vol_ptr = volume::make_volume(buf, cookie, volume_chunk_selector_, index_chunk_selector_);
    auto id = vol_ptr->id();

    {
        auto lg = std::scoped_lock(index_lock_);
        auto it = idx_tbl_map_.find(vol_ptr->id_str());
        DEBUG_ASSERT(it != idx_tbl_map_.end(), "index pid: {} not exists in recovery path, not expected!",
                     vol_ptr->id_str());
        vol_ptr->init_index_table(true /*is_recovery*/, it->second /* table */);

        // don't need it after volume is initialized with index table;
        idx_tbl_map_.erase(it);
    }

    {
        auto lg = std::scoped_lock(vol_lock_);
        DEBUG_ASSERT(vol_map_.find(id) == vol_map_.end(),
                     "volume id: {} already exists in recovery path, not expected!", boost::uuids::to_string(id));
        vol_map_.emplace(std::make_pair(id, vol_ptr));
        ordinal_reserver_->reserve(vol_ptr->ordinal());
    }

    if (vol_ptr->is_destroying()) {
        // resume volume destroying;
        LOGINFO("volume {} is in destroying state, resume destroy", vol_ptr->id_str());
        // fire-and-forget: remove_volume is a coroutine whose work is scheduled on a worker; start it detached.
        detail::detach(remove_volume(id));
    }
}

shared< hs_index_table_t > HomeBlocksImpl::recover_index_table(homestore::superblk< homestore::index_table_sb >&& sb) {
    auto pid_str = boost::uuids::to_string(sb->parent_uuid); // parent_uuid is the volume id
    {
        auto lg = std::scoped_lock(index_lock_);
        index_cfg_t cfg(homestore::hs()->index_service().node_size());
        cfg.m_leaf_node_type = btree_leaf_node_type;
        cfg.m_int_node_type = btree_int_node_type;

        LOGI("Recovering index table for  index_uuid: {}, parent_uuid: {}", boost::uuids::to_string(sb->uuid), pid_str);
        std::vector< chunk_num_t > chunk_ids(sb->get_chunk_ids(), sb->get_chunk_ids() + sb->index_num_chunks);
        bool success = index_chunk_selector_->recover_chunks(sb->ordinal, sb->pdev_id, sb->max_size_bytes, chunk_ids);
        if (!success) {
            LOGI("Failed to recover chunks for index table index_uuid: {}, parent_uuid: {} ordinal: {}",
                 boost::uuids::to_string(sb->uuid), pid_str, sb->ordinal);
        }
        auto tbl = std::make_shared< VolumeIndexTable >(std::move(sb), cfg);
        idx_tbl_map_.emplace(pid_str, tbl);
        return tbl->index_table();
    }
}

//
// The reason create volume needs a ref_cnt:
// 1. if graceful shutdow is received and visited volume map and check there is no volume being created.
// 2. after graceful shutdown release the vol map lock, create volume arrives and successfully take the vol map lock,
// 3. now we have a race that allow create volume to go through and graceful shutdown also happen in parallel which will
// cause crash;
//
async_result< volume_handle > HomeBlocksImpl::create_volume(volume_info vol_info) {
    if (is_restricted()) {
        LOGE("Can't serve volume create, System is in restricted mode.");
        co_return std::unexpected(std::errc::operation_not_supported);
    }

    inc_ref();
    auto id = vol_info.id;

    LOGI("[vol={}] is of capacity [{}B]", boost::uuids::to_string(id), vol_info.size_bytes);

    {
        auto lg = std::shared_lock(vol_lock_);
        vol_info.ordinal = ordinal_reserver_->reserve();
        if (vol_info.ordinal >= MAX_NUM_VOLUMES) {
            LOGE("No space to create volume with id: {}", boost::uuids::to_string(id));
            co_return std::unexpected(volume_error::INTERNAL_ERROR);
        }

        if (auto it = vol_map_.find(id); it != vol_map_.end()) {
            LOGW("create_volume with input id: {} already exists,", boost::uuids::to_string(id));
            dec_ref();
            co_return std::unexpected(std::errc::invalid_argument);
        }
    }

    auto vol_ptr = volume::make_volume(std::move(vol_info), volume_chunk_selector_, index_chunk_selector_);
    if (vol_ptr) {
        auto lg = std::scoped_lock(vol_lock_);
        vol_map_.emplace(std::make_pair(id, vol_ptr));
        LOGW("create_volume with input id: {} ordinal: {} ", boost::uuids::to_string(id), vol_ptr->info()->ordinal);
    } else {
        LOGE("failed to create volume with id: {}", boost::uuids::to_string(id));
        dec_ref();
        co_return std::unexpected(volume_error::INTERNAL_ERROR);
    }

    dec_ref();
    co_return vol_ptr;
}

//
// Why we don't need do ref_cnt for remove_volume:
// vol in destroying state already indicates an outstanding volume which consumed in no_outstanding_vols() API;
//
// Coroutine that performs the actual volume teardown on a worker reactor. It co_awaits volume::destroy()
// (which co_awaits the repl-dev removal and a forced index CP flush), so the reactor yields during those waits
// instead of parking -- which is what lets the CP flush complete. Launched via detach from remove_volume.
sisl::async::task< void > HomeBlocksImpl::do_remove_volume(volume_id_t id) {
    // 1. get the volume ptr from the map;
    volume_handle vol_ptr = nullptr;
    {
        auto lg = std::scoped_lock(vol_lock_);
        if (auto it = vol_map_.find(id); it != vol_map_.end()) {
            vol_ptr = it->second;
        } else {
            LOGWARN("volume with id {} not found, cannot remove", boost::uuids::to_string(id));
            co_return;
        }
    }

    vol_ptr->state_change(volume_state::DESTROYING);

    // if vol is already started with destroy or there is any outstanding reqs on the vol, we will not do anything
    // on this vol and let reaper thread to handle it
    if (vol_ptr->can_remove()) {
        // 2. do volume destroy;
        co_await vol_ptr->destroy();
#ifdef _PRERELEASE
        if (iomgr_flip::instance()->test_flip("vol_destroy_crash_simulation")) {
            crash_simulated_ = true;
            co_return;
        }
#endif
        // 3. remove volume from vol_map;
        {
            auto lg = std::scoped_lock(vol_lock_);
            vol_map_.erase(vol_ptr->id());
            ordinal_reserver_->unreserve(vol_ptr->info()->ordinal);
        }

        LOGINFO("volume {} ordinal={} removed successfully", vol_ptr->id_str(), vol_ptr->info()->ordinal);
    } else {
        LOGD("volume {} is in destroying state or has outstanding requests: {}, backing off and wait for GC to "
             "cleanup.",
             vol_ptr->id_str(), vol_ptr->num_outstanding_reqs());
    }
    // volume Destructor will be called after vol_ptr goes out of scope;
}

async_status HomeBlocksImpl::remove_volume(const volume_id_t& id) {
    if (is_restricted()) {
        LOGE("Can't serve volume remove, System is in restricted mode.");
        co_return std::unexpected(std::errc::operation_not_supported);
    }

    // The volume is marked DESTROYING only once do_remove_volume runs, so shutdown could miss a remove accepted now.
    if (is_shutting_down()) {
        LOGE("Can't serve volume remove, system is shutting down.");
        co_return std::unexpected(std::errc::operation_not_supported);
    }

    auto vol = get_volume(id);
    if (!vol) {
        LOGE("volume with id {} not found, cannot remove", boost::uuids::to_string(id));
        co_return std::unexpected(std::errc::invalid_argument);
    } else if ((*vol)->is_offline()) {
        LOGE("volume {} is offline, cannot remove", (*vol)->id_str());
        co_return std::unexpected(volume_error::OFFLINE);
    }

    LOGINFO("remove_volume with input id: {}", boost::uuids::to_string(id));
    // volume::destroy() co_awaits a forced CP flush (IndexTable::destroy) and the repl-dev removal, so it must
    // run as a coroutine on the worker reactor (detach) -- a blocking callable would park the reactor and the
    // flush it awaits could never run. See do_remove_volume.
    iomanager.run_on_forget(iomgr::reactor_regex::random_worker,
                            [this, id]() { detail::detach(do_remove_volume(id)); });

    co_return ok();
}

result< volume_handle > HomeBlocksImpl::get_volume(const volume_id_t& id) const {
    auto lg = std::shared_lock(vol_lock_);
    if (auto it = vol_map_.find(id); it != vol_map_.end()) { return it->second; }
    return std::unexpected(volume_error::UNKNOWN_VOLUME);
}

void HomeBlocksImpl::update_vol_sb_cb(uint64_t volume_ordinal, const std::vector< chunk_num_t >& chunk_ids) {
    volume_handle vol_ptr = nullptr;
    {
        auto lg = std::shared_lock(vol_lock_);
        for (auto it = vol_map_.begin(); it != vol_map_.end(); it++) {
            if (it->second->info()->ordinal == volume_ordinal) {
                vol_ptr = it->second;
                break;
            }
        }
    }

    RELEASE_ASSERT(vol_ptr != nullptr, "volume not found");
    vol_ptr->update_vol_sb_cb(chunk_ids);
}

result< volume_stats > HomeBlocksImpl::get_stats(volume_id_t id) const {
    auto lg = std::shared_lock(vol_lock_);
    auto it = vol_map_.find(id);
    if (it == vol_map_.end()) {
        LOGE("volume with id {} not found, cannot get stats", boost::uuids::to_string(id));
        return std::unexpected(volume_error::UNKNOWN_VOLUME);
    }

    volume_stats stats;
    it->second->get_stats(stats);
    return stats;
}

std::vector< volume_id_t > HomeBlocksImpl::volume_ids() const {
    auto lg = std::shared_lock(vol_lock_);
    std::vector< volume_id_t > vol_ids;
    vol_ids.reserve(vol_map_.size());
    for (const auto& it : vol_map_) {
        vol_ids.push_back(it.first);
    }
    return vol_ids;
}

// ======================= public data-plane: free functions over a volume_handle =======================
// (declared in homeblks/home_blocks.hpp). Each guards against a system/volume that cannot serve IO, converts the
// byte address to the volume's block granularity, builds an internal vol_interface_req, and drives
// volume::read/write. read/write resolve to the byte count transferred (sgs.size); unmap is currently a no-op.

// Shared guard. Returns the volume's block size on success, or the rejecting volume_error.
static result< uint32_t > io_precheck(volume_handle const& vol, uint64_t addr, uint64_t len, const char* op) {
    auto hb = HomeBlocksImpl::instance();
    if (hb->is_restricted()) {
        LOGE("Can't serve {}, system is in restricted mode.", op);
        return std::unexpected(std::errc::operation_not_supported);
    }
    if (vol->is_offline()) {
        LOGE("Can't serve {}, volume {} is offline.", op, vol->id_str());
        return std::unexpected(volume_error::OFFLINE);
    }
    if (vol->is_destroying() || hb->is_shutting_down()) {
        LOGE("Can't serve {}, volume {} is destroying or system is shutting down.", op, vol->id_str());
        return std::unexpected(std::errc::operation_not_supported);
    }
    auto const blk_size = vol->rd()->get_blk_size();
    if (addr % blk_size != 0 || len % blk_size != 0) {
        LOGE("Can't serve {} on volume {}: addr {} / len {} not block-aligned ({}B).", op, vol->id_str(), addr, len,
             blk_size);
        return std::unexpected(std::errc::invalid_argument);
    }
    return blk_size;
}

async_result< size_t > async_write(volume_handle const& vol, uint64_t addr, sisl::sg_list sgs) {
    auto blk_size = io_precheck(vol, addr, sgs.size, "write");
    if (!blk_size) co_return std::unexpected(blk_size.error());
    // TODO: scatter-gather (multi-iov) not yet plumbed through the write pipeline; single contiguous buffer only.
    if (sgs.iovs.size() != 1) co_return std::unexpected(std::errc::invalid_argument);
#ifdef _PRERELEASE
    if (HomeBlocksImpl::instance()->delay_fake_io(vol)) { co_return sgs.size; } // delayed; completes via flip later
#endif
    vol_io_guard guard{vol}; // marks this IO in-flight on the volume (and keeps it alive) for the op's duration
    io_req req{.buffer = static_cast< uint8_t* >(sgs.iovs[0].iov_base),
               .lba = addr / *blk_size,
               .nlbas = static_cast< lba_count_t >(sgs.size / *blk_size)};
    if (auto st = co_await vol->write(req); !st) { co_return std::unexpected(st.error()); }
    co_return sgs.size;
}

async_result< size_t > async_read(volume_handle const& vol, uint64_t addr, sisl::sg_list sgs) {
    auto blk_size = io_precheck(vol, addr, sgs.size, "read");
    if (!blk_size) co_return std::unexpected(blk_size.error());
    if (sgs.iovs.size() != 1) co_return std::unexpected(std::errc::invalid_argument);
#ifdef _PRERELEASE
    if (HomeBlocksImpl::instance()->delay_fake_io(vol)) { co_return sgs.size; }
#endif
    vol_io_guard guard{vol}; // marks this IO in-flight on the volume (and keeps it alive) for the op's duration
    io_req req{.buffer = static_cast< uint8_t* >(sgs.iovs[0].iov_base),
               .lba = addr / *blk_size,
               .nlbas = static_cast< lba_count_t >(sgs.size / *blk_size)};
    if (auto st = co_await vol->read(req); !st) { co_return std::unexpected(st.error()); }
    co_return sgs.size;
}

async_status async_unmap(volume_handle const& vol, uint64_t addr, uint64_t len) {
    LOGWARN("Unmap to vol: {} not implemented", vol->id_str());
    auto blk_size = io_precheck(vol, addr, len, "unmap");
    if (!blk_size) co_return std::unexpected(blk_size.error());
    co_return ok();
}

void HomeBlocksImpl::on_write(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                              const std::vector< homestore::multi_blk_id >& new_blkids,
                              cintrusive< homestore::repl_req_ctx >& ctx) {

    // We are not expecting log reply for a graceful restart;
    // if we are in recovery path, we must be recovering from a crash.
    DEBUG_ASSERT(ctx != nullptr || !is_graceful_shutdown(),
                 "repl ctx is null (recovery path) in graceful shutdown scenario, this is not expected!");

    repl_result_ctx< status >* repl_ctx{nullptr};
    if (ctx) { repl_ctx = boost::static_pointer_cast< repl_result_ctx< status > >(ctx).get(); }
    auto msg_header = reinterpret_cast< MsgHeader* >(const_cast< uint8_t* >(header.cbytes()));

    // Key contains the list of checksums and old blkids. Before we ack the client
    // request, we free the old blkid's. Also if its recovery we overwrite the index
    // with checksum and new blkid's. We need to overwrite index during recovery as all the
    // index writes may not be flushed to disk during crash.
    volume_handle vol_ptr{nullptr};
    auto journal_entry = reinterpret_cast< const VolJournalEntry* >(key.cbytes());
    auto key_buffer = reinterpret_cast< const uint8_t* >(journal_entry + 1);

    if (repl_ctx == nullptr) {
        // For recovery path repl_ctx and vol_ptr wont be available.
        auto lg = std::shared_lock(vol_lock_);
        auto it = vol_map_.find(msg_header->volume_id);
        RELEASE_ASSERT(it != vol_map_.end(), "Didnt find volume {}", boost::uuids::to_string(msg_header->volume_id));
        vol_ptr = it->second;

        // During log recovery overwrite new blkid and checksum to index.
        std::unordered_map< lba_t, BlockInfo > blocks_info;
        lba_t start_lba = journal_entry->start_lba;
        for (auto& blkid : new_blkids) {
            for (uint32_t i = 0; i < blkid.blk_count(); i++) {
                auto new_bid = blk_id{blkid.blk_num() + i, 1 /* nblks */, blkid.chunk_num()};
                auto csum = *reinterpret_cast< const homestore::csum_t* >(key_buffer);
                blocks_info.emplace(start_lba + i, BlockInfo{new_bid, blk_id{}, csum});
                key_buffer += sizeof(homestore::csum_t);
            }

            // We ignore the existing values we got in blocks_info from index as it will be
            // same checksum, blkid we see in the journal entry.
            lba_t end_lba = start_lba + blkid.blk_count() - 1;
            auto status = vol_ptr->indx_table()->write_to_index(start_lba, end_lba, blocks_info);
            RELEASE_ASSERT(status, "Index error during recovery");
            start_lba = end_lba + 1;
        }
    } else {
        // Avoid expensive lock during normal write flow.
        vol_ptr = repl_ctx->vol_ptr_;
        key_buffer += (journal_entry->nlbas * sizeof(homestore::csum_t));
    }

    // Free all the old blkids. This happens for both normal writes
    // and crash recovery. During recovery we also if it has been alloced,
    // because we could have stale log entries which have old blkid's
    // which may be already freed.
    for (uint32_t i = 0; i < journal_entry->num_old_blks; i++) {
        blk_id old_blkid = *reinterpret_cast< const blk_id* >(key_buffer);
        if (repl_ctx == nullptr) {
            if (homestore::hs()->data_service().is_blk_alloced(old_blkid)) {
                LOGT("volume write on commit free blk {} start_lba {}", old_blkid, journal_entry->start_lba);
                // free_blk_now is synchronous and returns a [[nodiscard]] status; this recovery-path free is
                // best-effort (matches the prior void-returning behavior), so the result is intentionally ignored.
                (void)homestore::hs()->data_service().free_blk_now(old_blkid);
            }
        } else {
            if (homestore::hs()->data_service().is_blk_alloced(old_blkid)) {
                LOGT("volume write on commit free blk {} start_lba {}", old_blkid, journal_entry->start_lba);
                // async_free_blks returns a lazy task; on_write is not a coroutine, so fire-and-forget it
                // (detach starts it) rather than dropping the un-awaited task (which would never run).
                detail::detach(vol_ptr->rd()->async_free_blks(lsn, old_blkid));
            }
        }
        key_buffer += sizeof(blk_id);
    }

#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("vol_write_crash_after_journal_write")) {
        // this is to simulate crash during write where both data and journal
        // is persisted. After recovery log entries are replayed.
        LOGINFO("volume write crash simulation flip is set, aborting");
        return;
    }
#endif

    // Deliver the journal-write completion to the awaiting volume::write coroutine;
    // value_awaitable::complete resumes it inline on this commit thread.
    if (repl_ctx) { repl_ctx->promise_.complete(ok()); }
}

#ifdef _PRERELEASE
bool HomeBlocksImpl::delay_fake_io(volume_handle v) {
    if (iomgr_flip::instance()->delay_flip("vol_fake_io_delay_simulation", [this, v]() mutable {
            LOGI("Resuming fake IO delay flip is done. Do nothing ");
            v->dec_ref();
        })) {
        LOGI("Slow down vol fake IO flip is enabled, scheduling to call later.");
        v->inc_ref();
        return true;
    }
    return false;
}
#endif

} // namespace homeblocks
