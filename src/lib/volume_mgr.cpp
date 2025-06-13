
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
#include <homestore/crc.h>
#include "volume/volume.hpp"
#include "homeblks_impl.hpp"

namespace homeblocks {
std::shared_ptr< VolumeManager > HomeBlocksImpl::volume_manager() { return shared_from_this(); }

void HomeBlocksImpl::on_vol_meta_blk_found(sisl::byte_view const& buf, void* cookie) {
    auto vol_ptr = Volume::make_volume(buf, cookie);
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
    }

    if (vol_ptr->is_destroying()) {
        // resume volume destroying;
        LOGINFO("Volume {} is in destroying state, resume destroy", vol_ptr->id_str());
        remove_volume(id);
    }
}

shared< VolumeIndexTable > HomeBlocksImpl::recover_index_table(homestore::superblk< homestore::index_table_sb >&& sb) {
    auto pid_str = boost::uuids::to_string(sb->parent_uuid); // parent_uuid is the volume id
    {
        auto lg = std::scoped_lock(index_lock_);
        index_cfg_t cfg(homestore::hs()->index_service().node_size());
        cfg.m_leaf_node_type = homestore::btree_node_type::PREFIX;
        cfg.m_int_node_type = homestore::btree_node_type::FIXED;

        LOGI("Recovering index table for  index_uuid: {}, parent_uuid: {}", boost::uuids::to_string(sb->uuid), pid_str);
        auto tbl = std::make_shared< VolumeIndexTable >(std::move(sb), cfg);
        idx_tbl_map_.emplace(pid_str, tbl);
        return tbl;
    }
}

//
// The reason create volume needs a ref_cnt:
// 1. if graceful shutdow is received and visited volume map and check there is no volume being created.
// 2. after graceful shutdown release the vol map lock, create volume arrives and successfully take the vol map lock,
// 3. now we have a race that allow create volume to go through and graceful shutdown also happen in parallel which will
// cause crash;
//
VolumeManager::NullAsyncResult HomeBlocksImpl::create_volume(VolumeInfo&& vol_info) {
    inc_ref();
    auto id = vol_info.id;
    LOGI("[vol={}] is of capacity [{}B]", boost::uuids::to_string(id), vol_info.size_bytes);

    {
        auto lg = std::shared_lock(vol_lock_);
        if (auto it = vol_map_.find(id); it != vol_map_.end()) {
            LOGW("create_volume with input id: {} already exists,", boost::uuids::to_string(id));
            dec_ref();
            return folly::makeUnexpected(VolumeError::INVALID_ARG);
        }
    }

    auto vol_ptr = Volume::make_volume(std::move(vol_info));
    if (vol_ptr) {
        auto lg = std::scoped_lock(vol_lock_);
        vol_map_.emplace(std::make_pair(id, vol_ptr));
    } else {
        LOGE("failed to create volume with id: {}", boost::uuids::to_string(id));
        dec_ref();
        return folly::makeUnexpected(VolumeError::INTERNAL_ERROR);
    }

    dec_ref();
    return folly::Unit();
}

//
// Why we don't need do ref_cnt for remove_volume:
// vol in destroying state already indicates an outstanding volume which consumed in no_outstanding_vols() API;
//
VolumeManager::NullAsyncResult HomeBlocksImpl::remove_volume(const volume_id_t& id) {
    iomanager.run_on_forget(iomgr::reactor_regex::random_worker, [this, id]() {
        LOGINFO("remove_volume with input id: {}", boost::uuids::to_string(id));
        // 1. get the volume ptr from the map;
        VolumePtr vol_ptr = nullptr;
        {
            auto lg = std::scoped_lock(vol_lock_);
            if (auto it = vol_map_.find(id); it != vol_map_.end()) {
                vol_ptr = it->second;
            } else {
                LOGWARN("Volume with id {} not found, cannot remove", boost::uuids::to_string(id));
                return folly::Unit();
            }
        }

        vol_ptr->state_change(vol_state::DESTROYING);

        // if vol is already started with destroy or there is any outstanding reqs on the vol, we will not do anything
        // on this vol and let reaper thread to handle it
        if (vol_ptr->can_remove()) {
            // 2. do volume destroy;
            vol_ptr->destroy();
#ifdef _PRERELEASE
            if (iomgr_flip::instance()->test_flip("vol_destroy_crash_simulation")) {
                crash_simulated_ = true;
                return folly::Unit();
            }
#endif
            // 3. remove volume from vol_map;
            {
                auto lg = std::scoped_lock(vol_lock_);
                vol_map_.erase(vol_ptr->id());
            }

            LOGINFO("Volume {} removed successfully", vol_ptr->id_str());
        } else {
            if (vol_ptr) {
                LOGD("Volume {} is in destroying state or has outstanding requests: {}, backing off and wait for GC to "
                     "cleanup.",
                     vol_ptr->id_str(), vol_ptr->num_outstanding_reqs());
            } else {
                LOGWARN("Volume with id {} not found, cannot remove", boost::uuids::to_string(id));
            }
        }
        // Volume Destructor will be called after vol_ptr goes out of scope;
        return folly::Unit();
    });

    return folly::Unit();
}

VolumePtr HomeBlocksImpl::lookup_volume(const volume_id_t& id) {
    auto lg = std::shared_lock(vol_lock_);
    if (auto it = vol_map_.find(id); it != vol_map_.end()) { return it->second; }
    return nullptr;
}

bool HomeBlocksImpl::get_stats(volume_id_t id, VolumeStats& stats) const { return true; }

void HomeBlocksImpl::get_volume_ids(std::vector< volume_id_t >& vol_ids) const {}

VolumeManager::NullAsyncResult HomeBlocksImpl::write(const VolumePtr& vol, const vol_interface_req_ptr& req) {
    if (vol->is_destroying() || is_shutting_down()) {
        LOGE(
            "Can't serve write, Volume {} is_destroying: {} is either in destroying state or System is shutting down. ",
            vol->id_str(), vol->is_destroying());
        return folly::makeUnexpected(VolumeError::UNSUPPORTED_OP);
    }

    req->back_ref(vol);
#ifdef _PRERELEASE
    if (delay_fake_io(vol)) {
        // If we are delaying IO, we return immediately without calling vol->write
        // and let the delay flip handle the completion later.
        return folly::Unit();
    }
#endif
    return vol->write(req);
}

VolumeManager::NullAsyncResult HomeBlocksImpl::read(const VolumePtr& vol, const vol_interface_req_ptr& req) {
    if (vol->is_destroying() || is_shutting_down()) {
        LOGE("Can't serve read, Volume {} is_destroying: {} is either in destroying state or System is shutting down. ",
             vol->id_str(), vol->is_destroying());
        return folly::makeUnexpected(VolumeError::UNSUPPORTED_OP);
    }

    req->back_ref(vol);
#ifdef _PRERELEASE
    if (delay_fake_io(vol)) {
        // If we are delaying IO, we return immediately without calling vol->read
        // and let the delay flip handle the completion later.
        return folly::Unit();
    }
#endif
    return vol->read(req);
}

VolumeManager::NullAsyncResult HomeBlocksImpl::unmap(const VolumePtr& vol, const vol_interface_req_ptr& req) {
    LOGWARN("Unmap to vol: {} not implemented", vol->id_str());

    if (vol->is_destroying() || is_shutting_down()) {
        LOGE(
            "Can't serve unmap, Volume {} is_destroying: {} is either in destroying state or System is shutting down. ",
            vol->id_str(), vol->is_destroying());
        return folly::makeUnexpected(VolumeError::UNSUPPORTED_OP);
    }

    return folly::Unit();
}

//
// we have to allow submit_io_batch even though a volume is in destroying state, because destroy relies on outstanding
// IOs to decrease to zero to proceed, e.g. submit_io_batch will allow outstanding io to complete;
//
void HomeBlocksImpl::submit_io_batch() { homestore::data_service().submit_io_batch(); }

void HomeBlocksImpl::on_write(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                              const std::vector< homestore::MultiBlkId >& new_blkids,
                              cintrusive< homestore::repl_req_ctx >& ctx) {
    repl_result_ctx< VolumeManager::NullResult >* repl_ctx{nullptr};
    if (ctx) { repl_ctx = boost::static_pointer_cast< repl_result_ctx< VolumeManager::NullResult > >(ctx).get(); }
    auto msg_header = r_cast< MsgHeader* >(const_cast< uint8_t* >(header.cbytes()));

    // Key contains the list of checksums and old blkids. Before we ack the client
    // request, we free the old blkid's. Also if its recovery we overwrite the index
    // with checksum and new blkid's. We need to overwrite index during recovery as all the
    // index writes may not be flushed to disk during crash.
    VolumePtr vol_ptr{nullptr};
    auto journal_entry = r_cast< const VolJournalEntry* >(key.cbytes());
    auto key_buffer = r_cast< const uint8_t* >(journal_entry + 1);

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
                auto new_bid = BlkId{blkid.blk_num() + i, 1 /* nblks */, blkid.chunk_num()};
                auto csum = *r_cast< const homestore::csum_t* >(key_buffer);
                blocks_info.emplace(start_lba + i, BlockInfo{new_bid, BlkId{}, csum});
                key_buffer += sizeof(homestore::csum_t);
            }

            // We ignore the existing values we got in blocks_info from index as it will be
            // same checksum, blkid we see in the journal entry.
            lba_t end_lba = start_lba + blkid.blk_count() - 1;
            auto status = vol_ptr->write_to_index(start_lba, end_lba, blocks_info);
            RELEASE_ASSERT(status, "Index error during recovery");
            start_lba = end_lba + 1;
        }
    } else {
        // Avoid expensive lock during normal write flow.
        vol_ptr = repl_ctx->vol_ptr_;
        key_buffer += (journal_entry->nlbas * sizeof(homestore::csum_t));
    }

    // Free all the old blkids. This happens for both normal writes
    // and crash recovery.
    for (uint32_t i = 0; i < journal_entry->num_old_blks; i++) {
        BlkId old_blkid = *r_cast< const BlkId* >(key_buffer);
        vol_ptr->rd()->async_free_blks(lsn, old_blkid);
        key_buffer += sizeof(BlkId);
    }

    if (repl_ctx) { repl_ctx->promise_.setValue(folly::Unit()); }
}

void vol_interface_req::back_ref(VolumePtr vol_ptr) {
    DEBUG_ASSERT(vol == nullptr, "not expecting back ref to volume when vol is already set, vol={}", vol->to_string());
    vol = vol_ptr;
    vol->inc_ref(1); // increase ref_cnt for the volume
}

void intrusive_ptr_release(vol_interface_req* req) {
    if (req->refcount.decrement_testz()) {
        req->vol->dec_ref(1);
        req->free_yourself();
    }
}

#ifdef _PRERELEASE
bool HomeBlocksImpl::delay_fake_io(VolumePtr v) {
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
