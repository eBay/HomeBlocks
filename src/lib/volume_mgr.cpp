
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

VolumeManager::NullAsyncResult HomeBlocksImpl::create_volume(VolumeInfo&& vol_info) {
    auto id = vol_info.id;
    LOGI("[vol={}] is of capacity [{}B]", boost::uuids::to_string(id), vol_info.size_bytes);

    {
        auto lg = std::shared_lock(vol_lock_);
        if (auto it = vol_map_.find(id); it != vol_map_.end()) {
            LOGW("create_volume with input id: {} already exists,", boost::uuids::to_string(id));
            return folly::makeUnexpected(VolumeError::INVALID_ARG);
        }
    }

    auto vol_ptr = Volume::make_volume(std::move(vol_info));
    if (vol_ptr) {
        auto lg = std::scoped_lock(vol_lock_);
        vol_map_.emplace(std::make_pair(id, vol_ptr));
    } else {
        LOGE("failed to create volume with id: {}", boost::uuids::to_string(id));
        return folly::makeUnexpected(VolumeError::INTERNAL_ERROR);
    }

    return folly::Unit();
}

VolumeManager::NullAsyncResult HomeBlocksImpl::remove_volume(const volume_id_t& id) {
    iomanager.run_on_forget(iomgr::reactor_regex::random_worker, [this, id]() {
        LOGINFO("remove_volume with input id: {}", boost::uuids::to_string(id));
        // 1. get the volume ptr from the map;
        VolumePtr vol_ptr = nullptr;
        {
            auto lg = std::scoped_lock(vol_lock_);
            if (auto it = vol_map_.find(id); it != vol_map_.end()) { vol_ptr = it->second; }
        }

        if (vol_ptr) {
            // 2. do volume destroy;
            vol_ptr->destroy();
#ifdef _PRERELEASE
            if (iomgr_flip::instance()->test_flip("vol_destroy_crash_simulation")) { return folly::Unit(); }
#endif
            // 3. remove volume from vol_map;
            {
                auto lg = std::scoped_lock(vol_lock_);
                vol_map_.erase(vol_ptr->id());
            }

            LOGINFO("Volume {} removed successfully", vol_ptr->id_str());
        } else {
            LOGWARN("remove_volume with input id: {} not found", boost::uuids::to_string(id));
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

VolumeManager::NullAsyncResult HomeBlocksImpl::write(const VolumePtr& vol_ptr, const vol_interface_req_ptr& vol_req,
                                                     bool part_of_batch) {

    // Step 1. Allocate new blkids. Homestore might return multiple blkid's pointing
    // to different contigious memory locations.
    auto data_size = vol_req->nlbas * vol_ptr->rd()->get_blk_size();
    std::vector< homestore::MultiBlkId > new_blkids;
    auto result = vol_ptr->rd()->alloc_blks(data_size, homestore::blk_alloc_hints{}, new_blkids);
    if (result) {
        LOGE("Failed to allocate blocks");
        return folly::makeUnexpected(VolumeError::NO_SPACE_LEFT);
    }

    // Step 2. Write the data to those allocated blkids.
    sisl::sg_list data_sgs;
    data_sgs.iovs.emplace_back(iovec{.iov_base = vol_req->buffer, .iov_len = data_size});
    data_sgs.size = data_size;
    return vol_ptr->rd()
        ->async_write(new_blkids, data_sgs, part_of_batch)
        .thenValue([this, vol_ptr, vol_req,
                    new_blkids = std::move(new_blkids)](auto&& result) -> VolumeManager::NullAsyncResult {
            if (result) { return folly::makeUnexpected(VolumeError::DRIVE_WRITE_ERROR); }

            using homestore::BlkId;
            std::vector< BlkId > old_blkids;
            std::unordered_map< lba_t, BlockInfo > blocks_info;
            auto blk_size = vol_ptr->rd()->get_blk_size();
            auto data_size = vol_req->nlbas * blk_size;
            auto data_buffer = vol_req->buffer;
            lba_t start_lba = vol_req->lba;
            for (auto& blkid : new_blkids) {
                DEBUG_ASSERT_EQ(blkid.num_pieces(), 1, "Multiple blkid pieces");

                // Split the large blkid to individual blkid having only one block because each LBA points
                // to a blkid containing single blk which is stored in index value. Calculate the checksum for each
                // block which is also stored in index.
                for (uint32_t i = 0; i < blkid.blk_count(); i++) {
                    auto new_bid = BlkId{blkid.blk_num() + i, 1 /* nblks */, blkid.chunk_num()};
                    auto csum = crc16_t10dif(init_crc_16, static_cast< unsigned char* >(data_buffer), blk_size);
                    blocks_info.emplace(start_lba + i, BlockInfo{new_bid, BlkId{}, csum});
                    data_buffer += blk_size;
                }

                // Step 3. For range [start_lba, end_lba] in this blkid, write the values to index.
                // Should there be any overwritten on existing lbas, old blocks to be freed will be collected
                // in blocks_info after write_to_index
                lba_t end_lba = start_lba + blkid.blk_count() - 1;
                auto status = write_to_index(vol_ptr, start_lba, end_lba, blocks_info);
                if (!status) { return folly::makeUnexpected(VolumeError::INDEX_ERROR); }

                start_lba = end_lba + 1;
            }

            // Collect all old blocks to write to journal.
            for (auto& [_, info] : blocks_info) {
                if (info.old_blkid.is_valid()) { old_blkids.emplace_back(info.old_blkid); }
            }

            auto csum_size = sizeof(homestore::csum_t) * vol_req->nlbas;
            auto old_blkids_size = sizeof(BlkId) * old_blkids.size();
            auto key_size = sizeof(VolJournalEntry) + csum_size + old_blkids_size;

            auto req = repl_result_ctx< VolumeManager::NullResult >::make(
                sizeof(HomeBlksMessageHeader) /* header size */, key_size);
            req->vol_ptr_ = vol_ptr;
            req->header()->msg_type = HomeBlksMsgType::WRITE;
            // Store volume id for recovery path (log replay)
            req->header()->volume_id = vol_ptr->id();

            // Step 4. Store lba, nlbas, list of checksum of each blk, list of old blkids as key in the journal.
            // New blkid's are written to journal by the homestore async_write_journal. After journal flush, on_commit
            // will be called where we free the old blkid's and the write iscompleted.
            VolJournalEntry hb_key{vol_req->lba, vol_req->nlbas, static_cast< uint16_t >(old_blkids.size())};
            auto key_buf = req->key_buf().bytes();
            std::memcpy(key_buf, &hb_key, sizeof(VolJournalEntry));
            key_buf += sizeof(VolJournalEntry);

            auto lba = vol_req->lba;
            for (lba_count_t count = 0; count < vol_req->nlbas; count++) {
                std::memcpy(key_buf, &blocks_info[lba].checksum, sizeof(homestore::csum_t));
                key_buf += sizeof(homestore::csum_t);
                lba++;
            }

            for (auto& blkid : old_blkids) {
                std::memcpy(key_buf, &blkid, sizeof(BlkId));
                key_buf += sizeof(BlkId);
            }

            vol_ptr->rd()->async_write_journal(new_blkids, req->cheader_buf(), req->ckey_buf(), data_size, req);
            return req->result().deferValue([this](const auto&& result) -> folly::Expected< folly::Unit, VolumeError > {
                if (result.hasError()) {
                    auto err = result.error();
                    return folly::makeUnexpected(err);
                }
                return folly::Unit();
            });
        });
}

VolumeManager::NullAsyncResult HomeBlocksImpl::read(const VolumePtr& vol_ptr, const vol_interface_req_ptr& vol_req,
                                                    bool part_of_batch) {
    // TODO remove when read is merged.
    auto data_size = vol_req->nlbas * vol_ptr->rd()->get_blk_size();
    sisl::sg_list data_sgs;
    data_sgs.iovs.emplace_back(iovec{.iov_base = vol_req->buffer, .iov_len = data_size});
    data_sgs.size = data_size;

    VolumeIndexKey key(vol_req->lba);
    VolumeIndexValue value;
    homestore::BtreeSingleGetRequest get_req(&key, &value);
    auto ret = vol_ptr->indx_table()->get(get_req);
    RELEASE_ASSERT(ret == homestore::btree_status_t::success, "Cant find lba");
    auto err = vol_ptr->rd()->async_read(value.blkid(), data_sgs, data_size).get();
    RELEASE_ASSERT(!err, "async_read failed");
    return folly::Unit();
}

VolumeManager::NullAsyncResult HomeBlocksImpl::unmap(const VolumePtr& vol, const vol_interface_req_ptr& req) {
    RELEASE_ASSERT(false, "Unmap Not implemented");
    return folly::Unit();
}

void HomeBlocksImpl::submit_io_batch() { homestore::data_service().submit_io_batch(); }

void HomeBlocksImpl::on_write(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                              const std::vector< homestore::MultiBlkId >& new_blkids,
                              cintrusive< homestore::repl_req_ctx >& ctx) {
    repl_result_ctx< VolumeManager::NullResult >* repl_ctx{nullptr};
    if (ctx) { repl_ctx = boost::static_pointer_cast< repl_result_ctx< VolumeManager::NullResult > >(ctx).get(); }
    auto msg_header = r_cast< HomeBlksMessageHeader* >(const_cast< uint8_t* >(header.cbytes()));

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
            auto status = write_to_index(vol_ptr, start_lba, end_lba, blocks_info);
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

} // namespace homeblocks
