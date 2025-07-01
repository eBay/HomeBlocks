
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
#include "volume.hpp"
#include "lib/homeblks_impl.hpp"
#include <homestore/replication_service.hpp>
#include <iomgr/iomgr_flip.hpp>

namespace homeblocks {
static VolumeError to_volume_error(std::error_code ec) {
    switch (ec.value()) {
    default:
        return VolumeError::UNKNOWN;
    }
}

// this API will be called by volume manager after volume sb is recovered and volume is created;
shared< VolumeIndexTable > Volume::init_index_table(bool is_recovery, shared< VolumeIndexTable > tbl) {
    if (!is_recovery) {
        index_cfg_t cfg(homestore::hs()->index_service().node_size());
        cfg.m_leaf_node_type = homestore::btree_node_type::PREFIX;
        cfg.m_int_node_type = homestore::btree_node_type::FIXED;

        // create index table;
        auto uuid = hb_utils::gen_random_uuid();

        // user_sb_size is not currently enabled in homestore;
        // parent uuid is used during recovery in homeblks layer;
        LOGI("Creating index table for volume: {}, index_uuid: {}, parent_uuid: {}", vol_info_->name,
             boost::uuids::to_string(uuid), boost::uuids::to_string(id()));
        indx_tbl_ = std::make_shared< VolumeIndexTable >(uuid, id() /*parent uuid*/, 0 /*user_sb_size*/, cfg);
    } else {
        indx_tbl_ = tbl;
    }

    homestore::hs()->index_service().add_index_table(indx_table());
    return indx_table();
}

Volume::Volume(sisl::byte_view const& buf, void* cookie, shared< VolumeChunkSelector > chunk_sel) :
        sb_{VOL_META_NAME}, chunk_selector_{chunk_sel} {
    sb_.load(buf, cookie);
    // generate volume info from sb;
    vol_info_ = std::make_shared< VolumeInfo >(sb_->id, sb_->size, sb_->page_size, sb_->name, sb_->ordinal);
    m_state_ = sb_->state;
    LOGI("Volume superblock loaded from disk, vol_info : {}", vol_info_->to_string());
}

bool Volume::init(bool is_recovery) {
    if (!is_recovery) {
        // first time creation of the Volume, let's write the superblock;

        // Allocate initial set of chunks for the volume with thin provisioning.
        uint32_t pdev_id;
        auto chunk_ids = chunk_selector_->allocate_init_chunks(vol_info_->ordinal, vol_info_->size_bytes, pdev_id);
        if (chunk_ids.empty()) {
            LOGE("Failed to allocate chunks for volume: {}, uuid: {}", vol_info_->name,
                 boost::uuids::to_string(vol_info_->id));
            return false;
        }

        // 0. create the superblock and store chunk id's
        sb_.create(sizeof(vol_sb_t) + (chunk_ids.size() * sizeof(homestore::chunk_num_t)));
        sb_->init(vol_info_->page_size, vol_info_->size_bytes, vol_info_->id, vol_info_->name, vol_info_->ordinal,
                  pdev_id, chunk_ids);

        // 1. create solo repl dev for volume;
        // members left empty on purpose for solo repl dev
        LOGI("Creating solo repl dev for volume: {}, uuid: {}", vol_info_->name, boost::uuids::to_string(id()));
        auto ret = homestore::hs()->repl_service().create_repl_dev(id(), {} /*members*/).get();
        if (ret.hasError()) {
            LOGE("Failed to create solo repl dev for volume: {}, uuid: {}, error: {}", vol_info_->name,
                 boost::uuids::to_string(vol_info_->id), ret.error());

            return false;
        }
        rd_ = ret.value();

        // 2. create the index table;
        init_index_table(false /*is_recovery*/);

        // 3. mark state as online;
        state_change(vol_state::ONLINE);

        LOGI("Created volume: {} uuid: {} ordinal: {} size: {} pdev: {} num_chunks: {}", vol_info_->name,
             boost::uuids::to_string(vol_info_->id), vol_info_->ordinal, vol_info_->size_bytes, pdev_id,
             chunk_ids.size());
    } else {
        // recovery path
        LOGI("Getting repl dev for volume: {}, uuid: {}", vol_info_->name, boost::uuids::to_string(id()));
        auto ret = homestore::hs()->repl_service().get_repl_dev(id());

        if (ret.hasError()) {
            LOGI("Volume in destroying state? Failed to get repl dev for volume name: {}, uuid: {}, error: {}",
                 vol_info_->name, boost::uuids::to_string(vol_info_->id), ret.error());
            rd_ = nullptr;
            // DEBUG_ASSERT(false, "Failed to get repl dev for volume");
            // return false;
        } else {
            rd_ = ret.value();
        }

        // Get the chunk id's from metablk and pass to chunk selector for recovery.
        std::vector< chunk_num_t > chunk_ids(sb_->get_chunk_ids(), sb_->get_chunk_ids() + sb_->num_chunks);
        bool success =
            chunk_selector_->recover_chunks(vol_info_->ordinal, sb_->pdev_id, vol_info_->size_bytes, chunk_ids);
        if (!success) {
            LOGI("Failed to recover chunks for volume name: {}, uuid: {}", vol_info_->name,
                 boost::uuids::to_string(vol_info_->id));
            return false;
        }

        LOGI("Recovered volume: {} uuid: {} ordinal: {} size: {} pdev: {} num_chunks: {}", vol_info_->name,
             boost::uuids::to_string(vol_info_->id), vol_info_->ordinal, vol_info_->size_bytes, sb_->pdev_id,
             chunk_ids.size());
        // index table will be recovered via in subsequent callback with init_index_table API;
    }

    // set the in memory state from superblock;
    m_state_ = sb_->state;
    return true;
}

void Volume::destroy() {
    LOGI("Start destroying volume: {}, uuid: {}", vol_info_->name, boost::uuids::to_string(id()));
    destroy_started_ = true;

    // 1. destroy the repl dev;
    if (rd_) {
        LOGI("Destroying repl dev for volume: {}", vol_info_->name);
        homestore::hs()->repl_service().remove_repl_dev(id()).get();
        rd_ = nullptr;
    }

#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("vol_destroy_crash_simulation")) {
        // this is to simulate crash during volume destroy;
        // volume should be able to resume destroy on next reboot;
        LOGINFO("Volume destroy crash simulation flip is set, aborting");
        return;
    }
#endif

    // 2. destroy the index table;
    if (indx_tbl_) {
        LOGI("Destroying index table for volume: {}, uuid: {}", vol_info_->name, boost::uuids::to_string(id()));
        homestore::hs()->index_service().remove_index_table(indx_tbl_);
        indx_tbl_->destroy();
        indx_tbl_ = nullptr;
    }

    // destroy the superblock which will remove sb from meta svc;
    sb_.destroy();

    // Release all the chunk's used by the volume. Superblock is destroyed before releasing
    // chunks, so that even after crash, these chunks will be available for other volumes.
    chunk_selector_->release_chunks(vol_info_->ordinal);
}

void Volume::update_vol_sb_cb(const std::vector< chunk_num_t >& chunk_ids) {
    // Update the volume superblk with latest set of chunk id's.
    uint32_t pdev_id = sb_->pdev_id;
    sb_.resize(sizeof(vol_sb_t) + (chunk_ids.size() * sizeof(homestore::chunk_num_t)));
    sb_->init(vol_info_->page_size, vol_info_->size_bytes, vol_info_->id, vol_info_->name, vol_info_->ordinal, pdev_id,
              chunk_ids);
    sb_.write();
}

VolumeManager::NullAsyncResult Volume::write(const vol_interface_req_ptr& vol_req) {
    // Step 1. Allocate new blkids. Homestore might return multiple blkid's pointing
    // to different contigious memory locations.
    auto data_size = vol_req->nlbas * rd()->get_blk_size();
    homestore::blk_alloc_hints hints;
    hints.application_hint = vol_info_->ordinal;
    std::vector< homestore::MultiBlkId > new_blkids;
    auto result = rd()->alloc_blks(data_size, hints, new_blkids);
    if (result) {
        LOGE("Failed to allocate blocks");
        return folly::makeUnexpected(VolumeError::NO_SPACE_LEFT);
    }

    // Step 2. Write the data to those allocated blkids.
    sisl::sg_list data_sgs;
    data_sgs.iovs.emplace_back(iovec{.iov_base = vol_req->buffer, .iov_len = data_size});
    data_sgs.size = data_size;
    return rd()
        ->async_write(new_blkids, data_sgs, vol_req->part_of_batch)
        .thenValue(
            [this, vol_req, new_blkids = std::move(new_blkids)](auto&& result) -> VolumeManager::NullAsyncResult {
                if (result) { return folly::makeUnexpected(VolumeError::DRIVE_WRITE_ERROR); }

                using homestore::BlkId;
                std::vector< BlkId > old_blkids;
                std::unordered_map< lba_t, BlockInfo > blocks_info;
                auto blk_size = rd()->get_blk_size();
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
                        LOGT("volume write blkid={} csum={}", new_bid.to_string(), csum);
                        data_buffer += blk_size;
                    }

                    // Step 3. For range [start_lba, end_lba] in this blkid, write the values to index.
                    // Should there be any overwritten on existing lbas, old blocks to be freed will be collected
                    // in blocks_info after write_to_index
                    lba_t end_lba = start_lba + blkid.blk_count() - 1;
                    auto status = write_to_index(start_lba, end_lba, blocks_info);
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

                auto req =
                    repl_result_ctx< VolumeManager::NullResult >::make(sizeof(MsgHeader) /* header size */, key_size);
                req->vol_ptr_ = shared_from_this();
                req->header()->msg_type = MsgType::WRITE;
                // Store volume id for recovery path (log replay)
                req->header()->volume_id = id();

                // Step 4. Store lba, nlbas, list of checksum of each blk, list of old blkids as key in the journal.
                // New blkid's are written to journal by the homestore async_write_journal. After journal flush,
                // on_commit will be called where we free the old blkid's and the write iscompleted.
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

#ifdef _PRERELEASE
                if (iomgr_flip::instance()->test_flip("vol_write_crash_after_data_write")) {
                    // this is to simulate crash during write where data is persisted journal is
                    // not persisted. After recovery there is no index for.
                    LOGINFO("Volume write crash simulation flip is set, aborting");
                    return folly::Unit();
                }
#endif

                rd()->async_write_journal(new_blkids, req->cheader_buf(), req->ckey_buf(), data_size, req);

                return req->result()
                    .via(&folly::InlineExecutor::instance())
                    .thenValue([this, vol_req](const auto&& result) -> folly::Expected< folly::Unit, VolumeError > {
                        if (result.hasError()) {
                            LOGE("Failed to write to journal for volume: {}, lba: {}, nlbas: {}, error: {}",
                                 vol_info_->name, vol_req->lba, vol_req->nlbas, result.error());
                            auto err = result.error();
                            return folly::makeUnexpected(err);
                        }
                        return folly::Unit();
                    });
            });
}

VolumeManager::Result< folly::Unit > Volume::write_to_index(lba_t start_lba, lba_t end_lba,
                                                            std::unordered_map< lba_t, BlockInfo >& blocks_info) {
    // Use filter callback to get the old blkid.
    homestore::put_filter_cb_t filter_cb = [&blocks_info](BtreeKey const& key, BtreeValue const& existing_value,
                                                          BtreeValue const& value) {
        auto lba = r_cast< const VolumeIndexKey& >(key).key();
        blocks_info[lba].old_blkid = r_cast< const VolumeIndexValue& >(existing_value).blkid();
        return homestore::put_filter_decision::replace;
    };

    // Write to prefix btree with key ranging from start_lba to end_lba.
    // For value shift() will get the blk_num and checksum for each lba.
    IndexValueContext app_ctx{&blocks_info, start_lba};
    const BlkId& start_blkid = blocks_info[start_lba].new_blkid;
    VolumeIndexValue value{start_blkid, blocks_info[start_lba].checksum};

    auto req = homestore::BtreeRangePutRequest< VolumeIndexKey >{
        homestore::BtreeKeyRange< VolumeIndexKey >{VolumeIndexKey{start_lba}, true, VolumeIndexKey{end_lba}, true},
        homestore::btree_put_type::UPSERT,
        r_cast< VolumeIndexValue* >(&value),
        r_cast< void* >(&app_ctx),
        std::numeric_limits< uint32_t >::max() /* batch_size */,
        filter_cb};
    auto result = indx_table()->put(req);
    if (result != homestore::btree_status_t::success) {
        LOGERROR("Failed to put to index range=({},{}) error={}", start_lba, end_lba, result);
        return folly::makeUnexpected(VolumeError::INDEX_ERROR);
    }

    return folly::Unit();
}

VolumeManager::NullAsyncResult Volume::read(const vol_interface_req_ptr& req) {
    // Step 1: get the blk ids from index table
    vol_read_ctx read_ctx{.vol_req = req, .blk_size = rd()->get_blk_size()};
    if (auto index_resp = read_from_index(req, read_ctx.index_kvs); index_resp.hasError()) {
        LOGE("Failed to read from index table for range=[{}, {}], volume id: {}, error: {}", req->lba, req->end_lba(),
             boost::uuids::to_string(id()), index_resp.error());
        return index_resp;
    }

    // Step 2: Consolidate the blocks by merging the contiguous blkids
    std::vector< folly::Future< std::error_code > > futs;
    read_blks_list_t blks_to_read;
    generate_blkids_to_read(read_ctx.index_kvs, blks_to_read);

    // Step 3: Submit the read requests to backend
    submit_read_to_backend(blks_to_read, req, futs);

    if (read_ctx.index_kvs.empty()) { return folly::Unit(); }

    // Step 4: verify the checksum after all the reads are done
    return folly::collectAllUnsafe(futs).thenValue([this, read_ctx](auto&& vf) -> VolumeManager::Result< folly::Unit > {
        for (auto const& err_c : vf) {
            if (sisl_unlikely(err_c.value())) {
                auto ec = err_c.value();
                return folly::makeUnexpected(to_volume_error(ec));
            }
        }
        // verify the checksum and return
        return verify_checksum(read_ctx);
    });
}

void Volume::generate_blkids_to_read(const index_kv_list_t& index_kvs, read_blks_list_t& blks_to_read) {
    for (uint32_t i = 0, start_idx = 0; i < index_kvs.size(); ++i) {
        auto const& [key, value] = index_kvs[i];
        bool is_contiguous = (i == 0 ||
                              (key.key() == index_kvs[i - 1].first.key() + 1 &&
                               value.blkid().blk_num() == index_kvs[i - 1].second.blkid().blk_num() + 1 &&
                               value.blkid().chunk_num() == index_kvs[i - 1].second.blkid().chunk_num()));
        if (is_contiguous && i < index_kvs.size() - 1) {
            // continue to the next entry if it is contiguous
            continue;
        }
        // prepare the previous contiguous blkids to read
        auto blk_num = index_kvs[start_idx].second.blkid().blk_num();
        auto chunk_num = index_kvs[start_idx].second.blkid().chunk_num();
        // if the last entry is part of the contiguous block,
        // we need to account for it in the blk_count
        auto blk_count = is_contiguous ? (i - start_idx + 1) : (i - start_idx);
        blks_to_read.emplace_back(index_kvs[start_idx].first.key(),
                                  homestore::MultiBlkId(blk_num, blk_count, chunk_num));
        start_idx = i;
        if (!is_contiguous && i == index_kvs.size() - 1) {
            // if the last entry is not contiguous, we need to add it as well
            blks_to_read.emplace_back(key.key(),
                                      homestore::MultiBlkId(value.blkid().blk_num(), 1, value.blkid().chunk_num()));
        }
    }
}

VolumeManager::Result< folly::Unit > Volume::verify_checksum(vol_read_ctx const& read_ctx) {
    auto read_buf = read_ctx.vol_req->buffer;
    for (uint64_t cur_lba = read_ctx.vol_req->lba, i = 0; i < read_ctx.index_kvs.size();) {
        auto const& [key, value] = read_ctx.index_kvs[i];
        // ignore the holes
        if (cur_lba != key.key()) {
            read_buf += (key.key() - cur_lba) * read_ctx.blk_size;
            cur_lba = key.key();
            continue;
        }
        auto checksum = crc16_t10dif(init_crc_16, static_cast< unsigned char* >(read_buf), read_ctx.blk_size);
        if (checksum != value.checksum()) {
            LOGE("crc mismatch for lba: {} start: {}, end: {} blk id {}, expected: {}, actual: {}", cur_lba,
                 read_ctx.vol_req->lba, read_ctx.vol_req->end_lba(), value.blkid().to_string(), value.checksum(),
                 checksum);
            return folly::makeUnexpected(VolumeError::CRC_MISMATCH);
        }

        read_buf += read_ctx.blk_size;
        ++i;
        ++cur_lba;
    }
    return folly::Unit();
}

void Volume::submit_read_to_backend(read_blks_list_t const& blks_to_read, const vol_interface_req_ptr& req,
                                    std::vector< folly::Future< std::error_code > >& futs) {
    auto* read_buf = req->buffer;
    DEBUG_ASSERT(read_buf != nullptr, "Read buffer is null");
    uint32_t prev_lba = req->lba;
    uint32_t prev_nblks = 0;
    for (uint32_t i = 0; i < blks_to_read.size(); ++i) {
        auto const& [start_lba, blkids] = blks_to_read[i];
        DEBUG_ASSERT(start_lba >= prev_lba + prev_nblks, "Invalid start lba: {}, prev_lba: {}, prev_nblks: {}",
                     start_lba, prev_lba, prev_nblks);
        auto holes_size = (start_lba - (prev_lba + prev_nblks)) * rd()->get_blk_size();
        // if there are holes, fill the holes with zeros
        if (holes_size > 0) {
            std::memset(read_buf, 0, holes_size);
            read_buf += holes_size;
        }
        sisl::sg_list sgs;
        sgs.size = blkids.blk_count() * rd()->get_blk_size();
        sgs.iovs.emplace_back(iovec{.iov_base = read_buf, .iov_len = sgs.size});
        read_buf += sgs.size;
        futs.emplace_back(rd()->async_read(blkids, sgs, sgs.size, req->part_of_batch));
        prev_lba = start_lba;
        prev_nblks = blkids.blk_count();
    }
    // if there are any holes at the end, fill them with zeros
    if (prev_lba + prev_nblks < req->end_lba() + 1) {
        auto holes_size = (req->end_lba() + 1 - (prev_lba + prev_nblks)) * rd()->get_blk_size();
        if (holes_size > 0) {
            std::memset(read_buf, 0, holes_size);
            read_buf += holes_size;
        }
    }
    DEBUG_ASSERT_EQ(read_buf - req->buffer, req->nlbas * rd()->get_blk_size(),
                    "Read buffer size mismatch, expected: {}, actual: {}", req->nlbas * rd()->get_blk_size(),
                    read_buf - req->buffer);
}

VolumeManager::Result< folly::Unit > Volume::read_from_index(const vol_interface_req_ptr& req,
                                                             index_kv_list_t& index_kvs) {
    homestore::BtreeQueryRequest< VolumeIndexKey > qreq{
        homestore::BtreeKeyRange< VolumeIndexKey >{VolumeIndexKey{req->lba}, VolumeIndexKey{req->end_lba()}},
        homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY};
    auto index_table = indx_table();
    auto inst = HomeBlocksImpl::instance();
    if (!index_table and inst->fc_on()) {
        inst->fault_containment(shared_from_this());
    } else {
        RELEASE_ASSERT(index_table != nullptr, "Index table is null for volume id: {}", boost::uuids::to_string(id()));
        if (auto ret = index_table->query(qreq, index_kvs); ret != homestore::btree_status_t::success) {
            return folly::makeUnexpected(VolumeError::INDEX_ERROR);
        }
    }

    return folly::Unit();
}

} // namespace homeblocks
