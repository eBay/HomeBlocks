
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
#include "coro_helpers.hpp"
#include <homestore/replication_service.hpp>
#include <iomgr/iomgr_flip.hpp>

namespace homeblocks {

uint64_t volume::get_index_size() {
    // Get approximate index size based on volume size. additional space for interior nodes.
    const int32_t index_kv_size = 32;
    uint64_t index_size = (vol_info_->size_bytes / vol_info_->page_size) * index_kv_size * 3;
    return index_size;
}

// this API will be called by volume manager after volume sb is recovered and volume is created;
shared< VolumeIndexTable > volume::init_index_table(bool is_recovery, shared< VolumeIndexTable > tbl) {
    if (!is_recovery) {
        index_cfg_t cfg(homestore::hs()->index_service().node_size());
        cfg.m_leaf_node_type = btree_leaf_node_type;
        cfg.m_int_node_type = btree_int_node_type;

        // create index table;
        auto uuid = boost::uuids::random_generator()();

        // user_sb_size is not currently enabled in homestore;
        // parent uuid is used during recovery in homeblks layer;
        auto index_size = get_index_size();
        LOGI("Creating index table for volume: {}, index_uuid: {}, parent_uuid: {} index_size: {}", vol_info_->name,
             boost::uuids::to_string(uuid), boost::uuids::to_string(id()), index_size);
        uint32_t pdev_id;
        auto chunk_ids = index_chunk_selector_->allocate_init_chunks(vol_info_->ordinal, index_size, pdev_id,
                                                                     false /* lazy alloc */);
        if (chunk_ids.empty()) {
            LOGE("Couldnt find chunks for index creation for volume: {}", vol_info_->name);
            return nullptr;
        }

        LOGI("index table is going to be created with {} chunks on pdev id {}", chunk_ids.size(), pdev_id);
        indx_tbl_ = std::make_shared< VolumeIndexTable >(uuid, id() /* parent uuid */, 0 /* user_sb_size */, cfg,
                                                         ordinal(), chunk_ids, pdev_id, index_size);
    } else {
        indx_tbl_ = tbl;
    }

    homestore::hs()->index_service().add_index_table(indx_tbl_->index_table());
    return indx_table();
}

volume::volume(sisl::byte_view const& buf, void* cookie, shared< VolumeChunkSelector > vol_chunk_sel,
               shared< VolumeChunkSelector > index_chunk_sel) :
        sb_{VOL_META_NAME}, volume_chunk_selector_{vol_chunk_sel}, index_chunk_selector_{index_chunk_sel} {
    sb_.load(buf, static_cast< homestore::meta_blk* >(cookie));
    // generate volume info from sb;
    vol_info_ = std::make_shared< volume_info >(sb_->id, sb_->size, sb_->page_size, sb_->name, sb_->ordinal);
    metrics_ = std::make_unique< VolumeMetrics >(vol_info_->name);
    m_state_ = sb_->state;
    LOGI("volume superblock loaded from disk, vol_info : {}", vol_info_->to_string());
}

bool volume::init(bool is_recovery) {
    if (!is_recovery) {
        // first time creation of the volume, let's write the superblock;

        // Allocate initial set of chunks for the volume with thin provisioning.
        shared< HomeBlocksImpl > hb = HomeBlocksImpl::instance();
        uint32_t pdev_id;
        auto chunk_ids = volume_chunk_selector_->allocate_init_chunks(vol_info_->ordinal, vol_info_->size_bytes,
                                                                      pdev_id, hb->dynamic_chunk_allocation());
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
        // create_repl_dev now returns a coroutine (async_result); drive it synchronously here (init is a
        // synchronous control-plane call) and inspect the unified result<T>.
        auto ret = detail::sync_get(homestore::hs()->repl_service().create_repl_dev(id(), {} /*members*/));
        if (!ret.has_value()) {
            LOGE("Failed to create solo repl dev for volume: {}, uuid: {}, error: {}", vol_info_->name,
                 boost::uuids::to_string(vol_info_->id), ret.error());

            return false;
        }
        rd_ = ret.value();

        // 2. create the index table;
        if (!init_index_table(false /*is_recovery*/)) {
            LOGE("Failed to create index for volume: {}", vol_info_->name);
            return false;
        }

        // 3. mark state as online;
        state_change(volume_state::ONLINE);

        LOGI("Created volume: {} uuid: {} ordinal: {} size: {} pdev: {} num_chunks: {}", vol_info_->name,
             boost::uuids::to_string(vol_info_->id), vol_info_->ordinal, vol_info_->size_bytes, pdev_id,
             chunk_ids.size());
    } else {
        // recovery path
        LOGI("Getting repl dev for volume: {}, uuid: {}", vol_info_->name, boost::uuids::to_string(id()));
        auto ret = homestore::hs()->repl_service().get_repl_dev(id());

        if (!ret.has_value()) {
            LOGI("volume in destroying state? Failed to get repl dev for volume name: {}, uuid: {}, error: {}",
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
            volume_chunk_selector_->recover_chunks(vol_info_->ordinal, sb_->pdev_id, vol_info_->size_bytes, chunk_ids);
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

sisl::async::task< void > volume::destroy() {
    LOGI("Start destroying volume: {}, uuid: {}", vol_info_->name, boost::uuids::to_string(id()));
    destroy_started_ = true;

    // 1. destroy the repl dev;
    if (rd_) {
        LOGI("Destroying repl dev for volume: {}", vol_info_->name);
        // remove_repl_dev is a coroutine (async_status); co_await it (rather than a blocking sync_get) so this
        // path can run on an iomgr reactor without parking it. Best-effort during destroy, result ignored.
        (void)co_await homestore::hs()->repl_service().remove_repl_dev(id());
        rd_ = nullptr;
    }

#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("vol_destroy_crash_simulation")) {
        // this is to simulate crash during volume destroy;
        // volume should be able to resume destroy on next reboot;
        LOGINFO("volume destroy crash simulation flip is set, aborting");
        co_return;
    }
#endif

    // 2. destroy the index table;
    if (indx_tbl_) {
        LOGI("Destroying index table for volume: {}, uuid: {}", vol_info_->name, boost::uuids::to_string(id()));
        // table superblk deletes in destroy(), hence it is safe to release chunks afterwards
        co_await indx_tbl_->destroy();
        index_chunk_selector_->release_chunks(vol_info_->ordinal);
        indx_tbl_ = nullptr;
    }

    // Stop chunk selection and resize for this volume before its superblock goes away: a resize worker persists the
    // new chunk list into the volume superblock.
    volume_chunk_selector_->quiesce_chunks(vol_info_->ordinal);

    // destroy the superblock which will remove sb from meta svc;
    sb_.destroy();

    // Release all the chunk's used by the volume. Superblock is destroyed before releasing
    // chunks, so that even after crash, these chunks will be available for other volumes.
    volume_chunk_selector_->release_chunks(vol_info_->ordinal);
}

void volume::update_vol_sb_cb(const std::vector< chunk_num_t >& chunk_ids) {
    // Update the volume superblk with latest set of chunk id's.
    uint32_t pdev_id = sb_->pdev_id;
    sb_.resize(sizeof(vol_sb_t) + (chunk_ids.size() * sizeof(homestore::chunk_num_t)));
    sb_->init(vol_info_->page_size, vol_info_->size_bytes, vol_info_->id, vol_info_->name, vol_info_->ordinal, pdev_id,
              chunk_ids);
    sb_.write();
}

async_status volume::write(io_req& vol_req) {
    vol_req.io_start_time = sisl::Clock::now();
    // Step 1. Allocate new blkids. Homestore might return multiple blkid's pointing
    // to different contigious memory locations.
    auto data_size = vol_req.nlbas * rd()->get_blk_size();
    homestore::blk_alloc_hints hints;
    hints.application_hint = vol_info_->ordinal;
    std::vector< homestore::multi_blk_id > new_blkids;
    // alloc_blks now returns homestore::status (std::expected); a value means success (was an error_code where
    // truthy meant failure -- hence the flipped check).
    if (auto const alloc_res = rd()->alloc_blks(data_size, hints, new_blkids); !alloc_res) {
        LOGE("Failed to allocate blocks data_size={}", data_size);
        co_return std::unexpected(std::errc::no_space_on_device);
    }
    COUNTER_INCREMENT(*metrics_, volume_write_count, 1);

    // Step 2. Write the data to those allocated blkids.
    vol_req.data_svc_start_time = sisl::Clock::now();
    sisl::sg_list data_sgs;
    data_sgs.iovs.emplace_back(iovec{.iov_base = vol_req.buffer, .iov_len = data_size});
    data_sgs.size = data_size;
    // NOTE: v8 io_batch is a reactor-local RAII scope, not the old cross-call part_of_batch accumulator, so we
    // issue the op un-batched (see HomeBlocksImpl::submit_io_batch()).
    if (auto const wr = co_await rd()->async_write(new_blkids, data_sgs, nullptr); !wr) {
        co_return std::unexpected(std::errc::io_error);
    }
    HISTOGRAM_OBSERVE(*metrics_, volume_data_write_latency, get_elapsed_time_us(vol_req.data_svc_start_time));
    vol_req.index_start_time = sisl::Clock::now();
    using homestore::blk_id;
    std::vector< blk_id > old_blkids;
    std::unordered_map< lba_t, BlockInfo > blocks_info;
    auto blk_size = rd()->get_blk_size();
    auto data_buffer = vol_req.buffer;
    lba_t start_lba = vol_req.lba;
    for (auto& blkid : new_blkids) {
        DEBUG_ASSERT_EQ(blkid.num_pieces(), 1, "Multiple blkid pieces");
        LOGT("volume write blkid={} start_lba={}", blkid.to_string(), start_lba);

        // Split the large blkid to individual blkid having only one block because each LBA points
        // to a blkid containing single blk which is stored in index value. Calculate the checksum for each
        // block which is also stored in index.
        for (uint32_t i = 0; i < blkid.blk_count(); i++) {
            auto new_bid = blk_id{blkid.blk_num() + i, 1 /* nblks */, blkid.chunk_num()};
            auto csum = crc16_t10dif(init_crc_16, static_cast< unsigned char* >(data_buffer), blk_size);
            blocks_info.emplace(start_lba + i, BlockInfo{new_bid, blk_id{}, csum});
            LOGT("volume write blkid={} csum={} start_lba={} lba={}", new_bid.to_string(),
                 blocks_info[start_lba + i].new_checksum, start_lba, start_lba + i);
            data_buffer += blk_size;
        }

        // Step 3. For range [start_lba, end_lba] in this blkid, write the values to index.
        // Should there be any overwritten on existing lbas, old blocks to be freed will be collected
        // in blocks_info after write_to_index
        lba_t end_lba = start_lba + blkid.blk_count() - 1;
        if (auto const idx_res = indx_table()->write_to_index(start_lba, end_lba, blocks_info); !idx_res) {
            co_return std::unexpected(volume_error::INDEX_ERROR);
        }

        start_lba = end_lba + 1;
    }
    HISTOGRAM_OBSERVE(*metrics_, volume_map_write_latency, get_elapsed_time_us(vol_req.index_start_time));

    vol_req.journal_start_time = sisl::Clock::now();
    // Collect all old blocks to write to journal.
    for (auto& [_, info] : blocks_info) {
        if (info.old_blkid.is_valid()) {
            LOGT("volume write start_lba={} old blkids={}", vol_req.lba, info.old_blkid.to_string());
            old_blkids.emplace_back(info.old_blkid);
        }
    }

    auto csum_size = sizeof(homestore::csum_t) * vol_req.nlbas;
    auto old_blkids_size = sizeof(blk_id) * old_blkids.size();
    auto key_size = sizeof(VolJournalEntry) + csum_size + old_blkids_size;

    auto req = repl_result_ctx< status >::make(sizeof(MsgHeader) /* header size */, key_size);
    req->vol_ptr_ = shared_from_this();
    req->header()->msg_type = MsgType::WRITE;
    // Store volume id for recovery path (log replay)
    req->header()->volume_id = id();

    // Step 4. Store lba, nlbas, list of checksum of each blk, list of old blkids as key in the journal.
    // New blkid's are written to journal by the homestore async_write_journal. After journal flush,
    // on_commit will be called where we free the old blkid's and the write iscompleted.
    VolJournalEntry hb_key{vol_req.lba, vol_req.nlbas, static_cast< uint16_t >(old_blkids.size())};
    auto key_buf = req->key_buf().bytes();
    std::memcpy(key_buf, &hb_key, sizeof(VolJournalEntry));
    key_buf += sizeof(VolJournalEntry);

    auto lba = vol_req.lba;
    for (lba_count_t count = 0; count < vol_req.nlbas; count++) {
        std::memcpy(key_buf, &blocks_info[lba].new_checksum, sizeof(homestore::csum_t));
        key_buf += sizeof(homestore::csum_t);
        lba++;
    }

    for (auto& blkid : old_blkids) {
        std::memcpy(key_buf, &blkid, sizeof(blk_id));
        key_buf += sizeof(blk_id);
    }

#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("vol_write_crash_after_data_write")) {
        // this is to simulate crash during write where data is persisted journal is
        // not persisted. After recovery there is no index for.
        LOGINFO("volume write crash simulation flip is set, aborting");
        co_return status();
    }
#endif

    rd()->async_write_journal(new_blkids, req->cheader_buf(), req->ckey_buf(), data_size, req);

    // Wait for the journal flush -> on_commit -> HomeBlocksImpl::on_write completion. The result is delivered
    // cross-thread via the repl_result_ctx's value_awaitable; we resume inline
    // on the commit thread.
    auto const jres = co_await req->promise_;
    if (!jres.has_value()) {
        LOGE("Failed to write to journal for volume: {}, lba: {}, nlbas: {}, error: {}", vol_info_->name, vol_req.lba,
             vol_req.nlbas, jres.error());
        co_return std::unexpected(jres.error());
    }
    HISTOGRAM_OBSERVE(*metrics_, volume_journal_write_latency, get_elapsed_time_us(vol_req.journal_start_time));
    auto write_size = vol_req.nlbas * rd()->get_blk_size();
    COUNTER_INCREMENT(*metrics_, volume_write_size_total, write_size);
    HISTOGRAM_OBSERVE(*metrics_, volume_write_size_distribution, write_size);
    HISTOGRAM_OBSERVE(*metrics_, volume_write_latency, get_elapsed_time_us(vol_req.io_start_time));
    co_return homeblocks::ok();
}

async_status volume::read(io_req& req) {
    req.io_start_time = sisl::Clock::now();
    // Step 1: get the blk ids from index table
    vol_read_ctx read_ctx{.req = &req, .blk_size = rd()->get_blk_size()};
    if (auto index_resp = indx_table()->read_from_index(req.lba, req.end_lba(), read_ctx.index_kvs);
        !index_resp.has_value()) {
        LOGE("Failed to read from index table for range=[{}, {}], volume id: {}, error: {}", req.lba, req.end_lba(),
             boost::uuids::to_string(id()), index_resp.error());
        co_return status();
    }
    HISTOGRAM_OBSERVE(*metrics_, volume_map_read_latency, get_elapsed_time_us(req.io_start_time));
    COUNTER_INCREMENT(*metrics_, volume_read_count, 1);

    // Step 2: Consolidate the blocks by merging the contiguous blkids
    std::vector< sisl::async::task< iomgr::io_result > > futs;
    // async_read takes sisl::sg_list by reference and the returned task is lazy (consumed only when when_all
    // starts it). The sg_lists must therefore outlive the co_await below, so we keep them here (stable
    // addresses via unique_ptr) rather than in submit_read_to_backend's per-iteration scope.
    std::vector< std::unique_ptr< sisl::sg_list > > sgs_keepalive;
    read_blks_list_t blks_to_read;
    generate_blkids_to_read(read_ctx.index_kvs, blks_to_read);

    // Step 3: Submit the read requests to backend
    req.data_svc_start_time = sisl::Clock::now();
    submit_read_to_backend(blks_to_read, req, futs, sgs_keepalive);

    if (read_ctx.index_kvs.empty()) { co_return status(); }

    // Step 4: wait for all the reads, then verify the checksum.
    auto const results = co_await sisl::async::when_all(std::move(futs));
    for (auto const& r : results) {
        if (sisl_unlikely(!r.has_value())) { co_return std::unexpected(r.error()); }
    }
    HISTOGRAM_OBSERVE(*metrics_, volume_data_read_latency, get_elapsed_time_us(read_ctx.req->data_svc_start_time));
    // verify the checksum and return
    co_return verify_checksum(read_ctx);
}

void volume::generate_blkids_to_read(const index_kv_list_t& index_kvs, read_blks_list_t& blks_to_read) {
    for (uint32_t i = 0, start_idx = 0; i < index_kvs.size(); ++i) {
        auto const& [key, value] = index_kvs[i];
        bool is_contiguous = (i == 0 ||
                              (key.lba() == index_kvs[i - 1].first.lba() + 1 &&
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
        blks_to_read.emplace_back(index_kvs[start_idx].first.lba(),
                                  homestore::multi_blk_id(blk_num, blk_count, chunk_num));
        start_idx = i;
        if (!is_contiguous && i == index_kvs.size() - 1) {
            // if the last entry is not contiguous, we need to add it as well
            blks_to_read.emplace_back(key.lba(),
                                      homestore::multi_blk_id(value.blkid().blk_num(), 1, value.blkid().chunk_num()));
        }
    }
}

status volume::verify_checksum(vol_read_ctx const& read_ctx) {
    auto read_buf = read_ctx.req->buffer;
    for (uint64_t cur_lba = read_ctx.req->lba, i = 0; i < read_ctx.index_kvs.size();) {
        auto const& [key, value] = read_ctx.index_kvs[i];
        // ignore the holes
        if (cur_lba != key.lba()) {
            read_buf += (key.lba() - cur_lba) * read_ctx.blk_size;
            cur_lba = key.lba();
            continue;
        }
        DEBUG_ASSERT_EQ(read_buf - read_ctx.req->buffer, (cur_lba - read_ctx.req->lba) * read_ctx.blk_size,
                        "Read buffer size mismatch, expected: {}, actual: {}",
                        (cur_lba - read_ctx.req->lba) * read_ctx.blk_size, read_buf - read_ctx.req->buffer);
        auto checksum = crc16_t10dif(init_crc_16, static_cast< unsigned char* >(read_buf), read_ctx.blk_size);
        if (checksum != value.checksum()) {
            LOGE("crc mismatch for lba: {} start: {}, end: {} blk id {}, expected: {}, actual: {}", cur_lba,
                 read_ctx.req->lba, read_ctx.req->end_lba(), value.blkid().to_string(), value.checksum(), checksum);
            return std::unexpected(volume_error::CRC_MISMATCH);
        }

        read_buf += read_ctx.blk_size;
        ++i;
        ++cur_lba;
    }
    auto read_size = read_ctx.req->nlbas * read_ctx.blk_size;
    COUNTER_INCREMENT(*metrics_, volume_read_size_total, read_size);
    HISTOGRAM_OBSERVE(*metrics_, volume_read_size_distribution, read_size);
    HISTOGRAM_OBSERVE(*metrics_, volume_read_latency, get_elapsed_time_us(read_ctx.req->io_start_time));
    return {};
}

void volume::submit_read_to_backend(read_blks_list_t const& blks_to_read, const io_req& req,
                                    std::vector< sisl::async::task< iomgr::io_result > >& futs,
                                    std::vector< std::unique_ptr< sisl::sg_list > >& sgs_keepalive) {
    auto* read_buf = req.buffer;
    auto inst = HomeBlocksImpl::instance();

    if (read_buf == nullptr && inst->fc_on()) {
        auto const reason = fmt::format("read_buf of volume: {} is null", this->to_string());
        inst->fault_containment(shared_from_this(), reason);
    } else {
        RELEASE_ASSERT(read_buf != nullptr, "Read buffer is null");
    }
    uint32_t prev_lba = req.lba;
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
        DEBUG_ASSERT_EQ(read_buf - req.buffer, (start_lba - req.lba) * rd()->get_blk_size(),
                        "Read buffer size mismatch, expected: {}, actual: {}",
                        (start_lba - req.lba) * rd()->get_blk_size(), read_buf - req.buffer);
        auto sgs = std::make_unique< sisl::sg_list >();
        sgs->size = blkids.blk_count() * rd()->get_blk_size();
        sgs->iovs.emplace_back(iovec{.iov_base = read_buf, .iov_len = sgs->size});
        read_buf += sgs->size;
        // un-batched read (see volume::write note on v8 io_batch). The sg_list must outlive the lazy task, so
        // it is owned by sgs_keepalive (in the caller's coroutine frame), not this loop scope.
        futs.emplace_back(rd()->async_read(blkids, *sgs, sgs->size, nullptr));
        sgs_keepalive.emplace_back(std::move(sgs));
        prev_lba = start_lba;
        prev_nblks = blkids.blk_count();
    }
    // if there are any holes at the end, fill them with zeros
    if (prev_lba + prev_nblks < req.end_lba() + 1) {
        auto holes_size = (req.end_lba() + 1 - (prev_lba + prev_nblks)) * rd()->get_blk_size();
        if (holes_size > 0) {
            std::memset(read_buf, 0, holes_size);
            read_buf += holes_size;
        }
    }
    DEBUG_ASSERT_EQ(read_buf - req.buffer, req.nlbas * rd()->get_blk_size(),
                    "Read buffer size mismatch, expected: {}, actual: {}", req.nlbas * rd()->get_blk_size(),
                    read_buf - req.buffer);
}

// Note: Metrics scrapping can happen at any point after volume instance is created and registered with metrics farm;
void VolumeMetrics::on_gather() {}

} // namespace homeblocks
