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
#include "volume_chunk_selector.hpp"
#include <homeblks/common.hpp>
#include <iomgr/iomgr_flip.hpp>

namespace homeblocks {

VolumeChunkSelector::VolumeChunkSelector(UpdateVolSbCb update_sb_cb) : m_update_vol_sb_cb(update_sb_cb) {
    m_volume_chunks.resize(MAX_NUM_VOLUMES);
}

void VolumeChunkSelector::add_chunk(homestore::cshared< Chunk >& chunk) {
    // Called during homestore start. Add to both all_chunks and per_device_chunk pool.
    // Later during volume recovery, assigned chunks are removed from the per_device_chunk pool.
    auto vol_chunk = std::make_shared< HBChunk >(chunk);
    auto chunk_id = homestore::VChunk(chunk).get_chunk_id();
    auto pdev_id = homestore::VChunk(chunk).get_pdev_id();
    m_all_chunks.emplace(chunk_id, vol_chunk);
    m_per_dev_chunks[pdev_id].emplace(chunk_id, vol_chunk);
    LOGDEBUG("Adding chunk id to selector {}", chunk_id);
}

std::vector< chunk_num_t > VolumeChunkSelector::allocate_volume_chunks(uint64_t volume_ordinal, uint64_t volume_size) {
    std::lock_guard lock(m_chunk_sel_mutex);

    RELEASE_ASSERT(volume_ordinal < m_volume_chunks.size(), "Invalid ordinal for volume {}", volume_ordinal);
    if (m_volume_chunks[volume_ordinal] != nullptr) {
        LOGW("Already allocated chunks for volume={}", volume_ordinal);
        std::vector< chunk_num_t > chunk_ids;
        for (auto& chunk : m_volume_chunks[volume_ordinal]->m_chunks) {
            if (chunk) { chunk_ids.emplace_back(chunk->get_chunk_id()); }
        }
        return chunk_ids;
    }

    uint64_t total_device_chunks{0};
    for (const auto& [_, chunks_list] : m_per_dev_chunks) {
        total_device_chunks += chunks_list.size();
    }

    // We lazily allocate active chunks and add to chunk vector.
    // Initially we create num_chunks_per_resize active chunks.
    auto chunk_size = m_all_chunks.begin()->second->size();
    auto volc = std::make_shared< VolumeChunksInfo >();
    volc->ordinal = volume_ordinal;
    volc->max_num_chunks = std::max(1UL, (volume_size + chunk_size - 1) / chunk_size);
    volc->num_active_chunks = std::min(volc->max_num_chunks, num_chunks_per_resize);

    // Check if we have enough chunks available for the volume.
    if (total_device_chunks == 0 || volc->num_active_chunks > total_device_chunks) {
        LOGE("Couldnt allocate chunks for volume={} total={} requested={}", volume_ordinal, total_device_chunks,
             volc->num_active_chunks.load());
        return {};
    }

    volc->m_chunks.resize(volc->max_num_chunks);
    m_volume_chunks[volume_ordinal] = volc;

    uint64_t idx = 0;
    std::vector< chunk_num_t > chunk_ids;
    std::string str;
    auto chunks = allocate_chunks_from_pdevs(volc->num_active_chunks);
    for (auto& chunk : chunks) {
        // Add the chunks to the volume chunk list.
        RELEASE_ASSERT(!chunk->m_vol_ordinal.has_value(), "Chunk assigned to volume {}", *chunk->m_vol_ordinal);
        chunk->m_vol_ordinal = volume_ordinal;
        chunk_ids.emplace_back(chunk->get_chunk_id());
        volc->m_chunks[idx++] = chunk;
        fmt::format_to(std::back_inserter(str), "{} ", chunk->get_chunk_id());
    }

    LOGI("Allocating initial num_chunks={} for volume={} chunks={}", chunk_ids.size(), volume_ordinal, str);
    return chunk_ids;
}

homestore::cshared< Chunk > VolumeChunkSelector::select_chunk(homestore::blk_count_t nblks,
                                                              const homestore::blk_alloc_hints& hints) {

    if (!hints.application_hint) { return nullptr; }
    uint64_t volume_ordinal = hints.application_hint.value();

    // We dont take lock on volumes vector and volume chunks vector
    // as they precreated and never changed
    auto volc = m_volume_chunks[volume_ordinal];

    // TODO to remove , keep trak of number of freed and alloc blks.
    // chunk select will have on_alloc_blk, on_free_blk
    uint32_t total_blks = 0, available_blks = 0;
    for (auto& chunk : volc->m_chunks) {
        if (!chunk) continue;
        total_blks += chunk->get_total_blks();
        available_blks += chunk->available_blks();
    }

    do {
#ifdef _PRERELEASE
        if (iomgr_flip::instance()->test_flip("vol_num_chunks_resize_op")) {
            // this is to simulate no blks available.
            LOGINFO("Volume resize op flip is set.");
            resize_volume_num_chunks(volc);
        }
#else
        if (((float)available_blks / total_blks) < 0.5) {
            // Check if number chunks needs to be increased.
            resize_volume_num_chunks(volc);
        }
#endif

        // This is the fastpath where we try to allocate the blks from the active chunks.
        // Traverse through active chunks in the vector and find the first chunk
        // which has some available blks. It may not satisfy all the nblks, in that case
        // virtual_dev will call select_chunk again.
        uint64_t num_active_chunks = volc->num_active_chunks;
        for (uint64_t i = 0; i < num_active_chunks; i++) {
            if (*volc->m_next_chunk_index >= num_active_chunks) { *volc->m_next_chunk_index = 0; }

            auto chunk = volc->m_chunks[*volc->m_next_chunk_index];
            *volc->m_next_chunk_index = ((*volc->m_next_chunk_index) + 1);
            if (chunk && chunk->available_blks() > 0) { return chunk->get_internal_chunk(); }
        }

    } while (true);

    return {};
}

void VolumeChunkSelector::resize_volume_num_chunks(shared< VolumeChunksInfo > volc) {
    auto idle = ResizeOp::Idle, inprogress = ResizeOp::InProgress;
    auto status = resize_op.compare_exchange_strong(idle, inprogress);
    if (!status) {
        // Some other thread is in process of adding the chunks.
        return;
    }

    // TODO chunk select will have on_alloc_blk, on_free_blk
    uint32_t total_blks = 0, available_blks = 0;
    for (auto& chunk : volc->m_chunks) {
        if (!chunk) continue;
        total_blks += chunk->get_total_blks();
        available_blks += chunk->available_blks();
    }

#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("vol_num_chunks_resize_op")) {
        // this is to simulate no blks available.
        LOGINFO("Volume resize op flip is set.");
    }
#else
    if (((float)available_blks / total_blks) > 0.5) {
        // Check again if another thread already did the resize.
        return;
    }
#endif

    // Spawn background task to create new chunks.
    LOGINFO("Initiating op to resize num chunks for volume={}", volc->ordinal);
    iomanager.run_on_forget(iomgr::reactor_regex::random_worker, [volc, this]() mutable {
        std::string str;
        auto chunks = allocate_chunks_from_pdevs(num_chunks_per_resize);

        // Add the new chunks to the active chunks
        auto indx = volc->num_active_chunks.load();
        for (auto chunk : chunks) {
            volc->m_chunks[indx] = chunk;
            indx++;
            fmt::format_to(std::back_inserter(str), "{} ", chunk->get_chunk_id());
        }

        std::vector< chunk_num_t > chunk_ids;
        for (auto& chunk : volc->m_chunks) {
            if (chunk) { chunk_ids.emplace_back(chunk->get_chunk_id()); }
        }

        // Persist the new chunk ids to the metablk of volume
        // before making them active chunks. Invoke the registered callback.
        m_update_vol_sb_cb(volc->ordinal, chunk_ids);

        // Update the number of active chunks and compelete the resize operation.
        LOGINFO("Resize op done. Allocated more chunks for volume={} num_chunks={} new_chunks={}", volc->ordinal,
                chunks.size(), str);
        volc->num_active_chunks = indx;
        resize_op.store(ResizeOp::Idle);
    });
}

std::vector< shared< VolumeChunkSelector::HBChunk > >
VolumeChunkSelector::allocate_chunks_from_pdevs(uint64_t num_chunks) {
    // Allocate the request number of chunks across physical devices. Remove chunk
    // from the per device map so that we dont allocate it to another volume.
    std::vector< shared< HBChunk > > chunks;
    uint64_t cur_pdev{0}, num_pdevs{m_per_dev_chunks.size()};
    for (uint64_t i = 0; i < num_chunks;) {
        if (!m_per_dev_chunks[cur_pdev].empty()) {
            auto iter = m_per_dev_chunks[cur_pdev].begin();
            auto chunk = iter->second;
            chunks.emplace_back(chunk);
            m_per_dev_chunks[cur_pdev].erase(iter);
            i++;
        }
        cur_pdev = (cur_pdev + 1) % num_pdevs;
    }
    return chunks;
}

void VolumeChunkSelector::recover_volume_chunks(uint64_t volume_ordinal, uint64_t volume_size,
                                                const std::vector< chunk_num_t >& chunk_ids) {
    std::lock_guard lock(m_chunk_sel_mutex);
    auto volc = m_volume_chunks[volume_ordinal];
    RELEASE_ASSERT(!volc, "Volume already exists");

    auto chunk_size = m_all_chunks.begin()->second->size();
    volc = std::make_shared< VolumeChunksInfo >();
    volc->ordinal = volume_ordinal;
    volc->max_num_chunks = std::max(1UL, (volume_size + chunk_size - 1) / chunk_size);
    volc->num_active_chunks = chunk_ids.size();
    volc->m_chunks.resize(volc->max_num_chunks);
    m_volume_chunks[volume_ordinal] = volc;

    std::string str;
    uint32_t indx = 0;
    for (auto& chunk_id : chunk_ids) {
        // Add the chunks to the volume chunk list.
        auto chunk = m_all_chunks[chunk_id];
        RELEASE_ASSERT(chunk, "Chunk not found {}", chunk_id);
        RELEASE_ASSERT(!chunk->m_vol_ordinal.has_value(), "Chunk assigned to volume {}", *chunk->m_vol_ordinal);
        chunk->m_vol_ordinal = volume_ordinal;
        volc->m_chunks[indx++] = chunk;

        // Remove from per device chunk pool as its assigned to this volume.
        auto res = m_per_dev_chunks[chunk->get_pdev_id()].erase(chunk_id);
        RELEASE_ASSERT(res == 1, "Chunk not found {}", chunk_id);
        fmt::format_to(std::back_inserter(str), "{} ", chunk_id);
    }

    LOGINFO("Recovered volume={} num_chunks={}", volume_ordinal, chunk_ids.size());
    LOGDEBUG("Recovered chunks={}", str);
}

void VolumeChunkSelector::release_volume_chunks(uint64_t volume_ordinal) {
    // Release the active chunks back to the per device chunk pool.
    std::lock_guard lock(m_chunk_sel_mutex);
    std::string str;
    uint64_t count = 0;
    auto volc = m_volume_chunks[volume_ordinal];
    RELEASE_ASSERT(volc, "Volume doesnt exists");

    for (auto chunk : volc->m_chunks) {
        if (chunk) {
            chunk->m_vol_ordinal.reset();
            m_per_dev_chunks[chunk->get_pdev_id()].emplace(chunk->get_chunk_id(), chunk);
            fmt::format_to(std::back_inserter(str), "{} ", chunk->get_chunk_id());
            count++;
        }
    }

    m_volume_chunks[volume_ordinal] = nullptr;
    LOGINFO("Released chunks for volume={} num_chunks={}", volume_ordinal, count);
    LOGDEBUG("Released chunks={}", str);
}

void VolumeChunkSelector::foreach_chunks(std::function< void(homestore::cshared< Chunk >&) >&& cb) {
    for (const auto& [_, vol_chunk] : m_all_chunks) {
        cb(vol_chunk->get_internal_chunk());
    }
}

std::vector< shared< VolumeChunkSelector::HBChunk > > VolumeChunkSelector::get_volume_chunks(uint64_t volume_ordinal) {
    std::lock_guard lock(m_chunk_sel_mutex);
    std::vector< shared< VolumeChunkSelector::HBChunk > > chunks;

    RELEASE_ASSERT(volume_ordinal < m_volume_chunks.size(), "Invalid ordinal for volume {}", volume_ordinal);
    if (!m_volume_chunks[volume_ordinal]) { return {}; }
    for (auto& chunk : m_volume_chunks[volume_ordinal]->m_chunks) {
        if (!chunk) { continue; }
        chunks.emplace_back(chunk);
    }
    return chunks;
}

uint64_t VolumeChunkSelector::num_free_chunks() {
    std::lock_guard lock(m_chunk_sel_mutex);
    uint64_t count = 0;
    for (const auto& [pdev, chunks] : m_per_dev_chunks) {
        count += chunks.size();
    }
    return count;
}

void VolumeChunkSelector::dump_per_pdev_chunks() {
    std::lock_guard lock(m_chunk_sel_mutex);
    for (const auto& [pdev, chunks] : m_per_dev_chunks) {
        std::string str;
        for (const auto& [chunk_id, _] : chunks) {
            fmt::format_to(std::back_inserter(str), "{} ", chunk_id);
        }
        LOGINFO("pdev={} num_chunks={} chunks={}", pdev, chunks.size(), str);
    }
}

void VolumeChunkSelector::dump_volume_chunks() {
    std::lock_guard lock(m_chunk_sel_mutex);
    for (uint32_t i = 0; i < m_volume_chunks.size(); i++) {
        if (!m_volume_chunks[i]) { continue; }
        std::string str;
        for (const auto& chunk : m_volume_chunks[i]->m_chunks) {
            if (!chunk) { continue; }
            fmt::format_to(std::back_inserter(str), "{} ", chunk->get_chunk_id());
        }
        LOGINFO("volume={} num_chunks={} chunks={}", i, m_volume_chunks[i]->m_chunks.size(), str);
    }
}

} // namespace homeblocks