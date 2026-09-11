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
#include "hb_internal.hpp"
#include <iomgr/iomgr_flip.hpp>
#include <homestore/index_service.hpp>

namespace homeblocks {

VolumeChunkSelector::VolumeChunkSelector(std::string module, UpdateVolSbCb update_sb_cb) :
        m_update_vol_sb_cb(update_sb_cb), m_module_name(module) {
    m_volume_chunks.resize(MAX_NUM_VOLUMES);
}

void VolumeChunkSelector::add_chunk(homestore::cshared< Chunk >& chunk) {
    // Called during homestore start. Add to both all_chunks and per_device_chunk pool.
    // Later during volume recovery, assigned chunks are removed from the per_device_chunk pool.
    auto vol_chunk = std::make_shared< HBChunk >(chunk);
    auto chunk_id = homestore::VChunk(chunk).get_chunk_id();
    auto pdev_id = homestore::VChunk(chunk).get_pdev_id();

    LOGDEBUG("Adding chunk id {} to selector {}", chunk_id, m_module_name);
    std::lock_guard lock(m_chunk_sel_mutex);
    m_all_chunks.emplace(chunk_id, vol_chunk);
    m_per_dev_chunks[pdev_id].emplace(chunk_id, vol_chunk);
}

std::vector< chunk_num_t > VolumeChunkSelector::allocate_init_chunks(uint64_t volume_ordinal, uint64_t volume_size,
                                                                     uint32_t& pdev_id, bool lazy_alloc) {
    RELEASE_ASSERT(volume_ordinal < m_volume_chunks.size(), "Invalid ordinal for volume {}", volume_ordinal);

    // Fast path: volume already exists
    {
        std::shared_lock lock{m_chunk_sel_mutex};
        if (m_volume_chunks[volume_ordinal] != nullptr) {
            LOGW("Already allocated chunks for volume={}", volume_ordinal);
            auto volc = m_volume_chunks[volume_ordinal];
            std::vector< chunk_num_t > chunk_ids;
            for (auto& chunk : volc->m_chunks) {
                if (chunk) { chunk_ids.emplace_back(chunk->get_chunk_id()); }
            }
            return chunk_ids;
        }
    }

    uint64_t chunk_size{0};
    {
        std::shared_lock lock{m_chunk_sel_mutex};
        if (m_all_chunks.empty()) {
            LOGE("No chunks available in system for volume={}", volume_ordinal);
            return {};
        }
        chunk_size = m_all_chunks.begin()->second->size();
    }

    auto volc = std::make_shared< VolumeChunksInfo >();
    volc->ordinal = volume_ordinal;
    volc->max_num_chunks = std::max(1UL, (volume_size + chunk_size - 1) / chunk_size);
    volc->num_active_chunks = std::min(volc->max_num_chunks, static_cast< uint64_t >(num_chunks_per_vol_init));

    if (!lazy_alloc) {
        // If its not lazy alloc, we precreate all the chunks.
        volc->num_active_chunks = volc->max_num_chunks;
    }

    // We lazily allocate active chunks and add to chunk vector.
    // Initially we create num_chunks_per_vol_init active chunks.
    auto chunks = allocate_init_chunks_from_pdev(volc->num_active_chunks, volc->max_num_chunks);
    if (chunks.empty()) {
        LOGE("Couldnt allocate chunks for volume={}", volume_ordinal);
        return {};
    }

    volc->pdev = (*chunks.begin())->get_pdev_id();
    pdev_id = volc->pdev;
    volc->m_chunks.resize(volc->max_num_chunks);

    std::string str;
    uint64_t idx = 0;
    std::vector< chunk_num_t > chunk_ids;
    chunk_ids.reserve(chunks.size());

    for (auto& chunk : chunks) {
        RELEASE_ASSERT(chunk->m_vol_ordinal == INVALID_VOL_ORDINAL, "Chunk assigned to volume {}",
                       chunk->m_vol_ordinal);
        chunk->m_vol_ordinal = volume_ordinal;
        chunk_ids.emplace_back(chunk->get_chunk_id());
        volc->m_chunks[idx++] = chunk;
        fmt::format_to(std::back_inserter(str), "{} ", chunk->get_chunk_id());
    }

    {
        std::unique_lock lock{m_chunk_sel_mutex};

        // Another thread may have created this volume after our fast-path check
        if (m_volume_chunks[volume_ordinal] != nullptr) {
            LOGW("Volume={} was created concurrently, returning reserved chunks", volume_ordinal);

            for (auto& chunk : chunks) {
                RELEASE_ASSERT(chunk, "null chunk");
                chunk->m_vol_ordinal = INVALID_VOL_ORDINAL;
                m_per_dev_chunks[chunk->get_pdev_id()].emplace(chunk->get_chunk_id(), chunk);
            }

            std::vector< chunk_num_t > existing_chunk_ids;
            for (auto& chunk : m_volume_chunks[volume_ordinal]->m_chunks) {
                if (chunk) { existing_chunk_ids.emplace_back(chunk->get_chunk_id()); }
            }
            return existing_chunk_ids;
        }

        m_volume_chunks[volume_ordinal] = volc;
    }

    LOGI("Allocating initial module={} num_chunks={} for volume={} chunks={}", m_module_name, chunk_ids.size(),
         volume_ordinal, str);
    return chunk_ids;
}

homestore::cshared< Chunk > VolumeChunkSelector::select_chunk(homestore::blk_count_t nblks,
                                                              const homestore::blk_alloc_hints& hints) {

    if (!hints.application_hint) { return nullptr; }
    uint64_t volume_ordinal = hints.application_hint.value();

    shared< VolumeChunksInfo > volc;

    {
        std::shared_lock lock{m_chunk_sel_mutex};

        if (volume_ordinal >= m_volume_chunks.size()) { return nullptr; }

        volc = m_volume_chunks[volume_ordinal];
        if (!volc || volc->releasing.load(std::memory_order_acquire)) { return nullptr; }

        volc->inflight_selects.fetch_add(1, std::memory_order_acq_rel);
    }

    // Hand-made RAII: decrement the counter on every exit path
    struct PinGuard {
        shared< VolumeChunksInfo > volc;
        ~PinGuard() {
            if (volc) { volc->inflight_selects.fetch_sub(1, std::memory_order_acq_rel); }
        }
    } pin{volc};

    // Recheck after pinning in case release started immediately after unlock.
    if (volc->releasing.load(std::memory_order_acquire)) { return nullptr; }

    do {
#ifdef _PRERELEASE
        if (iomgr_flip::instance()->test_flip("vol_num_chunks_force_resize_op")) {
            // this is to simulate no blks available.
            LOGI("volume resize op flip is set.");
            resize_volume_num_chunks(nblks, volc);
        }
#endif

        // TODO to remove , keep trak of number of freed and alloc blks.
        // Add chunk_selector interface to have additional functions on_alloc_blk, on_free_blk in homestore.
        uint64_t total_blks = 0, available_blks = 0;
        const auto active = volc->num_active_chunks.load(std::memory_order_acquire);
        for (uint64_t i = 0; i < active; ++i) {
            auto const& chunk = volc->m_chunks[i];
            if (!chunk) continue;
            total_blks += chunk->get_total_blks();
            available_blks += chunk->available_blks();
        }

        // If the ratio of available_blks to total_blks is less than half or there is a request of nblks
        // more than the available blks and there is room for more chunks then resize.
        auto usage_ratio = total_blks ? (float)available_blks / total_blks : 0.0f;
        if ((nblks > available_blks || usage_ratio < 0.5) && (active < volc->max_num_chunks)) {
            // Check if number chunks needs to be increased.
            resize_volume_num_chunks(nblks, volc);
        }

        // This is the fastpath where we try to allocate the blks from the active chunks.
        // Traverse through active chunks in the vector and find the first chunk
        // which has some available blks. It may not satisfy all the nblks, in that case
        // virtual_dev will call select_chunk again.
        uint64_t num_active_chunks = volc->num_active_chunks.load(std::memory_order_acquire);
        for (uint64_t i = 0; i < num_active_chunks; i++) {
            // Lock-free round-robin over the active chunks (shared atomic cursor).
            auto const idx = volc->m_next_chunk_index.fetch_add(1, std::memory_order_relaxed) % num_active_chunks;
            auto chunk = volc->m_chunks[idx];
            if (chunk && chunk->available_blks() > 0) { return chunk->get_internal_chunk(); }
        }

        if (volc->releasing.load(std::memory_order_acquire)) { return nullptr; }

        LOGT("Waiting to allocate more chunks active={} total={}", volc->num_active_chunks.load(),
             volc->max_num_chunks);
        LOGT("{}", dump_chunks());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } while (true);

    return {};
}

void VolumeChunkSelector::resize_volume_num_chunks(homestore::blk_count_t nblks, shared< VolumeChunksInfo > volc) {
    // Don't resize while releasing
    if (!volc || volc->releasing.load(std::memory_order_acquire)) { return; }

    auto idle = ResizeOp::Idle, inprogress = ResizeOp::InProgress;
    auto status = volc->resize_op.compare_exchange_strong(idle, inprogress);
    if (!status) {
        // Some other thread is in process of adding the chunks.
        return;
    }

    // TODO chunk select will have on_alloc_blk, on_free_blk
    // Only scan the published active prefix. Readers should treat
    // [0, num_active_chunks) as the only visible chunk range
    uint64_t total_blks = 0, available_blks = 0;
    const auto active = volc->num_active_chunks.load(std::memory_order_acquire);
    for (uint64_t i = 0; i < active; ++i) {
        auto const& chunk = volc->m_chunks[i];
        if (!chunk) continue;
        total_blks += chunk->get_total_blks();
        available_blks += chunk->available_blks();
    }

    bool force_resize = false;
#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("vol_num_chunks_force_resize_op")) {
        // this is to simulate no blks available.
        LOGI("volume resize op flip is set.");
        force_resize = true;
    }
#endif
    if (!force_resize) {
        auto usage_ratio = total_blks ? (float)available_blks / total_blks : 0.0f;
        if ((nblks < available_blks && usage_ratio > 0.5)) {
            // Check again if another thread already did the resize.
            LOGI("Another thread already completed the resize op.");
            volc->resize_op.store(ResizeOp::Idle);
            return;
        }
    }

    // Spawn background task to create new chunks.
    LOGD("Initiating op to resize num chunks for module={} volume={} available={} total={}", m_module_name,
         volc->ordinal, available_blks, total_blks);

    iomanager.run_on_forget(iomgr::reactor_regex::random_worker, [volc, this]() mutable {
        const auto num_chunks_to_alloc =
            std::min(static_cast< uint64_t >(num_chunks_per_resize),
                     (volc->max_num_chunks - volc->num_active_chunks.load(std::memory_order_acquire)));

        if (num_chunks_to_alloc == 0) {
            volc->resize_op.store(ResizeOp::Idle);
            return;
        }

        auto new_chunks = allocate_resize_chunks_from_pdev(volc->pdev, num_chunks_to_alloc);
        RELEASE_ASSERT(!new_chunks.empty(), "No chunks available for resize volume={}", volc->ordinal);

        // Build the metadata payload first, but do NOT publish new chunks yet.
        // That way select_chunk() still only sees the old active prefix.
        std::vector< chunk_num_t > chunk_ids;
        bool release_started = false;
        {
            std::shared_lock lock{m_chunk_sel_mutex};

            if (volc->releasing.load(std::memory_order_acquire)) {
                release_started = true;
            } else {
                const auto active = volc->num_active_chunks.load(std::memory_order_acquire);
                chunk_ids.reserve(active + new_chunks.size());

                for (uint64_t i = 0; i < active; ++i) {
                    auto const& chunk = volc->m_chunks[i];
                    if (chunk) { chunk_ids.emplace_back(chunk->get_chunk_id()); }
                }
            }
        }

        // If release started after we grabbed chunks from the free pool,
        // put them back instead of attaching them to the dying volume
        if (release_started) {
            std::unique_lock lock{m_chunk_sel_mutex};
            for (auto& chunk : new_chunks) {
                m_per_dev_chunks[chunk->get_pdev_id()].emplace(chunk->get_chunk_id(), chunk);
            }
            volc->resize_op.store(ResizeOp::Idle);
            return;
        }

        for (auto& chunk : new_chunks) {
            chunk_ids.emplace_back(chunk->get_chunk_id());
        }

        // Persist first, publish second
        m_update_vol_sb_cb(volc->ordinal, chunk_ids);

        std::string str;
        {
            std::unique_lock lock{m_chunk_sel_mutex};

            // Recheck after metadata update. Release may have started while
            // callback was running; if so, do not publish these chunks.
            if (volc->releasing.load(std::memory_order_acquire)) {
                for (auto& chunk : new_chunks) {
                    m_per_dev_chunks[chunk->get_pdev_id()].emplace(chunk->get_chunk_id(), chunk);
                }
                volc->resize_op.store(ResizeOp::Idle);
                return;
            }

            // Publish by writing chunk pointers first, then bumping
            // num_active_chunks. Readers only trust the active prefix.
            auto idx = volc->num_active_chunks.load(std::memory_order_relaxed);
            for (auto& chunk : new_chunks) {
                RELEASE_ASSERT(chunk->m_vol_ordinal == INVALID_VOL_ORDINAL, "Chunk assigned to volume {}",
                               chunk->m_vol_ordinal);
                chunk->m_vol_ordinal = volc->ordinal;
                volc->m_chunks[idx++] = chunk;
                fmt::format_to(std::back_inserter(str), "{}({}) ", chunk->get_chunk_id(), chunk->get_pdev_id());
            }

            volc->num_active_chunks.store(idx, std::memory_order_release);
        }

        volc->resize_op.store(ResizeOp::Idle);
        LOGI("Resize op done. Allocated more chunks for volume={} total={} new={} new_chunks={}", volc->ordinal,
             volc->num_active_chunks.load(), new_chunks.size(), str);
    });
}

std::vector< shared< VolumeChunkSelector::HBChunk > >
VolumeChunkSelector::allocate_init_chunks_from_pdev(uint64_t init_chunks, uint64_t total_chunks) {
    std::lock_guard lock(m_chunk_sel_mutex);
    std::vector< shared< HBChunk > > result;
    RELEASE_ASSERT(init_chunks <= total_chunks, "Invalid chunks requested");
    for (auto& [pdev, pdev_chunks] : m_per_dev_chunks) {
        // Find the physical device which has enough total_chunks needed for a volume.
        if (pdev_chunks.size() >= total_chunks) {
            auto iter = pdev_chunks.begin();
            // Assign the init_chunks from the device. Remove chunk from the per
            // device map so that we dont allocate it to another volume.
            for (uint32_t i = 0; i < init_chunks; i++) {
                result.emplace_back(iter->second);
                iter = pdev_chunks.erase(iter);
            }
            break;
        }
    }

    return result;
}

std::vector< shared< VolumeChunkSelector::HBChunk > >
VolumeChunkSelector::allocate_resize_chunks_from_pdev(uint32_t pdev_id, uint64_t num_chunks) {
    std::lock_guard lock(m_chunk_sel_mutex);
    std::vector< shared< HBChunk > > result;
    auto& chunks = m_per_dev_chunks[pdev_id];
    RELEASE_ASSERT(num_chunks <= chunks.size(), "Not enough chunks for volume");

    // Allocate chunks from this pdev pool.
    uint32_t count = 0;
    for (auto iter = chunks.begin(); iter != chunks.end() && count < num_chunks; count++) {
        result.emplace_back(iter->second);
        iter = chunks.erase(iter);
    }

    return result;
}

bool VolumeChunkSelector::recover_chunks(uint64_t volume_ordinal, uint32_t pdev, uint64_t volume_size,
                                         const std::vector< chunk_num_t >& chunk_ids) {
    std::unique_lock lock(m_chunk_sel_mutex);
    auto volc = m_volume_chunks[volume_ordinal];
    RELEASE_ASSERT(!volc, "volume already exists");

    auto chunk_size = m_all_chunks.begin()->second->size();
    volc = std::make_shared< VolumeChunksInfo >();
    volc->ordinal = volume_ordinal;
    volc->pdev = pdev;
    volc->max_num_chunks = std::max(1UL, (volume_size + chunk_size - 1) / chunk_size);

    volc->num_active_chunks = chunk_ids.size();
    volc->m_chunks.resize(volc->max_num_chunks);
    m_volume_chunks[volume_ordinal] = volc;

    std::string str;
    uint32_t indx = 0;
    for (auto& chunk_id : chunk_ids) {
        // Add the chunks to the volume chunk list.
        auto chunk = m_all_chunks[chunk_id];
        if (!chunk) {
            LOGE("Chunk not found vol={} chunk_id={}", volume_ordinal, chunk_id);
            return false;
        }
        RELEASE_ASSERT(chunk->m_vol_ordinal == INVALID_VOL_ORDINAL, "Chunk assigned to volume {}",
                       chunk->m_vol_ordinal);
        RELEASE_ASSERT(chunk->get_pdev_id() == pdev, "Invalid pdev for chunk");
        chunk->m_vol_ordinal = volume_ordinal;
        volc->m_chunks[indx++] = chunk;

        // Remove from per device chunk pool as its assigned to this volume.
        auto res = m_per_dev_chunks[chunk->get_pdev_id()].erase(chunk_id);
        RELEASE_ASSERT(res == 1, "Chunk not found {}", chunk_id);
        fmt::format_to(std::back_inserter(str), "{} ", chunk_id);
    }

    LOGI("Recovered volume={} num_chunks={}", volume_ordinal, chunk_ids.size());
    LOGDEBUG("Recovered chunks={}", str);
    return true;
}

// Release the active chunks back to the per device chunk pool
void VolumeChunkSelector::release_chunks(uint64_t volume_ordinal) {
    shared< VolumeChunksInfo > volc;

    {
        std::unique_lock lock(m_chunk_sel_mutex);
        volc = std::exchange(m_volume_chunks[volume_ordinal], nullptr);
        RELEASE_ASSERT(volc, "volume doesnt exists");
        volc->releasing.store(true, std::memory_order_release);
    }

    // Wait until no selector is still walking this volume, and no resize op is publishing
    while (volc->inflight_selects.load(std::memory_order_acquire) != 0 ||
           volc->resize_op.load(std::memory_order_acquire) != ResizeOp::Idle) {
        std::this_thread::yield();
    }

    auto release_fn = [this, volume_ordinal, volc]() mutable {
        std::string str;
        uint64_t cnt{};

        for (auto& chunk : volc->m_chunks) {
            if (!chunk) { continue; }

            fmt::format_to(std::back_inserter(str), "{} ", chunk->get_chunk_id());
            ++cnt;

            if (homestore::hs() && homestore::hs()->has_index_service()) [[likely]] {
                homestore::hs()->index_service().wb_cache().evict_chunk_blkids(*chunk->get_internal_chunk());
            }

            {
                std::unique_lock lock{m_chunk_sel_mutex};
                chunk->m_vol_ordinal = INVALID_VOL_ORDINAL;
                m_per_dev_chunks[chunk->get_pdev_id()].emplace(chunk->get_chunk_id(), chunk);
            }
        }

        volc->m_chunks.clear();
        volc->num_active_chunks.store(0, std::memory_order_release);

        LOGI("Released chunks for volume={} num_chunks={}", volume_ordinal, cnt);
        LOGDEBUG("Released chunks={}", str);
    };

    if (homestore::hs() && homestore::hs()->has_index_service()) [[likely]] {
        // Clear wbc entries in a separate thread
        iomanager.run_on_forget(iomgr::reactor_regex::random_worker, std::move(release_fn));
    } else {
        release_fn();
    }
}

void VolumeChunkSelector::foreach_chunks(std::function< void(homestore::cshared< Chunk >&) >&& cb) {
    for (const auto& [_, vol_chunk] : m_all_chunks) {
        cb(vol_chunk->get_internal_chunk());
    }
}

std::vector< shared< VolumeChunkSelector::HBChunk > > VolumeChunkSelector::get_chunks(uint64_t volume_ordinal) {
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

uint64_t VolumeChunkSelector::num_free_chunks() const {
    std::lock_guard lock(m_chunk_sel_mutex);
    uint64_t count = 0;
    for (const auto& [pdev, chunks] : m_per_dev_chunks) {
        count += chunks.size();
    }
    return count;
}

void VolumeChunkSelector::dump_per_pdev_chunks() const {
    std::lock_guard lock(m_chunk_sel_mutex);
    for (const auto& [pdev, chunks] : m_per_dev_chunks) {
        std::string str;
        for (const auto& [chunk_id, _] : chunks) {
            fmt::format_to(std::back_inserter(str), "{} ", chunk_id);
        }
        LOGI("pdev={} num_chunks={} chunks={}", pdev, chunks.size(), str);
    }
}

std::string VolumeChunkSelector::dump_chunks() const {
    std::lock_guard lock(m_chunk_sel_mutex);
    std::string str;
    for (uint32_t i = 0; i < m_volume_chunks.size(); i++) {
        if (!m_volume_chunks[i]) { continue; }
        fmt::format_to(std::back_inserter(str), "volume={} num_chunks={} chunks=", i,
                       m_volume_chunks[i]->m_chunks.size());
        for (const auto& chunk : m_volume_chunks[i]->m_chunks) {
            if (!chunk) { continue; }
            fmt::format_to(std::back_inserter(str), "{}({}/{}) ", chunk->get_chunk_id(), chunk->available_blks(),
                           chunk->get_total_blks());
        }
        fmt::format_to(std::back_inserter(str), "\n");
    }
    return str;
}

} // namespace homeblocks
