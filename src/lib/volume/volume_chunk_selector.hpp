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

#include <list>
#include <folly/ThreadLocal.h>
#include <homestore/chunk_selector.h>
#include <homestore/vchunk.h>
#include <homestore/homestore_decl.hpp>
#include <homestore/homestore.hpp>
#include "homeblks/common.hpp"

namespace homeblocks {

using chunk_num_t = homestore::chunk_num_t;
using Chunk = homestore::Chunk;

class VolumeChunkSelector : public homestore::ChunkSelector {
    static constexpr homestore::chunk_num_t num_chunks_per_vol_init = 1;
    static constexpr homestore::chunk_num_t num_chunks_per_resize = 3;
    static constexpr uint64_t INVALID_VOL_ORDINAL = UINT64_MAX;

    struct HBChunk : public homestore::VChunk {
        HBChunk(homestore::cshared< Chunk >& chunk) : homestore::VChunk(chunk) {}
        ~HBChunk() = default;
        uint64_t m_vol_ordinal{INVALID_VOL_ORDINAL};
    };

    struct VolumeChunksInfo {
        // List of active chunks allocated for the volume.
        // Each volume is assigned and physical device and
        // all chunks are allocated from same physical device.
        std::vector< shared< HBChunk > > m_chunks;

        // max_num_chunks is total chunks possible for whole volume
        // size. num_active_chunks is the number of chunks which is
        // used for allocation. next_chunk_index is thread local
        // which does round robin on the active chunks
        uint64_t max_num_chunks;
        std::atomic< uint64_t > num_active_chunks{0};
        folly::ThreadLocal< uint32_t > m_next_chunk_index;
        uint64_t ordinal;
        uint32_t pdev;
    };

public:
    using UpdateVolSbCb = std::function< void(uint64_t ordinal, const std::vector< chunk_num_t >&) >;
    VolumeChunkSelector(std::string module_name, UpdateVolSbCb update_sb_cb);
    ~VolumeChunkSelector() = default;

    // Allocate some initial set of chunks during volume or index create. The number is num_chunks_per_resize
    std::vector< chunk_num_t > allocate_init_chunks(uint64_t volume_ordinal, uint64_t volume_size, uint32_t& pdev_id);

    // Called during destroy of volume or index.
    void release_chunks(uint64_t volume_ordinal);

    // Called during recovery of volume or index .
    bool recover_chunks(uint64_t volume_ordinal, uint32_t pdev_id, uint64_t volume_size,
                        const std::vector< chunk_num_t >& chunk_ids);

    // Called by homestore during start.
    void add_chunk(homestore::cshared< Chunk >&) override;

    // Called by homestore during cp flush.
    void foreach_chunks(std::function< void(homestore::cshared< Chunk >&) >&& cb) override;

    // Called by homestore during blk alloc.
    homestore::cshared< Chunk > select_chunk(homestore::blk_count_t nblks,
                                             const homestore::blk_alloc_hints& hints) override;

    std::vector< shared< VolumeChunkSelector::HBChunk > > get_chunks(uint64_t volume_ordinal);
    uint64_t num_free_chunks() const;

private:
    std::vector< shared< HBChunk > > allocate_init_chunks_from_pdev(uint64_t init_chunks, uint64_t total_chunks);
    std::vector< shared< HBChunk > > allocate_resize_chunks_from_pdev(uint32_t pdev, uint64_t num_chunks);
    void resize_volume_num_chunks(homestore::blk_count_t nblks, shared< VolumeChunksInfo > volc);
    void dump_per_pdev_chunks() const;
    std::string dump_chunks() const;

private:
    enum class ResizeOp {
        Idle,
        InProgress,
    };

    // Store volume chunks details with index as volume ordinal.
    std::vector< shared< VolumeChunksInfo > > m_volume_chunks;

    using ChunkMap = std::unordered_map< chunk_num_t, shared< HBChunk > >;

    // Mapping of chunk id to chunk. All chunks assigned to volume and
    // unassigned chunks are stored in this pool.
    ChunkMap m_all_chunks;

    // Mapping from physical device to group of chunks which are available
    // for allocation. This pool is used for allocation of chunks to volume.
    // Chunks once allocated to volume are removed from this pool.
    std::unordered_map< uint64_t, ChunkMap > m_per_dev_chunks;
    mutable std::mutex m_chunk_sel_mutex;
    UpdateVolSbCb m_update_vol_sb_cb;
    std::atomic< ResizeOp > resize_op{ResizeOp::Idle};
    std::string m_module_name;
};

} // namespace homeblocks
