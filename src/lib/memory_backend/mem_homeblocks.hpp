#pragma once

#include <atomic>
#include <utility>

#include <folly/concurrency/ConcurrentHashMap.h>
#include "lib/homeblocks_impl.hpp"

namespace homeblocks {

class MemoryHomeBlocks : public HomeBlocksImpl {
    // VolumeManager
    VolumeManager::NullAsyncResult _create_volume(VolumeInfo&& vol_info) override;

    bool _get_stats(volume_id_t id, VolumeStats& stats) const override;
    void _get_volume_ids(std::vector< volume_id_t >& volume_ids) const override;

    HomeBlocksStats _get_stats() const override;

public:
    MemoryHomeBlocks(std::weak_ptr< HomeBlocksApplication >&& application);
    ~MemoryHomeBlocks() override = default;
};

} // namespace homeblocks
