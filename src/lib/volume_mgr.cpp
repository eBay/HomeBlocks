#include <boost/uuid/uuid_io.hpp>
#include "volume/volume.hpp"
#include "homeblks_impl.hpp"

namespace homeblocks {

std::shared_ptr< VolumeManager > HomeBlocksImpl::volume_manager() { return shared_from_this(); }

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

    {
        auto lg = std::scoped_lock(vol_lock_);
        vol_map_.emplace(std::make_pair(id, vol_ptr));
    }

    return folly::Unit();
}

VolumeManager::NullAsyncResult HomeBlocksImpl::remove_volume(const volume_id_t& id) { return folly::Unit(); }

VolumeInfoPtr HomeBlocksImpl::lookup_volume(const volume_id_t& id) { return nullptr; }

bool HomeBlocksImpl::get_stats(volume_id_t id, VolumeStats& stats) const { return true; }

void HomeBlocksImpl::get_volume_ids(std::vector< volume_id_t >& vol_ids) const {}
} // namespace homeblocks
