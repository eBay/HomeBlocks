#include <boost/uuid/uuid_io.hpp>
#include "volume/volume.hpp"
#include "homeblks_impl.hpp"

namespace homeblocks {

std::shared_ptr< VolumeManager > HomeBlocksImpl::volume_manager() { return shared_from_this(); }

void HomeBlocksImpl::on_vol_meta_blk_found(sisl::byte_view const& buf, void* cookie) {
    // auto sb = homestore::superblk< vol_sb_t >(VOL_META_NAME);
    // sb.load(buf, cookie);
    auto vol_ptr = Volume::make_volume(buf, cookie);
    vol_ptr->init_index_table(false);
    auto id = vol_ptr->id();
    {
        auto lg = std::scoped_lock(vol_lock_);
        DEBUG_ASSERT(vol_map_.find(id) == vol_map_.end(),
                     "volume id: {} already exists in recovery path, not expected!", boost::uuids::to_string(id));
        vol_map_.emplace(std::make_pair(id, vol_ptr));
    }
}

shared< VolumeIndexTable > HomeBlocksImpl::recover_index_table(homestore::superblk< homestore::index_table_sb >&& sb) {
    auto id = sb->parent_uuid;
    {
        auto lg = std::scoped_lock(vol_lock_);
        auto it = vol_map_.find(id);
        DEBUG_ASSERT(it != vol_map_.end(), "volume id: {} not exists in recovery path, not expected!",
                     boost::uuids::to_string(id));

        return it->second->init_index_table(true, std::move(sb));
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

VolumeManager::NullAsyncResult HomeBlocksImpl::remove_volume(const volume_id_t& id) { return folly::Unit(); }

VolumeInfoPtr HomeBlocksImpl::lookup_volume(const volume_id_t& id) { return nullptr; }

bool HomeBlocksImpl::get_stats(volume_id_t id, VolumeStats& stats) const { return true; }

void HomeBlocksImpl::get_volume_ids(std::vector< volume_id_t >& vol_ids) const {}
} // namespace homeblocks
