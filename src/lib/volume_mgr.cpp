
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
        cfg.m_int_node_type = homestore::btree_node_type::PREFIX;

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

        // 1. remove destroy volume and remove volume from vol_map;
        {
            auto lg = std::scoped_lock(vol_lock_);
            if (auto it = vol_map_.find(id); it != vol_map_.end()) {
                auto vol_ptr = it->second;
                vol_ptr->destroy();
#ifdef _PRERELEASE
                if (iomgr_flip::instance()->test_flip("vol_destroy_crash_simulation")) { return folly::Unit(); }
#endif
                vol_map_.erase(it);
                LOGINFO("Volume {} removed successfully", vol_ptr->id_str());
            } else {
                LOGWARN("remove_volume with input id: {} not found", boost::uuids::to_string(id));
            }

            // Volume Destructor will be called after vol_ptr goes out of scope;
        }

        return folly::Unit();
    });
    return folly::Unit();
}

VolumeInfoPtr HomeBlocksImpl::lookup_volume(const volume_id_t& id) {
    auto lg = std::shared_lock(vol_lock_);
    if (auto it = vol_map_.find(id); it != vol_map_.end()) { return it->second->info(); }
    return nullptr;
}

bool HomeBlocksImpl::get_stats(volume_id_t id, VolumeStats& stats) const { return true; }

void HomeBlocksImpl::get_volume_ids(std::vector< volume_id_t >& vol_ids) const {}
} // namespace homeblocks
