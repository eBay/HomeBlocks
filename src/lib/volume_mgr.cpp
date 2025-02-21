#include <boost/uuid/uuid_io.hpp>

#include "homeblks_impl.hpp"

namespace homeblocks {

std::shared_ptr< VolumeManager > HomeBlocksImpl::volume_manager() { return shared_from_this(); }

VolumeManager::NullAsyncResult HomeBlocksImpl::create_volume(VolumeInfo&& vol_info) {
    LOGI("[vol={}] is of capacity [{}B]", boost::uuids::to_string(vol_info.id), vol_info.size_bytes);
    return folly::Unit();
}

VolumeManager::NullAsyncResult HomeBlocksImpl::remove_volume(const volume_id_t& id) { return folly::Unit(); }

VolumeInfoPtr HomeBlocksImpl::lookup_volume(const volume_id_t& id) { return nullptr; }

bool HomeBlocksImpl::get_stats(volume_id_t id, VolumeStats& stats) const { return true; }

void HomeBlocksImpl::get_volume_ids(std::vector< volume_id_t >& vol_ids) const {}
} // namespace homeblocks
