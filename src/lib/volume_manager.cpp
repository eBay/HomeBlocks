#include <boost/uuid/uuid_io.hpp>

#include "homeblocks_impl.hpp"

namespace homeblocks {

std::shared_ptr< VolumeManager > HomeBlocksImpl::volume_manager() { return shared_from_this(); }

VolumeManager::NullAsyncResult HomeBlocksImpl::create_volume(VolumeInfo&& vol_info) {
    LOGI("[vol={}] is of capacity [{}B]", boost::uuids::to_string(vol_info.id), vol_info.size_bytes);
    return _create_volume(std::move(vol_info));
}

bool HomeBlocksImpl::get_stats(volume_id_t id, VolumeStats& stats) const { return _get_stats(id, stats); }
void HomeBlocksImpl::get_volume_ids(std::vector< volume_id_t >& vol_ids) const { return _get_volume_ids(vol_ids); }

} // namespace homeobject
