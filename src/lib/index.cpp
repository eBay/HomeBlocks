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
#include "volume/volume.hpp"
#include "homeblks_impl.hpp"

namespace homeblocks {

VolumeManager::Result< folly::Unit >
HomeBlocksImpl::write_to_index(const VolumePtr& vol_ptr, lba_t start_lba, lba_t end_lba,
                               std::unordered_map< lba_t, BlockInfo >& blocks_info) {

    // Use filter callback to get the old blkid.
    homestore::put_filter_cb_t filter_cb = [&blocks_info](BtreeKey const& key, BtreeValue const& existing_value,
                                                          BtreeValue const& value) {
        auto lba = r_cast< const VolumeIndexKey& >(key).key();
        blocks_info[lba].old_blkid = r_cast< const VolumeIndexValue& >(existing_value).blkid();
        return homestore::put_filter_decision::replace;
    };

    // Write to prefix btree with key ranging from start_lba to end_lba.
    // For value shift() will get the blk_num and checksum for each lba.
    IndexValueContext app_ctx{&blocks_info, start_lba};
    const BlkId& start_blkid = blocks_info[start_lba].new_blkid;
    VolumeIndexValue value{start_blkid, blocks_info[start_lba].checksum};

    auto req = homestore::BtreeRangePutRequest< VolumeIndexKey >{
        homestore::BtreeKeyRange< VolumeIndexKey >{VolumeIndexKey{start_lba}, true, VolumeIndexKey{end_lba}, true},
        homestore::btree_put_type::UPSERT,
        r_cast< VolumeIndexValue* >(&value),
        r_cast< void* >(&app_ctx),
        std::numeric_limits< uint32_t >::max() /* batch_size */,
        filter_cb};
    auto result = vol_ptr->indx_table()->put(req);
    if (result != homestore::btree_status_t::success) {
        LOGERROR("Failed to put to index range=({},{}) error={}", start_lba, end_lba, result);
        return folly::makeUnexpected(VolumeError::INDEX_ERROR);
    }

    return folly::Unit();
}

VolumeManager::Result< folly::Unit > HomeBlocksImpl::read_from_index(const VolumePtr& vol_ptr, const vol_interface_req_ptr& req,
                                                                    index_kv_list_t& index_kvs) {
    homestore::BtreeQueryRequest< VolumeIndexKey > qreq{homestore::BtreeKeyRange< VolumeIndexKey >{VolumeIndexKey{req->lba}, 
                                VolumeIndexKey{req->end_lba()}}, homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY};
    auto index_table = vol_ptr->indx_table();
    RELEASE_ASSERT(index_table != nullptr, "Index table is null for volume id: {}", boost::uuids::to_string(vol_ptr->id()));
    if (auto ret = index_table->query(qreq, index_kvs); ret != homestore::btree_status_t::success) {
        return folly::makeUnexpected(VolumeError::INDEX_ERROR);
    }
    return folly::Unit();
}

} // namespace homeblocks