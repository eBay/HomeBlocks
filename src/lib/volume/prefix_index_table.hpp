#pragma once

#include "index_kv_prefix.hpp"

namespace homeblocks {

static const auto btree_leaf_node_type = homestore::btree_node_type::PREFIX;
static const auto btree_int_node_type = homestore::btree_node_type::FIXED;

using index_kv_list_t = std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >;
using hs_index_table_t = homestore::IndexTable< VolumeIndexKey, VolumeIndexValue >;

class VolumeIndexTable {
    std::shared_ptr< hs_index_table_t > hs_index_table_;
public:
    template <typename... Args>
    VolumeIndexTable(Args&&... args) : hs_index_table_(std::make_shared< hs_index_table_t >(std::forward<Args>(args)...)) {
        LOGINFO("Created Prefix Index table, uuid {}", boost::uuids::to_string(hs_index_table_->uuid()));
    }

    std::shared_ptr< hs_index_table_t > index_table() {
        return hs_index_table_;
    }
    
    VolumeManager::Result< folly::Unit > write_to_index(lba_t start_lba, lba_t end_lba,
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
        auto result = hs_index_table_->put(req);
        if (result != homestore::btree_status_t::success) {
            LOGERROR("Failed to put to index range=({},{}) error={}", start_lba, end_lba, result);
            return folly::makeUnexpected(VolumeError::INDEX_ERROR);
        }

        return folly::Unit();
    }

    VolumeManager::Result< folly::Unit > read_from_index(const vol_interface_req_ptr& req,
                                                             index_kv_list_t& index_kvs) {
        homestore::BtreeQueryRequest< VolumeIndexKey > qreq{
            homestore::BtreeKeyRange< VolumeIndexKey >{VolumeIndexKey{req->lba}, VolumeIndexKey{req->end_lba()}},
            homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY};
        if (auto ret = hs_index_table_->query(qreq, index_kvs); ret != homestore::btree_status_t::success) {
            return folly::makeUnexpected(VolumeError::INDEX_ERROR);
        }
      return folly::Unit();
    }

    void destroy() {
        homestore::hs()->index_service().remove_index_table(hs_index_table_);
        hs_index_table_->destroy();
    }
};

} // namespace homeblocks