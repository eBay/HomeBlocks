#pragma once

#include "index_fixed_kv.hpp"

namespace homeblocks {

static const auto btree_leaf_node_type = homestore::btree_node_type::FIXED;
static const auto btree_int_node_type = homestore::btree_node_type::FIXED;

using index_kv_list_t = std::vector< std::pair< VolumeIndexKey, VolumeIndexValue > >;
using hs_index_table_t = homestore::IndexTable< VolumeIndexKey, VolumeIndexValue >;

class VolumeIndexTable {
    std::shared_ptr< hs_index_table_t > hs_index_table_;

public:
    template < typename... Args >
    VolumeIndexTable(Args&&... args) :
            hs_index_table_(std::make_shared< hs_index_table_t >(std::forward< Args >(args)...)) {
        LOGINFO("Created Fixed Index table, uuid {}", boost::uuids::to_string(hs_index_table_->uuid()));
    }

    std::shared_ptr< hs_index_table_t > index_table() { return hs_index_table_; }

    VolumeManager::Result< folly::Unit > write_to_index(lba_t start_lba, lba_t end_lba,
                                                        std::unordered_map< lba_t, BlockInfo >& blocks_info) {
        // Use filter callback to get the old blkid.
        homestore::put_filter_cb_t filter_cb = [&blocks_info](BtreeKey const& key, BtreeValue const& existing_value,
                                                              BtreeValue const& value) {
            auto lba = r_cast< const VolumeIndexKey& >(key).lba();
            auto& existing_value_vol_idx = r_cast< const VolumeIndexValue& >(existing_value);
            blocks_info[lba].old_blkid = existing_value_vol_idx.blkid();
            blocks_info[lba].old_checksum = existing_value_vol_idx.checksum();
            return homestore::put_filter_decision::replace;
        };

        // Write to fixed btree with key ranging from start_lba to end_lba.
        for (auto lba = start_lba; lba <= end_lba; ++lba) {
            VolumeIndexKey key{lba};
            auto& block_info = blocks_info[lba];
            VolumeIndexValue value{block_info.new_blkid, block_info.new_checksum};
            auto req = homestore::BtreeSinglePutRequest{&key, &value, homestore::btree_put_type::UPSERT,
                                                        nullptr /* existing value, not needed here */, filter_cb};
            auto result = hs_index_table_->put(req);
#ifdef _PRERELEASE
            if (iomgr_flip::instance()->test_flip("vol_index_partial_put_failure")) {
                // this is to simulate failure after partially writing to index.
                if (result != homestore::btree_status_t::success) {
                    LOGWARN("Put failed with error: {}, NOT EXPECTED!", result);
                } else if (lba > start_lba && ((lba - start_lba) >= (end_lba - start_lba) / 2)) {
                    // simulate failure after writing to half of the index
                    // restore the blkid at this lba to the old value
                    LOGINFO("vol_index_partial_put_failure flip is set, aborting");
                    value = VolumeIndexValue{blocks_info[lba].old_blkid, blocks_info[lba].old_checksum};
                    blocks_info[lba].old_blkid = homestore::BlkId{};
                    blocks_info[lba].old_checksum = 0;
                    auto req1 = homestore::BtreeSinglePutRequest{&key, &value, homestore::btree_put_type::UPSERT};
                    if (auto restore_lba_result = hs_index_table_->put(req1);
                        restore_lba_result != homestore::btree_status_t::success) {
                        LOGERROR("Failed to rollback lba {}, put error={}, NOT EXPECTED!", lba, restore_lba_result);
                    }
                    result = homestore::btree_status_t::not_supported;
                }
            }
#endif
            if (result != homestore::btree_status_t::success) {
                LOGERROR("Failed to put to index {}, error={}", lba, result);
                // rollback the lbas for which we have already written to the index table
                rollback_write(start_lba, lba - 1, blocks_info);
                return folly::makeUnexpected(VolumeError::INDEX_ERROR);
            }
        }

        return folly::Unit();
    }

    VolumeManager::Result< folly::Unit > read_from_index(const vol_interface_req_ptr& req, index_kv_list_t& index_kvs) {
        homestore::BtreeQueryRequest< VolumeIndexKey > qreq{
            homestore::BtreeKeyRange< VolumeIndexKey >{VolumeIndexKey{req->lba}, VolumeIndexKey{req->end_lba()}},
            homestore::BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY};
        if (auto ret = hs_index_table_->query(qreq, index_kvs); ret != homestore::btree_status_t::success) {
            return folly::makeUnexpected(VolumeError::INDEX_ERROR);
        }
        return folly::Unit();
    }

    void rollback_write(lba_t start_lba, lba_t end_lba, std::unordered_map< lba_t, BlockInfo >& blocks_info) {
        for (auto lba = start_lba; lba <= end_lba; ++lba) {
            VolumeIndexKey key{lba};
            VolumeIndexValue value;
            // If old_blk_id is valid, we need to restore it, otherwise remove the entry.
            if (blocks_info[lba].old_blkid.is_valid()) {
                value = VolumeIndexValue{blocks_info[lba].old_blkid, blocks_info[lba].old_checksum};
                auto req = homestore::BtreeSinglePutRequest{&key, &value, homestore::btree_put_type::UPSERT};
                if (auto result = hs_index_table_->put(req); result != homestore::btree_status_t::success) {
                    LOGERROR("Failed to rollback lba {}, put error={}", lba, result);
                }
            } else {
                auto req = homestore::BtreeSingleRemoveRequest{&key, &value};
                if (auto result = hs_index_table_->remove(req); result != homestore::btree_status_t::success) {
                    LOGERROR("Failed to rollback lba {}, remove error={}", lba, result);
                }
            }
        }
    }

    void destroy() {
        homestore::hs()->index_service().remove_index_table(hs_index_table_);
        hs_index_table_->destroy();
    }
};

} // namespace homeblocks