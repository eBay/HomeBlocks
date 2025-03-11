#include "volume.hpp"
#include <homestore/replication_service.hpp>

namespace homeblocks {

shared< VolumeIndexTable > Volume::init_index_table(bool is_recovery,
                                                    homestore::superblk< homestore::index_table_sb >&& sb) {
    // create index table;
    auto uuid = hb_utils::gen_random_uuid();

    index_cfg_t cfg(homestore::hs()->index_service().node_size());
    cfg.m_leaf_node_type = homestore::btree_node_type::PREFIX;
    cfg.m_int_node_type = homestore::btree_node_type::PREFIX;

    // user_sb_size is not currently enabled in homestore;
    // parent uuid is used during recovery in homeblks layer;
    indx_tbl_ = std::make_shared< VolumeIndexTable >(uuid, vol_info_.id /*parent uuid*/, 0 /*user_sb_size*/, cfg);

    homestore::hs()->index_service().add_index_table(indx_table());
    return indx_table();
}

Volume::Volume(sisl::byte_view const& buf, void* cookie) : sb_{VOL_META_NAME} {
    sb_.load(buf, cookie);
    // generate volume info from sb;
    vol_info_.page_size = sb_->page_size;
    vol_info_.size_bytes = sb_->size;
    vol_info_.id = sb_->id;
    vol_info_.name = sb_->name;
}

bool Volume::init(bool is_recovery) {
    if (!is_recovery) {
        sb_.create(sizeof(vol_sb_t));
        sb_->init(vol_info_.page_size, vol_info_.size_bytes, vol_info_.id, vol_info_.name);
        // write to disk;
        sb_.write();
    }

    // create solo repl dev for volume;
    // members left empty on purpose for solo repl dev
    auto ret = homestore::hs()->repl_service().create_repl_dev(uuid_, {} /*members*/).get();
    if (ret.hasError()) {
        LOGE("Failed to create solo repl dev for volume: {}, uuid: {}, error: {}", vol_info_.name,
             boost::uuids::to_string(vol_info_.id), ret.error());
        return false;
    }

    rd_ = ret.value();
    return true;
}

} // namespace homeblocks
