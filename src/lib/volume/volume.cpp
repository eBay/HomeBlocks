#include "volume.hpp"
#include <homestore/replication_service.hpp>

namespace homeblocks {

void Volume::init_index_table() {
    // create index table;
    auto uuid = hb_utils::gen_random_uuid();

    index_cfg_t cfg(homestore::hs()->index_service().node_size());
    cfg.m_leaf_node_type = homestore::btree_node_type::PREFIX;
    cfg.m_int_node_type = homestore::btree_node_type::PREFIX;

    // user_sb_size is not currently enabled in homestore;
    indx_tbl_ = std::make_shared< VolumeIndexTable >(uuid, vol_info_.id /*parent uuid*/, 0 /*user_sb_size*/, cfg);
}

Volume::Volume(sisl::byte_view const& buf, void* cookie) : sb_{VOL_META_NAME} {
    sb_.load(buf, cookie);
    // generate volume info from sb;
    vol_info_.page_size = sb_->page_size;
    vol_info_.size_bytes = sb_->size;
    vol_info_.id = sb_->id;
    vol_info_.name = sb_->name;
}

void Volume::init(bool is_recovery) {
    if (!is_recovery) {
        sb_.create(sizeof(vol_sb_t));
        sb_->init(vol_info_.page_size, vol_info_.size_bytes, vol_info_.id, vol_info_.name);
        // write to disk;
        sb_.write();
    }

    // initalize index table and save index handle;
    init_index_table();
    homestore::hs()->index_service().add_index_table(indx_table());

    // create repl dev. members left empty on purpose for solo repl dev
    rd_ = homestore::hs()->repl_service().create_repl_dev(uuid_, {} /*members*/).get().value();
}

} // namespace homeblocks
