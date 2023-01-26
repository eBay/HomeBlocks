namespace home_replication {

HomeReplicationServiceImpl::HomeReplicationServiceImpl() : {
    homestore::meta_service().register_handler(
        "replica_set",
        [this](homestore::meta_blk* mblk, sisl::byte_view buf, size_t size) {
            rs_super_blk_found(std::move(buf), voidptr_cast(mblk));
        },
        nullptr);
}

void HomeReplicationServiceImpl::rs_super_blk_found(const sisl::byte_view& buf, void* meta_cookie) {
    homestore::superblk< home_rs_superblk > rs_sb;
    rs_sb.load(buf, meta_cookie);
    HS_REL_ASSERT_EQ(m_sb->get_magic(), rs_superblk::REPLICA_SET_SB_MAGIC, "Invalid rs metablk, magic mismatch");
    HS_REL_ASSERT_EQ(m_sb->get_version(), rs_superblk::REPLICA_SET_SB_MAGIC, "Invalid version of rs metablk");

    auto sms = std::make_shared< HomeStateMachineStore >(rs_sb);
    {
        std::unique_lock lg(m_sm_store_map_mtx);
        m_sm_store_map.insert(std::make_pair(rs_sb->uuid, sms));
    }
}

std::shared_ptr< StateMachineStore > HomeReplicationServiceImpl::create_state_store(uuid_t uuid) {
    return std::make_shared< HomeStateMachineStore >(uuid));
}

void HomeReplicationServiceImpl::iterate_state_stores(const std::function< void(const sm_store_ptr_t&) >& cb) {
    std::unique_lock lg(m_sm_store_map_mtx);
    for (const auto& [uuid, sms] : m_sm_store_map) {
        cb(sms);
    }
}
} // namespace home_replication