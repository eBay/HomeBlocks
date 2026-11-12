
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
#include "listener.hpp"

namespace homeblocks {

void HBListener::on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                           std::vector< homestore::multi_blk_id > const& blkids,
                           cintrusive< homestore::repl_req_ctx >& ctx) {
    // on_commit called whenever journal has flushed log entries. header contains the msg type and volume
    // id, key contains the list of checksum, list of old blkids for write case. blkid's are the new blkid's
    // where data is written.
    const MsgHeader* msg_header = reinterpret_cast< const MsgHeader* >(header.cbytes());
    switch (msg_header->msg_type) {
    case MsgType::WRITE:
        hb_->on_write(lsn, header, key, blkids, ctx);
        break;

    case MsgType::READ:
        break;
    case MsgType::UNMAP:
        break;
    }
}

bool HBListener::on_pre_commit(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                               cintrusive< homestore::repl_req_ctx >& ctx) {
    return true;
}

void HBListener::on_error(homestore::ReplServiceError error, const sisl::blob& header, const sisl::blob& key,
                          cintrusive< homestore::repl_req_ctx >& ctx) {}

homestore::result< homestore::blk_alloc_hints >
HBListener::get_blk_alloc_hints(sisl::blob const& header, uint32_t data_size,
                                cintrusive< homestore::repl_req_ctx >& hs_ctx) {
    return homestore::blk_alloc_hints();
}

void HBListener::on_destroy(const homestore::group_id_t& group_id) {}

static bool is_create_volume(sisl::blob const& header) {
    return header.size() >= sizeof(CraftEntryHeader) &&
        reinterpret_cast< const CraftEntryHeader* >(header.cbytes())->type == CraftEntryType::CreateVolume;
}

volume_handle CraftGroupListener::craft_volume(int64_t lsn) const {
    if (auto vol = hb_->get_volume(group_id_); vol && (*vol)->craft_dev()) { return *vol; }
    LOGW("group={} lsn={}: no CRAFT volume on this node (yet), entry dropped", boost::uuids::to_string(group_id_), lsn);
    return nullptr;
}

void CraftGroupListener::on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                   std::vector< homestore::multi_blk_id > const& blkids,
                                   cintrusive< homestore::repl_req_ctx >& ctx) {
    if (is_create_volume(header)) {
        hb_->on_create_volume_commit(lsn, key, ctx, replaying_.load());
    } else if (auto vol = craft_volume(lsn)) {
        vol->craft_dev()->raft_listener().on_commit(lsn, header, key, blkids, ctx);
    }
}

void CraftGroupListener::on_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                     cintrusive< homestore::repl_req_ctx >& ctx) {
    if (is_create_volume(header)) {
        create_volume_ctx::complete(ctx, std::unexpected(std::errc::operation_canceled));
    } else if (auto vol = craft_volume(lsn)) {
        vol->craft_dev()->raft_listener().on_rollback(lsn, header, key, ctx);
    }
}

void CraftGroupListener::on_destroy(homestore::group_id_t const& group_id) { hb_->on_craft_group_destroyed(group_id); }

void CraftGroupListener::on_error(homestore::ReplServiceError error, sisl::blob const& header, sisl::blob const& key,
                                  cintrusive< homestore::repl_req_ctx >& ctx) {
    if (is_create_volume(header)) {
        create_volume_ctx::complete(ctx, std::unexpected(error));
    } else if (auto vol = craft_volume(-1)) {
        vol->craft_dev()->raft_listener().on_error(error, header, key, ctx);
    }
}

} // namespace homeblocks
