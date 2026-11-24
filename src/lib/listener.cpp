
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

// Only a published device has anything to roll back or fail: committed entries are all it is ever handed.
shared< CraftReplDev > CraftGroupListener::craft_device() const {
    if (auto vol = hb_->get_volume(group_id_)) { return (*vol)->craft_dev_ref(); }
    return nullptr;
}

void CraftGroupListener::on_commit(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                   std::vector< homestore::multi_blk_id > const& blkids,
                                   cintrusive< homestore::repl_req_ctx >& ctx) {
    if (is_create_volume(header)) {
        hb_->on_create_volume_commit(lsn, key, ctx, replaying_.load());
    } else if (auto vol = hb_->get_volume(group_id_);
               !vol || !(*vol)->deliver_craft_entry(lsn, header, key, blkids, ctx)) {
        // No replica here, and none coming (its local create failed, or it is being destroyed): reported once.
        if (!entries_dropped_logged_.test_and_set()) {
            LOGW("group={} lsn={}: no CRAFT replica on this node, the group's committed entries are dropped",
                 boost::uuids::to_string(group_id_), lsn);
        }
    }
}

void CraftGroupListener::on_rollback(int64_t lsn, sisl::blob const& header, sisl::blob const& key,
                                     cintrusive< homestore::repl_req_ctx >& ctx) {
    if (is_create_volume(header)) {
        create_volume_ctx::complete(ctx, std::unexpected(std::errc::operation_canceled));
    } else if (auto const dev = craft_device()) {
        dev->raft_listener().on_rollback(lsn, header, key, ctx);
    }
}

void CraftGroupListener::on_destroy(homestore::group_id_t const& group_id) {
    hb_->remove_craft_snapshot(group_id);
    hb_->on_craft_group_destroyed(group_id);
}

static void log_member_change(homestore::group_id_t const& group_id, char const* event, std::string const& task_id,
                              homestore::replica_member_info const& out, homestore::replica_member_info const& in) {
    LOGW("group={}: {} (task {}, out={}, in={}): membership changes are not supported yet",
         boost::uuids::to_string(group_id), event, task_id, boost::uuids::to_string(out.id),
         boost::uuids::to_string(in.id));
}

void CraftGroupListener::on_start_replace_member(std::string const& task_id,
                                                 homestore::replica_member_info const& member_out,
                                                 homestore::replica_member_info const& member_in,
                                                 homestore::trace_id_t) {
    log_member_change(group_id_, "replace member started", task_id, member_out, member_in);
}

void CraftGroupListener::on_complete_replace_member(std::string const& task_id,
                                                    homestore::replica_member_info const& member_out,
                                                    homestore::replica_member_info const& member_in,
                                                    homestore::trace_id_t) {
    log_member_change(group_id_, "replace member completed", task_id, member_out, member_in);
}

void CraftGroupListener::on_clean_replace_member_task(std::string const& task_id,
                                                      homestore::replica_member_info const& member_out,
                                                      homestore::replica_member_info const& member_in,
                                                      homestore::trace_id_t) {
    log_member_change(group_id_, "replace member task cleaned", task_id, member_out, member_in);
}

void CraftGroupListener::on_remove_member(homestore::replica_id_t const& member, homestore::trace_id_t) {
    LOGW("group={}: member {} removed: membership changes are not supported yet", boost::uuids::to_string(group_id_),
         boost::uuids::to_string(member));
}

int CraftGroupListener::read_snapshot_obj(std::shared_ptr< homestore::snapshot_context > context,
                                          std::shared_ptr< homestore::snapshot_obj >) {
    COUNTER_INCREMENT(hb_->metrics(), craft_snapshot_transfers_refused, 1);
    // nuraft asks again at every heartbeat: logged once.
    if (!snapshot_refusal_logged_.test_and_set()) {
        LOGE("group={}: refused a RAFT snapshot up to lsn {} to a member behind the log compaction: CRAFT can't "
             "serve one yet, so that member stays behind until it is replaced",
             boost::uuids::to_string(group_id_), context->get_lsn());
    }
    return -1;
}

homestore::async_status CraftGroupListener::create_snapshot(std::shared_ptr< homestore::snapshot_context > context) {
    std::lock_guard lk{snapshot_mu_};
    if (auto const last = last_snapshot_locked(); last && context->get_lsn() <= last->get_lsn()) {
        co_return homestore::ok();
    }
    // Durable before nuraft compacts the log on it.
    hb_->save_craft_snapshot(group_id_, *context);
    last_snapshot_ = std::move(context);
    co_return homestore::ok();
}

std::shared_ptr< homestore::snapshot_context > CraftGroupListener::last_snapshot() {
    std::lock_guard lk{snapshot_mu_};
    return last_snapshot_locked();
}

// After a restart, the one the group's record kept, until nuraft takes the next.
std::shared_ptr< homestore::snapshot_context > CraftGroupListener::last_snapshot_locked() {
    if (!last_snapshot_) {
        if (auto ctx = hb_->craft_snapshot(group_id_); ctx.size() > 0) {
            if (auto const rd = device()) { last_snapshot_ = rd->deserialize_snapshot_context(ctx); }
        }
    }
    return last_snapshot_;
}

bool CraftGroupListener::apply_snapshot(std::shared_ptr< homestore::snapshot_context > context) {
    // Unreachable while every leader refuses to send one. Applying one would leave this replica without the entries the
    // snapshot covers; failing it makes nuraft stop this process instead.
    LOGE("group={}: a RAFT snapshot up to lsn {} arrived, which CRAFT can't apply", boost::uuids::to_string(group_id_),
         context->get_lsn());
    return false;
}

void CraftGroupListener::on_error(homestore::ReplServiceError error, sisl::blob const& header, sisl::blob const& key,
                                  cintrusive< homestore::repl_req_ctx >& ctx) {
    if (is_create_volume(header)) {
        create_volume_ctx::complete(ctx, std::unexpected(error));
    } else if (auto const dev = craft_device()) {
        dev->raft_listener().on_error(error, header, key, ctx);
    }
}

} // namespace homeblocks
