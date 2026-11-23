
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

#include <algorithm>
#include <cstring>

#include <homestore/replication_service.hpp>

#include "craft/craft_raft_entries.hpp"
#include "craft/craft_repl_dev.hpp"
#include "craft/craft_snapshot.hpp"
#include "craft/craft_snapshot_reader.hpp"
#include "home_blks_config.hpp"
#include "coro_helpers.hpp"
#include <homestore/checkpoint/cp_mgr.hpp>

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

void CraftGroupListener::on_destroy(homestore::group_id_t const& group_id) { hb_->on_craft_group_destroyed(group_id); }

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

// ─── snapshots (docs/craft/resync.md 4.9) ───────────────────────────────────

namespace {
constexpr uint64_t k_mib = 1024ull * 1024ull;
constexpr homestore::csum_t k_craft_crc16_seed = 0x8005; // CraftReplDev's checksums (craft_repl_dev.cpp)

// One transfer, held in nuraft's user_ctx from its first object until free_user_snp_ctx.
struct snapshot_transfer {
    int64_t lsn;
    unique< craft_snapshot::snapshot_source > source; // declared before the reader, which reads through it
    craft_snapshot::snapshot_reader reader;
};
} // namespace

int CraftGroupListener::refuse_snapshot(int64_t lsn, char const* why) {
    COUNTER_INCREMENT(hb_->metrics(), craft_snapshot_transfers_refused, 1);
    // nuraft asks again at every heartbeat: logged once.
    if (!snapshot_refusal_logged_.test_and_set()) {
        LOGE("group={}: can't serve the RAFT snapshot up to lsn {}: {}; nuraft asks again later",
             boost::uuids::to_string(group_id_), lsn, why);
    }
    return -1;
}

homestore::async_status CraftGroupListener::create_snapshot(std::shared_ptr< homestore::snapshot_context > context) {
    auto const lsn = context->get_lsn();
    {
        std::lock_guard lk{snapshot_mu_};
        if (last_snapshot_ && lsn <= last_snapshot_->get_lsn()) { co_return homestore::ok(); }
        last_snapshot_ = context;
    }
    // Captured now, while the local state is L's: nuraft is still on L's commit (see capture_snapshot_tuple()).
    auto const dev = craft_device();
    auto const vol = hb_->get_volume(group_id_);
    if (!dev || !vol) { co_return homestore::ok(); } // no replica here: nothing this member could serve
    auto const& info = *(*vol)->info();
    auto const lba_size = dev->lba_size();
    craft_snapshot::snapshot_meta meta{.raft_idx = lsn, .tuple = dev->capture_snapshot_tuple(), .lba_size = lba_size};
    meta.digest = craft_snapshot::tuple_digest(meta.tuple);
    meta.total_lbas = info.size_bytes / lba_size;
    // A region must fit in k_max_batch batches of at least one LBA... and of batch_mb at most.
    auto const batch_bytes = std::max< uint64_t >(HB_DYNAMIC_CONFIG(craft_snapshot_batch_mb) * k_mib, lba_size);
    auto region_lbas = std::max< uint64_t >(HB_DYNAMIC_CONFIG(craft_snapshot_region_mb) * k_mib / lba_size, 1);
    region_lbas = std::min< uint64_t >(region_lbas, (batch_bytes / lba_size) * (craft_snapshot::k_max_batch - 1));
    meta.region_lbas = region_lbas;
    meta.region_count = craft_snapshot::region_count(meta.total_lbas, region_lbas);
    meta.volume_def.resize(create_volume_key_size(info.members.size(), info.name.size()));
    serialize_create_volume(meta.volume_def.data(), info);
    hb_->save_craft_snapshot(group_id_, lsn, craft_snapshot::encode(meta), context->serialize());
    LOGI("group={}: snapshot at raft lsn {}: W={} S={} term={} empties={} regions={}",
         boost::uuids::to_string(group_id_), lsn, meta.tuple.data_watermark, meta.tuple.sync_watermark,
         meta.tuple.session_term, meta.tuple.empty_lsns.size(), meta.region_count);
    co_return homestore::ok();
}

std::shared_ptr< homestore::snapshot_context > CraftGroupListener::last_snapshot() {
    std::lock_guard lk{snapshot_mu_};
    if (last_snapshot_) { return last_snapshot_; }
    // After a restart: the record this member kept.
    auto const rec = hb_->craft_snapshot(group_id_);
    if (!rec) { return nullptr; }
    auto const rd = homestore::hs()->repl_service().get_repl_dev(group_id_);
    if (!rd) { return nullptr; } // asked before the group is registered: asked again later
    sisl::io_blob_safe blob{static_cast< uint32_t >(rec->ctx.size())};
    std::memcpy(blob.bytes(), rec->ctx.data(), rec->ctx.size());
    try {
        last_snapshot_ = (*rd)->deserialize_snapshot_context(blob);
    } catch (std::exception const& e) {
        // Only an optimization is lost: nuraft creates the next snapshot at its usual distance.
        LOGW("group={}: the snapshot record at raft lsn {} doesn't deserialize ({}): removed",
             boost::uuids::to_string(group_id_), rec->raft_idx, e.what());
        hb_->remove_craft_snapshot(group_id_);
        return nullptr;
    }
    return last_snapshot_;
}

int CraftGroupListener::read_snapshot_obj(std::shared_ptr< homestore::snapshot_context > context,
                                          std::shared_ptr< homestore::snapshot_obj > snp_obj) {
    auto const lsn = context->get_lsn();
    auto*& slot = snp_obj->user_ctx;
    auto* t = static_cast< snapshot_transfer* >(slot);
    if (t && t->lsn != lsn) { // a new context on the same transfer slot
        delete t;
        slot = t = nullptr;
    }
    if (!t) {
        if (!HB_DYNAMIC_CONFIG(craft_snapshot_enabled)) {
            return refuse_snapshot(lsn, "serving snapshots is off (craft_snapshot_enabled)");
        }
        auto const rec = hb_->craft_snapshot(group_id_);
        if (!rec || rec->raft_idx != lsn) { return refuse_snapshot(lsn, "no record of that snapshot here"); }
        auto const dev = craft_device();
        if (!dev) { return refuse_snapshot(lsn, "no replica of the volume here"); }
        auto obj = craft_snapshot::decode(rec->meta.data(), rec->meta.size());
        if (!obj || !std::holds_alternative< craft_snapshot::snapshot_meta >(*obj)) {
            return refuse_snapshot(lsn, "its record doesn't decode");
        }
        auto meta = std::get< craft_snapshot::snapshot_meta >(std::move(*obj));
        // The receiver replays every slot above W from the members' journals; this one's must still hold them.
        // TODO(RetainFrom, resync.md 4.8): pin W on every journal source, then validate, instead of this local check.
        if (dev->journal_reclaimed_upto() > meta.tuple.data_watermark) {
            return refuse_snapshot(lsn, "this member's journal is reclaimed past its W");
        }
        auto source = CraftReplDev::make_snapshot_source(dev, meta.total_lbas);
        auto& src = *source;
        auto const batch_bytes = HB_DYNAMIC_CONFIG(craft_snapshot_batch_mb) * k_mib;
        t = new snapshot_transfer{
            lsn, std::move(source),
            craft_snapshot::snapshot_reader{std::move(meta), src, batch_bytes, k_craft_crc16_seed}};
        slot = t;
        LOGI("group={}: serving the snapshot at raft lsn {}", boost::uuids::to_string(group_id_), lsn);
    }
    auto reply = t->reader.serve(snp_obj->offset);
    using st = craft_snapshot::snapshot_reader::status;
    switch (reply.st) {
    case st::ok:
        snp_obj->blob = sisl::io_blob_safe{static_cast< uint32_t >(reply.blob.size())};
        std::memcpy(snp_obj->blob.bytes(), reply.blob.data(), reply.blob.size());
        COUNTER_INCREMENT(hb_->metrics(), craft_snapshot_objects_sent, 1);
        COUNTER_INCREMENT(hb_->metrics(), craft_snapshot_bytes_sent, reply.blob.size());
        return 0;
    case st::last:
        snp_obj->is_last_obj = true;
        LOGI("group={}: snapshot at raft lsn {} sent (resends {}, checksum retries {})",
             boost::uuids::to_string(group_id_), lsn, t->reader.resends(), t->reader.checksum_retries());
        return 0;
    case st::bad_cursor: // an empty object: the receiver asks for what it wants next again
        LOGW("group={}: snapshot object {:#x} out of order: reset", boost::uuids::to_string(group_id_),
             snp_obj->offset);
        return 0;
    case st::error:
        break;
    }
    COUNTER_INCREMENT(hb_->metrics(), craft_snapshot_read_errors, 1);
    return -1; // nuraft asks for the same object again
}

void CraftGroupListener::free_user_snp_ctx(void*& user_snp_ctx) {
    delete static_cast< snapshot_transfer* >(user_snp_ctx);
    user_snp_ctx = nullptr;
}

// ── the receiver ──

namespace {
// How long the install barrier waits for ops already running to finish.
constexpr std::chrono::seconds k_install_drain_timeout{30};

uint64_t id_of(uint64_t region, uint32_t batch) {
    return *craft_snapshot::obj_id{.region = region, .batch = batch}.pack();
}

// The object after region `r` is fully installed: the next region's first batch, or the trailer.
uint64_t after_region(uint64_t r, uint64_t regions) { return r < regions ? id_of(r + 1, 1) : id_of(regions + 1, 0); }
} // namespace

bool CraftGroupListener::load_receive_state() {
    if (recv_) { return true; }
    auto const rec = hb_->craft_snapshot_receive_record(group_id_);
    if (!rec) { return false; }
    auto obj = craft_snapshot::decode(rec->meta.data(), rec->meta.size());
    if (!obj || !std::holds_alternative< craft_snapshot::snapshot_meta >(*obj)) { return false; }
    recv_ = receive_state{.hdr = rec->hdr, .meta = std::get< craft_snapshot::snapshot_meta >(std::move(*obj))};
    return true;
}

void CraftGroupListener::save_receive_state() {
    hb_->save_craft_snapshot_receive(
        HomeBlocksImpl::craft_snapshot_receive{.hdr = recv_->hdr, .meta = craft_snapshot::encode(recv_->meta)});
}

void CraftGroupListener::write_snapshot_obj(std::shared_ptr< homestore::snapshot_context > context,
                                            std::shared_ptr< homestore::snapshot_obj > snp_obj) {
    // Leaving snp_obj->offset as it came asks the leader for the same object again.
    auto const lsn = context->get_lsn();
    auto const group = boost::uuids::to_string(group_id_);
    load_receive_state();

    if (snp_obj->is_last_obj) {
        if (!recv_ || recv_->hdr.raft_idx != lsn) {
            LOGE("group={}: snapshot at raft lsn {} ended with no install of it in progress", group, lsn);
            return;
        }
        if (recv_->hdr.completed) { return; }
        auto const dev = craft_device();
        if (!dev) { return; }
        if (auto r = dev->complete_snapshot_install(recv_->meta, recv_->hdr.w_hi, recv_->hdr.p,
                                                    recv_->hdr.control_only != 0);
            !r) {
            LOGE("group={}: completing the snapshot install at raft lsn {} failed: {}", group, lsn,
                 r.error().message());
            return; // apply_snapshot refuses it, which stops this process: never serve a half-installed volume
        }
        recv_->hdr.completed = 1;
        save_receive_state();
        return;
    }

    auto const obj = craft_snapshot::decode(snp_obj->blob.cbytes(), snp_obj->blob.size());
    if (!obj) {
        LOGW("group={}: snapshot object {:#x} corrupt: asked again", group, snp_obj->offset);
        return;
    }

    if (std::holds_alternative< craft_snapshot::snapshot_meta >(*obj)) {
        auto const& meta = std::get< craft_snapshot::snapshot_meta >(*obj);
        auto dev = craft_device();
        if (!dev) {
            // No replica here: a learner, or a member that lost it. The snapshot compacted the CreateVolume entry away.
            auto info = parse_create_volume(sisl::blob{const_cast< uint8_t* >(meta.volume_def.data()),
                                                       static_cast< uint32_t >(meta.volume_def.size())});
            if (!info) {
                LOGE("group={}: the snapshot's volume definition doesn't parse", group);
                return;
            }
            if (auto r = hb_->create_craft_volume_from_snapshot(std::move(*info)); !r) {
                LOGE("group={}: creating the volume for the snapshot failed: {}", group, r.error().message());
                return;
            }
            dev = craft_device();
            if (!dev) { return; }
            LOGI("group={}: volume created from the snapshot at raft lsn {}", group, lsn);
        }
        auto const regions = meta.region_count;
        // Resume (requirement 6): the same transfer identity continues after the cursor.
        if (recv_ && !recv_->hdr.completed && recv_->hdr.raft_idx == lsn && recv_->meta.digest == meta.digest &&
            dev->snapshot_installing()) {
            snp_obj->offset = recv_->hdr.control_only ? craft_snapshot::LAST_OBJ_ID
                : recv_->hdr.next_region <= regions   ? id_of(recv_->hdr.next_region, 1)
                                                      : id_of(regions + 1, 0);
            LOGI("group={}: resuming the snapshot at raft lsn {} from region {}", group, lsn, recv_->hdr.next_region);
            return;
        }
        auto const was_installing = dev->snapshot_installing();
        auto const p = dev->begin_snapshot_install(k_install_drain_timeout);
        if (!p) { return; } // asked again
        craft_snapshot::snapshot_receive_sb_t hdr{};
        hdr.group_id = group_id_;
        hdr.raft_idx = lsn;
        // A transfer restarted on a member already behind the barrier keeps the P taken when it went up.
        hdr.p = was_installing && recv_ ? recv_->hdr.p : *p;
        // Never roll back data this member certified (requirement 4): below its commit_lsn, control only.
        hdr.control_only = meta.tuple.data_watermark < dev->commit_lsn() ? 1 : 0;
        recv_ = receive_state{.hdr = hdr, .meta = meta};
        save_receive_state();
        snp_obj->offset = hdr.control_only ? craft_snapshot::LAST_OBJ_ID : regions > 0 ? id_of(1, 1) : id_of(1, 0);
        LOGI("group={}: installing the snapshot at raft lsn {}: W={} S={} P={} {}", group, lsn,
             meta.tuple.data_watermark, meta.tuple.sync_watermark, hdr.p,
             hdr.control_only ? "control only" : "with data");
        return;
    }

    if (!recv_ || recv_->hdr.raft_idx != lsn || recv_->hdr.completed) {
        snp_obj->offset = id_of(0, 0); // no install of this snapshot in progress: start over
        return;
    }
    auto const regions = recv_->meta.region_count;

    if (std::holds_alternative< craft_snapshot::region_batch >(*obj)) {
        auto const& b = std::get< craft_snapshot::region_batch >(*obj);
        if (b.region != recv_->hdr.next_region) {
            snp_obj->offset =
                recv_->hdr.next_region <= regions ? id_of(recv_->hdr.next_region, 1) : id_of(regions + 1, 0);
            return;
        }
        auto const dev = craft_device();
        if (!dev) { return; }
        if (auto r = dev->install_snapshot_batch(b); !r) {
            LOGW("group={}: installing region {} batch {} failed: {}; asked again", group, b.region, b.batch,
                 r.error().message());
            return;
        }
        if (!b.last_in_region) {
            snp_obj->offset = id_of(b.region, b.batch + 1);
            return;
        }
        // Region boundary: the new index durable first, then the blocks it replaced freed, then the cursor.
        if (!detail::sync_get(homestore::cp_mgr().trigger_cp_flush(true))) {
            LOGW("group={}: checkpoint at the end of region {} failed: the region is asked again", group, b.region);
            snp_obj->offset = id_of(b.region, 1);
            return;
        }
        dev->release_snapshot_frees();
        recv_->hdr.next_region = b.region + 1;
        save_receive_state();
        snp_obj->offset = after_region(b.region, regions);
        return;
    }

    auto const& t = std::get< craft_snapshot::snapshot_trailer >(*obj);
    recv_->hdr.w_hi = t.w_hi;
    save_receive_state();
    snp_obj->offset = craft_snapshot::LAST_OBJ_ID;
}

bool CraftGroupListener::apply_snapshot(std::shared_ptr< homestore::snapshot_context > context) {
    auto const lsn = context->get_lsn();
    load_receive_state();
    if (!recv_ || recv_->hdr.raft_idx != lsn || !recv_->hdr.completed) {
        // Applying one not installed would leave this replica without the entries it covers; failing makes nuraft
        // stop this process instead.
        LOGE("group={}: the RAFT snapshot up to lsn {} isn't installed here", boost::uuids::to_string(group_id_), lsn);
        return false;
    }
    hb_->remove_craft_snapshot_receive(group_id_);
    recv_.reset();
    {
        std::lock_guard lk{snapshot_mu_};
        if (!last_snapshot_ || lsn > last_snapshot_->get_lsn()) { last_snapshot_ = context; }
    }
    LOGI("group={}: RAFT snapshot up to lsn {} applied", boost::uuids::to_string(group_id_), lsn);
    return true;
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
