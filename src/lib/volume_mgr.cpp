
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
#include <algorithm>
#include <chrono>
#include <future>
#include <limits>
#include <optional>
#include <set>
#include <thread>
#include <boost/uuid/uuid_io.hpp>
#include <iomgr/iomgr.hpp>
#include <iomgr/iomgr_flip.hpp>
#include <homestore/crc.hpp>
#include <homestore/replication_service.hpp>
#include "volume/volume.hpp"
#include "homeblks_impl.hpp"
#include "coro_helpers.hpp"
#include "home_blks_config.hpp"

namespace homeblocks {

void HomeBlocksImpl::on_vol_meta_blk_found(sisl::byte_view const& buf, void* cookie) {
    auto vol_ptr = volume::make_volume(buf, cookie, volume_chunk_selector_, index_chunk_selector_);
    auto id = vol_ptr->id();
    // A volume works only in the mode it was created in, and a CRAFT volume loaded without the RAFT service finds no
    // group and is torn down below. Only a restart in its mode fixes that: stop before anything changes on disk,
    // whatever the FC setting.
    RELEASE_ASSERT(vol_ptr->is_craft() == is_craft(),
                   "volume {} was created in {} mode, but HomeBlocks runs in {} mode: restart it in {} mode",
                   vol_ptr->id_str(), enum_name(vol_ptr->info()->repl_mode), enum_name(config_.repl_mode),
                   enum_name(vol_ptr->info()->repl_mode));

    {
        auto lg = std::scoped_lock(index_lock_);
        auto it = idx_tbl_map_.find(vol_ptr->id_str());
        if (it != idx_tbl_map_.end()) {
            vol_ptr->init_index_table(true /*is_recovery*/, it->second.first /* table */);

            // don't need it after volume is initialized with index table;
            idx_tbl_map_.erase(it);
        } else {
            // A destroy that crashed after destroying the index table but before the volume superblock leaves the
            // volume without index table; the destroy resumed below skips it. Any other volume must have one.
            RELEASE_ASSERT(vol_ptr->is_destroying(), "index pid: {} not exists in recovery path, not expected!",
                           vol_ptr->id_str());
            LOGINFO("volume {} has no index table, it was destroyed before the crash", vol_ptr->id_str());
        }
    }

    {
        auto lg = std::scoped_lock(vol_lock_);
        DEBUG_ASSERT(vol_map_.find(id) == vol_map_.end(),
                     "volume id: {} already exists in recovery path, not expected!", boost::uuids::to_string(id));
        vol_map_.emplace(std::make_pair(id, vol_ptr));
        // Its index table, recovered before it, already holds the ordinal; one destroyed before the crash doesn't.
        if (!ordinal_reserver_->is_reserved(vol_ptr->ordinal())) { ordinal_reserver_->reserve(vol_ptr->ordinal()); }
    }

    if (vol_ptr->is_destroying()) {
        // resume volume destroying;
        LOGINFO("volume {} is in destroying state, resume destroy", vol_ptr->id_str());
        start_volume_removal(id);
    } else if (vol_ptr->is_craft() && !vol_ptr->rd()) {
        // Its RAFT group was destroyed while this node was down, or with the destroy still pending at the crash (when
        // HomeStore drops the group without calling on_destroy): the replica goes with it.
        LOGINFO("volume {} has no RAFT group anymore, removing it", vol_ptr->id_str());
        start_volume_removal(id);
    } else if (vol_ptr->is_craft()) {
        // Only now that its index table is installed: the CraftReplDev attached when the journal replays keeps it. A
        // CRAFT volume torn down above leaves its journal unopened, and HomeStore purges it.
        vol_ptr->reopen_craft_journal();
    }
}

shared< hs_index_table_t > HomeBlocksImpl::recover_index_table(homestore::superblk< homestore::index_table_sb >&& sb) {
    auto pid_str = boost::uuids::to_string(sb->parent_uuid); // parent_uuid is the volume id
    {
        auto lg = std::scoped_lock(index_lock_);
        index_cfg_t cfg(homestore::hs()->index_service().node_size());
        cfg.m_leaf_node_type = btree_leaf_node_type;
        cfg.m_int_node_type = btree_int_node_type;

        LOGI("Recovering index table for  index_uuid: {}, parent_uuid: {}", boost::uuids::to_string(sb->uuid), pid_str);
        std::vector< chunk_num_t > chunk_ids(sb->get_chunk_ids(), sb->get_chunk_ids() + sb->index_num_chunks);
        // Chunks it fails to claim stay in the free pool, where startup resets them as unowned: never continue
        bool success = index_chunk_selector_->recover_chunks(sb->ordinal, sb->pdev_id, sb->max_size_bytes, chunk_ids);
        RELEASE_ASSERT(success, "Failed to recover chunks for index table index_uuid: {}, parent_uuid: {} ordinal: {}",
                       boost::uuids::to_string(sb->uuid), pid_str, sb->ordinal);
        // The table holds its ordinal from here on, whether or not a volume comes to take it: a volume given the
        // ordinal anew would get the table's chunks.
        ordinal_reserver_->reserve(sb->ordinal);
        uint64_t const ordinal = sb->ordinal;
        auto tbl = std::make_shared< VolumeIndexTable >(std::move(sb), cfg);
        idx_tbl_map_.emplace(pid_str, std::pair{tbl, ordinal});
        return tbl->index_table();
    }
}

//
// The reason create volume needs a ref_cnt:
// 1. if graceful shutdow is received and visited volume map and check there is no volume being created.
// 2. after graceful shutdown release the vol map lock, create volume arrives and successfully take the vol map lock,
// 3. now we have a race that allow create volume to go through and graceful shutdown also happen in parallel which will
// cause crash;
//
async_result< volume_handle > HomeBlocksImpl::create_volume(volume_info vol_info) {
    // Counted before anything else and released on every exit: a shutdown either waits for this create or is seen by
    // it below, before it touches HomeStore.
    struct CreateRef {
        HomeBlocksImpl& hb;
        ~CreateRef() { hb.dec_ref(); }
    };
    inc_ref();
    CreateRef const ref{*this};
    if (is_restricted()) {
        LOGE("Can't serve volume create, System is in restricted mode.");
        co_return std::unexpected(std::errc::operation_not_supported);
    }
    if (is_shutting_down()) {
        LOGE("Can't serve volume create, system is shutting down.");
        co_return std::unexpected(std::errc::operation_not_supported);
    }
    if (is_craft()) { co_return co_await create_craft_volume(std::move(vol_info)); }
    if (vol_info.repl_mode != replication_mode::DISABLED) {
        LOGE("vol={}: a CRAFT volume needs an instance in CRAFT mode", boost::uuids::to_string(vol_info.id));
        co_return std::unexpected(std::errc::not_supported);
    }

    auto id = vol_info.id;

    LOGI("[vol={}] is of capacity [{}B]", boost::uuids::to_string(id), vol_info.size_bytes);

    {
        auto lg = std::shared_lock(vol_lock_);
        vol_info.ordinal = ordinal_reserver_->reserve();
        if (vol_info.ordinal >= MAX_NUM_VOLUMES) {
            LOGE("No space to create volume with id: {}", boost::uuids::to_string(id));
            co_return std::unexpected(volume_error::INTERNAL_ERROR);
        }

        if (auto it = vol_map_.find(id); it != vol_map_.end()) {
            LOGW("create_volume with input id: {} already exists,", boost::uuids::to_string(id));
            co_return std::unexpected(std::errc::invalid_argument);
        }
    }

    auto vol_ptr = volume::make_volume(std::move(vol_info), volume_chunk_selector_, index_chunk_selector_);
    if (vol_ptr) {
        auto lg = std::scoped_lock(vol_lock_);
        vol_map_.emplace(std::make_pair(id, vol_ptr));
        LOGW("create_volume with input id: {} ordinal: {} ", boost::uuids::to_string(id), vol_ptr->info()->ordinal);
    } else {
        LOGE("failed to create volume with id: {}", boost::uuids::to_string(id));
        co_return std::unexpected(volume_error::INTERNAL_ERROR);
    }

    co_return vol_ptr;
}

// A RAFT group on its way out: its destroy is proposed (DESTROYING), or committed (DESTROYED) and waiting for
// HomeStore's reaper, which runs on_destroy repl_dev_cleanup_interval_sec (a minute by default) later, or done.
static bool is_going_away(homestore::repl_dev const& group) {
    auto const stage = group.get_stage();
    return stage == homestore::repl_dev_stage_t::DESTROYING || stage == homestore::repl_dev_stage_t::DESTROYED ||
        stage == homestore::repl_dev_stage_t::PERMANENT_DESTROYED;
}

// CRAFT create on the member that receives it: forms the partition's RAFT group with every member, then replicates the
// volume itself through the group's first entry, CreateVolume. Returns once that entry committed and this member
// created its replica; the other members create theirs as they apply it.
async_result< volume_handle > HomeBlocksImpl::create_craft_volume(volume_info info) {
    if (auto const valid = validate_craft_volume(info); !valid) { co_return std::unexpected(valid.error()); }
    auto const id = info.id;
    // Looked up before taking vol_lock_, so this never calls into HomeStore while holding it.
    auto const group = homestore::hs()->repl_service().get_repl_dev(id);
    {
        auto lg = std::scoped_lock(vol_lock_);
        if (auto it = vol_map_.find(id); it != vol_map_.end()) {
            // A replica whose group is gone or going is removed with it: e.g. a failed create whose CreateVolume still
            // committed before its rollback's destroy. Not a volume to hand back, whatever its parameters.
            if (it->second->is_destroying() || (it->second->is_craft() && (!group || is_going_away(**group)))) {
                LOGE("vol={}: its previous replica is still being removed", boost::uuids::to_string(id));
                co_return std::unexpected(std::errc::operation_in_progress);
            }
            // A retry of a create that already succeeded here.
            auto const& existing = *it->second->info();
            if (it->second->is_craft() && existing.size_bytes == info.size_bytes &&
                existing.page_size == info.page_size && existing.name == info.name) {
                co_return it->second;
            }
            LOGE("vol={}: already exists with different parameters: {}", boost::uuids::to_string(id),
                 existing.to_string());
            co_return std::unexpected(std::errc::invalid_argument);
        }
        if (!creating_vols_.insert(id).second) { co_return std::unexpected(std::errc::operation_in_progress); }
    }

    // Group formation blocks this thread until the election and every member's join are done, and HomeStore drives
    // it with sync_get: it must never run on a reactor. A thread of its own is safe whatever thread the caller is on.
    auto done = std::make_shared< sisl::async::value_awaitable< result< volume_handle > > >();
    std::thread([self = shared_from_this(), info = std::move(info), done]() mutable {
        done->complete(self->form_craft_volume(std::move(info)));
    }).detach();
    auto ret = co_await *done;

    {
        auto lg = std::scoped_lock(vol_lock_);
        creating_vols_.erase(id);
    }
    co_return ret;
}

status HomeBlocksImpl::validate_craft_volume(volume_info const& info) const {
    auto const id_str = boost::uuids::to_string(info.id);
    if (info.repl_mode != replication_mode::CRAFT) {
        LOGE("vol={}: an instance in CRAFT mode only creates CRAFT volumes", id_str);
        return std::unexpected(std::errc::invalid_argument);
    }
    auto members = info.members;
    std::sort(members.begin(), members.end());
    if (members.empty() || members.size() > std::numeric_limits< uint16_t >::max() ||
        std::adjacent_find(members.begin(), members.end()) != members.end() ||
        !std::binary_search(members.begin(), members.end(), our_uuid())) {
        LOGE("vol={}: members must be distinct and include this node {}; got {}", id_str,
             boost::uuids::to_string(our_uuid()), info.members.size());
        return std::unexpected(std::errc::invalid_argument);
    }
    // CraftReplDev writes and checksums one page per HomeStore data block, whose size is the node's.
    if (info.page_size != DATA_BLK_SIZE) {
        LOGE("vol={}: page size {} is not this node's data block size {}", id_str, info.page_size, DATA_BLK_SIZE);
        return std::unexpected(std::errc::invalid_argument);
    }
    // The whole name must come back from the superblock, which keeps it as a C string: a retry, also after a restart,
    // is matched against the stored one.
    if (info.name.size() > volume::MAX_NAME_LEN || info.name.find('\0') != std::string::npos) {
        LOGE("vol={}: invalid name of {} bytes (at most {}, without NUL)", id_str, info.name.size(),
             volume::MAX_NAME_LEN);
        return std::unexpected(std::errc::invalid_argument);
    }
    return ok();
}

// The CreateVolume entry for `info`, in a ctx that collects its outcome.
static intrusive< create_volume_ctx > make_create_volume_ctx(volume_info const& info) {
    CreateVolumeEntry const entry{.id = info.id,
                                  .size_bytes = info.size_bytes,
                                  .page_size = static_cast< uint32_t >(info.page_size),
                                  .members = info.members,
                                  .name = info.name};
    auto ctx = intrusive< create_volume_ctx >{new create_volume_ctx()};
    ctx->hdr_buf_ = sisl::io_blob_safe{static_cast< uint32_t >(sizeof(CraftEntryHeader)), 0};
    reinterpret_cast< CraftEntryHeader* >(ctx->hdr_buf_.bytes())->type = CraftEntryType::CreateVolume;
    ctx->key_buf_ =
        sisl::io_blob_safe{static_cast< uint32_t >(create_volume_key_size(entry.members.size(), entry.name.size())), 0};
    serialize_create_volume(ctx->key_buf_.bytes(), entry);
    return ctx;
}

result< volume_handle > HomeBlocksImpl::form_craft_volume(volume_info info) {
    auto const id = info.id;
    auto const id_str = boost::uuids::to_string(id);

    // An earlier group of this volume is still being destroyed, e.g. by a failed create's rollback: wait for it to go.
    // create_repl_dev would hand it back as if formed, proposing into it fails, and destroying it again restarts its
    // reap (or, if that destroy is refused, revives it).
    if (auto const old = homestore::hs()->repl_service().get_repl_dev(id); old && is_going_away(**old)) {
        LOGE("vol={}: an earlier RAFT group of this volume is still being destroyed", id_str);
        return std::unexpected(std::errc::operation_in_progress);
    }

    // 1. Form the group: this node creates it, wins its first election, then adds every other member.
    std::set< homestore::replica_id_t > const members(info.members.begin(), info.members.end());
    auto rd = detail::sync_get(homestore::hs()->repl_service().create_repl_dev(id, members));
    if (!rd) {
        LOGE("vol={}: failed to form its RAFT group: {}", id_str, rd.error().message());
        remove_craft_group(id);
        return std::unexpected(rd.error());
    }

    // create_repl_dev returns once the RAFT layer reports this node as the group's leader. For a group id used before
    // in this process (a create retried after its group was destroyed), that layer still holds the old group's leader
    // flag, so it may not have waited: wait for the leadership itself before proposing. The leadership and the commit
    // share one deadline.
    auto const deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(HB_DYNAMIC_CONFIG(craft_create_volume_timeout_ms));
    while (!(*rd)->is_leader() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    // 2. Replicate the volume itself: every member creates its replica when this entry commits.
    auto ctx = make_create_volume_ctx(info);
    auto applied = ctx->done_.get_future();
    bool propose = (*rd)->is_leader();
    if (!propose) {
        LOGE("vol={}: not the leader of its RAFT group in time, CreateVolume not proposed", id_str);
        ctx->complete(std::unexpected(std::errc::timed_out));
    }
#ifdef _PRERELEASE
    if (propose && iomgr_flip::instance()->test_flip("craft_create_volume_propose_error")) {
        LOGI("vol={}: flip craft_create_volume_propose_error set, CreateVolume not proposed", id_str);
        ctx->complete(std::unexpected(std::errc::io_error));
        propose = false;
    }
#endif
    if (propose) { (*rd)->async_alloc_write(ctx->hdr_buf_, ctx->key_buf_, sisl::sg_list{}, ctx); }

    status st = std::unexpected(std::make_error_condition(std::errc::timed_out));
    if (applied.wait_until(deadline) == std::future_status::ready) { st = applied.get(); }
    if (!st) {
        LOGE("vol={}: CreateVolume did not commit and apply here: {}", id_str, st.error().message());
        remove_craft_group(id);
        return std::unexpected(st.error());
    }
    LOGI("vol={}: created, RAFT group formed with {} members", id_str, info.members.size());
    return get_volume(id);
}

// Rolls a failed create back: destroys the RAFT group on every member that joined it. A member that already created its
// replica tears it down when the group is destroyed (on_destroy). Bounded: without a quorum the destroy cannot commit,
// and it stays pending in the group.
void HomeBlocksImpl::remove_craft_group(volume_id_t const& id) {
    auto removed = std::make_shared< std::promise< status > >();
    auto fut = removed->get_future();
    detail::detach([](async_status remove, std::shared_ptr< std::promise< status > > r) -> sisl::async::task< void > {
        r->set_value(co_await std::move(remove));
    }(homestore::hs()->repl_service().remove_repl_dev(id), removed));

    auto const timeout = std::chrono::milliseconds(HB_DYNAMIC_CONFIG(craft_create_volume_timeout_ms));
    if (fut.wait_for(timeout) != std::future_status::ready) {
        LOGE("vol={}: destroying its RAFT group did not finish in {}ms", boost::uuids::to_string(id), timeout.count());
    } else if (auto const st = fut.get(); !st) {
        LOGE("vol={}: failed to destroy its RAFT group: {}", boost::uuids::to_string(id), st.error().message());
    }
}

void HomeBlocksImpl::on_create_volume_commit(int64_t lsn, sisl::blob const& key,
                                             cintrusive< homestore::repl_req_ctx > const& ctx, bool replaying) {
    auto entry = parse_create_volume(key);
    if (!entry) {
        LOGE("lsn={}: malformed CreateVolume entry ({} bytes), ignored", lsn, key.size());
        create_volume_ctx::complete(ctx, std::unexpected(volume_error::INVALID_ENTRY));
        return;
    }
    auto const id_str = boost::uuids::to_string(entry->id);
    if (std::find(entry->members.begin(), entry->members.end(), our_uuid()) == entry->members.end()) {
        LOGE("vol={} lsn={}: this node is not a member of the volume, CreateVolume ignored", id_str, lsn);
        create_volume_ctx::complete(ctx, std::unexpected(std::errc::invalid_argument));
        return;
    }
    if (auto vol = get_volume(entry->id)) {
        if ((*vol)->is_destroying()) {
            // An earlier replica of the same volume, its group destroyed, is still being removed here.
            LOGE("vol={} lsn={}: its previous replica is still being removed, CreateVolume fails here", id_str, lsn);
            create_volume_ctx::complete(ctx, std::unexpected(std::errc::operation_in_progress));
            return;
        }
        // Already applied here: a replay, or a commit re-delivered after a restart.
        create_volume_ctx::complete(ctx, ok());
        return;
    }
    if (replaying) {
        // Replay re-applies entries that committed before the restart. Its volume superblock is written before the
        // commit completes, so a missing one means the create failed here back then. Leave it failed: a journal (a
        // logdev) must not be created while the log store service replays.
        LOGW("vol={} lsn={}: CreateVolume failed here before the restart, not retried by the replay", id_str, lsn);
        return;
    }

    auto const st = create_craft_volume_local(std::move(*entry));
    if (!st) { LOGE("vol={} lsn={}: failed to create the volume locally: {}", id_str, lsn, st.error().message()); }
    create_volume_ctx::complete(ctx, st);
}

status HomeBlocksImpl::create_craft_volume_local(CreateVolumeEntry&& entry) {
    volume_info info{entry.id, entry.size_bytes, entry.page_size, std::move(entry.name)};
    info.repl_mode = replication_mode::CRAFT;
    info.members = std::move(entry.members);
    auto const id_str = boost::uuids::to_string(info.id);
#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("craft_create_volume_apply_error")) {
        LOGI("vol={}: flip craft_create_volume_apply_error set, local create fails", id_str);
        return std::unexpected(volume_error::INTERNAL_ERROR);
    }
#endif

    // A create that crashed between its index table and its superblock left the table behind (recovered without a
    // volume). Take it over, with the ordinal it holds since its recovery: a second table for the same volume would
    // compete with it at the next recovery.
    std::optional< std::pair< shared< VolumeIndexTable >, uint64_t > > orphan_idx;
    {
        auto lg = std::scoped_lock(index_lock_);
        if (auto it = idx_tbl_map_.find(id_str); it != idx_tbl_map_.end()) {
            orphan_idx = std::move(it->second);
            idx_tbl_map_.erase(it);
        }
    }

    if (orphan_idx) {
        DEBUG_ASSERT(ordinal_reserver_->is_reserved(orphan_idx->second),
                     "vol={}: leftover index table's ordinal {} free", id_str, orphan_idx->second);
        info.ordinal = orphan_idx->second;
    } else {
        auto lg = std::scoped_lock(vol_lock_);
        info.ordinal = ordinal_reserver_->reserve();
        if (info.ordinal >= MAX_NUM_VOLUMES) {
            ordinal_reserver_->unreserve(info.ordinal);
            LOGE("vol={}: no free volume ordinal", id_str);
            return std::unexpected(std::errc::no_space_on_device);
        }
    }

    auto const ordinal = info.ordinal;
    auto vol = volume::make_craft_volume(std::move(info), volume_chunk_selector_, index_chunk_selector_,
                                         dynamic_chunk_allocation(), orphan_idx ? orphan_idx->first : nullptr);
    if (!vol) {
        if (orphan_idx) {
            // The table stays behind, holding its ordinal, for the next create of this volume to take over.
            auto lg = std::scoped_lock(index_lock_);
            idx_tbl_map_.emplace(id_str, std::move(*orphan_idx));
        } else {
            // Nothing is left under the ordinal: a failed create releases its chunks and journal.
            ordinal_reserver_->unreserve(ordinal);
        }
        return std::unexpected(volume_error::INTERNAL_ERROR);
    }
    auto lg = std::scoped_lock(vol_lock_);
    vol_map_.emplace(vol->id(), vol);
    return ok();
}

//
// Why we don't need do ref_cnt for remove_volume:
// vol in destroying state already indicates an outstanding volume which consumed in no_outstanding_vols() API;
//
// Coroutine that performs the actual volume teardown on a worker reactor. It co_awaits volume::destroy()
// (which co_awaits the repl-dev removal and a forced index CP flush), so the reactor yields during those waits
// instead of parking -- which is what lets the CP flush complete. Launched via detach from remove_volume.
sisl::async::task< void > HomeBlocksImpl::do_remove_volume(volume_id_t id) {
    // Hand-made RAII: release the count taken by remove_volume on every exit path
    struct RemovalGuard {
        std::atomic< uint32_t >& pending;
        ~RemovalGuard() { pending.fetch_sub(1); }
    } removal_guard{pending_vol_removals_};

    // 1. get the volume ptr from the map;
    volume_handle vol_ptr = nullptr;
    {
        auto lg = std::scoped_lock(vol_lock_);
        if (auto it = vol_map_.find(id); it != vol_map_.end()) {
            vol_ptr = it->second;
        } else {
            LOGWARN("volume with id {} not found, cannot remove", boost::uuids::to_string(id));
            co_return;
        }
    }

    vol_ptr->state_change(volume_state::DESTROYING);

    // if vol is already started with destroy or there is any outstanding reqs on the vol, we will not do anything
    // on this vol and let reaper thread to handle it
    if (vol_ptr->can_remove()) {
        // 2. do volume destroy;
        co_await vol_ptr->destroy();
#ifdef _PRERELEASE
        if (iomgr_flip::instance()->test_flip("vol_destroy_crash_simulation") ||
            iomgr_flip::instance()->test_flip("vol_destroy_crash_before_chunk_release") ||
            iomgr_flip::instance()->test_flip("vol_destroy_crash_after_index_destroy")) {
            co_return;
        }
#endif
        // 3. remove volume from vol_map;
        {
            auto lg = std::scoped_lock(vol_lock_);
            vol_map_.erase(vol_ptr->id());
            ordinal_reserver_->unreserve(vol_ptr->info()->ordinal);
        }

        LOGINFO("volume {} ordinal={} removed successfully", vol_ptr->id_str(), vol_ptr->info()->ordinal);
    } else {
        LOGD("volume {} is in destroying state or has outstanding requests: {}, backing off and wait for GC to "
             "cleanup.",
             vol_ptr->id_str(), vol_ptr->num_outstanding_reqs());
    }
    // volume Destructor will be called after vol_ptr goes out of scope;
}

async_status HomeBlocksImpl::remove_volume(const volume_id_t& id) {
    if (is_restricted()) {
        LOGE("Can't serve volume remove, System is in restricted mode.");
        co_return std::unexpected(std::errc::operation_not_supported);
    }

    auto vol = get_volume(id);
    if (!vol) {
        LOGE("volume with id {} not found, cannot remove", boost::uuids::to_string(id));
        co_return std::unexpected(std::errc::invalid_argument);
    } else if ((*vol)->is_offline()) {
        LOGE("volume {} is offline, cannot remove", (*vol)->id_str());
        co_return std::unexpected(volume_error::OFFLINE);
    } else if ((*vol)->is_craft()) {
        // A CRAFT volume goes through its RAFT group, which every member's replica follows; removing a single replica
        // would leave the group behind. That removal is not exposed yet.
        LOGE("volume {} is a CRAFT volume, remove_volume does not support it yet", (*vol)->id_str());
        co_return std::unexpected(std::errc::operation_not_supported);
    }

    LOGINFO("remove_volume with input id: {}", boost::uuids::to_string(id));
    if (!start_volume_removal(id)) { co_return std::unexpected(std::errc::operation_not_supported); }
    co_return ok();
}

bool HomeBlocksImpl::start_volume_removal(volume_id_t const& id) {
    if (is_restricted()) {
        LOGE("Can't remove volume {}, system is in restricted mode.", boost::uuids::to_string(id));
        return false;
    }
    // Shutdown waits for pending removals. Count this one before checking for shutdown: shutdown sets its flag before
    // checking the count, so either shutdown waits for this removal or this removal sees the flag.
    pending_vol_removals_.fetch_add(1);
    if (is_shutting_down()) {
        pending_vol_removals_.fetch_sub(1);
        LOGE("Can't remove volume {}, system is shutting down.", boost::uuids::to_string(id));
        return false;
    }

    // volume::destroy() co_awaits a forced CP flush (IndexTable::destroy) and the repl-dev removal, so it must
    // run as a coroutine on the worker reactor (detach) -- a blocking callable would park the reactor and the
    // flush it awaits could never run. See do_remove_volume.
    iomanager.run_on_forget(iomgr::reactor_regex::random_worker,
                            [this, id]() { detail::detach(do_remove_volume(id)); });
    return true;
}

void HomeBlocksImpl::on_craft_group_destroyed(volume_id_t const& id) {
    if (!get_volume(id)) { return; } // never created here, or already removed
    LOGINFO("volume {}: its RAFT group was destroyed, removing the local replica", boost::uuids::to_string(id));
    start_volume_removal(id);
}

result< volume_handle > HomeBlocksImpl::get_volume(const volume_id_t& id) const {
    auto lg = std::shared_lock(vol_lock_);
    if (auto it = vol_map_.find(id); it != vol_map_.end()) { return it->second; }
    return std::unexpected(volume_error::UNKNOWN_VOLUME);
}

void HomeBlocksImpl::update_vol_sb_cb(uint64_t volume_ordinal, const std::vector< chunk_num_t >& chunk_ids) {
    volume_handle vol_ptr = nullptr;
    {
        auto lg = std::shared_lock(vol_lock_);
        for (auto it = vol_map_.begin(); it != vol_map_.end(); it++) {
            if (it->second->info()->ordinal == volume_ordinal) {
                vol_ptr = it->second;
                break;
            }
        }
    }

    RELEASE_ASSERT(vol_ptr != nullptr, "volume not found");
    vol_ptr->update_vol_sb_cb(chunk_ids);
}

result< volume_stats > HomeBlocksImpl::get_stats(volume_id_t id) const {
    auto lg = std::shared_lock(vol_lock_);
    auto it = vol_map_.find(id);
    if (it == vol_map_.end()) {
        LOGE("volume with id {} not found, cannot get stats", boost::uuids::to_string(id));
        return std::unexpected(volume_error::UNKNOWN_VOLUME);
    }

    volume_stats stats;
    it->second->get_stats(stats);
    return stats;
}

std::vector< volume_id_t > HomeBlocksImpl::volume_ids() const {
    auto lg = std::shared_lock(vol_lock_);
    std::vector< volume_id_t > vol_ids;
    vol_ids.reserve(vol_map_.size());
    for (const auto& it : vol_map_) {
        vol_ids.push_back(it.first);
    }
    return vol_ids;
}

// ======================= public data-plane: free functions over a volume_handle =======================
// (declared in homeblks/home_blocks.hpp). Each guards against a system/volume that cannot serve IO, converts the
// byte address to the volume's block granularity, builds an internal vol_interface_req, and drives
// volume::read/write. read/write resolve to the byte count transferred (sgs.size); unmap is currently a no-op.

// Shared guard. Returns the volume's block size on success, or the rejecting volume_error.
static result< uint32_t > io_precheck(volume_handle const& vol, uint64_t addr, uint64_t len, const char* op) {
    auto hb = HomeBlocksImpl::instance();
    if (hb->is_restricted()) {
        LOGE("Can't serve {}, system is in restricted mode.", op);
        return std::unexpected(std::errc::operation_not_supported);
    }
    if (vol->is_craft()) {
        // A CRAFT volume's data rides the CRAFT API; a legacy op would propose a data entry into its RAFT group.
        LOGE("Can't serve {} on CRAFT volume {}, use the CRAFT API.", op, vol->id_str());
        return std::unexpected(std::errc::operation_not_supported);
    }
    if (vol->is_offline()) {
        LOGE("Can't serve {}, volume {} is offline.", op, vol->id_str());
        return std::unexpected(volume_error::OFFLINE);
    }
    if (vol->is_destroying() || hb->is_shutting_down()) {
        LOGE("Can't serve {}, volume {} is destroying or system is shutting down.", op, vol->id_str());
        return std::unexpected(std::errc::operation_not_supported);
    }
    auto const blk_size = vol->rd()->get_blk_size();
    if (addr % blk_size != 0 || len % blk_size != 0) {
        LOGE("Can't serve {} on volume {}: addr {} / len {} not block-aligned ({}B).", op, vol->id_str(), addr, len,
             blk_size);
        return std::unexpected(std::errc::invalid_argument);
    }
    return blk_size;
}

async_result< size_t > async_write(volume_handle const& vol, uint64_t addr, sisl::sg_list sgs) {
    auto blk_size = io_precheck(vol, addr, sgs.size, "write");
    if (!blk_size) co_return std::unexpected(blk_size.error());
    // TODO: scatter-gather (multi-iov) not yet plumbed through the write pipeline; single contiguous buffer only.
    if (sgs.iovs.size() != 1) co_return std::unexpected(std::errc::invalid_argument);
#ifdef _PRERELEASE
    if (HomeBlocksImpl::instance()->delay_fake_io(vol)) { co_return sgs.size; } // delayed; completes via flip later
#endif
    vol_io_guard guard{vol}; // marks this IO in-flight on the volume (and keeps it alive) for the op's duration
    io_req req{.buffer = static_cast< uint8_t* >(sgs.iovs[0].iov_base),
               .lba = addr / *blk_size,
               .nlbas = static_cast< lba_count_t >(sgs.size / *blk_size)};
    if (auto st = co_await vol->write(req); !st) { co_return std::unexpected(st.error()); }
    co_return sgs.size;
}

async_result< size_t > async_read(volume_handle const& vol, uint64_t addr, sisl::sg_list sgs) {
    auto blk_size = io_precheck(vol, addr, sgs.size, "read");
    if (!blk_size) co_return std::unexpected(blk_size.error());
    if (sgs.iovs.size() != 1) co_return std::unexpected(std::errc::invalid_argument);
#ifdef _PRERELEASE
    if (HomeBlocksImpl::instance()->delay_fake_io(vol)) { co_return sgs.size; }
#endif
    vol_io_guard guard{vol}; // marks this IO in-flight on the volume (and keeps it alive) for the op's duration
    io_req req{.buffer = static_cast< uint8_t* >(sgs.iovs[0].iov_base),
               .lba = addr / *blk_size,
               .nlbas = static_cast< lba_count_t >(sgs.size / *blk_size)};
    if (auto st = co_await vol->read(req); !st) { co_return std::unexpected(st.error()); }
    co_return sgs.size;
}

async_status async_unmap(volume_handle const& vol, uint64_t addr, uint64_t len) {
    LOGWARN("Unmap to vol: {} not implemented", vol->id_str());
    auto blk_size = io_precheck(vol, addr, len, "unmap");
    if (!blk_size) co_return std::unexpected(blk_size.error());
    co_return ok();
}

void HomeBlocksImpl::on_write(int64_t lsn, const sisl::blob& header, const sisl::blob& key,
                              const std::vector< homestore::multi_blk_id >& new_blkids,
                              cintrusive< homestore::repl_req_ctx >& ctx) {

    // We are not expecting log reply for a graceful restart;
    // if we are in recovery path, we must be recovering from a crash.
    DEBUG_ASSERT(ctx != nullptr || !is_graceful_shutdown(),
                 "repl ctx is null (recovery path) in graceful shutdown scenario, this is not expected!");

    repl_result_ctx< status >* repl_ctx{nullptr};
    if (ctx) { repl_ctx = boost::static_pointer_cast< repl_result_ctx< status > >(ctx).get(); }
    auto msg_header = reinterpret_cast< MsgHeader* >(const_cast< uint8_t* >(header.cbytes()));

    // Key contains the list of checksums and old blkids. Before we ack the client
    // request, we free the old blkid's. Also if its recovery we overwrite the index
    // with checksum and new blkid's. We need to overwrite index during recovery as all the
    // index writes may not be flushed to disk during crash.
    volume_handle vol_ptr{nullptr};
    auto journal_entry = reinterpret_cast< const VolJournalEntry* >(key.cbytes());
    auto key_buffer = reinterpret_cast< const uint8_t* >(journal_entry + 1);

    if (repl_ctx == nullptr) {
        // For recovery path repl_ctx and vol_ptr wont be available.
        auto lg = std::shared_lock(vol_lock_);
        auto it = vol_map_.find(msg_header->volume_id);
        RELEASE_ASSERT(it != vol_map_.end(), "Didnt find volume {}", boost::uuids::to_string(msg_header->volume_id));
        vol_ptr = it->second;

        // During log recovery overwrite new blkid and checksum to index.
        std::unordered_map< lba_t, BlockInfo > blocks_info;
        lba_t start_lba = journal_entry->start_lba;
        for (auto& blkid : new_blkids) {
            for (uint32_t i = 0; i < blkid.blk_count(); i++) {
                auto new_bid = blk_id{blkid.blk_num() + i, 1 /* nblks */, blkid.chunk_num()};
                auto csum = *reinterpret_cast< const homestore::csum_t* >(key_buffer);
                blocks_info.emplace(start_lba + i, BlockInfo{new_bid, blk_id{}, csum});
                key_buffer += sizeof(homestore::csum_t);
            }

            // We ignore the existing values we got in blocks_info from index as it will be
            // same checksum, blkid we see in the journal entry.
            lba_t end_lba = start_lba + blkid.blk_count() - 1;
            auto status = vol_ptr->indx_table()->write_to_index(start_lba, end_lba, blocks_info);
            RELEASE_ASSERT(status, "Index error during recovery");
            start_lba = end_lba + 1;
        }
    } else {
        // Avoid expensive lock during normal write flow.
        vol_ptr = repl_ctx->vol_ptr_;
        key_buffer += (journal_entry->nlbas * sizeof(homestore::csum_t));
    }

    // Free all the old blkids. This happens for both normal writes
    // and crash recovery. During recovery we also if it has been alloced,
    // because we could have stale log entries which have old blkid's
    // which may be already freed.
    for (uint32_t i = 0; i < journal_entry->num_old_blks; i++) {
        blk_id old_blkid = *reinterpret_cast< const blk_id* >(key_buffer);
        if (repl_ctx == nullptr) {
            if (homestore::hs()->data_service().is_blk_alloced(old_blkid)) {
                LOGT("volume write on commit free blk {} start_lba {}", old_blkid, journal_entry->start_lba);
                // free_blk_now is synchronous and returns a [[nodiscard]] status; this recovery-path free is
                // best-effort (matches the prior void-returning behavior), so the result is intentionally ignored.
                (void)homestore::hs()->data_service().free_blk_now(old_blkid);
            }
        } else {
            if (homestore::hs()->data_service().is_blk_alloced(old_blkid)) {
                LOGT("volume write on commit free blk {} start_lba {}", old_blkid, journal_entry->start_lba);
                // async_free_blks returns a lazy task; on_write is not a coroutine, so fire-and-forget it
                // (detach starts it) rather than dropping the un-awaited task (which would never run).
                detail::detach(vol_ptr->rd()->async_free_blks(lsn, old_blkid));
            }
        }
        key_buffer += sizeof(blk_id);
    }

#ifdef _PRERELEASE
    if (iomgr_flip::instance()->test_flip("vol_write_crash_after_journal_write")) {
        // this is to simulate crash during write where both data and journal
        // is persisted. After recovery log entries are replayed.
        LOGINFO("volume write crash simulation flip is set, aborting");
        return;
    }
#endif

    // Deliver the journal-write completion to the awaiting volume::write coroutine;
    // value_awaitable::complete resumes it inline on this commit thread.
    if (repl_ctx) { repl_ctx->promise_.complete(ok()); }
}

#ifdef _PRERELEASE
bool HomeBlocksImpl::delay_fake_io(volume_handle v) {
    if (iomgr_flip::instance()->delay_flip("vol_fake_io_delay_simulation", [this, v]() mutable {
            LOGI("Resuming fake IO delay flip is done. Do nothing ");
            v->dec_ref();
        })) {
        LOGI("Slow down vol fake IO flip is enabled, scheduling to call later.");
        v->inc_ref();
        return true;
    }
    return false;
}
#endif

} // namespace homeblocks
