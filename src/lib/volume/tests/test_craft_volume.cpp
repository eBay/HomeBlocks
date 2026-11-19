/*********************************************************************************
 * Modifications Copyright 2026 eBay Inc.
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

// CRAFT volume creation (SDSTOR-22894) on a single node: HomeBlocks in CRAFT mode, each volume a one-member RAFT
// group. Covers what doesn't need peers: the create flow end to end, its validation and idempotency, the legacy-op
// guards, the range of CRAFT IO, the journal reattach at restart, the rollback of a failed create, and a replica's
// teardown when its group is destroyed. test_craft_volume_multi covers a group with several members.

#include <csignal>
#include <filesystem>
#include <limits>
#include <set>
#include <spawn.h>
#include <sys/wait.h>
#include <gtest/gtest.h>
#include <sisl/options/options.h>
#include <sisl/flip/flip_client.hpp>
#include <iomgr/iomgr_flip.hpp>
#include <homestore/homestore.hpp>
#include <homestore/blkdata_service.hpp>
#include <homestore/checkpoint/cp_mgr.hpp>
#include <homestore/replication_service.hpp>
#include "hb_internal.hpp"
#include "home_blks_config.hpp"
#include "test_common.hpp"
#include <volume/volume.hpp>

SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTION_GROUP(test_craft_volume_setup,
                  (repl_port, "", "repl_port", "this node's RAFT port",
                   ::cxxopts::value< uint16_t >()->default_value("47451"), "port"),
                  (crash_repl_port, "", "crash_repl_port", "the RAFT port of the crash case's child processes",
                   ::cxxopts::value< uint16_t >()->default_value("47453"), "port"),
                  (crash_phase, "", "crash_phase", "internal: runs as the crash case's child, before (1) or after (2)",
                   ::cxxopts::value< uint32_t >()->default_value("0"), "phase"));

// `config`: --config_path / --override_config, used to shorten HomeStore's repl dev reaper interval.
SISL_OPTIONS_ENABLE(logging, test_common_setup, test_craft_volume_setup, config)

extern char** environ;

std::unique_ptr< test_common::HBTestHelper > g_helper;

using namespace homeblocks;

class CraftVolumeTest : public ::testing::Test {
public:
    // A new volume each call: `base` and a random uuid name it, and its id derives from that name.
    volume_info craft_vol_info(std::string const& base) {
        volume_info info;
        info.name = fmt::format("{}_{}", base, boost::uuids::to_string(boost::uuids::random_generator()()));
        info.id = craft_volume_id(info.name);
        info.size_bytes = 1024 * 1024 * 1024;
        info.page_size = 4096;
        info.repl_mode = replication_mode::CRAFT;
        info.members = {g_helper->svc_id()};
        return info;
    }

    static volume_info clone(volume_info const& info) {
        volume_info c{info.id, info.size_bytes, info.page_size, info.name};
        c.repl_mode = info.repl_mode;
        c.members = info.members;
        return c;
    }

    result< volume_handle > create(volume_info info) {
        return homeblocks::detail::sync_get(hb()->create_volume(std::move(info)));
    }

    shared< home_blocks > hb() { return g_helper->inst(); }

    static shared< homestore::repl_dev > group(volume_id_t const& id) {
        auto rd = homestore::hs()->repl_service().get_repl_dev(id);
        return rd ? rd.value() : nullptr;
    }

    static int64_t groups_without_replica() { return g_helper->groups_without_replica(); }

    // Waits until the RAFT group is gone from HomeStore: its destroy committed and the reaper removed it.
    static bool wait_for_group_gone(volume_id_t const& id, std::chrono::seconds timeout = std::chrono::seconds(60)) {
        auto const deadline = std::chrono::steady_clock::now() + timeout;
        while (group(id) != nullptr && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return group(id) == nullptr;
    }

    template < typename Pred >
    static bool wait_until(Pred&& pred, std::chrono::seconds timeout = std::chrono::seconds(30)) {
        auto const deadline = std::chrono::steady_clock::now() + timeout;
        while (!pred() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return pred();
    }

    // Removes a volume a test is done with, so that the suite's small index chunks last: its group is destroyed
    // through its leader, this node once its one-member group has elected it (again, after a restart).
    static void remove(volume_id_t const& id) {
        ASSERT_TRUE(wait_until(
            [&id] {
                auto const rd = group(id);
                return rd != nullptr && rd->is_leader();
            },
            std::chrono::seconds(60)));
        ASSERT_TRUE(homeblocks::detail::sync_get(homestore::hs()->repl_service().remove_repl_dev(id)));
        ASSERT_TRUE(g_helper->wait_for_volumes_removed({id}, std::chrono::seconds(60)));
    }

    // An InternalLogin the volume's group committed at raft_lsn, handed to its device as HomeStore does, without a RAFT
    // round.
    static void commit_login(volume_handle const& vol, uint64_t client_token, uint64_t term, int64_t raft_lsn) {
        CraftEntryHeader header{CraftEntryType::InternalLogin};
        InternalLoginPayload key{client_token, term};
        cintrusive< homestore::repl_req_ctx > ctx{};
        vol->craft_dev()->raft_listener().on_commit(
            raft_lsn, sisl::blob{reinterpret_cast< uint8_t* >(&header), sizeof(header)},
            sisl::blob{reinterpret_cast< uint8_t* >(&key), sizeof(key)}, {}, ctx);
    }

    // An InternalLogin through the volume's group, as a login commits it: at a RAFT index the device's entries after a
    // restart follow, unlike commit_login()'s.
    static status propose_login(volume_id_t const& id, uint64_t client_token, uint64_t term) {
        CraftEntryHeader header{CraftEntryType::InternalLogin};
        InternalLoginPayload key{client_token, term};
        return homeblocks::detail::sync_get(
            make_homestore_raft_proposer(group(id), peer_id_t{})
                ->propose(sisl::blob{reinterpret_cast< uint8_t* >(&header), sizeof(header)},
                          sisl::blob{reinterpret_cast< uint8_t* >(&key), sizeof(key)}));
    }

    // A SyncRSCommitLSN through the volume's group, voiding the slots `voided`.
    static status propose_sync(volume_id_t const& id, int64_t rs_commit_lsn, uint64_t client_token,
                               std::vector< int64_t > const& voided) {
        CraftEntryHeader header{CraftEntryType::SyncRSCommitLSN};
        std::vector< uint8_t > key(sync_rs_commit_lsn_key_size(voided.size()));
        serialize_sync_rs_commit_lsn(key.data(), rs_commit_lsn, client_token, voided);
        return homeblocks::detail::sync_get(
            make_homestore_raft_proposer(group(id), peer_id_t{})
                ->propose(sisl::blob{reinterpret_cast< uint8_t* >(&header), sizeof(header)},
                          sisl::blob{key.data(), static_cast< uint32_t >(key.size())}));
    }

    // A SyncRSCommitLSN the volume's group committed at raft_lsn, without Empty verdicts, handed to its device as
    // HomeStore does.
    static void commit_sync(volume_handle const& vol, int64_t rs_commit_lsn, uint64_t client_token, int64_t raft_lsn) {
        CraftEntryHeader header{CraftEntryType::SyncRSCommitLSN};
        std::vector< uint8_t > key(sync_rs_commit_lsn_key_size(0));
        serialize_sync_rs_commit_lsn(key.data(), rs_commit_lsn, client_token, {});
        cintrusive< homestore::repl_req_ctx > ctx{};
        vol->craft_dev()->raft_listener().on_commit(
            raft_lsn, sisl::blob{reinterpret_cast< uint8_t* >(&header), sizeof(header)},
            sisl::blob{key.data(), static_cast< uint32_t >(key.size())}, {}, ctx);
    }

    static constexpr uint64_t k_page = 4096;

    // Writes slot dlsn: one page at page dlsn, every byte 'a' + dlsn.
    static status write_slot(volume_handle const& vol, craft::client_hdr const& hdr, int64_t dlsn) {
        sisl::io_blob_safe data(k_page, 512);
        std::memset(data.bytes(), 'a' + dlsn, k_page);
        auto const w = homeblocks::detail::sync_get(
            homeblocks::async_write(vol, hdr, dlsn, dlsn * k_page, k_page,
                                    sisl::sg_list{.size = k_page, .iovs = {iovec{data.bytes(), k_page}}}));
        if (!w) return std::unexpected(w.error());
        return ok();
    }

    // Whether page dlsn reads back as write_slot() wrote it, at horizon read_lsn.
    static bool reads_back(volume_handle const& vol, craft::client_hdr const& hdr, int64_t read_lsn, int64_t dlsn) {
        sisl::io_blob_safe dest(k_page, 512);
        auto const r = homeblocks::detail::sync_get(
            homeblocks::async_read(vol, hdr, read_lsn, dlsn * k_page, k_page,
                                   sisl::sg_list{.size = k_page, .iovs = {iovec{dest.bytes(), k_page}}}));
        if (!r) {
            LOGERROR("read of slot {} failed: {}", dlsn, r.error().message());
            return false;
        }
        return std::all_of(dest.cbytes(), dest.cbytes() + k_page, [dlsn](uint8_t b) { return b == 'a' + dlsn; });
    }

#ifdef _PRERELEASE
    // At the next restart, HomeStore never reports the replay of this volume's journal, as if its log store were
    // missing.
    static void lose_journal_replay(volume_id_t const& id) {
        flip::FlipClient* fc = iomgr_flip::client_instance();
        flip::FlipCondition this_volume;
        fc->create_condition("", flip::Operator::EQUAL, boost::uuids::to_string(id), &this_volume);
        flip::FlipFrequency freq;
        freq.set_count(1);
        freq.set_percent(100);
        fc->inject_noreturn_flip("craft_journal_replay_lost", std::array< flip::FlipCondition, 1 >{this_volume}, freq);
    }

    // The volumes with a CRAFT state record in the meta service, read as a boot reads them.
    static std::set< volume_id_t > state_records() {
        std::set< volume_id_t > ids;
        for (auto const& [id, sb] :
             std::dynamic_pointer_cast< HomeBlocksImpl >(g_helper->inst())->read_craft_state_records()) {
            ids.insert(id);
        }
        return ids;
    }
#endif
};

TEST_F(CraftVolumeTest, CreateFormsGroupAndAttachesCraftDev) {
    auto info = craft_vol_info("craft_vol_create");
    auto const id = info.id;
    auto vol = create(std::move(info));
    ASSERT_TRUE(vol) << vol.error().message();
    ASSERT_TRUE((*vol)->is_craft());
    ASSERT_NE((*vol)->craft_dev(), nullptr);
    ASSERT_EQ(hb()->get_volume(id).value_or(nullptr), *vol);

    // The volume's RAFT group: this node formed it, leads it, and its persisted config lists the members.
    auto rd = group(id);
    ASSERT_NE(rd, nullptr);
    EXPECT_TRUE(rd->is_leader());
    auto const quorum = rd->get_replication_quorum();
    EXPECT_EQ(quorum.size(), 1u);
    EXPECT_NE(std::find(quorum.begin(), quorum.end(), g_helper->svc_id()), quorum.end());
}

TEST_F(CraftVolumeTest, CreateRetryReturnsTheVolume) {
    auto const info = craft_vol_info("craft_vol_retry");
    auto const id = info.id;
    auto first = create(clone(info));
    ASSERT_TRUE(first);
    auto second = create(clone(info));
    ASSERT_TRUE(second);
    EXPECT_EQ(*first, *second);

    // Same id, different parameters: not a retry.
    auto other = clone(info);
    other.size_bytes *= 2;
    auto clash = create(std::move(other));
    ASSERT_FALSE(clash);
    EXPECT_EQ(clash.error(), std::errc::invalid_argument);
    EXPECT_EQ(hb()->get_volume(id).value_or(nullptr), *first);
}

// Membership changes are not supported yet: the call answers not_supported and the group keeps its members.
TEST_F(CraftVolumeTest, ReplaceMemberNotSupportedYet) {
    auto info = craft_vol_info("craft_vol_replace");
    auto const id = info.id;
    ASSERT_TRUE(create(std::move(info)));

    auto const st =
        homeblocks::detail::sync_get(hb()->replace_member(id, g_helper->svc_id(), boost::uuids::random_generator()()));
    ASSERT_FALSE(st);
    EXPECT_EQ(st.error(), std::errc::not_supported);
    EXPECT_EQ(group(id)->get_replication_quorum(), std::vector< homestore::replica_id_t >{g_helper->svc_id()});
}

TEST_F(CraftVolumeTest, CreateRejectsInvalidRequests) {
    auto expect_rejected = [this](volume_info info) {
        auto const id = info.id;
        auto vol = create(std::move(info));
        ASSERT_FALSE(vol);
        EXPECT_EQ(vol.error(), std::errc::invalid_argument);
        EXPECT_FALSE(hb()->get_volume(id));
        EXPECT_EQ(group(id), nullptr); // nothing formed
    };

    auto no_members = craft_vol_info("no_members");
    no_members.members.clear();
    expect_rejected(std::move(no_members));

    auto not_a_member = craft_vol_info("not_a_member");
    not_a_member.members = {boost::uuids::random_generator()()};
    expect_rejected(std::move(not_a_member));

    auto duplicated = craft_vol_info("duplicated");
    duplicated.members.push_back(g_helper->svc_id());
    expect_rejected(std::move(duplicated));

    auto not_craft = craft_vol_info("not_craft");
    not_craft.repl_mode = replication_mode::DISABLED;
    expect_rejected(std::move(not_craft));

    // A CRAFT page is a HomeStore data block.
    for (uint64_t const page_size : {0ul, 512ul, 8192ul}) {
        auto wrong_page = craft_vol_info("wrong_page");
        wrong_page.page_size = page_size;
        expect_rejected(std::move(wrong_page));
    }

    // Whole pages only, and at least one: a partial last page could never be written.
    for (uint64_t const size : {0ul, 4096ul + 1, (1ul << 30) - 2048}) {
        auto wrong_size = craft_vol_info("wrong_size");
        wrong_size.size_bytes = size;
        expect_rejected(std::move(wrong_size));
    }

    // Longer than the superblock keeps, or with a NUL: it would come back cut short after a restart.
    expect_rejected(craft_vol_info(std::string(volume::MAX_NAME_LEN + 1, 'n')));
    expect_rejected(craft_vol_info(std::string("nul\0name", 8)));

    // An id that isn't its name's: a create of that name on another member would form another group. Also an existing
    // volume's id under another name.
    auto foreign_id = craft_vol_info("foreign_id");
    foreign_id.id = boost::uuids::random_generator()();
    expect_rejected(std::move(foreign_id));
    auto const existing = craft_vol_info("craft_vol_renamed");
    ASSERT_TRUE(create(clone(existing)));
    auto renamed = clone(existing);
    renamed.name += "_other";
    auto const vol = create(std::move(renamed));
    ASSERT_FALSE(vol);
    EXPECT_EQ(vol.error(), std::errc::invalid_argument);
    EXPECT_TRUE(hb()->get_volume(existing.id)); // untouched
}

TEST_F(CraftVolumeTest, LegacyOpsRejected) {
    auto vol = create(craft_vol_info("craft_vol_legacy_ops"));
    ASSERT_TRUE(vol);

    std::vector< uint8_t > buf(4096);
    sisl::sg_list sgs{.size = buf.size(), .iovs = {iovec{buf.data(), buf.size()}}};
    auto w = homeblocks::detail::sync_get(async_write(*vol, 0, sgs));
    auto r = homeblocks::detail::sync_get(async_read(*vol, 0, sgs));
    ASSERT_FALSE(w);
    EXPECT_EQ(w.error(), std::errc::operation_not_supported);
    ASSERT_FALSE(r);
    EXPECT_EQ(r.error(), std::errc::operation_not_supported);

    auto removed = homeblocks::detail::sync_get(hb()->remove_volume((*vol)->id()));
    ASSERT_FALSE(removed);
    EXPECT_EQ(removed.error(), std::errc::operation_not_supported);
    EXPECT_TRUE(hb()->get_volume((*vol)->id()));
}

// A CRAFT write or read must lie within the volume, also where addr + len wraps around.
TEST_F(CraftVolumeTest, IOBeyondTheVolumeRejected) {
    auto info = craft_vol_info("craft_vol_beyond");
    uint64_t const size = info.size_bytes;
    auto vol = create(std::move(info));
    ASSERT_TRUE(vol);
    craft::client_hdr const hdr{0 /* term */, -1 /* commit_lsn */, -1 /* all_committed_lsn */};
    uint64_t const page = 4096;
    std::vector< uint8_t > buf(2 * page);
    auto read = [&](uint64_t addr, uint64_t len) {
        return homeblocks::detail::sync_get(homeblocks::async_read(
            *vol, hdr, 0 /* read_lsn */, addr, len, sisl::sg_list{.size = len, .iovs = {iovec{buf.data(), len}}}));
    };

    // The last page is in: never written, so a hole.
    auto const last = read(size - page, page);
    ASSERT_TRUE(last) << last.error().message();

    // Past the end, across it, and wrapping around 2^64.
    for (auto const& [addr, len] : std::vector< std::pair< uint64_t, uint64_t > >{
             {size, page}, {size - page, 2 * page}, {std::numeric_limits< uint64_t >::max() - page + 1, 2 * page}}) {
        auto const w = homeblocks::detail::sync_get(
            homeblocks::async_write(*vol, hdr, 1 /* dlsn */, addr, len, sisl::sg_list{} /* zeros */));
        ASSERT_FALSE(w) << addr << "+" << len;
        EXPECT_EQ(w.error(), std::errc::invalid_argument) << addr << "+" << len;
        auto const r = read(addr, len);
        ASSERT_FALSE(r) << addr << "+" << len;
        EXPECT_EQ(r.error(), std::errc::invalid_argument) << addr << "+" << len;
    }
}

TEST_F(CraftVolumeTest, RestartReattachesJournal) {
    auto const info = craft_vol_info("craft_vol_restart");
    auto const id = info.id;
    ASSERT_TRUE(create(clone(info)));

    // Twice: a journal HomeStore purged as unopened at the first restart would be gone at the second.
    for (int i = 0; i < 2; ++i) {
        g_helper->restart();
        auto vol = hb()->get_volume(id);
        ASSERT_TRUE(vol);
        EXPECT_TRUE((*vol)->is_craft());
        EXPECT_NE((*vol)->craft_dev(), nullptr) << "journal not reattached after restart " << i;
        EXPECT_NE(group(id), nullptr);
    }

    // A retry of the original create still matches the recovered volume, its name read back from the superblock.
    auto retry = create(clone(info));
    ASSERT_TRUE(retry) << retry.error().message();
    EXPECT_EQ(*retry, hb()->get_volume(id).value_or(nullptr));

    // Creating after a restart: a new journal logdev, now that the log store service has started.
    auto after = create(craft_vol_info("craft_vol_after_restart"));
    ASSERT_TRUE(after);
    EXPECT_NE((*after)->craft_dev(), nullptr);
}

#ifdef _PRERELEASE
// A reattach that takes a while (held here by a flip): init returns only once the device is recovered and published.
TEST_F(CraftVolumeTest, InitWaitsForTheReattach) {
    auto info = craft_vol_info("craft_vol_slow_reattach");
    auto const id = info.id;
    ASSERT_TRUE(create(std::move(info)));
    g_helper->set_retval_flip("craft_reattach_delay_ms", 2000 /* ms */, 1000 /* every volume */);
    auto const start = std::chrono::steady_clock::now();
    g_helper->restart();
    g_helper->remove_flip("craft_reattach_delay_ms");
    EXPECT_GE(std::chrono::steady_clock::now() - start, std::chrono::seconds(2)) << "the reattach wasn't held";
    auto vol = hb()->get_volume(id);
    ASSERT_TRUE(vol);
    EXPECT_NE((*vol)->craft_dev(), nullptr);
    remove(id);
}
#endif

// A partition's session is in its durable state record, persisted before the login takes effect: a restart restores it,
// and so does the next one.
TEST_F(CraftVolumeTest, SessionSurvivesRestarts) {
    auto info = craft_vol_info("craft_vol_session");
    auto const id = info.id;
    auto vol = create(std::move(info));
    ASSERT_TRUE(vol);
    commit_login(*vol, 42 /* client_token */, 7 /* term */, 1000 /* raft_lsn */);
    ASSERT_TRUE(wait_until([&vol] { return (*vol)->craft_dev()->term() == 7; }));
#ifdef _PRERELEASE
    EXPECT_TRUE(state_records().contains(id));
#endif

    for (int i = 0; i < 2; ++i) {
        g_helper->restart();
        auto again = hb()->get_volume(id);
        ASSERT_TRUE(again);
        ASSERT_NE((*again)->craft_dev(), nullptr) << "restart " << i;
        EXPECT_EQ((*again)->craft_dev()->term(), 7u) << "restart " << i;
        EXPECT_EQ((*again)->craft_dev()->client_token(), 42u) << "restart " << i;
    }
    remove(id);
}

// Writes a member acknowledged but nothing committed to the index yet live only in its journal: a restart rebuilds them
// from it, and they read back.
TEST_F(CraftVolumeTest, UnappliedJournalWritesSurviveARestart) {
    auto info = craft_vol_info("craft_vol_journal_writes");
    auto const id = info.id;
    auto vol = create(std::move(info));
    ASSERT_TRUE(vol);
    commit_login(*vol, 42 /* client_token */, 7 /* term */, 1000 /* raft_lsn */);
    ASSERT_TRUE(wait_until([&vol] { return (*vol)->craft_dev()->term() == 7; }));
    craft::client_hdr const hdr{7 /* term */, -1 /* commit_lsn */, -1 /* all_committed_lsn */};
    for (int64_t dlsn = 0; dlsn < 3; ++dlsn) {
        auto const w = write_slot(*vol, hdr, dlsn);
        ASSERT_TRUE(w) << w.error().message();
    }

    g_helper->restart();
    auto again = hb()->get_volume(id);
    ASSERT_TRUE(again);
    ASSERT_NE((*again)->craft_dev(), nullptr);
    EXPECT_EQ((*again)->craft_dev()->last_append_lsn(), 2);
    EXPECT_EQ((*again)->craft_dev()->commit_lsn(), -1);
    for (int64_t dlsn = 0; dlsn < 3; ++dlsn) {
        EXPECT_TRUE(reads_back(*again, hdr, 2 /* read_lsn */, dlsn)) << "slot " << dlsn;
    }
    remove(id);
}

// Slots a SyncRSCommitLSN committed to the index read back after a restart, as do those it left in the journal.
TEST_F(CraftVolumeTest, CommittedSlotsSurviveARestart) {
    auto info = craft_vol_info("craft_vol_committed_slots");
    auto const id = info.id;
    auto vol = create(std::move(info));
    ASSERT_TRUE(vol);
    commit_login(*vol, 42 /* client_token */, 7 /* term */, 1000 /* raft_lsn */);
    ASSERT_TRUE(wait_until([&vol] { return (*vol)->craft_dev()->term() == 7; }));
    craft::client_hdr const hdr{7 /* term */, -1 /* commit_lsn */, -1 /* all_committed_lsn */};
    for (int64_t dlsn = 0; dlsn < 4; ++dlsn) {
        auto const w = write_slot(*vol, hdr, dlsn);
        ASSERT_TRUE(w) << w.error().message();
    }
    commit_sync(*vol, 2 /* rs_commit_lsn */, 42 /* client_token */, 1001 /* raft_lsn */);
    ASSERT_TRUE(wait_until([&vol] { return (*vol)->craft_dev()->commit_lsn() == 2; }));

    g_helper->restart();
    auto again = hb()->get_volume(id);
    ASSERT_TRUE(again);
    ASSERT_NE((*again)->craft_dev(), nullptr);
    EXPECT_EQ((*again)->craft_dev()->last_append_lsn(), 3);
    for (int64_t dlsn = 0; dlsn < 4; ++dlsn) {
        EXPECT_TRUE(reads_back(*again, hdr, 3 /* read_lsn */, dlsn)) << "slot " << dlsn;
    }
    remove(id);
}

// The device proposes through its own RAFT group, and again after a restart: a resolution the client asks for commits a
// SyncRSCommitLSN, which the partition has applied, its slots committed to the index, when the resolution returns.
TEST_F(CraftVolumeTest, RequestResolutionCommitsThroughTheGroup) {
    auto info = craft_vol_info("craft_vol_resolution");
    auto const id = info.id;
    auto vol = create(std::move(info));
    ASSERT_TRUE(vol);
    auto const login = propose_login(id, 42 /* client_token */, 7 /* term */);
    ASSERT_TRUE(login) << login.error().message();
    ASSERT_TRUE(wait_until([&vol] { return (*vol)->craft_dev()->term() == 7; }));
    craft::client_hdr const hdr{7 /* term */, -1 /* commit_lsn */, -1 /* all_committed_lsn */};
    for (int64_t dlsn = 0; dlsn < 3; ++dlsn) {
        auto const w = write_slot(*vol, hdr, dlsn);
        ASSERT_TRUE(w) << w.error().message();
    }

    auto const res = homeblocks::detail::sync_get(homeblocks::request_resolution(*vol, hdr, 2 /* upto */));
    ASSERT_TRUE(res) << res.error().message();
    EXPECT_EQ(res->resolved_upto, 2);
    EXPECT_TRUE(res->empty_slots.empty());
    EXPECT_EQ((*vol)->craft_dev()->commit_lsn(), 2);
    for (int64_t dlsn = 0; dlsn < 3; ++dlsn) {
        EXPECT_TRUE(reads_back(*vol, hdr, 2 /* read_lsn */, dlsn)) << "slot " << dlsn;
    }

    g_helper->restart();
    auto again = hb()->get_volume(id);
    ASSERT_TRUE(again);
    ASSERT_TRUE(wait_until(
        [&id] {
            auto const rd = group(id);
            return rd != nullptr && rd->is_leader();
        },
        std::chrono::seconds(60)));
    auto const w = write_slot(*again, hdr, 3);
    ASSERT_TRUE(w) << w.error().message();
    auto const res2 = homeblocks::detail::sync_get(homeblocks::request_resolution(*again, hdr, 3 /* upto */));
    ASSERT_TRUE(res2) << res2.error().message();
    EXPECT_EQ(res2->resolved_upto, 3);
    EXPECT_EQ((*again)->craft_dev()->commit_lsn(), 3);
    remove(id);
}

// A restart keeps the blocks of every live slot allocated: those at or below the durable commit frontier, which the
// allocator kept from their write, and those above it, which the recovery commits again. They read back after new
// writes that the restarted allocator serves.
TEST_F(CraftVolumeTest, RestartKeepsTheBlocksOfLiveSlots) {
    // A checkpoint at every commit, so the frontier moves past the slots committed below.
    auto const interval = HB_DYNAMIC_CONFIG(sync_rs_commit_lsn_interval);
    HB_SETTINGS_FACTORY().modifiable_settings([](auto& s) { s.sync_rs_commit_lsn_interval = 1; });
    auto info = craft_vol_info("craft_vol_live_blocks");
    auto const id = info.id;
    auto vol = create(std::move(info));
    HB_SETTINGS_FACTORY().modifiable_settings([interval](auto& s) { s.sync_rs_commit_lsn_interval = interval; });
    ASSERT_TRUE(vol);
    auto const login = propose_login(id, 42 /* client_token */, 7 /* term */);
    ASSERT_TRUE(login) << login.error().message();
    ASSERT_TRUE(wait_until([&vol] { return (*vol)->craft_dev()->term() == 7; }));
    craft::client_hdr const hdr{7 /* term */, -1 /* commit_lsn */, -1 /* all_committed_lsn */};
    for (int64_t dlsn = 0; dlsn < 4; ++dlsn) {
        auto const w = write_slot(*vol, hdr, dlsn);
        ASSERT_TRUE(w) << w.error().message();
    }
    // Slots 0 and 1 go to the index, and the frontier past them; 2 and 3 stay in the journal.
    auto const res = homeblocks::detail::sync_get(homeblocks::request_resolution(*vol, hdr, 1 /* upto */));
    ASSERT_TRUE(res) << res.error().message();
    ASSERT_TRUE(wait_until([&vol] { return (*vol)->craft_dev()->persisted_durable_commit() >= 1; }));

    g_helper->restart();
    auto again = hb()->get_volume(id);
    ASSERT_TRUE(again);
    ASSERT_TRUE(wait_until(
        [&id] {
            auto const rd = group(id);
            return rd != nullptr && rd->is_leader();
        },
        std::chrono::seconds(60)));
    // Slots 0 and 1's blocks, as the index holds them, and 2 and 3's, as the journal does.
    for (int64_t dlsn = 0; dlsn < 2; ++dlsn) {
        index_kv_list_t kvs;
        ASSERT_TRUE((*again)->indx_table()->read_from_index(dlsn, dlsn, kvs));
        ASSERT_EQ(kvs.size(), 1u) << "slot " << dlsn;
        EXPECT_TRUE(homestore::data_service().is_blk_alloced(kvs[0].second.blkid())) << "slot " << dlsn;
    }
#ifdef _PRERELEASE
    for (int64_t dlsn = 2; dlsn < 4; ++dlsn) {
        auto const slot = homeblocks::detail::sync_get((*again)->craft_dev()->journal_slot_for_test(dlsn));
        ASSERT_TRUE(slot) << slot.error().message();
        auto pieces = slot->blkid.iterate();
        while (auto const piece = pieces.next()) {
            EXPECT_TRUE(homestore::data_service().is_blk_alloced(*piece)) << "slot " << dlsn;
        }
    }
#endif
    for (int64_t dlsn = 4; dlsn < 12; ++dlsn) {
        auto const w = write_slot(*again, hdr, dlsn);
        ASSERT_TRUE(w) << w.error().message();
    }
    for (int64_t dlsn = 0; dlsn < 4; ++dlsn) {
        EXPECT_TRUE(reads_back(*again, hdr, 3 /* read_lsn */, dlsn)) << "slot " << dlsn;
    }
    remove(id);
}

#ifdef _PRERELEASE
// A journal that never replays at a restart: the partition's device can't come, and the entries committed meanwhile
// can't be saved. The partition is faulted for good, its record marked before init ends, so a restart whose journal
// replays faults it again rather than bring it back from a record that lacks them.
TEST_F(CraftVolumeTest, AJournalThatNeverReplaysFaultsThePartitionForGood) {
    auto info = craft_vol_info("craft_vol_unreplayed");
    auto const id = info.id;
    auto vol = create(std::move(info));
    ASSERT_TRUE(vol);
    auto const login = propose_login(id, 42 /* client_token */, 7 /* term */);
    ASSERT_TRUE(login) << login.error().message();
    ASSERT_TRUE(wait_until([&vol] { return (*vol)->craft_dev()->term() == 7; }));

    lose_journal_replay(id);
    g_helper->restart();
    auto again = hb()->get_volume(id);
    ASSERT_TRUE(again);
    EXPECT_EQ((*again)->craft_dev(), nullptr) << "a device without its journal";
    auto records = std::dynamic_pointer_cast< HomeBlocksImpl >(g_helper->inst())->read_craft_state_records();
    ASSERT_TRUE(records.contains(id));
    auto const loaded = make_homestore_state_store(id, std::move(records.at(id)))->load();
    ASSERT_FALSE(loaded) << "its record isn't marked faulted";
    EXPECT_EQ(loaded.error(), std::make_error_condition(std::errc::state_not_recoverable));

    g_helper->restart();
    again = hb()->get_volume(id);
    ASSERT_TRUE(again);
    ASSERT_NE((*again)->craft_dev(), nullptr) << "its journal replays again";
    EXPECT_TRUE((*again)->craft_dev()->is_faulted());
    remove(id);
}

// A RAFT entry a checkpoint covers is never replayed after a restart. One that hasn't applied yet survives in the
// partition's durable state, which the checkpoint saves with it, and the restart applies it from there.
TEST_F(CraftVolumeTest, CheckpointKeepsAnEntryThatHasNotApplied) {
    auto info = craft_vol_info("craft_vol_checkpoint");
    auto const id = info.id;
    auto vol = create(std::move(info));
    ASSERT_TRUE(vol);
    auto* const dev = (*vol)->craft_dev();
    dev->pause_apply();
    commit_login(*vol, 42 /* client_token */, 7 /* term */, 1000 /* raft_lsn */); // never delivered again
    ASSERT_TRUE(homeblocks::detail::sync_get(homestore::cp_mgr().trigger_cp_flush(true /* force */)));
    EXPECT_EQ(dev->term(), 0u) << "applied before the checkpoint";
    // Saved by this checkpoint, not only by the shutdown's below: a crash now would find it too.
    auto records = std::dynamic_pointer_cast< HomeBlocksImpl >(g_helper->inst())->read_craft_state_records();
    ASSERT_TRUE(records.contains(id));
    auto const image = make_homestore_state_store(id, std::move(records.at(id)))->load();
    ASSERT_TRUE(image && *image);
    ASSERT_EQ((*image)->pending.size(), 1u);
    EXPECT_EQ((*image)->pending[0].type, CraftEntryType::InternalLogin);
    EXPECT_EQ((*image)->pending[0].term, 7u);

    g_helper->restart();
    auto again = hb()->get_volume(id);
    ASSERT_TRUE(again);
    ASSERT_NE((*again)->craft_dev(), nullptr);
    ASSERT_TRUE(wait_until([&again] { return (*again)->craft_dev()->term() == 7; }));
    EXPECT_EQ((*again)->craft_dev()->client_token(), 42u);
    remove(id);
}
#endif

// The group is destroyed (here directly; by the partition's removal or a create rollback in practice): each member's
// replica follows it, through on_destroy, and its durable state record with it.
TEST_F(CraftVolumeTest, GroupDestroyRemovesReplica) {
    auto info = craft_vol_info("craft_vol_group_destroy");
    auto const id = info.id;
    auto vol = create(std::move(info));
    ASSERT_TRUE(vol);
    commit_login(*vol, 42 /* client_token */, 7 /* term */, 1000 /* raft_lsn */);
    ASSERT_TRUE(wait_until([&vol] { return (*vol)->craft_dev()->term() == 7; }));
#ifdef _PRERELEASE
    ASSERT_TRUE(state_records().contains(id));
#endif
    // A CRAFT op the replica serves until then, in the session: it changes nothing.
    craft::client_hdr const hdr{7 /* term */, -1 /* commit_lsn */, -1 /* all_committed_lsn */};
    auto alive = homeblocks::detail::sync_get(homeblocks::keep_alive(*vol, hdr));
    ASSERT_TRUE(alive) << alive.error().message();

    ASSERT_TRUE(homeblocks::detail::sync_get(homestore::hs()->repl_service().remove_repl_dev(id)));
    // From the destroy on, not only once the reaper removes the replica, its CRAFT ops are refused.
    auto early = homeblocks::detail::sync_get(homeblocks::keep_alive(*vol, hdr));
    ASSERT_FALSE(early);
    EXPECT_EQ(early.error(), std::errc::operation_not_supported);

    ASSERT_TRUE(g_helper->wait_for_volumes_removed({id}, std::chrono::seconds(60)));
    ASSERT_TRUE(wait_for_group_gone(id));

    // The replica's CRAFT device went with it: a CRAFT op through a handle kept from before is refused.
    EXPECT_EQ((*vol)->craft_dev(), nullptr);
    auto late = homeblocks::detail::sync_get(homeblocks::keep_alive(*vol, hdr));
    ASSERT_FALSE(late);
    EXPECT_EQ(late.error(), std::errc::operation_not_supported);
#ifdef _PRERELEASE
    EXPECT_FALSE(state_records().contains(id));
#endif
}

// Same, but the node restarts right after the destroy committed: whether the reaper ran first (on_destroy) or HomeStore
// dropped the destroy-pending group at load (no on_destroy, and the volume no device), the replica is removed, and its
// durable state record with it.
TEST_F(CraftVolumeTest, GroupDestroyedAcrossRestartRemovesReplica) {
    auto info = craft_vol_info("craft_vol_destroy_restart");
    auto const id = info.id;
    auto vol = create(std::move(info));
    ASSERT_TRUE(vol);
    commit_login(*vol, 42 /* client_token */, 7 /* term */, 1000 /* raft_lsn */);
    ASSERT_TRUE(wait_until([&vol] { return (*vol)->craft_dev()->term() == 7; }));
#ifdef _PRERELEASE
    ASSERT_TRUE(state_records().contains(id));
#endif

    ASSERT_TRUE(homeblocks::detail::sync_get(homestore::hs()->repl_service().remove_repl_dev(id)));
    g_helper->restart();
    ASSERT_TRUE(g_helper->wait_for_volumes_removed({id}, std::chrono::seconds(60)));
#ifdef _PRERELEASE
    EXPECT_FALSE(state_records().contains(id));
#endif
}

// After the group's destroy commits, its replica stays until HomeStore's reaper runs on_destroy (a minute in
// production, a second here). A create retry in that window must not return it as created.
TEST_F(CraftVolumeTest, CreateRetryWhileGroupIsDestroyedIsInProgress) {
    auto const info = craft_vol_info("craft_vol_retry_destroyed");
    auto const id = info.id;
    ASSERT_TRUE(create(clone(info)));
    ASSERT_TRUE(homeblocks::detail::sync_get(homestore::hs()->repl_service().remove_repl_dev(id)));

    ASSERT_TRUE(hb()->get_volume(id)); // still here, waiting for the reaper
    auto retry = create(clone(info));
    ASSERT_FALSE(retry);
    EXPECT_EQ(retry.error(), std::errc::operation_in_progress);

    ASSERT_TRUE(g_helper->wait_for_volumes_removed({id}, std::chrono::seconds(60)));
    ASSERT_TRUE(wait_for_group_gone(id));
    ASSERT_TRUE(create(clone(info)));
}

#ifdef _PRERELEASE
// A failed create rolls back: the group is destroyed and nothing remains, so the same volume can be created again.
// One failure after the group formed and before CreateVolume is proposed, one in the apply of a committed CreateVolume.
TEST_F(CraftVolumeTest, FailedCreateRollsBack) {
    for (std::string const flip : {"craft_create_volume_propose_error", "craft_create_volume_apply_error"}) {
        auto const info = craft_vol_info("craft_vol_rollback");
        auto const id = info.id;
        g_helper->set_flip_point(flip, 1);
        auto vol = create(clone(info));
        g_helper->remove_flip(flip);
        ASSERT_FALSE(vol) << flip;
        EXPECT_FALSE(hb()->get_volume(id)) << flip;

        // A retry while the rolled-back group waits for the reaper leaves it alone and reports the removal in progress.
        ASSERT_NE(group(id), nullptr) << flip;
        auto retry = create(clone(info));
        ASSERT_FALSE(retry) << flip;
        EXPECT_EQ(retry.error(), std::errc::operation_in_progress) << flip;

        ASSERT_TRUE(wait_for_group_gone(id)) << flip;
        ASSERT_TRUE(create(clone(info))) << flip;
    }
}

// A create whose local apply failed and whose rollback never ran (the receiver crashed first) leaves its group without
// a replica here. A retry neither proposes into that group nor destroys it, also when the group shows up only after
// the retry's own check (as a peer's join may install one), and after a restart: once the group is destroyed, the
// volume can be created again. Meanwhile the node reports the group among those without a replica here.
TEST_F(CraftVolumeTest, RetryLeavesAnUnfinishedGroupAlone) {
    auto const info = craft_vol_info("craft_vol_unfinished");
    auto const id = info.id;
    auto const without_replica = groups_without_replica();
    g_helper->set_flip_point("craft_create_volume_apply_error", 1);
    g_helper->set_flip_point("craft_create_volume_no_rollback", 1);
    ASSERT_FALSE(create(clone(info)));
    g_helper->remove_flip("craft_create_volume_apply_error");
    g_helper->remove_flip("craft_create_volume_no_rollback");

    auto const expect_left_alone = [this, &info, &id, without_replica] {
        auto retry = create(clone(info));
        ASSERT_FALSE(retry);
        EXPECT_EQ(retry.error(), std::errc::operation_in_progress);
        EXPECT_FALSE(hb()->get_volume(id));
        auto const rd = group(id);
        ASSERT_NE(rd, nullptr);
        EXPECT_FALSE(is_going_away(*rd));
        EXPECT_EQ(groups_without_replica(), without_replica + 1);
    };
    expect_left_alone();

    // create_repl_dev hands the retry this group, which this node leads. Proposed into, its CreateVolume would fail
    // here, and the rollback would destroy the group.
    g_helper->set_flip_point("craft_create_volume_skip_group_check", 1);
    g_helper->set_flip_point("craft_create_volume_apply_error", 1);
    expect_left_alone();
    g_helper->remove_flip("craft_create_volume_skip_group_check");
    g_helper->remove_flip("craft_create_volume_apply_error");

    g_helper->restart();
    expect_left_alone();

    // Destroyed through its leader: this node, once its one-member group has elected it again after the restart.
    auto const leads = [&id] {
        auto const rd = group(id);
        return rd != nullptr && rd->is_leader();
    };
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (!leads() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    ASSERT_TRUE(leads());
    ASSERT_TRUE(homeblocks::detail::sync_get(homestore::hs()->repl_service().remove_repl_dev(id)));
    EXPECT_EQ(groups_without_replica(), without_replica); // being destroyed, if not gone yet
    ASSERT_TRUE(wait_for_group_gone(id));
    auto vol = create(clone(info));
    ASSERT_TRUE(vol) << vol.error().message();
    EXPECT_NE((*vol)->craft_dev(), nullptr);
    EXPECT_EQ(groups_without_replica(), without_replica);
}

// A create that crashed between its index table and its superblock leaves the table behind. The table keeps its
// ordinal across restarts, so no other volume gets its chunks, and the volume's next create takes it over, also after
// one that failed with it. A create too large for the table fails instead.
TEST_F(CraftVolumeTest, LeftoverIndexTableIsTakenOver) {
    auto const info = craft_vol_info("craft_vol_leftover_index");
    auto const id = info.id;
    auto const table_ordinals = [] {
        std::set< uint32_t > ords;
        for (uint32_t o = 0; o < MAX_NUM_VOLUMES; ++o) {
            if (homestore::hs()->index_service().get_index_table(o)) { ords.insert(o); }
        }
        return ords;
    };
    auto const before = table_ordinals();

    g_helper->set_flip_point("craft_create_volume_crash_after_index", 1);
    ASSERT_FALSE(create(clone(info)));
    g_helper->remove_flip("craft_create_volume_crash_after_index");
    g_helper->restart();

    // The table came back without its volume.
    std::vector< uint32_t > leftover;
    for (auto const o : table_ordinals()) {
        if (!before.contains(o)) { leftover.push_back(o); }
    }
    ASSERT_EQ(leftover.size(), 1u);
    auto const ordinal = leftover.front();
    ASSERT_FALSE(hb()->get_volume(id));

    // No other volume is given its ordinal.
    volume_id_t other_id;
    {
        auto other = create(craft_vol_info("craft_vol_leftover_other"));
        ASSERT_TRUE(other) << other.error().message();
        EXPECT_NE((*other)->ordinal(), ordinal);
        other_id = (*other)->id();
    }

    // A larger create of the volume fails rather than take over a table sized for a smaller one, and leaves it.
    ASSERT_TRUE(wait_for_group_gone(id));
    auto larger = clone(info);
    larger.size_bytes *= 2;
    ASSERT_FALSE(create(std::move(larger)));

    // A create of the volume that fails after taking the table over leaves it to the next one.
    ASSERT_TRUE(wait_for_group_gone(id));
    g_helper->set_flip_point("craft_create_volume_init_error", 1);
    ASSERT_FALSE(create(clone(info)));
    g_helper->remove_flip("craft_create_volume_init_error");
    ASSERT_TRUE(wait_for_group_gone(id));
    {
        auto vol = create(clone(info));
        ASSERT_TRUE(vol) << vol.error().message();
        EXPECT_EQ((*vol)->ordinal(), ordinal);
        EXPECT_EQ((*vol)->indx_table()->index_table()->ordinal(), ordinal);
    }

    g_helper->restart();
    auto vol = hb()->get_volume(id);
    ASSERT_TRUE(vol);
    EXPECT_EQ((*vol)->ordinal(), ordinal);
    EXPECT_NE((*vol)->craft_dev(), nullptr);
    EXPECT_TRUE(hb()->get_volume(other_id));
}
#endif

#ifdef _PRERELEASE
// ── An abrupt crash: ACrashReplaysWhatCommittedPastTheCheckpoint runs this binary twice more, as DISABLED_CrashBefore
// and then DISABLED_CrashAfter, on devices, a RAFT port and settings of their own (main()) ──

// The first child: a session and journal writes that a checkpoint covers, then RAFT entries committed past it: a Sync
// voiding a slot this member holds, a second login, a write, and a Sync that commits but never applies, so nothing here
// records it. Then it is killed, so no checkpoint at shutdown covers them.
TEST_F(CraftVolumeTest, DISABLED_CrashBefore) {
    if (SISL_OPTIONS["crash_phase"].as< uint32_t >() != 1) { GTEST_SKIP() << "the crash case's first child only"; }
    auto info = craft_vol_info("craft_vol_crash");
    auto const id = info.id;
    auto vol = create(std::move(info));
    ASSERT_TRUE(vol);
    auto* const dev = (*vol)->craft_dev();
    auto const login = propose_login(id, 42 /* client_token */, 7 /* term */);
    ASSERT_TRUE(login) << login.error().message();
    ASSERT_TRUE(wait_until([dev] { return dev->term() == 7; }));
    craft::client_hdr const hdr7{7 /* term */, -1 /* commit_lsn */, -1 /* all_committed_lsn */};
    for (int64_t dlsn = 0; dlsn < 4; ++dlsn) {
        auto const w = write_slot(*vol, hdr7, dlsn);
        ASSERT_TRUE(w) << w.error().message();
    }
    ASSERT_TRUE(homeblocks::detail::sync_get(homestore::cp_mgr().trigger_cp_flush(true /* force */)));

    auto const voiding = propose_sync(id, 3 /* rs_commit_lsn */, 42 /* client_token */, {2});
    ASSERT_TRUE(voiding) << voiding.error().message();
    ASSERT_TRUE(wait_until([dev] { return dev->commit_lsn() == 3; }));
    auto const relogin = propose_login(id, 44 /* client_token */, 9 /* term */);
    ASSERT_TRUE(relogin) << relogin.error().message();
    ASSERT_TRUE(wait_until([dev] { return dev->term() == 9; }));
    craft::client_hdr const hdr9{9 /* term */, -1 /* commit_lsn */, -1 /* all_committed_lsn */};
    auto const w = write_slot(*vol, hdr9, 4);
    ASSERT_TRUE(w) << w.error().message();
    // Applied, the last Sync could be recorded as applied before the kill, and the restart would rightly skip it. Its
    // proposal waits for the apply: a thread of its own makes it, which the kill ends. Committed, it raised the
    // watermark.
    dev->pause_apply();
    std::thread([id] { (void)propose_sync(id, 4 /* rs_commit_lsn */, 44 /* client_token */, {}); }).detach();
    ASSERT_TRUE(wait_until([dev] { return dev->durable_state().sync_watermark == 4; }));
    std::raise(SIGKILL);
}

// The second child, restarted on the first one's devices with its journal's reattach held (main()): HomeStore's RAFT
// log replay hands the volume the entries committed past the checkpoint, held until the device has recovered. The
// device comes back with the session and the verdict, persisted before they took effect, skips the entries whose effect
// it holds, and applies the last Sync, which only the replay brought back.
TEST_F(CraftVolumeTest, DISABLED_CrashAfter) {
    if (SISL_OPTIONS["crash_phase"].as< uint32_t >() != 2) { GTEST_SKIP() << "the crash case's second child only"; }
    auto const ids = hb()->volume_ids();
    ASSERT_EQ(ids.size(), 1u);
    auto vol = hb()->get_volume(ids[0]);
    ASSERT_TRUE(vol);
    auto* const dev = (*vol)->craft_dev();
    ASSERT_NE(dev, nullptr);
    EXPECT_FALSE(dev->is_faulted());
    EXPECT_EQ(dev->term(), 9u);
    EXPECT_EQ(dev->client_token(), 44u);
    EXPECT_TRUE(wait_until([dev] { return dev->commit_lsn() == 4; })) << "commit_lsn " << dev->commit_lsn();

    craft::client_hdr const hdr{9 /* term */, -1 /* commit_lsn */, -1 /* all_committed_lsn */};
    auto const late = write_slot(*vol, hdr, 2);
    ASSERT_FALSE(late) << "a write to the slot voided before the crash";
    EXPECT_EQ(late.error(), make_error_condition(volume_error::EMPTY_SLOT));
    for (int64_t dlsn : {0, 1, 3, 4}) {
        EXPECT_TRUE(reads_back(*vol, hdr, 4 /* read_lsn */, dlsn)) << "slot " << dlsn;
    }
    // Every slot's blocks are the allocator's: slot 4's, written past the checkpoint, because the recovery committed
    // them again, and slot 2's, because nothing freed them twice: the free its verdict made before the crash, the crash
    // undid (its blocks leak), and the replayed Sync isn't applied again.
    for (int64_t dlsn = 0; dlsn <= 4; ++dlsn) {
        auto const slot = homeblocks::detail::sync_get(dev->journal_slot_for_test(dlsn));
        ASSERT_TRUE(slot) << "slot " << dlsn << ": " << slot.error().message();
        auto pieces = slot->blkid.iterate();
        while (auto const piece = pieces.next()) {
            EXPECT_TRUE(homestore::data_service().is_blk_alloced(*piece)) << "slot " << dlsn;
        }
    }
}

// Runs this binary as the crash case's child `phase` and returns its wait status.
static int run_crash_child(uint32_t phase) {
    std::vector< std::string > args{
        "/proc/self/exe", fmt::format("--crash_phase={}", phase),
        fmt::format("--gtest_filter=CraftVolumeTest.DISABLED_Crash{}", phase == 1 ? "Before" : "After"),
        "--gtest_also_run_disabled_tests",
        fmt::format("--repl_port={}", SISL_OPTIONS["crash_repl_port"].as< uint16_t >()),
        fmt::format("--index_chunk_size_mb={}", SISL_OPTIONS["index_chunk_size_mb"].as< uint32_t >()),
        fmt::format("--data_chunk_size_mb={}", SISL_OPTIONS["data_chunk_size_mb"].as< uint32_t >()),
        "--config_path=craft_volume_crash_cfg",
        // Only the test's own checkpoint: what commits after it, a timer's can't cover.
        "--override_config", "homestore_config.generic.cp_timer_us:3600000000"};
    std::vector< char* > argv;
    for (auto& a : args) {
        argv.push_back(a.data());
    }
    argv.push_back(nullptr);
    pid_t pid;
    if (posix_spawn(&pid, argv[0], nullptr, nullptr, argv.data(), environ) != 0) { return -1; }
    int status = 0;
    if (waitpid(pid, &status, 0) != pid) { return -1; }
    return status;
}

// An abrupt crash with RAFT entries committed past the last checkpoint: the replay of the RAFT log brings them back,
// into the volume's hold while its journal's reattach is held, and the device recovers with every effect once.
TEST_F(CraftVolumeTest, ACrashReplaysWhatCommittedPastTheCheckpoint) {
    auto const crashed = run_crash_child(1);
    ASSERT_TRUE(crashed != -1 && WIFSIGNALED(crashed) && WTERMSIG(crashed) == SIGKILL) << "wait status " << crashed;
    auto const recovered = run_crash_child(2);
    EXPECT_TRUE(recovered != -1 && WIFEXITED(recovered) && WEXITSTATUS(recovered) == 0) << "wait status " << recovered;
}
#endif

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    char** orig_argv = argv;

    std::vector< std::string > args;
    for (int i = 0; i < argc; ++i) {
        args.emplace_back(argv[i]);
    }

    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_common_setup, test_craft_volume_setup, config);
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    // The settings files (with any --override_config) are written there when HomeStore registers its settings.
    if (SISL_OPTIONS.count("config_path")) {
        std::filesystem::create_directories(SISL_OPTIONS["config_path"].as< std::string >());
    }

    // A child of the crash case runs on devices of its own: the first one formats them, the second restarts on them.
    auto const crash_phase = SISL_OPTIONS["crash_phase"].as< uint32_t >();
    g_helper = std::make_unique< test_common::HBTestHelper >(
        crash_phase ? "test_craft_volume_crash" : "test_craft_volume", args, orig_argv);
    auto const port = SISL_OPTIONS["repl_port"].as< uint16_t >();
    g_helper->set_config_hook([port](home_blocks_config& cfg) {
        cfg.repl_mode = replication_mode::CRAFT;
        cfg.repl_svc_port = port;
        // A single node: every group has this node as its only member.
        cfg.lookup_peer = [port](peer_id_t const&) { return std::pair< std::string, uint16_t >{"127.0.0.1", port}; };
    });
#ifdef _PRERELEASE
    // After the crash, the journal's reattach is held: the RAFT log replay hands its entries to the volume's hold.
    if (crash_phase == 2) { g_helper->set_retval_flip("craft_reattach_delay_ms", 5000 /* ms */, 1000); }
#endif
    g_helper->setup(crash_phase != 2 /* format */);
    auto ret = RUN_ALL_TESTS();
    g_helper->teardown();

    return ret;
}
