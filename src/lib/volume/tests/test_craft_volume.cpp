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
// guards, the journal reattach at restart, the rollback of a failed create, and a replica's teardown when its group
// is destroyed. test_craft_volume_multi covers a group with several members.

#include <filesystem>
#include <set>
#include <gtest/gtest.h>
#include <sisl/options/options.h>
#include <sisl/flip/flip_client.hpp>
#include <iomgr/iomgr_flip.hpp>
#include <homestore/homestore.hpp>
#include <homestore/replication_service.hpp>
#include "hb_internal.hpp"
#include "test_common.hpp"
#include <volume/volume.hpp>

SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTION_GROUP(test_craft_volume_setup,
                  (repl_port, "", "repl_port", "this node's RAFT port",
                   ::cxxopts::value< uint16_t >()->default_value("47451"), "port"));

// `config`: --config_path / --override_config, used to shorten HomeStore's repl dev reaper interval.
SISL_OPTIONS_ENABLE(logging, test_common_setup, test_craft_volume_setup, config)

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

    // Waits until the RAFT group is gone from HomeStore: its destroy committed and the reaper removed it.
    static bool wait_for_group_gone(volume_id_t const& id, std::chrono::seconds timeout = std::chrono::seconds(60)) {
        auto const deadline = std::chrono::steady_clock::now() + timeout;
        while (group(id) != nullptr && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return group(id) == nullptr;
    }
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

// The group is destroyed (here directly; by the partition's removal or a create rollback in practice): each member's
// replica follows it, through on_destroy.
TEST_F(CraftVolumeTest, GroupDestroyRemovesReplica) {
    auto info = craft_vol_info("craft_vol_group_destroy");
    auto const id = info.id;
    auto vol = create(std::move(info));
    ASSERT_TRUE(vol);
    // A CRAFT op the replica serves until then: no session, so it changes nothing.
    craft::client_hdr const hdr{0 /* term */, -1 /* commit_lsn */, -1 /* all_committed_lsn */};
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
}

// Same, but the node restarts right after the destroy committed: whether the reaper ran first (on_destroy) or HomeStore
// dropped the destroy-pending group at load (no on_destroy), the replica is removed.
TEST_F(CraftVolumeTest, GroupDestroyedAcrossRestartRemovesReplica) {
    auto info = craft_vol_info("craft_vol_destroy_restart");
    auto const id = info.id;
    ASSERT_TRUE(create(std::move(info)));

    ASSERT_TRUE(homeblocks::detail::sync_get(homestore::hs()->repl_service().remove_repl_dev(id)));
    g_helper->restart();
    ASSERT_TRUE(g_helper->wait_for_volumes_removed({id}, std::chrono::seconds(60)));
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
// volume can be created again.
TEST_F(CraftVolumeTest, RetryLeavesAnUnfinishedGroupAlone) {
    auto const info = craft_vol_info("craft_vol_unfinished");
    auto const id = info.id;
    g_helper->set_flip_point("craft_create_volume_apply_error", 1);
    g_helper->set_flip_point("craft_create_volume_no_rollback", 1);
    ASSERT_FALSE(create(clone(info)));
    g_helper->remove_flip("craft_create_volume_apply_error");
    g_helper->remove_flip("craft_create_volume_no_rollback");

    auto const expect_left_alone = [this, &info, &id] {
        auto retry = create(clone(info));
        ASSERT_FALSE(retry);
        EXPECT_EQ(retry.error(), std::errc::operation_in_progress);
        EXPECT_FALSE(hb()->get_volume(id));
        auto const rd = group(id);
        ASSERT_NE(rd, nullptr);
        EXPECT_FALSE(is_going_away(*rd));
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
    ASSERT_TRUE(wait_for_group_gone(id));
    auto vol = create(clone(info));
    ASSERT_TRUE(vol) << vol.error().message();
    EXPECT_NE((*vol)->craft_dev(), nullptr);
}

// A create that crashed between its index table and its superblock leaves the table behind. The table keeps its
// ordinal across restarts, so no other volume gets its chunks, and the volume's next create takes it over, also after
// one that failed with it.
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

    g_helper = std::make_unique< test_common::HBTestHelper >("test_craft_volume", args, orig_argv);
    auto const port = SISL_OPTIONS["repl_port"].as< uint16_t >();
    g_helper->set_config_hook([port](home_blocks_config& cfg) {
        cfg.repl_mode = replication_mode::CRAFT;
        cfg.repl_svc_port = port;
        // A single node: every group has this node as its only member.
        cfg.lookup_peer = [port](peer_id_t const&) { return std::pair< std::string, uint16_t >{"127.0.0.1", port}; };
    });
    g_helper->setup();
    auto ret = RUN_ALL_TESTS();
    g_helper->teardown();

    return ret;
}
