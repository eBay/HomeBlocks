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

// CRAFT volume creation (SDSTOR-22894) across a multi-member RAFT group: one process per member. Replica 0 starts the
// others (re-executing this binary with --replica_num) and receives every create; the processes share nothing but a
// shared-memory barrier, and each one checks its own HomeBlocks. Every member must end up with the volume, keep it
// across a restart of the whole group, and lose it again when a create fails after the followers applied it; a member
// left without a replica must not take the group down. Each test creates its own volume.

#include <filesystem>
#include <spawn.h>
#include <sys/wait.h>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <gtest/gtest.h>
#include <sisl/options/options.h>
#include <sisl/flip/flip_client.hpp>
#include <iomgr/iomgr_flip.hpp>
#include <homestore/homestore.hpp>
#include <homestore/replication_service.hpp>
#include "hb_internal.hpp"
#include "test_common.hpp"
#include <volume/volume.hpp>

extern char** environ;

SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTION_GROUP(test_craft_volume_multi_setup,
                  (replica_num, "", "replica_num", "this process's member index; replica 0 starts the others",
                   ::cxxopts::value< uint32_t >()->default_value("0"), "number"),
                  (num_replicas, "", "num_replicas", "members of every group",
                   ::cxxopts::value< uint32_t >()->default_value("3"), "number"),
                  (base_port, "", "base_port", "member i's RAFT port is base_port + i",
                   ::cxxopts::value< uint16_t >()->default_value("47460"), "port"),
                  (shm_name, "", "shm_name", "shared memory of the run, set by replica 0",
                   ::cxxopts::value< std::string >()->default_value(""), "name"),
                  (wait_timeout_sec, "", "wait_timeout_sec", "how long a member waits for the others",
                   ::cxxopts::value< uint32_t >()->default_value("120"), "seconds"));

// `config`: --config_path / --override_config, used to shorten HomeStore's repl dev reaper interval.
SISL_OPTIONS_ENABLE(logging, test_common_setup, test_craft_volume_multi_setup, config)

using namespace homeblocks;
namespace bip = boost::interprocess;

std::unique_ptr< test_common::HBTestHelper > g_helper;

// A barrier over shared memory: every member reaches the same step before any goes on.
class MemberBarrier {
    struct State {
        bip::interprocess_mutex mtx;
        bip::interprocess_condition cv;
        uint32_t arrived{0};
        uint64_t generation{0};
        int64_t value{0};
    };

public:
    MemberBarrier(std::string const& name, bool create) : name_{name}, owner_{create} {
        if (create) {
            bip::shared_memory_object::remove(name.c_str());
            shm_ = bip::shared_memory_object{bip::create_only, name.c_str(), bip::read_write};
            shm_.truncate(sizeof(State));
            region_ = bip::mapped_region{shm_, bip::read_write};
            state_ = new (region_.get_address()) State{};
        } else {
            shm_ = bip::shared_memory_object{bip::open_only, name.c_str(), bip::read_write};
            region_ = bip::mapped_region{shm_, bip::read_write};
            state_ = static_cast< State* >(region_.get_address());
        }
    }
    ~MemberBarrier() {
        if (owner_) { bip::shared_memory_object::remove(name_.c_str()); }
    }

    // False if not every member arrived in time: one of them failed or hung.
    bool sync(uint32_t members, uint32_t timeout_sec) {
        bip::scoped_lock< bip::interprocess_mutex > lk{state_->mtx};
        auto const gen = state_->generation;
        if (++state_->arrived == members) {
            state_->arrived = 0;
            ++state_->generation;
            state_->cv.notify_all();
            return true;
        }
        auto const deadline =
            boost::posix_time::microsec_clock::universal_time() + boost::posix_time::seconds(timeout_sec);
        return state_->cv.timed_wait(lk, deadline, [this, gen] { return state_->generation != gen; });
    }

    // A value one member hands the others: they read it after the next sync.
    void publish(int64_t v) {
        bip::scoped_lock< bip::interprocess_mutex > lk{state_->mtx};
        state_->value = v;
    }
    int64_t published() {
        bip::scoped_lock< bip::interprocess_mutex > lk{state_->mtx};
        return state_->value;
    }

private:
    std::string name_;
    bool owner_;
    bip::shared_memory_object shm_;
    bip::mapped_region region_;
    State* state_{nullptr};
};

std::unique_ptr< MemberBarrier > g_barrier;

static uint32_t me() { return SISL_OPTIONS["replica_num"].as< uint32_t >(); }
static uint32_t num_members() { return SISL_OPTIONS["num_replicas"].as< uint32_t >(); }
static uint32_t wait_timeout_sec() { return SISL_OPTIONS["wait_timeout_sec"].as< uint32_t >(); }
static uint16_t port_of(uint32_t i) { return SISL_OPTIONS["base_port"].as< uint16_t >() + i; }

// Every process derives the same ids: member i's svc id, and the test volumes'.
static peer_id_t member_id(uint32_t i) {
    return boost::uuids::string_generator()(fmt::format("{:08x}-0000-4000-8000-0000c0ffee00", i + 1));
}
static volume_id_t test_volume_id(uint32_t n) {
    return boost::uuids::string_generator()(fmt::format("{:08x}-0000-4000-8000-0000ba5e0000", n));
}

class CraftVolumeMultiTest : public ::testing::Test {
protected:
    static bool sync() { return g_barrier->sync(num_members(), wait_timeout_sec()); }

    static shared< home_blocks > hb() { return g_helper->inst(); }

    static volume_info vol_info(volume_id_t const& id, std::string const& name) {
        volume_info info{id, 1024 * 1024 * 1024, 4096, name};
        info.repl_mode = replication_mode::CRAFT;
        for (uint32_t i = 0; i < num_members(); ++i) {
            info.members.push_back(member_id(i));
        }
        return info;
    }

    static shared< homestore::repl_dev > group(volume_id_t const& id) {
        auto rd = homestore::hs()->repl_service().get_repl_dev(id);
        return rd ? rd.value() : nullptr;
    }

    // Polls `pred` until it holds or the wait timeout passes.
    template < typename Pred >
    static bool wait_until(Pred&& pred) {
        auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(wait_timeout_sec());
        while (!pred() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return pred();
    }
};

TEST_F(CraftVolumeMultiTest, CreateReachesEveryMember) {
    ASSERT_TRUE(sync());
    auto const id = test_volume_id(1);
    if (me() == 0) {
        auto vol = homeblocks::detail::sync_get(hb()->create_volume(vol_info(id, "craft_multi_1")));
        ASSERT_TRUE(vol) << vol.error().message();
        EXPECT_NE((*vol)->craft_dev(), nullptr);
    }

    // Every member creates its replica as it applies CreateVolume; the receiver does not wait for them.
    ASSERT_TRUE(wait_until([&id] {
        auto vol = hb()->get_volume(id);
        return vol && (*vol)->craft_dev() != nullptr;
    }));
    auto rd = group(id);
    ASSERT_NE(rd, nullptr);
    EXPECT_EQ(rd->get_replication_quorum().size(), num_members());
    ASSERT_TRUE(sync());
}

TEST_F(CraftVolumeMultiTest, VolumeSurvivesRestartOfEveryMember) {
    ASSERT_TRUE(sync());
    auto const id = test_volume_id(4);
    if (me() == 0) {
        auto vol = homeblocks::detail::sync_get(hb()->create_volume(vol_info(id, "craft_multi_4")));
        ASSERT_TRUE(vol) << vol.error().message();
    }
    ASSERT_TRUE(wait_until([&id] {
        auto vol = hb()->get_volume(id);
        return vol && (*vol)->craft_dev() != nullptr;
    }));
    ASSERT_TRUE(sync()); // every member has its replica before any restarts

    g_helper->restart();
    ASSERT_TRUE(sync()); // every member is back before anyone checks

    auto vol = hb()->get_volume(id);
    ASSERT_TRUE(vol);
    EXPECT_NE((*vol)->craft_dev(), nullptr);
    // The group is whole again: its rejoined members elected a leader.
    ASSERT_TRUE(wait_until([&id] {
        auto rd = group(id);
        return rd && !rd->get_leader_id().is_nil();
    }));
    ASSERT_TRUE(sync());
}

#ifdef _PRERELEASE
TEST_F(CraftVolumeMultiTest, FailedCreateLeavesNoReplica) {
    ASSERT_TRUE(sync());
    auto const id = test_volume_id(2);
    if (me() == 0) {
        // The receiver's own apply fails while the followers' succeed: the rollback destroys the group, and every
        // follower removes the replica it created.
        g_helper->set_flip_point("craft_create_volume_apply_error", 1);
        auto vol = homeblocks::detail::sync_get(hb()->create_volume(vol_info(id, "craft_multi_2")));
        g_helper->remove_flip("craft_create_volume_apply_error");
        ASSERT_FALSE(vol);
    }
    ASSERT_TRUE(sync());

    ASSERT_TRUE(wait_until([&id] { return !hb()->get_volume(id) && group(id) == nullptr; }));
    ASSERT_TRUE(sync());
}

// A follower's own apply fails, so the group lives on without a replica there. A create sent to that follower leaves
// the group alone: it neither proposes into nor destroys it, and the other members keep their replicas.
TEST_F(CraftVolumeMultiTest, CreateOnMemberWithoutReplicaKeepsTheGroup) {
    ASSERT_TRUE(sync());
    auto const id = test_volume_id(3);
    if (me() == 1) { g_helper->set_flip_point("craft_create_volume_apply_error", 1); }
    ASSERT_TRUE(sync());
    if (me() == 0) {
        auto vol = homeblocks::detail::sync_get(hb()->create_volume(vol_info(id, "craft_multi_3")));
        ASSERT_TRUE(vol) << vol.error().message();
        // At least CreateVolume's LSN, appended before it committed. Not the commit LSN: the create returns from the
        // apply, before HomeStore advances that.
        g_barrier->publish(group(id)->get_last_append_lsn());
    }
    ASSERT_TRUE(wait_until([&id] { return group(id) != nullptr; }));
    if (me() != 1) {
        ASSERT_TRUE(wait_until([&id] { return hb()->get_volume(id).has_value(); }));
    }
    ASSERT_TRUE(sync());

    // Member 1 has applied CreateVolume, and failed to, once it committed up to that LSN: HomeStore advances the commit
    // LSN after the apply.
    if (me() == 1) {
        auto const created_at = g_barrier->published();
        ASSERT_TRUE(wait_until([&id, created_at] {
            auto const rd = group(id);
            return rd != nullptr && rd->get_last_commit_lsn() >= created_at;
        }));
        ASSERT_FALSE(hb()->get_volume(id));
        auto retry = homeblocks::detail::sync_get(hb()->create_volume(vol_info(id, "craft_multi_3")));
        ASSERT_FALSE(retry);
        EXPECT_EQ(retry.error(), std::errc::operation_in_progress);
    }
    ASSERT_TRUE(sync());

    EXPECT_NE(group(id), nullptr);
    if (me() != 1) { EXPECT_TRUE(hb()->get_volume(id)); }
    if (me() == 1) { g_helper->remove_flip("craft_create_volume_apply_error"); }
    ASSERT_TRUE(sync());
}
#endif

// Replica 0 starts the other members: this binary again, with their own --replica_num, the shared memory, and a
// settings directory each (they rewrite their settings files at startup).
static std::vector< pid_t > start_members(std::vector< std::string > const& args, std::string const& shm_name) {
    std::vector< pid_t > pids;
    for (uint32_t i = 1; i < num_members(); ++i) {
        std::vector< std::string > child_args;
        for (auto const& a : args) {
            if (!a.starts_with("--replica_num") && !a.starts_with("--shm_name") && !a.starts_with("--config_path")) {
                child_args.push_back(a);
            }
        }
        child_args.push_back(fmt::format("--replica_num={}", i));
        child_args.push_back(fmt::format("--shm_name={}", shm_name));
        if (SISL_OPTIONS.count("config_path")) {
            child_args.push_back(
                fmt::format("--config_path={}_{}", SISL_OPTIONS["config_path"].as< std::string >(), i));
        }

        std::vector< char* > argv;
        for (auto& a : child_args) {
            argv.push_back(a.data());
        }
        argv.push_back(nullptr);
        pid_t pid;
        RELEASE_ASSERT_EQ(posix_spawn(&pid, argv[0], nullptr, nullptr, argv.data(), environ), 0,
                          "failed to start member {}", i);
        pids.push_back(pid);
    }
    return pids;
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    char** orig_argv = argv;

    std::vector< std::string > args;
    for (int i = 0; i < argc; ++i) {
        args.emplace_back(argv[i]);
    }

    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_common_setup, test_craft_volume_multi_setup, config);
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    // Each member runs on files of its own: a device list would be shared, and wiped, by all of them.
    if (SISL_OPTIONS.count("device_list")) {
        LOGERROR("test_craft_volume_multi gives every member its own files: --device_list is not supported");
        return 1;
    }
    if (SISL_OPTIONS.count("config_path")) {
        std::filesystem::create_directories(SISL_OPTIONS["config_path"].as< std::string >());
    }

    std::vector< pid_t > members;
    if (me() == 0) {
        auto const shm_name = fmt::format("hb_craft_volume_multi_{}", getpid());
        g_barrier = std::make_unique< MemberBarrier >(shm_name, true /* create */);
        members = start_members(args, shm_name);
    } else {
        g_barrier = std::make_unique< MemberBarrier >(SISL_OPTIONS["shm_name"].as< std::string >(), false);
    }

    g_helper =
        std::make_unique< test_common::HBTestHelper >(fmt::format("test_craft_volume_multi_{}", me()), args, orig_argv);
    auto const id = member_id(me());
    g_helper->set_config_hook([id](home_blocks_config& cfg) {
        cfg.app_mem_size_mb = 4 * 1024; // three of them on one host
        cfg.on_svc_id = [id]() -> async_result< peer_id_t > { co_return id; };
        cfg.repl_mode = replication_mode::CRAFT;
        cfg.repl_svc_port = port_of(me());
        cfg.lookup_peer = [](peer_id_t const& peer) {
            for (uint32_t i = 0; i < num_members(); ++i) {
                if (member_id(i) == peer) { return std::pair< std::string, uint16_t >{"127.0.0.1", port_of(i)}; }
            }
            return std::pair< std::string, uint16_t >{};
        };
    });
    g_helper->setup();
    auto ret = RUN_ALL_TESTS();
    g_barrier->sync(num_members(), wait_timeout_sec()); // nobody shuts down while another member still needs it
    g_helper->teardown();

    for (auto const pid : members) {
        int status = 0;
        waitpid(pid, &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            LOGERROR("member with pid {} failed, status {}", pid, status);
            ret = 1;
        }
    }
    return ret;
}
