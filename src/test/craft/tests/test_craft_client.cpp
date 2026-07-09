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

// Unit test for craft_client -- the client-side CRAFT sequencing (dLSN assignment, broadcast + quorum,
// commit frontier, read horizon) the ublk driver drives. It runs against the in-memory reference model
// via make_memory_replica_set, with NO ublk device and NO iomgr, so it is a fast CI check of the
// sequencing logic before any /dev/ublkbN validation. n=1 is the current stepping stone; n=3 (with
// force_subquorum) exercises the quorum path the next step builds on.

#include <cstdint>
#include <memory>
#include <vector>

#include <boost/uuid/uuid_generators.hpp>
#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homeblks/home_blocks.hpp>

#include "craft_test_util.hpp"
#include "model/mem_craft_volume.hpp" // make_memory_replica_set, mem_replica_id (via mem_craft_cluster.hpp)
#include "client/craft_client.hpp"

SISL_LOGGING_DEF(homeblocks)
SISL_LOGGING_INIT(homeblocks)
SISL_OPTIONS_ENABLE(logging)

using namespace homeblocks;

using namespace homeblocks::craft::test;

namespace {
constexpr uint64_t TOKEN = 0xABCDEFULL;

struct Cluster {
    craft::MemReplicaHandles set;
    volume_id_t vid;
    std::shared_ptr< craft::craft_client > client;
};

// Stand up an n-replica in-memory set + a client over it (the client copies the handle vector, so `set`
// retains net for fault injection), and log in. Asserts the login succeeds.
Cluster make_cluster(uint32_t n, uint32_t leader = 0) {
    volume_info info;
    info.id = boost::uuids::random_generator()();
    info.size_bytes = 1ULL << 20;
    info.page_size = PAGE;
    info.name = "craft-client-test";
    info.repl_mode = replication_mode::CRAFT;
    volume_id_t const vid = info.id;

    auto set = craft::make_memory_replica_set(std::move(info), n);
    auto client = std::make_shared< craft::craft_client >(set.handles, leader); // copy handles; set keeps net
    EXPECT_TRUE(rg(client->login(TOKEN)).has_value());
    EXPECT_EQ(client->lba_size(), PAGE);
    return Cluster{std::move(set), vid, std::move(client)};
}

// convenience wrappers
bool wr(craft::craft_client& c, uint64_t off_blk, std::vector< uint8_t >& buf) {
    return rg(c.write(blk(off_blk), buf.size(), one_iov(buf))).has_value();
}
std::vector< uint8_t > rd(craft::craft_client& c, uint64_t off_blk, uint64_t nblk = 1) {
    std::vector< uint8_t > dest(nblk * PAGE, 0xEE);
    auto r = rg(c.read(blk(off_blk), nblk * PAGE, one_iov(dest)));
    EXPECT_TRUE(r.has_value());
    return dest;
}

// Only the leader accepts the write, so it journals the slot while quorum(3)=2 goes unmet: the slot stays
// unresolved and the leader physically holds it.
void write_subquorum(Cluster& cl, uint64_t off_blk, std::vector< uint8_t >& buf) {
    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 0)});
    EXPECT_FALSE(wr(*cl.client, off_blk, buf));
    cl.set.net->clear_faults();
}
} // namespace

// --- n=1: the stepping stone ---

TEST(CraftClient, N1_WriteReadRoundtrip) {
    auto cl = make_cluster(1);
    auto buf = page_of(0x5A);
    ASSERT_TRUE(wr(*cl.client, 9, buf));
    EXPECT_EQ(rd(*cl.client, 9), buf);
}

TEST(CraftClient, N1_UnwrittenReadsZeros) {
    auto cl = make_cluster(1);
    EXPECT_EQ(rd(*cl.client, 3), page_of(0)); // never written -> hole -> zeros
}

TEST(CraftClient, N1_OverwriteNewestWins) {
    auto cl = make_cluster(1);
    auto older = page_of(0xAA);
    auto newer = page_of(0xBB);
    ASSERT_TRUE(wr(*cl.client, 5, older));
    ASSERT_TRUE(wr(*cl.client, 5, newer)); // higher dLSN to the same LBA
    EXPECT_EQ(rd(*cl.client, 5), newer);
}

TEST(CraftClient, N1_CommitAdvancesOnAck) {
    auto cl = make_cluster(1);
    EXPECT_EQ(cl.client->commit_lsn(), -1); // nothing acked yet
    auto b0 = page_of(0x11);
    auto b1 = page_of(0x22);
    ASSERT_TRUE(wr(*cl.client, 0, b0));
    EXPECT_EQ(cl.client->commit_lsn(), 0); // n=1: quorum instantly met -> commit on ack
    ASSERT_TRUE(wr(*cl.client, 1, b1));
    EXPECT_EQ(cl.client->commit_lsn(), 1);
    EXPECT_EQ(cl.client->read_horizon(), 1);
    EXPECT_TRUE(rg(cl.client->flush()).has_value());
}

// --- n=3: quorum path the next step builds on ---

TEST(CraftClient, N3_QuorumWriteRead) {
    auto cl = make_cluster(3);
    auto buf = page_of(0x77);
    ASSERT_TRUE(wr(*cl.client, 12, buf)); // broadcast to 3, all ack
    EXPECT_EQ(cl.client->commit_lsn(), 0);
    EXPECT_EQ(rd(*cl.client, 12), buf);
}

TEST(CraftClient, N3_SubquorumStillCommits) {
    auto cl = make_cluster(3);
    // Drop one replica: 2 of 3 still ack, which meets quorum(3)=2.
    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 0), craft::mem_replica_id(cl.vid, 1)});
    auto buf = page_of(0x33);
    ASSERT_TRUE(wr(*cl.client, 4, buf));
    EXPECT_EQ(rd(*cl.client, 4), buf); // leader (index 0) is kept, so the read is served
}

TEST(CraftClient, N3_BelowQuorumFails) {
    auto cl = make_cluster(3);
    // Keep only the leader: 1 of 3 acks, below quorum(3)=2 -> the write must fail.
    cl.set.net->force_subquorum({craft::mem_replica_id(cl.vid, 0)});
    auto buf = page_of(0x44);
    EXPECT_FALSE(wr(*cl.client, 6, buf));
}

// --- the read horizon: a sub-quorum write must never leak into a read ---

TEST(CraftClient, N3_FailedWriteDoesNotLeakIntoReads) {
    auto cl = make_cluster(3);
    auto v0 = page_of(0x10);
    auto v1 = page_of(0x11);
    auto v2 = page_of(0x12);

    ASSERT_TRUE(wr(*cl.client, 4, v0)); // dLSN 0: durable at block 4
    EXPECT_EQ(cl.client->commit_lsn(), 0);
    write_subquorum(cl, 4, v1);         // dLSN 1: unresolved, and the leader HOLDS it
    ASSERT_TRUE(wr(*cl.client, 9, v2)); // dLSN 2: acks, pushing the horizon above the hole

    EXPECT_EQ(cl.client->commit_lsn(), 0) << "commit must stay pinned beneath the unresolved dLSN 1";
    EXPECT_EQ(cl.client->read_horizon(), 2);

    // Reading block 4 at the raw horizon would let the leader serve its journaled dLSN 1, which may yet
    // be Empty'd -- a later read would then regress. The client clamps below it and serves dLSN 0.
    EXPECT_EQ(rd(*cl.client, 4), v0);
    EXPECT_EQ(cl.client->winner_scans(), 1u) << "the per-block winner pass is what made this safe";

    EXPECT_EQ(rd(*cl.client, 9), v2); // an unrelated block still reads at the full horizon
}

TEST(CraftClient, N3_AckedOverwriteShadowsAFailedWrite) {
    auto cl = make_cluster(3);
    auto v0 = page_of(0x20);
    auto v1 = page_of(0x21);
    auto v2 = page_of(0x22);

    ASSERT_TRUE(wr(*cl.client, 6, v0)); // dLSN 0
    write_subquorum(cl, 6, v1);         // dLSN 1: failed, leader holds it
    ASSERT_TRUE(wr(*cl.client, 6, v2)); // dLSN 2: same block, acks

    // Highest dLSN wins per LBA, so dLSN 2 fully shadows the unresolved dLSN 1: it can never be the version
    // a read sees, and the read proceeds at the full horizon.
    EXPECT_EQ(rd(*cl.client, 6), v2);
    EXPECT_EQ(cl.client->commit_lsn(), 0);
}

TEST(CraftClient, N3_SplitReadServesEachBlockAtItsOwnHorizon) {
    auto cl = make_cluster(3);
    auto v0 = page_of(0x30, 2); // blocks 4-5
    auto v1 = page_of(0x31, 2); // blocks 4-5, will fail
    auto v2 = page_of(0x32);    // block 4 only

    ASSERT_TRUE(wr(*cl.client, 4, v0)); // dLSN 0: both blocks durable
    write_subquorum(cl, 4, v1);         // dLSN 1: failed, leader journals it over BOTH blocks
    ASSERT_TRUE(wr(*cl.client, 4, v2)); // dLSN 2: acks, but covers only block 4

    // Block 4's newest version is the acked dLSN 2, which supersedes the failed dLSN 1 there. Block 5's is
    // the failed dLSN 1 itself, which the leader physically holds. One horizon cannot serve both.
    std::vector< uint8_t > want;
    want.insert(want.end(), v2.begin(), v2.end());          // block 4 -> v2 (read at the horizon)
    want.insert(want.end(), v0.begin(), v0.begin() + PAGE); // block 5 -> v0 (clamped below dLSN 1)
    EXPECT_EQ(rd(*cl.client, 4, 2), want);
    EXPECT_EQ(cl.client->commit_lsn(), 0) << "the failed write still pins commit";
}

// --- session establishment ---

TEST(CraftClient, LoginFollowsLeaderRedirect) {
    // Aim login at replica 2, a follower: it succeeds with term == 0 + leader_hint rather than erroring.
    auto cl = make_cluster(3, /*leader=*/2);
    EXPECT_GT(cl.client->term(), 0ULL);

    auto buf = page_of(0x55);
    ASSERT_TRUE(wr(*cl.client, 3, buf));
    EXPECT_EQ(rd(*cl.client, 3), buf);
}

TEST(CraftClient, RecoveryLoginReadsDurableDataNotZeros) {
    auto cl = make_cluster(3);
    auto buf = page_of(0x77);
    ASSERT_TRUE(wr(*cl.client, 2, buf));
    ASSERT_TRUE(wr(*cl.client, 7, buf));
    EXPECT_EQ(cl.client->commit_lsn(), 1);

    // A fresh client logs into the same replicas. login() hands back rs_commit_lsn = 1, which must seed
    // the read horizon as well as the frontier -- seeding only next_dlsn leaves H at -1 and reads zeros.
    auto fresh = std::make_shared< craft::craft_client >(cl.set.handles);
    ASSERT_TRUE(rg(fresh->login(TOKEN + 1)).has_value());
    EXPECT_EQ(fresh->commit_lsn(), 1);
    EXPECT_EQ(fresh->read_horizon(), 1);
    EXPECT_EQ(rd(*fresh, 2), buf);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    sisl::logging::SetLogPattern("[%D %T%z] [%^%L%$] [%t] %v");
    return RUN_ALL_TESTS();
}
