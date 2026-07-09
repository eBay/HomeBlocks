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

// End-to-end test of the in-memory CRAFT reference model through the PUBLIC volume_handle API
// (home_blocks.hpp: login / async_write / async_read / keep_alive) -- exactly the surface a real CRAFT
// client is built against. The handles come from make_memory_replica_set (create_memory_volume),
// the in-process stand-in for "create the volume on each of three servers".

#include <cstdint>
#include <utility>
#include <vector>

#include <boost/uuid/uuid_generators.hpp>
#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homeblks/home_blocks.hpp>

#include "craft_test_util.hpp"
#include "model/mem_craft_volume.hpp"

// libhomeblocks defines the `homeblocks` log module in homeblks_impl.o, but this test never odr-uses
// that object (it drives create_memory_volume, not init_homeblocks), so the static archive does not
// pull it in. Define the module here so logging init finds it.
SISL_LOGGING_DEF(homeblocks)
SISL_LOGGING_INIT(homeblocks)
SISL_OPTIONS_ENABLE(logging)

using namespace homeblocks;

using namespace homeblocks::craft::test;

namespace {
constexpr uint64_t TOKEN = 0xABCDEFULL;

volume_info mk_info() {
    volume_info vi;
    vi.id = boost::uuids::random_generator()();
    vi.size_bytes = 1ULL << 20;
    vi.page_size = PAGE;
    vi.name = "craft-e2e";
    vi.repl_mode = replication_mode::CRAFT;
    return vi;
}
} // namespace

// Full path: stand up the in-process replica set, then drive login -> write (broadcast) -> keep_alive
// (commit) -> read entirely through the public free functions over volume_handle.
TEST(CraftMemE2E, LoginWriteCommitReadViaPublicApi) {
    auto set = craft::make_memory_replica_set(mk_info(), 3);
    ASSERT_EQ(set.handles.size(), 3u);

    auto lr = rg(login(set.handles[0], TOKEN)); // leader is replica 0
    ASSERT_TRUE(lr.has_value());
    EXPECT_EQ(lr->members.size(), 3u);
    EXPECT_EQ(lr->term, 1u);
    EXPECT_EQ(lr->dLSN, -1);
    EXPECT_EQ(lr->lba_size, PAGE);
    uint64_t const term = lr->term;

    // The client assigns dLSN 0, broadcasts async_write to every replica handle, and counts acks.
    auto buf = page_of(0x5A);
    int acks = 0;
    for (auto& h : set.handles) {
        if (rg(async_write(h, chdr(term), /*dlsn*/ 0, /*addr*/ blk(9), /*len*/ blk(1), one_iov(buf))).has_value())
            ++acks;
    }
    EXPECT_EQ(acks, 3); // quorum (all three) acked

    for (auto& h : set.handles) { // keep_alive carries the commit watermark (no standalone commit verb)
        ASSERT_TRUE(rg(keep_alive(h, chdr(term, /*commit_lsn*/ 0))).has_value());
    }
    std::vector< uint8_t > dest(PAGE, 0xEE);
    auto rr = rg(async_read(set.handles[2], chdr(term), /*H*/ 0, blk(9), blk(1), one_iov(dest)));
    ASSERT_TRUE(rr.has_value());
    ASSERT_EQ(rr->size(), 1u);
    EXPECT_FALSE((*rr)[0].hole);
    EXPECT_EQ(dest, buf);
}

// login to a follower handle returns a redirect (term==0, leader_hint) through the public API.
TEST(CraftMemE2E, LoginOnFollowerReturnsLeaderHint) {
    auto set = craft::make_memory_replica_set(mk_info(), 3);
    auto lr = rg(login(set.handles[1], TOKEN));
    ASSERT_TRUE(lr.has_value());
    EXPECT_EQ(lr->term, 0u);                       // term==0 signals redirect
    EXPECT_EQ(lr->leader_hint, set.net->leader()); // hint points at the current leader
}

// a zero write (empty buffer) reads back as a hole through the public API.
TEST(CraftMemE2E, ZeroWriteHoleViaPublicApi) {
    auto set = craft::make_memory_replica_set(mk_info(), 3);
    auto lr = rg(login(set.handles[0], TOKEN));
    ASSERT_TRUE(lr.has_value());
    uint64_t const term = lr->term;
    auto& h = set.handles[0];

    ASSERT_TRUE(rg(async_write(h, chdr(term), 0, blk(3), blk(1), /*empty => zero write*/ sisl::sg_list{})).has_value());
    ASSERT_TRUE(rg(keep_alive(h, chdr(term, 0))).has_value());
    std::vector< uint8_t > dest(PAGE, 0xEE);
    auto rr = rg(async_read(h, chdr(term), 0, blk(3), blk(1), one_iov(dest)));
    ASSERT_TRUE(rr.has_value());
    ASSERT_EQ(rr->size(), 1u);
    EXPECT_TRUE((*rr)[0].hole);
    EXPECT_EQ(dest, page_of(0));
}

// explicit logout tears down the session; subsequent writes via the public API fail with STALE_TERM.
TEST(CraftMemE2E, LogoutClearsSessionViaPublicApi) {
    auto set = craft::make_memory_replica_set(mk_info(), 3);
    auto lr = rg(login(set.handles[0], TOKEN));
    ASSERT_TRUE(lr.has_value());
    uint64_t const term = lr->term;

    ASSERT_TRUE(rg(logout(set.handles[0], chdr(term))).has_value());

    // All handles should now reject the old term.
    auto buf = page_of(0x5A);
    for (auto& h : set.handles) {
        auto bad = rg(async_write(h, chdr(term), 0, blk(0), blk(1), one_iov(buf)));
        ASSERT_FALSE(bad.has_value());
        EXPECT_EQ(bad.error(), make_error_condition(craft_error::STALE_TERM));
    }
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    sisl::logging::SetLogPattern("[%D %T%z] [%^%L%$] [%t] %v");
    return RUN_ALL_TESTS();
}
