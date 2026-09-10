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

// Heavy integration test for CraftReplDev::commit()/read() against a REAL VolumeIndexTable and real
// data blocks -- closing the gap between test_craft_commit.cpp's fake-index unit tests and
// production reality (in particular, VolumeIndexTable::delete_lba_range has no direct unit test
// anywhere else; the all_zeros-unmap test below is it).
//
// There is no production call site that constructs a CRAFT-mode CraftReplDev yet (repl_mode::CRAFT
// on volume_info is declared but not wired to anything) -- so this test creates an ordinary volume
// purely to get a real, fully chunk-allocated VolumeIndexTable (and a real ordinal for routing
// alloc_write_data) via the exact same create_volume() path every other volume test already uses,
// rather than replicating volume::init_index_table's internal chunk-allocation plumbing by hand.
// CRAFT's own journal is a separate, freshly created home_log_store (matching
// test_craft_homestore_backend.cpp's make_logstore() helper) -- distinct from the volume's own
// replication journal, which this test never touches.
//
// No _PRERELEASE seam is needed anywhere here: read() is public, and every call uses term=0, which
// matches CraftPartitionState's default (login() is still a stub, so there is no other way to reach
// a real nonzero term) -- everything drives through CraftReplDev's real client-facing API.
//
// Links the full homeblocks library, same as test_craft_homestore_backend.cpp, because both a real
// home_log_store and a real VolumeIndexTable require a running HomeStore instance.

#include <algorithm>
#include <cstring>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/options/options.h>
#include <homestore/logstore_service.hpp>
#include <iomgr/iomgr.hpp>

#include "hb_internal.hpp"
#include "craft/craft_repl_dev.hpp"
#include "coro_helpers.hpp"
#include "volume/volume.hpp" // volume::indx_table() / ordinal() -- home_blocks.hpp only forward-declares volume
#include "test_common.hpp"

SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTIONS_ENABLE(logging, test_common_setup)
SISL_LOGGING_DECL(test_craft_commit_hs)

std::unique_ptr< test_common::HBTestHelper > g_helper;

using namespace homeblocks;

static constexpr uint32_t k_page_size = 4096;

class CraftCommitHsTest : public ::testing::Test {
protected:
    // One real volume (hence one real, chunk-allocated VolumeIndexTable) shared across every test in
    // this suite -- each TEST_F creating its own would exhaust the test device pool's chunk capacity
    // after only a couple of volumes. Tests stay independent by using disjoint LBA ranges.
    static void SetUpTestSuite() {
        volume_info vinfo{boost::uuids::random_generator()(), /* size = */ 64ull * 1024 * 1024, k_page_size,
                          "craft_commit_hs_vol"};
        auto vol_r = homeblocks::detail::sync_get(g_helper->inst()->create_volume(std::move(vinfo)));
        ASSERT_TRUE(vol_r.has_value());
        s_vol = vol_r.value();
    }

    static void TearDownTestSuite() { s_vol.reset(); }

    // Each test still gets its own fresh journal (log store) and CraftReplDev -- only the underlying
    // real index/data infrastructure is shared.
    void SetUp() override {
        auto flush_mode =
            static_cast< homestore::flush_mode_t >(static_cast< uint32_t >(homestore::flush_mode_t::TIMER) |
                                                   static_cast< uint32_t >(homestore::flush_mode_t::INLINE));
        auto logdev_id = homestore::logstore_service().create_new_logdev(flush_mode);
        auto logstore = homestore::logstore_service().create_new_log_store(logdev_id, /* append_mode = */ false);
        ASSERT_TRUE(logstore != nullptr);

        auto backend = make_homestore_journal_backend(logstore, s_vol->ordinal(), k_page_size);
        dev_ = CraftReplDev::create(s_vol->id(), std::move(backend), k_page_size, s_vol->indx_table());
    }

    // term=0 matches CraftPartitionState's default -- login() is still a stub, so there is no other
    // way to reach a real nonzero term; every call in this file uses it. content/dest are
    // sisl::io_blob_safe (not a plain std::vector) because homestore::data_service()'s real
    // async_alloc_write/async_read need a properly aligned buffer.
    auto do_write(int64_t dlsn, lba_t lba, uint32_t nlbas, sisl::io_blob_safe const& content, int64_t commit_lsn) {
        sisl::sg_list data;
        data.size = content.size();
        data.iovs.push_back(iovec{const_cast< uint8_t* >(content.cbytes()), content.size()});
        return homeblocks::detail::sync_get(dev_->write(craft::client_hdr{0, commit_lsn, -1}, dlsn, lba * k_page_size,
                                                        nlbas * k_page_size, std::move(data)));
    }

    auto do_write_zeros(int64_t dlsn, lba_t lba, uint32_t nlbas, int64_t commit_lsn) {
        sisl::sg_list empty_data{};
        return homeblocks::detail::sync_get(dev_->write(craft::client_hdr{0, commit_lsn, -1}, dlsn, lba * k_page_size,
                                                        nlbas * k_page_size, std::move(empty_data)));
    }

    auto do_read(int64_t read_lsn, lba_t lba, uint32_t nlbas, sisl::io_blob_safe& dest_buf) {
        std::memset(dest_buf.bytes(), 0xFF, dest_buf.size()); // non-zero filler so hole-zeroing is observable
        sisl::sg_list dest;
        dest.size = dest_buf.size();
        dest.iovs.push_back(iovec{dest_buf.bytes(), dest_buf.size()});
        return homeblocks::detail::sync_get(dev_->read(craft::client_hdr{0, -1, -1}, read_lsn, lba * k_page_size,
                                                       nlbas * k_page_size, std::move(dest)));
    }

    static bool all_zero(sisl::io_blob_safe const& buf) {
        return std::all_of(buf.cbytes(), buf.cbytes() + buf.size(), [](uint8_t b) { return b == 0; });
    }

    static volume_handle s_vol;
    std::shared_ptr< CraftReplDev > dev_;
};
volume_handle CraftCommitHsTest::s_vol;

// A write piggybacking its own commit_lsn applies to the real index immediately; a subsequent read
// at or above that lsn serves the real data back correctly (content and extents both verified).
TEST_F(CraftCommitHsTest, WriteCommitReadRoundTrip) {
    sisl::io_blob_safe content{k_page_size, 512};
    std::memset(content.bytes(), 0xAB, content.size());
    ASSERT_TRUE(do_write(/* dlsn = */ 0, /* lba = */ 0, /* nlbas = */ 1, content, /* commit_lsn = */ 0).has_value());

    sisl::io_blob_safe dest{k_page_size, 512};
    auto r = do_read(/* read_lsn = */ 0, /* lba = */ 0, /* nlbas = */ 1, dest);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_FALSE(r->extents[0].hole);
    EXPECT_EQ(std::memcmp(dest.cbytes(), content.cbytes(), content.size()), 0);
}

// The ack must reflect the piggyback commit's OWN progress, not a snapshot taken before it ran.
// The first write into a fresh partition piggybacks commit_lsn=0 (nothing to stall on, applies
// immediately) -- a pre-commit snapshot would incorrectly report the partition's initial state
// (commit_lsn=-1) rather than the commit_lsn=0 this very call just achieved.
TEST_F(CraftCommitHsTest, WriteAckReflectsPostCommitSnapshot) {
    constexpr lba_t k_lba = 20;
    sisl::io_blob_safe content{k_page_size, 512};
    std::memset(content.bytes(), 0xEF, content.size());
    auto r = do_write(/* dlsn = */ 0, k_lba, /* nlbas = */ 1, content, /* commit_lsn = */ 0);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->commit_lsn, 0); // post-commit value, not the pre-commit -1
}

// A write with NO commit piggyback (commit_lsn=-1) never reaches the real index -- the data is
// still locally readable via the journal-tail overlay alone.
TEST_F(CraftCommitHsTest, OverlayReadableBeforeCommit) {
    constexpr lba_t k_lba = 10;
    sisl::io_blob_safe content{k_page_size, 512};
    std::memset(content.bytes(), 0xCD, content.size());
    ASSERT_TRUE(do_write(/* dlsn = */ 0, k_lba, /* nlbas = */ 1, content, /* commit_lsn = */ -1).has_value());

    sisl::io_blob_safe dest{k_page_size, 512};
    auto r = do_read(/* read_lsn = */ 0, k_lba, /* nlbas = */ 1, dest);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_FALSE(r->extents[0].hole);
    EXPECT_EQ(std::memcmp(dest.cbytes(), content.cbytes(), content.size()), 0);
}

// Unmapping a previously-committed LBA (all_zeros write, committed) must reclaim it from the REAL
// VolumeIndexTable (delete_lba_range) -- a subsequent read sees a hole, not stale data. This is the
// only exercise of delete_lba_range against a real index anywhere in the CRAFT test suite.
TEST_F(CraftCommitHsTest, AllZerosUnmapReclaimsRealBlock) {
    constexpr lba_t k_lba = 20;
    sisl::io_blob_safe content{k_page_size, 512};
    std::memset(content.bytes(), 0xEE, content.size());
    ASSERT_TRUE(do_write(/* dlsn = */ 0, k_lba, /* nlbas = */ 1, content, /* commit_lsn = */ 0).has_value());
    ASSERT_TRUE(do_write_zeros(/* dlsn = */ 1, k_lba, /* nlbas = */ 1, /* commit_lsn = */ 1).has_value());

    sisl::io_blob_safe dest{k_page_size, 512};
    auto r = do_read(/* read_lsn = */ 1, k_lba, /* nlbas = */ 1, dest);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_TRUE(r->extents[0].hole);
    EXPECT_TRUE(all_zero(dest));
}

// A multi-LBA write/commit/read round-trips distinct per-LBA content correctly through the real
// checksum-verified read path (not just single-LBA mock block numbers).
TEST_F(CraftCommitHsTest, MultiLbaWriteCommitReadRoundTrip) {
    constexpr lba_t k_lba = 30;
    constexpr uint32_t k_nlbas = 3;
    sisl::io_blob_safe content{k_nlbas * k_page_size, 512};
    for (uint32_t i = 0; i < k_nlbas; ++i)
        std::memset(content.bytes() + i * k_page_size, int(i + 1), k_page_size);
    ASSERT_TRUE(do_write(/* dlsn = */ 0, k_lba, k_nlbas, content, /* commit_lsn = */ 0).has_value());

    sisl::io_blob_safe dest{k_nlbas * k_page_size, 512};
    auto r = do_read(/* read_lsn = */ 0, k_lba, k_nlbas, dest);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->extents.size(), 1u);
    EXPECT_FALSE(r->extents[0].hole);
    EXPECT_EQ(r->extents[0].len, k_nlbas * k_page_size);
    EXPECT_EQ(std::memcmp(dest.cbytes(), content.cbytes(), content.size()), 0);
}

// The end-to-end visibility transition via the real, public keep_alive() entry point (not just
// do_commit()-style test seams): before keep_alive() advances commit_lsn, a write is only readable
// via the overlay; after, the SAME content is served from the real index instead. A second write to
// the SAME LBA above the new commit point (still overlay-only) makes the two sources definitively
// distinguishable by content: a read below it must see the FIRST write's content (proving it came
// from the index, since the overlay's current entry for this LBA is the second write, not the
// first) while a read at-or-above it must see the SECOND write's (still overlay-only) content.
TEST_F(CraftCommitHsTest, KeepAliveAdvancesCommitThenReadServesIndex) {
    constexpr lba_t k_lba = 100;
    sisl::io_blob_safe content{k_page_size, 512};
    std::memset(content.bytes(), 0xF0, content.size());
    ASSERT_TRUE(do_write(/* dlsn = */ 0, k_lba, /* nlbas = */ 1, content, /* commit_lsn = */ -1).has_value());

    sisl::io_blob_safe dest{k_page_size, 512};
    auto r1 = do_read(/* read_lsn = */ 0, k_lba, /* nlbas = */ 1, dest);
    ASSERT_TRUE(r1.has_value());
    EXPECT_FALSE(r1->extents[0].hole);
    EXPECT_EQ(std::memcmp(dest.cbytes(), content.cbytes(), content.size()), 0);

    auto ka = homeblocks::detail::sync_get(dev_->keep_alive(craft::client_hdr{0, /* commit_lsn = */ 0, -1}));
    ASSERT_TRUE(ka.has_value());
    EXPECT_EQ(ka->commit_lsn, 0);

    // Second write to the SAME LBA, still uncommitted (piggybacked commit_lsn stays at 0) -- this
    // becomes the overlay's only entry for k_lba, distinct in content from the first write.
    sisl::io_blob_safe content2{k_page_size, 512};
    std::memset(content2.bytes(), 0x3C, content2.size());
    ASSERT_TRUE(do_write(/* dlsn = */ 1, k_lba, /* nlbas = */ 1, content2, /* commit_lsn = */ 0).has_value());

    // A read at read_lsn=0 is below the second write's dlsn=1, so the horizon clamp holds the
    // overlay's (now content2) entry back -- this MUST come from the index, and MUST be content1.
    auto r2 = do_read(/* read_lsn = */ 0, k_lba, /* nlbas = */ 1, dest);
    ASSERT_TRUE(r2.has_value());
    EXPECT_FALSE(r2->extents[0].hole);
    EXPECT_EQ(std::memcmp(dest.cbytes(), content.cbytes(), content.size()), 0)
        << "read_lsn=0 must be served from the index (content1), proving index-sourcing";

    // A read at read_lsn=1 is within the second write's horizon -- served from the overlay, content2.
    auto r3 = do_read(/* read_lsn = */ 1, k_lba, /* nlbas = */ 1, dest);
    ASSERT_TRUE(r3.has_value());
    EXPECT_FALSE(r3->extents[0].hole);
    EXPECT_EQ(std::memcmp(dest.cbytes(), content2.cbytes(), content2.size()), 0)
        << "read_lsn=1 must be served from the overlay (content2)";
}

// N real threads concurrently call write() on the SAME CraftReplDev instance, each to its own
// disjoint LBA (matching how multiple concurrent client write RPCs for the same partition would
// arrive in production before CraftReplDev's own locking serializes the parts that need it) --
// proving no write is lost or corrupted under genuine concurrent access to the real backend/index.
TEST_F(CraftCommitHsTest, ConcurrentWritesToDisjointLbaRangesAllLand) {
    constexpr lba_t k_base_lba = 200;
    constexpr int kThreads = 4;
    constexpr int kPerThread = 5;

    std::vector< std::thread > threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([this, t]() {
            for (int k = 0; k < kPerThread; ++k) {
                lba_t const lba = k_base_lba + t * kPerThread + k;
                int64_t const dlsn = static_cast< int64_t >(t * kPerThread + k); // globally unique
                sisl::io_blob_safe content{k_page_size, 512};
                std::memset(content.bytes(), t + 1, content.size());
                auto r = do_write(dlsn, lba, /* nlbas = */ 1, content, /* commit_lsn = */ dlsn);
                EXPECT_TRUE(r.has_value()) << "thread=" << t << " k=" << k;
            }
        });
    }
    for (auto& th : threads)
        th.join();

    // read_lsn far beyond every dlsn so every entry -- committed or still overlay-only, depending on
    // how far each write's own piggybacked commit happened to stall -- is within horizon.
    for (int t = 0; t < kThreads; ++t) {
        for (int k = 0; k < kPerThread; ++k) {
            lba_t const lba = k_base_lba + t * kPerThread + k;
            sisl::io_blob_safe dest{k_page_size, 512};
            auto r = do_read(kThreads * kPerThread, lba, /* nlbas = */ 1, dest);
            ASSERT_TRUE(r.has_value()) << "lba=" << lba;
            ASSERT_EQ(r->extents.size(), 1u) << "lba=" << lba;
            EXPECT_FALSE(r->extents[0].hole) << "lba=" << lba;
            std::vector< uint8_t > expected(k_page_size, static_cast< uint8_t >(t + 1));
            EXPECT_EQ(std::memcmp(dest.cbytes(), expected.data(), expected.size()), 0) << "lba=" << lba;
        }
    }
}

// N real threads all call keep_alive() targeting the same commit_lsn concurrently against the real
// index -- commit_running_ must serialize them so the apply loop runs to completion exactly once
// (from whichever thread wins), with every slot correctly committed and readable afterward.
TEST_F(CraftCommitHsTest, ConcurrentKeepAliveSerializesCommitAgainstRealIndex) {
    constexpr lba_t k_base_lba = 300;
    constexpr int kSlots = 20;

    for (int i = 0; i < kSlots; ++i) {
        sisl::io_blob_safe content{k_page_size, 512};
        std::memset(content.bytes(), i + 1, content.size());
        ASSERT_TRUE(
            do_write(/* dlsn = */ i, k_base_lba + i, /* nlbas = */ 1, content, /* commit_lsn = */ -1).has_value());
    }

    constexpr int kThreads = 6;
    std::vector< std::thread > threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([this, kSlots]() {
            auto r =
                homeblocks::detail::sync_get(dev_->keep_alive(craft::client_hdr{0, /* commit_lsn = */ kSlots - 1, -1}));
            EXPECT_TRUE(r.has_value());
        });
    }
    for (auto& th : threads)
        th.join();

    EXPECT_EQ(dev_->commit_lsn(), kSlots - 1);

    for (int i = 0; i < kSlots; ++i) {
        sisl::io_blob_safe dest{k_page_size, 512};
        auto r = do_read(kSlots - 1, k_base_lba + i, /* nlbas = */ 1, dest);
        ASSERT_TRUE(r.has_value()) << "i=" << i;
        EXPECT_FALSE(r->extents[0].hole) << "i=" << i;
        std::vector< uint8_t > expected(k_page_size, static_cast< uint8_t >(i + 1));
        EXPECT_EQ(std::memcmp(dest.cbytes(), expected.data(), expected.size()), 0) << "i=" << i;
    }
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    char** orig_argv = argv;
    std::vector< std::string > args;
    for (int i = 0; i < argc; ++i) {
        args.emplace_back(argv[i]);
    }

    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_common_setup);
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");

    g_helper = std::make_unique< test_common::HBTestHelper >("test_craft_commit_hs", args, orig_argv);
    g_helper->setup();
    auto ret = RUN_ALL_TESTS();
    g_helper->teardown();
    return ret;
}
