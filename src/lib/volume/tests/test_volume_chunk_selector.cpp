#include <string>
#include <latch>
#include <gtest/gtest.h>
#include <sisl/flip/flip_client.hpp>
#include <iomgr/iomgr_flip.hpp>
#include <sisl/options/options.h>
#include "hb_internal.hpp"
#include <volume/volume.hpp>
#include <volume/volume_chunk_selector.hpp>
#include "test_common.hpp"

#include <future>
#include <thread>
#include <atomic>
#include <algorithm>
#include <unordered_set>

SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTION_GROUP(test_volume_chunk_selector,
                  (num_vols, "", "num_vols", "number of volumes", ::cxxopts::value< uint32_t >()->default_value("2"),
                   "number"));

SISL_OPTIONS_ENABLE(logging, test_common_setup, test_volume_chunk_selector)
SISL_LOGGING_DECL(test_volume_chunk_selector)

using namespace homeblocks;

namespace homestore {
// This part take from HomeObject test code.
// This is a fake implementation of Chunk/VChunk to avoid linking with homestore instance.
// if redefinition error was seen while building this file, any api being added in homestore::Chunk/VChunk, also needs
// to add one here to avoid redefinition error.
// Compiler will get confused if symbol can't be resolved locally(e.g. in this file), and will try to find it in
// homestore library which will cause redefine error.
class Chunk : public std::enable_shared_from_this< Chunk > {
public:
    uint32_t available_blks() const { return m_available_blks; }

    void set_available_blks(uint32_t available_blks) { m_available_blks = available_blks; }

    uint32_t get_defrag_nblks() const { return m_defrag_nblks; }

    void set_defrag_nblks(uint32_t defrag_nblks) { m_defrag_nblks = defrag_nblks; }

    uint32_t get_pdev_id() const { return m_pdev_id; }

    void set_pdev_id(uint32_t pdev_id) { m_pdev_id = pdev_id; }

    uint16_t get_chunk_id() const { return m_chunk_id; }

    blk_num_t get_total_blks() const { return m_total_blks; }
    void set_chunk_id(uint16_t chunk_id) { m_chunk_id = chunk_id; }
    uint64_t size() const { return 16 * Ki; }

    Chunk(uint32_t pdev_id, uint16_t chunk_id, uint32_t available_blks = 0, uint32_t defrag_nblks = 0) {
        m_available_blks = available_blks == 0 ? size() / 4096 : available_blks;
        m_total_blks = m_available_blks;
        m_pdev_id = pdev_id;
        m_chunk_id = chunk_id;
        m_defrag_nblks = defrag_nblks;
    }

    uint32_t m_available_blks;
    uint32_t m_total_blks;
    uint32_t m_pdev_id;
    uint16_t m_chunk_id;
    uint32_t m_defrag_nblks;
};

VChunk::VChunk(cshared< Chunk >& chunk) : m_internal_chunk(chunk) {}

void VChunk::set_user_private(const sisl::blob& data) {}

const uint8_t* VChunk::get_user_private() const { return nullptr; };

blk_num_t VChunk::available_blks() const { return m_internal_chunk->available_blks(); }

blk_num_t VChunk::get_defrag_nblks() const { return m_internal_chunk->get_defrag_nblks(); }

uint32_t VChunk::get_pdev_id() const { return m_internal_chunk->get_pdev_id(); }

uint16_t VChunk::get_chunk_id() const { return m_internal_chunk->get_chunk_id(); }

blk_num_t VChunk::get_total_blks() const { return m_internal_chunk->get_total_blks(); }

uint64_t VChunk::size() const { return m_internal_chunk->size(); }

void VChunk::reset() {}

cshared< Chunk > VChunk::get_internal_chunk() const { return m_internal_chunk; }

// void VChunk::reset_block_allocator() {}

} // namespace homestore

template < typename Pred >
void wait_until(Pred&& pred, std::chrono::milliseconds timeout = std::chrono::milliseconds{5000},
                std::chrono::milliseconds poll = std::chrono::milliseconds{50}) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(poll);
    }
    ASSERT_TRUE(pred());
}

class ChunkSelectorTest : public ::testing::Test {
public:
    ChunkSelectorTest() {}
    void SetUp() override {}

    void TearDown() override {}

    auto add_chunks_per_pdev(shared< VolumeChunkSelector > chunk_sel, uint32_t pdevs, uint32_t num_chunks_per_pdev) {
        std::vector< shared< homestore::Chunk > > chunks;
        for (uint32_t i = 0, chunk_id = 0; i < pdevs; i++) {
            for (uint32_t j = 0; j < num_chunks_per_pdev; j++) {
                auto chunk = std::make_shared< homestore::Chunk >(i /*pdev*/, chunk_id++);
                chunk_sel->add_chunk(chunk);
                chunks.emplace_back(chunk);
            }
        }
        return chunks;
    }

#ifdef _PRERELEASE
    void set_flip_point(const std::string flip_name) {
        flip::FlipCondition null_cond;
        flip::FlipFrequency freq;
        freq.set_count(2);
        freq.set_percent(100);
        m_fc.inject_noreturn_flip(flip_name, {null_cond}, freq);
        LOGI("Flip {} set", flip_name);
    }
#endif

private:
#ifdef _PRERELEASE
    flip::FlipClient m_fc{iomgr_flip::instance()};
#endif
};

TEST_F(ChunkSelectorTest, AllocateReleaseChunksTest) {
    auto chunk_sel =
        std::make_shared< VolumeChunkSelector >("test", [this](uint64_t, const std::vector< chunk_num_t >&) {});

    uint32_t pdevs = 3, num_chunks_per_pdev = 20, pdev_id;
    // Add chunks to chunk selector, each chunk is 16KB, so 3 * 20 * 16KB = 960KB
    add_chunks_per_pdev(chunk_sel, pdevs, num_chunks_per_pdev);
    auto init_num_free_chunks = chunk_sel->num_free_chunks();

    // Allocate chunks for multiple volumes to simulate volume create.
    auto vol1_chunks = chunk_sel->allocate_init_chunks(0 /* ordinal */, 180 * Ki, pdev_id);
    RELEASE_ASSERT(!vol1_chunks.empty(), "no chunks");

    // Since we lazily allocate chunks, we can allocate more than sum
    // of all volume sizes. Make sure same chunks not allocated to multiple volumes.
    for (uint32_t i = 1; i < 5; i++) {
        auto vol_chunks = chunk_sel->allocate_init_chunks(i /* ordinal */, 180 * Ki, pdev_id);
        RELEASE_ASSERT(!vol_chunks.empty(), "no chunks");
        bool noDuplicates = std::none_of(vol1_chunks.begin(), vol1_chunks.end(), [&](int elem) {
            return std::unordered_set< int >(vol_chunks.begin(), vol_chunks.end()).count(elem) > 0;
        });
        RELEASE_ASSERT(noDuplicates, "Duplicate chunks allocated across volumes");
    }

    // Release all chunks to simulate volume destroy.
    for (uint32_t i = 0; i < 5; i++) {
        chunk_sel->release_chunks(i /* ordinal */);
    }

    wait_until([&] { return chunk_sel->num_free_chunks() == init_num_free_chunks; }, std::chrono::seconds{10},
               std::chrono::milliseconds{100});

    // All the chunks will be free.
    RELEASE_ASSERT_EQ(init_num_free_chunks, chunk_sel->num_free_chunks(), "num free chunks mismatch");
}

TEST_F(ChunkSelectorTest, SelectChunksTest) {
    auto chunk_sel =
        std::make_shared< VolumeChunkSelector >("test", [this](uint64_t, const std::vector< chunk_num_t >&) {});
    uint32_t pdevs = 3, num_chunks_per_pdev = 20, pdev_id;
    // Add chunks to chunk selector, each chunk is 16KB, so 3 * 20 * 16KB = 960KB
    add_chunks_per_pdev(chunk_sel, pdevs, num_chunks_per_pdev);

    // Create volume, get its active chunk list, do select_chunk multiple times.
    // Make sure every time chunk is selected in round robin fashion from the active ones.
    auto vol1_chunks = chunk_sel->allocate_init_chunks(0 /* ordinal */, 180 * Ki, pdev_id);
    RELEASE_ASSERT(!vol1_chunks.empty(), "no chunks");

    // Get the active chunks and make sure its round robin in selection.
    auto active_chunks = chunk_sel->get_chunks(0);
    std::vector< homestore::chunk_num_t > active_chunk_ids;
    for (auto& chunk : active_chunks) {
        active_chunk_ids.emplace_back(chunk->get_chunk_id());
    }
    for (int i = 0; i < 20; i++) {
        homestore::blk_alloc_hints hints;
        hints.application_hint = 0;
        auto chunk = chunk_sel->select_chunk(1 /* nblks */, hints);
        RELEASE_ASSERT(chunk, "Chunk not available");
        RELEASE_ASSERT_EQ(chunk->get_chunk_id(), active_chunk_ids[i % active_chunk_ids.size()], "invalid chunk");
    }
}

#ifdef _PRERELEASE
TEST_F(ChunkSelectorTest, ResizeNumChunksTest) {
    auto latch = std::make_shared< std::latch >(1);
    auto chunk_sel = std::make_shared< VolumeChunkSelector >(
        "test", [latch, this](uint64_t vol_ord, const std::vector< chunk_num_t >& chunk_ids) {
            RELEASE_ASSERT(!chunk_ids.empty(), "Empty chunk ids");
            latch->count_down();
        });

    // Set the flip point to trigger resize op.
    set_flip_point("vol_num_chunks_force_resize_op");

    // Add chunks to chunk selector, each chunk is 16KB, so 3 * 20 * 16KB = 960KB
    uint32_t pdevs = 3, num_chunks_per_pdev = 20, pdev_id;
    add_chunks_per_pdev(chunk_sel, pdevs, num_chunks_per_pdev);

    auto initial_chunk_ids = chunk_sel->allocate_init_chunks(0 /* ordinal */, 180 * Ki, pdev_id);
    RELEASE_ASSERT(!initial_chunk_ids.empty(), "no chunks");
    auto initial_chunks = chunk_sel->get_chunks(0);

    // This will trigger resize op where additional chunks are created.
    // We wait till we get the callback notification with new chunk id's
    homestore::blk_alloc_hints hints;
    hints.application_hint = 0;
    auto chunk = chunk_sel->select_chunk(1 /* nblks */, hints);
    RELEASE_ASSERT(chunk, "Chunk not available");
    latch->wait();

    auto resized_chunks = chunk_sel->get_chunks(0);
    RELEASE_ASSERT_GT(resized_chunks.size(), initial_chunks.size(), "Resize op failed");
}
#endif

TEST_F(ChunkSelectorTest, RecoverChunksTest) {
    auto chunk_sel =
        std::make_shared< VolumeChunkSelector >("test", [this](uint64_t, const std::vector< chunk_num_t >&) {});
    uint32_t pdevs = 3, num_chunks_per_pdev = 20;
    // Add chunks to chunk selector, each chunk is 16KB, so 3 * 20 * 16KB = 960KB
    add_chunks_per_pdev(chunk_sel, pdevs, num_chunks_per_pdev);

    std::vector< homestore::chunk_num_t > chunk_ids;
    for (uint32_t i = 0; i < 10; i++) {
        chunk_ids.push_back(i);
    }

    // Simulate a recovery.
    chunk_sel->recover_chunks(0 /* ordinal*/, 0 /*pdev */, 180 * Ki, chunk_ids);
    homestore::blk_alloc_hints hints;
    hints.application_hint = 0;
    auto chunk = chunk_sel->select_chunk(1 /* nblks */, hints);
    RELEASE_ASSERT(chunk, "Chunk not available");
}

TEST_F(ChunkSelectorTest, ConcurrentAllocateSameVolumeTest) {
    auto chunk_sel =
        std::make_shared< VolumeChunkSelector >("test", [this](uint64_t, const std::vector< chunk_num_t >&) {});

    add_chunks_per_pdev(chunk_sel, 2 /*pdevs*/, 10 /*chunks per pdev*/);

    std::vector< chunk_num_t > r1, r2;
    uint32_t pdev1{UINT32_MAX}, pdev2{UINT32_MAX};
    std::atomic< bool > go{false};

    auto worker = [&](std::vector< chunk_num_t >& out, uint32_t& pdev) {
        while (!go.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        out = chunk_sel->allocate_init_chunks(0 /* same volume */, 64 * Ki, pdev);
    };

    std::thread t1(worker, std::ref(r1), std::ref(pdev1));
    std::thread t2(worker, std::ref(r2), std::ref(pdev2));

    go.store(true, std::memory_order_release);
    t1.join();
    t2.join();

    ASSERT_FALSE(r1.empty());
    ASSERT_FALSE(r2.empty());

    std::sort(r1.begin(), r1.end());
    std::sort(r2.begin(), r2.end());

    EXPECT_EQ(r1, r2);
    EXPECT_EQ(pdev1, pdev2);

    auto chunks = chunk_sel->get_chunks(0);
    ASSERT_EQ(chunks.size(), r1.size());

    std::unordered_set< chunk_num_t > uniq{r1.begin(), r1.end()};
    EXPECT_EQ(uniq.size(), r1.size());
}

TEST_F(ChunkSelectorTest, SelectChunkReturnsNullWhenOutOfSpaceTest) {
    auto chunk_sel =
        std::make_shared< VolumeChunkSelector >("test", [this](uint64_t, const std::vector< chunk_num_t >&) {});

    // Single chunk volume, but no space in the chunk
    auto chunk = std::make_shared< homestore::Chunk >(0 /*pdev*/, 0 /*chunk_id*/);
    chunk->set_available_blks(0);
    chunk_sel->add_chunk(chunk);

    uint32_t pdev_id{UINT32_MAX};
    auto ids = chunk_sel->allocate_init_chunks(0 /* ordinal */, 8 * Ki /* <= one chunk */, pdev_id, false);
    ASSERT_FALSE(ids.empty());

    homestore::blk_alloc_hints hints;
    hints.application_hint = 0;

    auto start = std::chrono::steady_clock::now();
    auto out = chunk_sel->select_chunk(1 /* nblks */, hints);
    auto dur = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(out, nullptr);
    EXPECT_LT(dur, std::chrono::seconds(2));
}

TEST_F(ChunkSelectorTest, ConcurrentSelectAndReleaseTest) {
    auto chunk_sel =
        std::make_shared< VolumeChunkSelector >("test", [this](uint64_t, const std::vector< chunk_num_t >&) {});

    add_chunks_per_pdev(chunk_sel, 2 /*pdevs*/, 10 /*chunks per pdev*/);
    const auto total_free_before_alloc = chunk_sel->num_free_chunks();

    uint32_t pdev_id{UINT32_MAX};
    auto ids = chunk_sel->allocate_init_chunks(0 /* ordinal */, 64 * Ki, pdev_id);
    ASSERT_FALSE(ids.empty());

    homestore::blk_alloc_hints hints;
    hints.application_hint = 0;

    std::atomic< bool > done{false};
    std::atomic< uint32_t > select_success{0};
    std::atomic< uint32_t > select_null{0};

    std::thread selector([&] {
        while (!done.load(std::memory_order_acquire)) {
            auto chunk = chunk_sel->select_chunk(1 /* nblks */, hints);
            if (chunk) {
                ++select_success;
            } else {
                ++select_null;
                break;
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::thread releaser([&] {
        chunk_sel->release_chunks(0 /* ordinal */);
        done.store(true, std::memory_order_release);
    });

    releaser.join();
    selector.join();

    wait_until([&] { return chunk_sel->num_free_chunks() == total_free_before_alloc; },
               std::chrono::milliseconds{10000});

    EXPECT_EQ(chunk_sel->num_free_chunks(), total_free_before_alloc);
    EXPECT_TRUE(chunk_sel->get_chunks(0).empty());
    EXPECT_GT(select_success.load() + select_null.load(), 0u);
}

TEST_F(ChunkSelectorTest, ResizeCallbackBlockedThenReleaseTest) {
    std::promise< void > cb_entered_promise;
    auto cb_entered = cb_entered_promise.get_future();

    std::promise< void > allow_cb_exit_promise;
    auto allow_cb_exit = allow_cb_exit_promise.get_future();

    auto chunk_sel = std::make_shared< VolumeChunkSelector >(
        "test", [&, this](uint64_t, const std::vector< chunk_num_t >& chunk_ids) {
            EXPECT_FALSE(chunk_ids.empty());
            cb_entered_promise.set_value();
            allow_cb_exit.wait();
        });

    add_chunks_per_pdev(chunk_sel, 1 /*pdevs*/, 10 /*chunks per pdev*/);
    const auto total_free_before_alloc = chunk_sel->num_free_chunks();

    uint32_t pdev_id{UINT32_MAX};
    auto ids = chunk_sel->allocate_init_chunks(0 /* ordinal */, 64 * Ki /* max_num_chunks > 1 */, pdev_id, true);
    ASSERT_FALSE(ids.empty());

    // Exhaust currently active chunks so select_chunk() must try resize
    auto active_chunks = chunk_sel->get_chunks(0);
    ASSERT_FALSE(active_chunks.empty());
    for (auto& c : active_chunks) {
        c->get_internal_chunk()->set_available_blks(0);
    }

    homestore::blk_alloc_hints hints;
    hints.application_hint = 0;

    std::thread selector([&] { (void)chunk_sel->select_chunk(1 /* nblks */, hints); });

    ASSERT_EQ(cb_entered.wait_for(std::chrono::seconds(2)), std::future_status::ready);

    std::thread releaser([&] { chunk_sel->release_chunks(0 /* ordinal */); });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    allow_cb_exit_promise.set_value();

    selector.join();
    releaser.join();

    wait_until([&] { return chunk_sel->num_free_chunks() == total_free_before_alloc; },
               std::chrono::milliseconds{10000});

    EXPECT_EQ(chunk_sel->num_free_chunks(), total_free_before_alloc);
    EXPECT_TRUE(chunk_sel->get_chunks(0).empty());
}

TEST_F(ChunkSelectorTest, RecoverReleaseReallocateTest) {
    auto chunk_sel =
        std::make_shared< VolumeChunkSelector >("test", [this](uint64_t, const std::vector< chunk_num_t >&) {});

    add_chunks_per_pdev(chunk_sel, 1 /*pdevs*/, 10 /*chunks per pdev*/);
    const auto total_free = chunk_sel->num_free_chunks();

    std::vector< homestore::chunk_num_t > recovered_ids{0, 1, 2};
    ASSERT_TRUE(chunk_sel->recover_chunks(0 /* ordinal */, 0 /* pdev */, 48 * Ki, recovered_ids));

    auto recovered_chunks = chunk_sel->get_chunks(0);
    ASSERT_EQ(recovered_chunks.size(), recovered_ids.size());

    chunk_sel->release_chunks(0 /* ordinal */);

    wait_until([&] { return chunk_sel->num_free_chunks() == total_free; }, std::chrono::milliseconds{10000});

    uint32_t pdev_id{UINT32_MAX};
    auto new_ids = chunk_sel->allocate_init_chunks(1 /* new ordinal */, 48 * Ki, pdev_id, true);
    ASSERT_FALSE(new_ids.empty());
    EXPECT_EQ(pdev_id, 0u);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, test_common_setup, test_volume_chunk_selector);
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    sisl::logging::SetLogger("test_volume_chunk_selector");
    sisl::logging::SetLogPattern("[%D %T%z] [%^%L%$] [%n] [%t] %v");
    ioenvironment.with_iomgr(iomgr::iomgr_params{.num_threads = 4});

    auto ret = RUN_ALL_TESTS();
    iomanager.stop();
    return ret;
}
