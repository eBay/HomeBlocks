// The CRAFT snapshot's wire format (craft_snapshot.hpp): id packing, the sealed header, every payload's round trip,
// and that decode() refuses anything a receiver must ask for again.

#include <cstring>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <functional>
#include <map>

#include <homestore/crc.hpp>

#include "craft/craft_snapshot.hpp"
#include "craft/craft_snapshot_reader.hpp"
#include "hb_internal.hpp"

SISL_LOGGING_DEF(HOMEBLOCKS_LOG_MODS)
SISL_LOGGING_INIT(HOMEBLOCKS_LOG_MODS)
SISL_OPTIONS_ENABLE(logging)

using namespace homeblocks::craft_snapshot;

namespace {

control_tuple sample_tuple() {
    return control_tuple{.session_term = 7,
                         .client_token = 0xfeed,
                         .fence_term = 0,
                         .aborted_upto = 0,
                         .data_watermark = 100,
                         .sync_watermark = 110,
                         .empty_lsns = {101, 105}};
}

snapshot_meta sample_meta() {
    auto const t = sample_tuple();
    return snapshot_meta{.raft_idx = 42,
                         .digest = tuple_digest(t),
                         .tuple = t,
                         .volume_def = {1, 2, 3, 4},
                         .lba_size = 4096,
                         .total_lbas = 1000,
                         .region_lbas = 256,
                         .region_count = region_count(1000, 256)};
}

region_batch sample_batch() {
    region_batch b{.region = 2, .batch = 1, .start = 256, .end = 300, .last_in_region = true, .lba_size = 8};
    b.lbas = {{.lba = 256, .csum = 0x1111}, {.lba = 270, .csum = 0x2222}};
    b.data.resize(b.lbas.size() * b.lba_size);
    for (size_t i = 0; i < b.data.size(); ++i)
        b.data[i] = static_cast< uint8_t >(i);
    return b;
}

template < typename T >
T decode_as(std::vector< uint8_t > const& buf) {
    auto const obj = decode(buf.data(), buf.size());
    EXPECT_TRUE(obj.has_value());
    EXPECT_TRUE(obj && std::holds_alternative< T >(*obj));
    return obj ? std::get< T >(*obj) : T{};
}

} // namespace

TEST(CraftSnapshotFormat, ObjectIds) {
    EXPECT_EQ(obj_id{}.pack(), k_app_bit) << "the first application id HomeStore hands over is 1 << 63, i.e. 0 | 0";
    auto const id = obj_id{.region = 3, .batch = 9}.pack();
    ASSERT_TRUE(id);
    EXPECT_EQ(obj_id::unpack(*id), (obj_id{.region = 3, .batch = 9}));
    EXPECT_FALSE((obj_id{.region = k_max_region, .batch = k_max_batch}.pack())) << "all ones is LAST_OBJ_ID";
    EXPECT_TRUE((obj_id{.region = k_max_region, .batch = k_max_batch - 1}.pack()));
    EXPECT_FALSE((obj_id{.region = k_max_region + 1}.pack()));
    EXPECT_FALSE((obj_id{.batch = k_max_batch + 1}.pack()));
    EXPECT_FALSE(obj_id::unpack(5)) << "bit 63 clear: HomeStore's own message";
    EXPECT_FALSE(obj_id::unpack(LAST_OBJ_ID));
}

TEST(CraftSnapshotFormat, MetaRoundTrip) {
    auto const m = sample_meta();
    EXPECT_EQ(decode_as< snapshot_meta >(encode(m)), m);
}

TEST(CraftSnapshotFormat, BatchAndTrailerRoundTrip) {
    auto const b = sample_batch();
    EXPECT_EQ(decode_as< region_batch >(encode(b)), b);
    region_batch empty{.region = 1, .batch = 1, .start = 0, .end = 256, .last_in_region = true, .lba_size = 4096};
    EXPECT_EQ(decode_as< region_batch >(encode(empty)), empty) << "a region with no mapped LBA is all holes";
    EXPECT_EQ(decode_as< snapshot_trailer >(encode(snapshot_trailer{.w_hi = 123})), snapshot_trailer{.w_hi = 123});
}

TEST(CraftSnapshotFormat, DigestTracksTheTuple) {
    auto t = sample_tuple();
    auto const d = tuple_digest(t);
    EXPECT_EQ(tuple_digest(t), d) << "deterministic";
    t.data_watermark = 99;
    EXPECT_NE(tuple_digest(t), d) << "same L, a different tuple: a different transfer";
}

TEST(CraftSnapshotFormat, CorruptionIsRefused) {
    auto buf = encode(sample_batch());
    for (size_t pos : {size_t{0}, sizeof(object_header) - 1, sizeof(object_header) + 3, buf.size() - 1}) {
        auto bad = buf;
        bad[pos] ^= 0x40;
        EXPECT_FALSE(decode(bad.data(), bad.size())) << "byte " << pos;
    }
    EXPECT_FALSE(decode(buf.data(), buf.size() - 1)) << "truncated";
    EXPECT_FALSE(decode(buf.data(), sizeof(object_header) - 1));
    EXPECT_FALSE(decode(nullptr, 0));
}

TEST(CraftSnapshotFormat, InvalidPayloadsAreRefused) {
    auto m = sample_meta();
    m.digest ^= 1;
    EXPECT_FALSE(decode(encode(m).data(), encode(m).size())) << "digest doesn't match the tuple";

    m = sample_meta();
    m.tuple.empty_lsns = {100}; // at W: not inside (W, S]
    m.digest = tuple_digest(m.tuple);
    auto buf = encode(m);
    EXPECT_FALSE(decode(buf.data(), buf.size()));

    m = sample_meta();
    m.region_count += 1;
    buf = encode(m);
    EXPECT_FALSE(decode(buf.data(), buf.size())) << "region count doesn't match the geometry";

    auto b = sample_batch();
    std::swap(b.lbas[0], b.lbas[1]);
    buf = encode(b);
    EXPECT_FALSE(decode(buf.data(), buf.size())) << "LBAs not ascending";

    b = sample_batch();
    b.lbas[1].lba = b.end;
    buf = encode(b);
    EXPECT_FALSE(decode(buf.data(), buf.size())) << "an LBA outside its batch";

    b = sample_batch();
    b.region = 0;
    buf = encode(b);
    EXPECT_FALSE(decode(buf.data(), buf.size())) << "region 0 is the meta object";
}

TEST(CraftSnapshotFormat, RegionGeometry) {
    EXPECT_EQ(region_count(1000, 256), 4u);
    EXPECT_EQ(region_count(1024, 256), 4u);
    EXPECT_EQ(region_count(0, 256), 0u);
    EXPECT_EQ(region_range(1, 1000, 256), (std::pair< lba_t, lba_t >{0, 256}));
    EXPECT_EQ(region_range(4, 1000, 256), (std::pair< lba_t, lba_t >{768, 1000})) << "the last region is shorter";
    EXPECT_EQ(region_range(5, 1000, 256), (std::pair< lba_t, lba_t >{0, 0}));
    EXPECT_EQ(region_range(0, 1000, 256), (std::pair< lba_t, lba_t >{0, 0}));
}

// ── the donor's reader ──────────────────────────────────────────────────────

namespace {

constexpr homestore::csum_t k_seed = 0x8005;
constexpr uint32_t k_lba = 8;

// A volume in memory: blocks on chunk 1, an index of lba -> {block, checksum}. Hooks let a test change the volume
// while a read is in flight.
struct fake_volume : snapshot_source {
    uint64_t lbas{0};
    int64_t append_lsn{0};
    std::map< homestore::blk_num_t, std::vector< uint8_t > > blocks;
    std::map< lba_t, index_entry > index;
    homestore::blk_num_t next_blk{100};
    int block_reads{0};
    bool fail_reads{false};
    std::function< void() > before_read{};

    uint32_t lba_size() const override { return k_lba; }
    uint64_t total_lbas() const override { return lbas; }
    int64_t last_append_lsn() const override { return append_lsn; }
    std::optional< std::vector< index_entry > > read_index(lba_t start, lba_t end) override {
        std::vector< index_entry > out;
        for (auto it = index.lower_bound(start); it != index.end() && it->first < end; ++it)
            out.push_back(it->second);
        return out;
    }
    bool read_blocks(homestore::blk_id const& b, uint8_t* buf) override {
        ++block_reads;
        if (fail_reads) return false;
        if (auto hook = std::exchange(before_read, nullptr)) hook();
        for (homestore::blk_count_t i = 0; i < b.blk_count(); ++i) {
            auto const& blk = blocks.at(b.blk_num() + i);
            std::copy(blk.begin(), blk.end(), buf + i * k_lba);
        }
        return true;
    }

    // Maps lba to a new block holding `fill`.
    void write(lba_t lba, uint8_t fill) {
        std::vector< uint8_t > d(k_lba, fill);
        auto const blk = next_blk++;
        blocks[blk] = d;
        index[lba] = index_entry{
            .lba = lba, .blkid = homestore::blk_id{blk, 1, 1}, .csum = crc16_t10dif(k_seed, d.data(), d.size())};
    }
};

snapshot_meta meta_for(fake_volume const& v, uint64_t region_lbas) {
    auto const t = sample_tuple();
    return snapshot_meta{.raft_idx = 9,
                         .digest = tuple_digest(t),
                         .tuple = t,
                         .lba_size = k_lba,
                         .total_lbas = v.lbas,
                         .region_lbas = region_lbas,
                         .region_count = region_count(v.lbas, region_lbas)};
}

uint64_t id(uint64_t r, uint32_t b) { return *obj_id{.region = r, .batch = b}.pack(); }

// Walks a whole transfer the way a receiver with no prior state does; returns the objects in order.
std::vector< snapshot_object > walk(snapshot_reader& rd, size_t max_objects = 1000) {
    std::vector< snapshot_object > out;
    uint64_t next = id(0, 0);
    while (out.size() < max_objects) {
        auto const r = rd.serve(next);
        if (r.st == snapshot_reader::status::last) break;
        EXPECT_EQ(r.st, snapshot_reader::status::ok);
        if (r.st != snapshot_reader::status::ok) break;
        auto obj = decode(r.blob.data(), r.blob.size());
        EXPECT_TRUE(obj);
        if (!obj) break;
        out.push_back(*obj);
        if (std::holds_alternative< snapshot_meta >(*obj)) {
            auto const& m = std::get< snapshot_meta >(*obj);
            next = m.region_count > 0 ? id(1, 1) : id(1, 0);
        } else if (std::holds_alternative< region_batch >(*obj)) {
            auto const& b = std::get< region_batch >(*obj);
            auto const regions = std::get< snapshot_meta >(out.front()).region_count;
            next = !b.last_in_region ? id(b.region, b.batch + 1)
                                     : (b.region < regions ? id(b.region + 1, 1) : id(regions + 1, 0));
        } else {
            next = LAST_OBJ_ID;
        }
    }
    return out;
}

// The volume a transfer describes: lba -> data of the mapped LBAs.
std::map< lba_t, std::vector< uint8_t > > rebuilt(std::vector< snapshot_object > const& objs) {
    std::map< lba_t, std::vector< uint8_t > > vol;
    for (auto const& o : objs) {
        if (!std::holds_alternative< region_batch >(o)) continue;
        auto const& b = std::get< region_batch >(o);
        for (lba_t l = b.start; l < b.end; ++l)
            vol.erase(l); // the batch is the range exactly
        for (size_t i = 0; i < b.lbas.size(); ++i)
            vol[b.lbas[i].lba] = std::vector< uint8_t >(b.data.begin() + i * k_lba, b.data.begin() + (i + 1) * k_lba);
    }
    return vol;
}

std::map< lba_t, std::vector< uint8_t > > contents(fake_volume const& v) {
    std::map< lba_t, std::vector< uint8_t > > out;
    for (auto const& [lba, e] : v.index)
        out[lba] = v.blocks.at(e.blkid.blk_num());
    return out;
}

} // namespace

TEST(CraftSnapshotReader, AWholeTransferCarriesTheVolume) {
    fake_volume v;
    v.lbas = 40;
    v.append_lsn = 77;
    for (lba_t l : {0, 1, 2, 3, 9, 15, 16, 17, 31, 39})
        v.write(l, static_cast< uint8_t >(l + 1));
    snapshot_reader rd{meta_for(v, 16), v, 3 * k_lba, k_seed}; // regions of 16 LBAs, batches of 3 LBAs
    auto const objs = walk(rd);
    ASSERT_GE(objs.size(), 3u);
    EXPECT_EQ(std::get< snapshot_meta >(objs.front()), meta_for(v, 16));
    EXPECT_EQ(std::get< snapshot_trailer >(objs.back()).w_hi, 77);
    for (auto const& o : objs) {
        if (std::holds_alternative< region_batch >(o)) { EXPECT_LE(std::get< region_batch >(o).lbas.size(), 3u); }
    }
    EXPECT_EQ(rebuilt(objs), contents(v));
    EXPECT_LT(v.block_reads, 10) << "contiguous blocks are read as one run";
}

TEST(CraftSnapshotReader, AnEmptyVolumeIsMetaThenTrailer) {
    fake_volume v;
    auto const meta = meta_for(v, 16);
    snapshot_reader rd{meta, v, 64, k_seed};
    auto const objs = walk(rd);
    ASSERT_EQ(objs.size(), 2u);
    EXPECT_TRUE(std::holds_alternative< snapshot_trailer >(objs.back()));
}

TEST(CraftSnapshotReader, AResendRebuildsTheSameRange) {
    fake_volume v;
    v.lbas = 16;
    for (lba_t l = 0; l < 10; ++l)
        v.write(l, 0x10);
    snapshot_reader rd{meta_for(v, 16), v, 4 * k_lba, k_seed};
    ASSERT_EQ(rd.serve(id(0, 0)).st, snapshot_reader::status::ok);
    auto const first = rd.serve(id(1, 1));
    auto const second = rd.serve(id(1, 2));
    v.write(5, 0x55); // lands in batch 2's range
    auto const again = rd.serve(id(1, 2));
    ASSERT_EQ(again.st, snapshot_reader::status::ok);
    EXPECT_EQ(rd.resends(), 1u);
    auto const b2 = std::get< region_batch >(*decode(second.blob.data(), second.blob.size()));
    auto const b2r = std::get< region_batch >(*decode(again.blob.data(), again.blob.size()));
    EXPECT_EQ(b2.start, b2r.start);
    EXPECT_EQ(b2.end, b2r.end);
    EXPECT_NE(b2.data, b2r.data) << "the range is read again, live";
    (void)first;
}

TEST(CraftSnapshotReader, CursorRules) {
    fake_volume v;
    v.lbas = 64;
    for (lba_t l = 0; l < 64; l += 2)
        v.write(l, 1);
    snapshot_reader rd{meta_for(v, 16), v, 2 * k_lba, k_seed};
    EXPECT_EQ(rd.serve(id(0, 0)).st, snapshot_reader::status::ok);
    EXPECT_EQ(rd.serve(id(1, 3)).st, snapshot_reader::status::bad_cursor) << "a batch whose start isn't known yet";
    EXPECT_EQ(rd.serve(id(3, 1)).st, snapshot_reader::status::ok) << "a receiver resuming at region 3";
    EXPECT_EQ(rd.serve(id(3, 2)).st, snapshot_reader::status::ok);
    EXPECT_EQ(rd.serve(id(2, 1)).st, snapshot_reader::status::ok) << "or restarting an earlier region";
    EXPECT_EQ(rd.serve(id(9, 1)).st, snapshot_reader::status::bad_cursor) << "no region 9";
    EXPECT_EQ(rd.serve(5).st, snapshot_reader::status::bad_cursor) << "not an application id";
    EXPECT_EQ(rd.serve(id(0, 0)).st, snapshot_reader::status::ok) << "0 | 0 always starts over";
    EXPECT_EQ(rd.serve(LAST_OBJ_ID).st, snapshot_reader::status::last);
}

TEST(CraftSnapshotReader, ABlockReusedMidReadIsReadAgain) {
    fake_volume v;
    v.lbas = 16;
    v.write(3, 0x33);
    snapshot_reader rd{meta_for(v, 16), v, 64, k_seed};
    // While the batch's read is in flight, lba 3 is overwritten and its old block reused for other data.
    v.before_read = [&v] {
        auto const old = v.index.at(3).blkid.blk_num();
        v.write(3, 0x44);
        v.blocks[old].assign(k_lba, 0xEE);
    };
    auto const objs = walk(rd);
    EXPECT_GE(rd.checksum_retries(), 1u);
    EXPECT_EQ(rebuilt(objs), contents(v)) << "the newer version is sent, never the reused block";
}

TEST(CraftSnapshotReader, AnLbaUnmappedMidReadBecomesAHole) {
    fake_volume v;
    v.lbas = 16;
    v.write(3, 0x33);
    v.write(4, 0x34);
    snapshot_reader rd{meta_for(v, 16), v, 64, k_seed};
    v.before_read = [&v] {
        v.blocks[v.index.at(3).blkid.blk_num()].assign(k_lba, 0); // freed and zeroed
        v.index.erase(3);
    };
    auto const objs = walk(rd);
    EXPECT_EQ(rebuilt(objs), contents(v));
}

TEST(CraftSnapshotReader, ReadFailuresAndLastingCorruptionAreErrors) {
    fake_volume v;
    v.lbas = 16;
    v.write(1, 0x11);
    snapshot_reader rd{meta_for(v, 16), v, 64, k_seed};
    ASSERT_EQ(rd.serve(id(0, 0)).st, snapshot_reader::status::ok);
    v.fail_reads = true;
    EXPECT_EQ(rd.serve(id(1, 1)).st, snapshot_reader::status::error);
    v.fail_reads = false;
    EXPECT_EQ(rd.serve(id(1, 1)).st, snapshot_reader::status::ok) << "the same id asked again succeeds";

    v.blocks[v.index.at(1).blkid.blk_num()][0] ^= 0xFF; // corrupt on disk, the index unchanged
    EXPECT_EQ(rd.serve(id(1, 1)).st, snapshot_reader::status::error);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    return RUN_ALL_TESTS();
}
