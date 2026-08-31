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

// homeblocks::JournalSlot (craft_repl_dev.hpp) is a deliberate independent copy of
// craft::JournalSlot (craft_client's include/craft/peer.hpp) -- see the comment at
// craft_repl_dev.hpp:33-44 for why they aren't the same type. Nothing pins the two
// definitions together, so this TU is that pin:
//
//   1. Per-field static_asserts (offsetof + type) below -- catch a field reorder or a
//      same-size type swap that a plain sizeof() comparison would miss.
//   2. Round-trip tests through craft_client's REAL peer-plane wire codec
//      (craft::peer::encode_fetch_data_rsp / decode_fetch_data_rsp) -- catch a codec-side
//      change that per-field struct compatibility alone wouldn't (e.g. a change to
//      slot_desc's wire layout in peer_codec.cpp).
//
// This is the only craft test binary that links craft_client::craft_client (not just
// craft_client::craft_types), since encode_fetch_data_rsp/decode_fetch_data_rsp live in
// peer_codec.cpp, part of that component.

#include <cstddef>
#include <cstring>
#include <sys/uio.h>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>
#include <craft/peer.hpp>
#include <sisl/fds/buffer.hpp>

#include "craft/craft_repl_dev.hpp"

namespace homeblocks {
namespace {

// ── compile-time layout canary ────────────────────────────────────────────────
//
// No offsetof here: JournalSlot contains a sisl::sg_list (wraps a std::vector), and
// standard-layout requires every non-static data member's type to itself be standard-layout
// (recursively) -- a vector-backed member disqualifies the whole struct, making offsetof on it
// conditionally-supported (a hard error under this project's -Wpedantic -Werror). sizeof and
// decltype/is_same_v have no such restriction and still catch a field being added/removed/retyped.

static_assert(std::is_same_v< decltype(JournalSlot::lsn), decltype(craft::JournalSlot::lsn) >);
static_assert(std::is_same_v< decltype(JournalSlot::is_empty), decltype(craft::JournalSlot::is_empty) >);
static_assert(std::is_same_v< decltype(JournalSlot::all_zeros), decltype(craft::JournalSlot::all_zeros) >);
static_assert(std::is_same_v< decltype(JournalSlot::lba), decltype(craft::JournalSlot::lba) >);
static_assert(std::is_same_v< decltype(JournalSlot::len), decltype(craft::JournalSlot::len) >);
static_assert(sizeof(JournalSlot) == sizeof(craft::JournalSlot),
              "homeblocks::JournalSlot must stay layout-compatible with craft::JournalSlot "
              "(craft_client's include/craft/peer.hpp)");

// ── round-trip helpers ────────────────────────────────────────────────────────

craft::JournalSlot to_craft_slot(JournalSlot const& s) {
    craft::JournalSlot out;
    out.lsn = s.lsn;
    out.is_empty = s.is_empty;
    out.all_zeros = s.all_zeros;
    out.lba = s.lba;
    out.len = s.len;
    out.data = s.data;
    return out;
}

// decode_fetch_data_rsp expects one contiguous blob; encode_fetch_data_rsp splits header from
// per-slot data blobs for zero-copy sends. Concatenate them here (owned by the returned vector)
// to simulate what a real transport delivers into a single receive buffer.
std::vector< uint8_t > flatten(craft::peer::fetch_data_rsp_encoded const& enc) {
    std::vector< uint8_t > out(enc.header);
    for (auto const& b : enc.data_blobs) {
        out.insert(out.end(), b.cbytes(), b.cbytes() + b.size());
    }
    return out;
}

// decode_fetch_data_rsp's returned slots borrow their data iovecs from the input blob (see the
// buffer contract in peer.hpp), so the flattened backing buffer must outlive the decoded slots --
// bundle them together rather than returning the slots alone and dangling on return.
struct RoundTripResult {
    std::vector< uint8_t > buffer; // must outlive `slots`; their iovecs point into it
    std::vector< craft::JournalSlot > slots;
};

RoundTripResult round_trip(std::vector< JournalSlot > const& slots) {
    std::vector< craft::JournalSlot > craft_slots;
    craft_slots.reserve(slots.size());
    for (auto const& s : slots) {
        craft_slots.push_back(to_craft_slot(s));
    }
    auto encoded = craft::peer::encode_fetch_data_rsp(craft_slots);
    auto flat = flatten(encoded);
    // Non-owning view over `flat`, matching how peer.hpp's own blobs() wraps its buffers.
    sisl::io_blob blob{flat.data(), static_cast< uint32_t >(flat.size()), false};
    auto decoded = craft::peer::decode_fetch_data_rsp(blob);
    EXPECT_TRUE(decoded.has_value());
    return RoundTripResult{std::move(flat), decoded.value_or(std::vector< craft::JournalSlot >{})};
}

// ── behavioral tests ──────────────────────────────────────────────────────────

TEST(JournalSlotWireCompliance, RoundTripsDataSlot) {
    uint8_t payload[16];
    for (int i = 0; i < 16; ++i) {
        payload[i] = static_cast< uint8_t >(i);
    }

    JournalSlot slot;
    slot.lsn = 42;
    slot.lba = 100;
    slot.len = 4;
    slot.data.iovs.push_back(iovec{payload, sizeof(payload)});
    slot.data.size = sizeof(payload);

    auto rt = round_trip({slot});
    auto const& result = rt.slots;
    ASSERT_EQ(result.size(), 1u);
    auto const& d = result[0];
    EXPECT_EQ(d.lsn, slot.lsn);
    EXPECT_EQ(d.lba, slot.lba);
    EXPECT_EQ(d.len, slot.len);
    EXPECT_FALSE(d.is_empty);
    EXPECT_FALSE(d.all_zeros);
    ASSERT_EQ(d.data.size, sizeof(payload));
    ASSERT_EQ(d.data.iovs.size(), 1u);
    EXPECT_EQ(std::memcmp(d.data.iovs[0].iov_base, payload, sizeof(payload)), 0);
}

TEST(JournalSlotWireCompliance, RoundTripsEmptySlot) {
    JournalSlot slot;
    slot.lsn = 7;
    slot.is_empty = true;

    auto rt = round_trip({slot});
    auto const& result = rt.slots;
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].lsn, 7);
    EXPECT_TRUE(result[0].is_empty);
    EXPECT_EQ(result[0].data.size, 0u);
}

TEST(JournalSlotWireCompliance, RoundTripsZeroWriteSlot) {
    JournalSlot slot;
    slot.lsn = 9;
    slot.lba = 5;
    slot.len = 8;
    slot.all_zeros = true;

    auto rt = round_trip({slot});
    auto const& result = rt.slots;
    ASSERT_EQ(result.size(), 1u);
    EXPECT_TRUE(result[0].all_zeros);
    EXPECT_FALSE(result[0].is_empty);
    EXPECT_EQ(result[0].data.size, 0u);
}

TEST(JournalSlotWireCompliance, RoundTripsMixedBatch) {
    uint8_t payload[8];
    for (int i = 0; i < 8; ++i) {
        payload[i] = static_cast< uint8_t >(0xA0 + i);
    }

    JournalSlot data_slot;
    data_slot.lsn = 1;
    data_slot.lba = 2;
    data_slot.len = 2;
    data_slot.data.iovs.push_back(iovec{payload, sizeof(payload)});
    data_slot.data.size = sizeof(payload);

    JournalSlot zero_slot;
    zero_slot.lsn = 2;
    zero_slot.lba = 4;
    zero_slot.len = 2;
    zero_slot.all_zeros = true;

    JournalSlot empty_slot;
    empty_slot.lsn = 3;
    empty_slot.is_empty = true;

    auto rt = round_trip({data_slot, zero_slot, empty_slot});
    auto const& result = rt.slots;
    ASSERT_EQ(result.size(), 3u);

    EXPECT_EQ(result[0].lsn, 1);
    EXPECT_FALSE(result[0].is_empty);
    EXPECT_FALSE(result[0].all_zeros);
    ASSERT_EQ(result[0].data.iovs.size(), 1u);
    EXPECT_EQ(std::memcmp(result[0].data.iovs[0].iov_base, payload, sizeof(payload)), 0);

    EXPECT_EQ(result[1].lsn, 2);
    EXPECT_TRUE(result[1].all_zeros);
    EXPECT_EQ(result[1].data.size, 0u);

    EXPECT_EQ(result[2].lsn, 3);
    EXPECT_TRUE(result[2].is_empty);
    EXPECT_EQ(result[2].data.size, 0u);
}

} // namespace
} // namespace homeblocks

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}