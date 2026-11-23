#pragma once

// The CRAFT snapshot's wire format (docs/craft/resync.md, section 4.9).
//
// nuraft moves a snapshot as a sequence of objects the receiver pulls by a 64-bit id; HomeBlocks supplies their
// content through the RAFT listener's snapshot callbacks, the way HomeObject's ReplicationStateMachine does. The
// ids and the sealed header follow HomeObject's (replication_message.hpp):
//
//   id = 1 << 63 | region (48 bits) << 15 | batch (15 bits)       LAST_OBJ_ID = all ones: end of the snapshot
//
//   0 | 0       meta: the transfer identity {L, digest}, the control tuple captured at L, the volume's definition
//               (what a member needs to create it) and the region geometry
//   r | b       r >= 1: one batch of region r, the volume over [start, end) exactly (every mapped LBA with its data
//               and checksum; holes implicit)
//   R+1 | 0     trailer: W_hi
//
// Bit 63 is always set: HomeStore keeps ids with it clear for its own messages, so the first id the application sees
// is 1 << 63, i.e. 0 | 0. Everything here is pure (encode, decode, id packing), so it is tested without HomeStore.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <homestore/homestore_decl.hpp> // csum_t

namespace homeblocks::craft_snapshot {

using lba_t = uint64_t;

// ── object ids ──────────────────────────────────────────────────────────────

inline constexpr uint64_t k_app_bit = 1ull << 63;
inline constexpr uint64_t LAST_OBJ_ID = ~0ull;
inline constexpr uint64_t k_max_region = (1ull << 48) - 1;
inline constexpr uint32_t k_max_batch = (1u << 15) - 1;

struct obj_id {
    uint64_t region{0};
    uint32_t batch{0};

    // The packed id, or nullopt if a field overflows or the id would be LAST_OBJ_ID.
    std::optional< uint64_t > pack() const;
    // The fields of an application id (bit 63 set, not LAST_OBJ_ID), or nullopt.
    static std::optional< obj_id > unpack(uint64_t id);

    bool operator==(obj_id const&) const = default;
};

// ── payloads ────────────────────────────────────────────────────────────────

// The control state a snapshot carries, captured at its RAFT index L (section 4.9, "The control tuple is captured at
// L, not when served"): what the receiver merges, plus the data watermark W, the Sync watermark S and the Empty
// verdicts in (W, S].
struct control_tuple {
    uint64_t session_term{0};
    uint64_t client_token{0};
    uint64_t fence_term{0};              // zero until fences are carried
    uint64_t aborted_upto{0};            // zero until aborts are carried
    int64_t data_watermark{-1};          // W: every slot at or below is in the index
    int64_t sync_watermark{-1};          // S: the highest rs_commit_lsn applied
    std::vector< int64_t > empty_lsns{}; // the Empty verdicts in (W, S], ascending

    bool operator==(control_tuple const&) const = default;
};

// A digest of the tuple, deterministic across builds. With L it is the transfer identity a receiver resumes against:
// two leaders can hold different tuples at the same L.
uint32_t tuple_digest(control_tuple const& t);

// Object 0 | 0.
struct snapshot_meta {
    int64_t raft_idx{-1}; // L
    uint32_t digest{0};   // tuple_digest(tuple)
    control_tuple tuple{};
    std::vector< uint8_t > volume_def{}; // serialize_create_volume()'s encoding of the volume
    uint32_t lba_size{0};
    uint64_t total_lbas{0};
    uint64_t region_lbas{0}; // LBAs per region; region r (>= 1) is [(r-1) * region_lbas, r * region_lbas)
    uint64_t region_count{0};

    bool operator==(snapshot_meta const&) const = default;
};

struct snapshot_lba {
    lba_t lba{0};
    homestore::csum_t csum{0};

    bool operator==(snapshot_lba const&) const = default;
};

// Object r | b (r >= 1): the volume over [start, end) is exactly these LBAs, holes included.
struct region_batch {
    uint64_t region{0};
    uint32_t batch{0};
    lba_t start{0};
    lba_t end{0};
    bool last_in_region{false};
    uint32_t lba_size{0};
    std::vector< snapshot_lba > lbas{}; // ascending, within [start, end)
    std::vector< uint8_t > data{};      // lbas.size() * lba_size bytes, in the same order

    bool operator==(region_batch const&) const = default;
};

// Object R+1 | 0.
struct snapshot_trailer {
    int64_t w_hi{-1}; // the donor's last_append_lsn when it read its last region

    bool operator==(snapshot_trailer const&) const = default;
};

using snapshot_object = std::variant< snapshot_meta, region_batch, snapshot_trailer >;

// ── sealed header ───────────────────────────────────────────────────────────

inline constexpr uint64_t k_magic = 0x5450'4e53'5446'5243ull; // "CRFTSNPT" little-endian
inline constexpr uint16_t k_version = 1;

enum class msg_type : uint8_t { meta = 1, region_batch = 2, trailer = 3 };

#pragma pack(push, 1)
struct object_header {
    uint64_t magic{k_magic};
    uint16_t version{k_version};
    uint8_t type{0};
    uint8_t reserved{0};
    uint32_t payload_size{0};
    uint32_t payload_crc{0};
    uint32_t header_crc{0}; // over the header with this field zero
};
#pragma pack(pop)

// The encoded object: a sealed header, then the payload.
std::vector< uint8_t > encode(snapshot_meta const& m);
std::vector< uint8_t > encode(region_batch const& b);
std::vector< uint8_t > encode(snapshot_trailer const& t);

// The object in buf, or nullopt if anything is off: header or payload CRC, magic, version, type, sizes, or a payload
// that violates its own invariants (an LBA outside its batch, not ascending, data of the wrong length, Empty verdicts
// outside (W, S] or not ascending, a digest that doesn't match its tuple). A receiver that gets nullopt asks for the
// same object again.
std::optional< snapshot_object > decode(uint8_t const* buf, size_t len);

// ── the donor's record of its last snapshot ────────────────────────────────

// One meta blk record per CRAFT group: the last snapshot this member created, as create_snapshot() captured it. Its
// object 0 | 0 is served from here on every retry and after a restart, so a receiver always sees the tuple of L, not
// one sampled when the object is read. Followed by meta_len bytes (the encoded object 0 | 0) and ctx_len bytes
// (nuraft's serialized snapshot context).
#pragma pack(push, 1)
struct snapshot_record_sb_t {
    static constexpr uint64_t k_magic = 0x5243'5350'4e53'4643ull; // "CFSNPSCR" little-endian
    static constexpr uint32_t k_version = 1;

    uint64_t magic{k_magic};
    uint32_t version{k_version};
    boost::uuids::uuid group_id{}; // the CRAFT volume (and RAFT group) id
    int64_t raft_idx{-1};          // L
    uint32_t meta_len{0};
    uint32_t ctx_len{0};
};
#pragma pack(pop)
inline const std::string snapshot_record_meta_name{"CraftSnapshot"};

// One meta blk record per CRAFT group on a receiver: the transfer it is installing (requirement 6). Written after the
// barrier is up and at every region boundary (after a checkpoint, so the cursor never runs ahead of durable data). A
// restart resumes after next_region only if the snapshot's object 0 | 0 carries the same transfer identity
// {raft_idx, digest}. Followed by meta_len bytes: the encoded object 0 | 0.
#pragma pack(push, 1)
struct snapshot_receive_sb_t {
    static constexpr uint64_t k_magic = 0x5643'5250'4e53'4643ull; // "CFSNPRCV" little-endian
    static constexpr uint32_t k_version = 1;

    uint64_t magic{k_magic};
    uint32_t version{k_version};
    boost::uuids::uuid group_id{};
    int64_t raft_idx{-1};    // L
    int64_t p{-1};           // the receiver's last_append_lsn when the barrier went up
    int64_t w_hi{-1};        // from the trailer, once received
    uint64_t next_region{1}; // regions before it are installed and durable
    uint8_t control_only{0};
    uint8_t completed{0}; // completion is durable; apply_snapshot is still to come
    uint32_t meta_len{0};
};
#pragma pack(pop)
inline const std::string snapshot_receive_meta_name{"CraftSnapshotRecv"};

// ── region geometry ─────────────────────────────────────────────────────────

// Regions of region_lbas LBAs each over total_lbas (the last one may be shorter). Zero if either is zero.
uint64_t region_count(uint64_t total_lbas, uint64_t region_lbas);
// [start, end) of region r (1-based); empty if r is out of range.
std::pair< lba_t, lba_t > region_range(uint64_t r, uint64_t total_lbas, uint64_t region_lbas);

} // namespace homeblocks::craft_snapshot
