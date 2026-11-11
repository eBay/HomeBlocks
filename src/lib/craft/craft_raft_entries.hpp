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
#pragma once

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <sisl/fds/buffer.hpp>

// ─── CRAFT RAFT entry types ───────────────────────────────────────────────────
//
// Three entry types ride the RAFT log in the CRAFT backend. None carries
// volume write data — write data travels point-to-point via the client broadcast
// and is journalled locally. These entries carry only control-plane metadata:
//
//   SyncRSCommitLSN — advances the replica-set commit watermark and carries
//     the Empty verdicts (slots the leader declared unreachable by quorum).
//     Proposed by the leader; applied by every replica.
//
//   InternalLogin — establishes a new client session (token + term). Proposed
//     by the leader as step 4 of the login sequence; applied by every replica.
//     All subsequent IO with a mismatched term is rejected STALE_TERM.
//
//   CreateVolume — the partition's first entry: proposed by the member that formed
//     the RAFT group, and again by its leader for a create that did not finish there;
//     every member creates its local volume when it first commits.
//     Applied by the volume layer (the partition has no CraftReplDev before it).
//
// Wire layout:
//   header blob → CraftEntryHeader  (discriminant only)
//   key blob    → SyncRSCommitLSNPayload | InternalLoginPayload | CreateVolumeHeader
//
// For SyncRSCommitLSN the key blob is variable-length: the fixed
// SyncRSCommitLSNPayload struct is followed immediately by
// payload.num_empty_slots × int64_t (the Empty-verdicted LSNs).
//
// Parsing: reinterpret_cast<const T*>(blob.cbytes()) — same pattern as
// MsgHeader / VolJournalEntry in volume.hpp.

namespace homeblocks {

ENUM(CraftEntryType, uint8_t, SyncRSCommitLSN, InternalLogin, CreateVolume);

// ─── header blob ─────────────────────────────────────────────────────────────

struct CraftEntryHeader {
    CraftEntryType type;
};

// ─── key blob: SyncRSCommitLSN ───────────────────────────────────────────────

// Fixed prefix. Immediately followed by num_empty_slots × int64_t.
#pragma pack(push, 1)
struct SyncRSCommitLSNPayload {
    int64_t rs_commit_lsn;
    uint64_t client_token;
    uint32_t num_empty_slots;
};

// ─── key blob: InternalLogin ──────────────────────────────────────────────────

struct InternalLoginPayload {
    uint64_t client_token;
    uint64_t term;
};

// ─── key blob: CreateVolume ───────────────────────────────────────────────────

// The key blob's fixed-size part, its header: num_members × member uuid follow it, then name_len name bytes. No
// ordinal: that is local to each node. The uuid is raw bytes: a packed field can't bind to uuid's copy operations.
// `version` comes first: an entry written in another layout is rejected rather than misread.
struct CreateVolumeHeader {
    static constexpr uint8_t VERSION{1};
    uint8_t version;
    uint8_t id[sizeof(boost::uuids::uuid)];
    uint64_t size_bytes;
    uint32_t page_size;
    uint16_t num_members;
    uint16_t name_len;
};
#pragma pack(pop)

// Decoded CreateVolume entry.
struct CreateVolumeEntry {
    boost::uuids::uuid id;
    uint64_t size_bytes{0};
    uint32_t page_size{0};
    std::vector< boost::uuids::uuid > members;
    std::string name;
};

// ─── helpers ──────────────────────────────────────────────────────────────────

inline size_t sync_rs_commit_lsn_key_size(size_t num_empty) {
    return sizeof(SyncRSCommitLSNPayload) + num_empty * sizeof(int64_t);
}

// Writes the fixed payload followed by a packed int64_t array into buf. Caller must size buf via
// sync_rs_commit_lsn_key_size(empty_slots.size()) first.
inline void serialize_sync_rs_commit_lsn(uint8_t* buf, int64_t rs_commit_lsn, uint64_t client_token,
                                         const std::vector< int64_t >& empty_slots) {
    // SyncRSCommitLSNPayload's 3 members total 20 bytes but sizeof rounds to 24 (8-byte alignment) --
    // zero the whole fixed prefix first so that trailing padding, which rides along as part of the
    // persisted/replicated entry, doesn't leak prior buffer contents or make equivalent entries
    // byte-different.
    std::memset(buf, 0, sizeof(SyncRSCommitLSNPayload));

    auto* p = reinterpret_cast< SyncRSCommitLSNPayload* >(buf);
    p->rs_commit_lsn = rs_commit_lsn;
    p->client_token = client_token;
    p->num_empty_slots = static_cast< uint32_t >(empty_slots.size());
    // empty_slots.data() may be null when empty; memcpy(dest, nullptr, 0) is UB regardless of count.
    if (!empty_slots.empty()) { std::memcpy(p + 1, empty_slots.data(), empty_slots.size() * sizeof(int64_t)); }
}

// Reads the packed int64_t array immediately following the fixed prefix in `key` (the trailing data
// serialize_sync_rs_commit_lsn wrote). `key` is persisted RAFT log data replayed on apply -- a truncated or
// corrupt entry must not be trusted, so the fixed prefix and the declared num_empty_slots are validated
// against key's actual size before the vector is ever constructed. Returns std::nullopt if the entry
// doesn't parse.
inline std::optional< std::vector< int64_t > > parse_empty_slots(sisl::blob const& key) {
    if (key.size() < sizeof(SyncRSCommitLSNPayload)) { return std::nullopt; }

    const auto* p = reinterpret_cast< const SyncRSCommitLSNPayload* >(key.cbytes());

    const size_t empty_count = p->num_empty_slots;
    const size_t expected_size = sizeof(SyncRSCommitLSNPayload) + empty_count * sizeof(int64_t);
    // The size needs to match exactly. This restriction can be lifted later
    if (key.size() != expected_size) { return std::nullopt; }

    const auto* src = reinterpret_cast< const int64_t* >(p + 1);
    return std::vector< int64_t >(src, src + empty_count);
}

inline size_t create_volume_key_size(size_t num_members, size_t name_len) {
    return sizeof(CreateVolumeHeader) + num_members * sizeof(boost::uuids::uuid) + name_len;
}

// Writes e into buf. Caller must size buf via create_volume_key_size(e.members.size(), e.name.size()) first, and keep
// both counts within uint16_t.
inline void serialize_create_volume(uint8_t* buf, CreateVolumeEntry const& e) {
    auto* p = reinterpret_cast< CreateVolumeHeader* >(buf);
    p->version = CreateVolumeHeader::VERSION;
    std::memcpy(p->id, &e.id, sizeof(p->id));
    p->size_bytes = e.size_bytes;
    p->page_size = e.page_size;
    p->num_members = static_cast< uint16_t >(e.members.size());
    p->name_len = static_cast< uint16_t >(e.name.size());
    auto* tail = reinterpret_cast< uint8_t* >(p + 1);
    for (auto const& m : e.members) {
        std::memcpy(tail, &m, sizeof(m));
        tail += sizeof(m);
    }
    if (!e.name.empty()) { std::memcpy(tail, e.name.data(), e.name.size()); }
}

// `key` is persisted RAFT log data replayed on apply: validated against its version and declared counts before anything
// is trusted, like parse_empty_slots. Returns std::nullopt if the entry doesn't parse.
inline std::optional< CreateVolumeEntry > parse_create_volume(sisl::blob const& key) {
    if (key.size() < sizeof(CreateVolumeHeader)) { return std::nullopt; }
    const auto* p = reinterpret_cast< const CreateVolumeHeader* >(key.cbytes());
    if (p->version != CreateVolumeHeader::VERSION) { return std::nullopt; }
    if (key.size() != create_volume_key_size(p->num_members, p->name_len)) { return std::nullopt; }

    CreateVolumeEntry e;
    std::memcpy(&e.id, p->id, sizeof(p->id));
    e.size_bytes = p->size_bytes;
    e.page_size = p->page_size;
    const auto* tail = reinterpret_cast< const uint8_t* >(p + 1);
    e.members.resize(p->num_members);
    for (auto& m : e.members) {
        std::memcpy(&m, tail, sizeof(m));
        tail += sizeof(m);
    }
    e.name.assign(reinterpret_cast< const char* >(tail), p->name_len);
    return e;
}

} // namespace homeblocks
