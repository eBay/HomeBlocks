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
#include <homeblks/home_blocks.hpp>
#include <sisl/fds/buffer.hpp>

// ─── CRAFT RAFT entry types ───────────────────────────────────────────────────
//
// Three entry types ride the RAFT log in the CRAFT backend. None carries
// volume write data — write data travels point-to-point via the client broadcast
// and is journalled locally. These entries carry only control-plane metadata:
//
//   SyncRSCommitLSN — advances the replica-set commit watermark and carries
//     the Empty verdicts (slots the leader declared unreachable by quorum), and
//     the session it was proposed in. Proposed by the leader; applied by every
//     replica.
//
//   InternalLogin — establishes a new client session (token + term), starting at
//     the login's watermark rs_commit_lsn. Proposed by the leader as the last step
//     of the login sequence; applied by every replica. All subsequent IO with a
//     mismatched term is rejected STALE_TERM.
//
//   CreateVolume — the partition's first entry: proposed by the member that formed
//     the RAFT group; every member creates its local volume when it commits.
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
// Each key starts with its own version byte, so a key written in another layout is
// rejected rather than misread; the header carries only the entry type, whose values
// are fixed (append new types, never reorder them). All of these are persisted in the
// RAFT log: a layout change is a format change.
//
// Parsing: parse_sync_rs_commit_lsn, parse_internal_login and parse_create_volume
// below, which validate the persisted key before trusting it.

namespace homeblocks {

ENUM(CraftEntryType, uint8_t, SyncRSCommitLSN, InternalLogin, CreateVolume);
static_assert(static_cast< uint8_t >(CraftEntryType::SyncRSCommitLSN) == 0 &&
                  static_cast< uint8_t >(CraftEntryType::InternalLogin) == 1 &&
                  static_cast< uint8_t >(CraftEntryType::CreateVolume) == 2,
              "CraftEntryType values are persisted in the RAFT log: append new types, never reorder them");

// ─── header blob ─────────────────────────────────────────────────────────────

struct CraftEntryHeader {
    CraftEntryType type;
};
static_assert(sizeof(CraftEntryHeader) == 1, "CraftEntryHeader is persisted in the RAFT log");

// ─── key blob: SyncRSCommitLSN ───────────────────────────────────────────────

// Fixed prefix. Immediately followed by num_empty_slots × int64_t. `version` comes first, as in InternalLoginPayload.
// term is the session the entry was proposed in: one that commits behind a newer InternalLogin is rejected.
#pragma pack(push, 1)
struct SyncRSCommitLSNPayload {
    static constexpr uint8_t VERSION{1};
    uint8_t version;
    int64_t rs_commit_lsn;
    uint64_t client_token;
    uint64_t term;
    uint32_t num_empty_slots;
};

// ─── key blob: InternalLogin ──────────────────────────────────────────────────

// `version` comes first, as in CreateVolumeHeader. rs_commit_lsn is the watermark the login settled, -1 on an empty
// volume.
struct InternalLoginPayload {
    static constexpr uint8_t VERSION{1};
    uint8_t version;
    uint64_t client_token;
    uint64_t term;
    int64_t rs_commit_lsn;
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
static_assert(sizeof(SyncRSCommitLSNPayload) == 1 + 3 * sizeof(uint64_t) + sizeof(uint32_t),
              "SyncRSCommitLSNPayload must stay packed");
static_assert(sizeof(InternalLoginPayload) == 1 + 3 * sizeof(uint64_t), "InternalLoginPayload must stay packed");
static_assert(sizeof(CreateVolumeHeader) ==
                  1 + sizeof(boost::uuids::uuid) + sizeof(uint64_t) + sizeof(uint32_t) + 2 * sizeof(uint16_t),
              "CreateVolumeHeader must stay packed");

// ─── helpers ──────────────────────────────────────────────────────────────────

inline size_t sync_rs_commit_lsn_key_size(size_t num_empty) {
    return sizeof(SyncRSCommitLSNPayload) + num_empty * sizeof(int64_t);
}

// Writes the fixed payload followed by a packed int64_t array into buf. Caller must size buf via
// sync_rs_commit_lsn_key_size(empty_slots.size()) first.
inline void serialize_sync_rs_commit_lsn(uint8_t* buf, int64_t rs_commit_lsn, uint64_t client_token, uint64_t term,
                                         const std::vector< int64_t >& empty_slots) {
    // Zero the whole fixed prefix first so that any padding, which rides along as part of the persisted/replicated
    // entry, doesn't leak prior buffer contents or make equivalent entries byte-different.
    std::memset(buf, 0, sizeof(SyncRSCommitLSNPayload));

    auto* p = reinterpret_cast< SyncRSCommitLSNPayload* >(buf);
    p->version = SyncRSCommitLSNPayload::VERSION;
    p->rs_commit_lsn = rs_commit_lsn;
    p->client_token = client_token;
    p->term = term;
    p->num_empty_slots = static_cast< uint32_t >(empty_slots.size());
    // empty_slots.data() may be null when empty; memcpy(dest, nullptr, 0) is UB regardless of count.
    if (!empty_slots.empty()) { std::memcpy(p + 1, empty_slots.data(), empty_slots.size() * sizeof(int64_t)); }
}

// A parsed SyncRSCommitLSN: the payload's fields, unpacked, and its Empty verdicts.
struct SyncRSCommitLSNEntry {
    int64_t rs_commit_lsn;
    uint64_t client_token;
    uint64_t term;
    std::vector< int64_t > empty_slots;
};

// `key` is persisted RAFT log data replayed on apply -- a truncated or corrupt entry must not be trusted, so its
// version and its declared num_empty_slots are validated against key's actual size before anything is read. The
// earlier, unversioned layouts never match in size. Returns std::nullopt if the entry doesn't parse.
inline std::optional< SyncRSCommitLSNEntry > parse_sync_rs_commit_lsn(sisl::blob const& key) {
    if (key.size() < sizeof(SyncRSCommitLSNPayload)) { return std::nullopt; }
    SyncRSCommitLSNPayload p;
    std::memcpy(&p, key.cbytes(), sizeof(p));
    if (p.version != SyncRSCommitLSNPayload::VERSION) { return std::nullopt; }
    // The size needs to match exactly. This restriction can be lifted later
    if (key.size() != sync_rs_commit_lsn_key_size(p.num_empty_slots)) { return std::nullopt; }

    // Copied rather than cast: the slots follow an odd-sized prefix, so they aren't int64_t-aligned.
    std::vector< int64_t > empty_slots(p.num_empty_slots);
    if (!empty_slots.empty()) {
        std::memcpy(empty_slots.data(), key.cbytes() + sizeof(p), empty_slots.size() * sizeof(int64_t));
    }
    return SyncRSCommitLSNEntry{p.rs_commit_lsn, p.client_token, p.term, std::move(empty_slots)};
}

// Writes an InternalLogin key into buf, which must hold sizeof(InternalLoginPayload) bytes.
inline void serialize_internal_login(uint8_t* buf, uint64_t client_token, uint64_t term, int64_t rs_commit_lsn) {
    auto* p = reinterpret_cast< InternalLoginPayload* >(buf);
    p->version = InternalLoginPayload::VERSION;
    p->client_token = client_token;
    p->term = term;
    p->rs_commit_lsn = rs_commit_lsn;
}

// A parsed InternalLogin: the payload's fields, unpacked so callers can bind references to them.
struct InternalLoginEntry {
    uint64_t client_token;
    uint64_t term;
    int64_t rs_commit_lsn;
};

// `key` is persisted RAFT log data replayed on apply, like parse_create_volume's. Returns std::nullopt if its size or
// version is wrong or its rs_commit_lsn is below -1.
inline std::optional< InternalLoginEntry > parse_internal_login(sisl::blob const& key) {
    if (key.size() != sizeof(InternalLoginPayload)) { return std::nullopt; }
    InternalLoginPayload p;
    std::memcpy(&p, key.cbytes(), sizeof(p));
    if (p.version != InternalLoginPayload::VERSION || p.rs_commit_lsn < -1) { return std::nullopt; }
    return InternalLoginEntry{p.client_token, p.term, p.rs_commit_lsn};
}

inline size_t create_volume_key_size(size_t num_members, size_t name_len) {
    return sizeof(CreateVolumeHeader) + num_members * sizeof(boost::uuids::uuid) + name_len;
}

// Writes the replicated part of v into buf: its id, size, page size, members and name. The rest is each node's own (the
// ordinal) or implied (CRAFT). Caller must size buf via create_volume_key_size(v.members.size(), v.name.size()) first,
// and keep both counts within uint16_t and the page size within uint32_t.
inline void serialize_create_volume(uint8_t* buf, volume_info const& v) {
    auto* p = reinterpret_cast< CreateVolumeHeader* >(buf);
    p->version = CreateVolumeHeader::VERSION;
    std::memcpy(p->id, &v.id, sizeof(p->id));
    p->size_bytes = v.size_bytes;
    p->page_size = static_cast< uint32_t >(v.page_size);
    p->num_members = static_cast< uint16_t >(v.members.size());
    p->name_len = static_cast< uint16_t >(v.name.size());
    auto* tail = reinterpret_cast< uint8_t* >(p + 1);
    for (auto const& m : v.members) {
        std::memcpy(tail, &m, sizeof(m));
        tail += sizeof(m);
    }
    if (!v.name.empty()) { std::memcpy(tail, v.name.data(), v.name.size()); }
}

// `key` is persisted RAFT log data replayed on apply: validated against its version and declared counts before anything
// is trusted, like parse_sync_rs_commit_lsn. Returns the CRAFT volume it defines, without an ordinal (each node assigns
// its own), or std::nullopt if the entry doesn't parse.
inline std::optional< volume_info > parse_create_volume(sisl::blob const& key) {
    if (key.size() < sizeof(CreateVolumeHeader)) { return std::nullopt; }
    const auto* p = reinterpret_cast< const CreateVolumeHeader* >(key.cbytes());
    if (p->version != CreateVolumeHeader::VERSION) { return std::nullopt; }
    if (key.size() != create_volume_key_size(p->num_members, p->name_len)) { return std::nullopt; }

    volume_id_t id;
    std::memcpy(&id, p->id, sizeof(p->id));
    const auto* tail = reinterpret_cast< const uint8_t* >(p + 1);
    std::vector< peer_id_t > members(p->num_members);
    for (auto& m : members) {
        std::memcpy(&m, tail, sizeof(m));
        tail += sizeof(m);
    }
    volume_info v{id, p->size_bytes, p->page_size, std::string(reinterpret_cast< const char* >(tail), p->name_len)};
    v.repl_mode = replication_mode::CRAFT;
    v.members = std::move(members);
    return v;
}

} // namespace homeblocks
