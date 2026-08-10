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
#include <vector>

#include <sisl/fds/buffer.hpp>

// ─── CRAFT RAFT entry types ───────────────────────────────────────────────────
//
// Two entry types ride the RAFT log in the CRAFT backend. Neither carries
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
// Wire layout:
//   header blob → CraftEntryHeader  (discriminant only)
//   key blob    → SyncRSCommitLSNPayload | InternalLoginPayload
//
// For SyncRSCommitLSN the key blob is variable-length: the fixed
// SyncRSCommitLSNPayload struct is followed immediately by
// payload.num_empty_slots × int64_t (the Empty-verdicted LSNs).
//
// Parsing: reinterpret_cast<const T*>(blob.cbytes()) — same pattern as
// MsgHeader / VolJournalEntry in volume.hpp.

namespace homeblocks {

enum class CraftEntryType : uint8_t {
    SyncRSCommitLSN = 1,
    InternalLogin   = 2,
};

// ─── header blob ─────────────────────────────────────────────────────────────

struct CraftEntryHeader {
    CraftEntryType type;
};

// ─── key blob: SyncRSCommitLSN ───────────────────────────────────────────────

// Fixed prefix. Immediately followed by num_empty_slots × int64_t.
struct SyncRSCommitLSNPayload {
    int64_t  rs_commit_lsn;
    uint64_t client_token;
    uint32_t num_empty_slots;
};

// ─── key blob: InternalLogin ──────────────────────────────────────────────────

struct InternalLoginPayload {
    uint64_t client_token;
    uint64_t term;
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

    auto* p            = reinterpret_cast< SyncRSCommitLSNPayload* >(buf);
    p->rs_commit_lsn   = rs_commit_lsn;
    p->client_token    = client_token;
    p->num_empty_slots = static_cast< uint32_t >(empty_slots.size());
    // empty_slots.data() may be null when empty; memcpy(dest, nullptr, 0) is UB regardless of count.
    if (!empty_slots.empty()) {
        std::memcpy(p + 1, empty_slots.data(), empty_slots.size() * sizeof(int64_t));
    }
}

// Reads the packed int64_t array immediately following the fixed prefix in `key` (the trailing data
// serialize_sync_rs_commit_lsn wrote). `key` is persisted RAFT log data replayed on apply -- a truncated or
// corrupt entry must not be trusted, so the fixed prefix and the declared num_empty_slots are validated
// against key's actual size before the vector is ever constructed. Returns std::nullopt if the entry
// doesn't parse.
inline std::optional< std::vector< int64_t > > parse_empty_slots(sisl::blob const& key) {
    if (key.size() < sizeof(SyncRSCommitLSNPayload)) { return std::nullopt; }

    const auto* p = reinterpret_cast< const SyncRSCommitLSNPayload* >(key.cbytes());

    const size_t empty_count  = p->num_empty_slots;
    const size_t expected_size = sizeof(SyncRSCommitLSNPayload) + empty_count * sizeof(int64_t);
    if (key.size() != expected_size) { return std::nullopt; }

    const auto* src = reinterpret_cast< const int64_t* >(p + 1);
    return std::vector< int64_t >(src, src + empty_count);
}

} // namespace homeblocks
