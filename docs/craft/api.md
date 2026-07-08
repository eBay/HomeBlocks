# HomeBlocks CRAFT C++ API

The CRAFT client-facing surface is a set of **free functions over a `volume_handle`** in the public
header `home_blocks.hpp` (parallel to the existing `async_read`/`async_write` block ops, which are now
`[[deprecated]]`). **One `volume_handle` == one replica device**: in production each is a different
server; the client holds one handle per member and does the CRAFT work itself (assign `dLSN`,
broadcast the write to every member, tally quorum, route the read to one member). The calls dispatch
through an internal per-replica interface, `craft_replica`, which both the production `CraftReplDev`
and the in-memory reference model (`src/lib/craft/memory/`) implement independently.

All methods are async/coroutine-style (`async_result<T>` / `async_status`) matching the existing
HomeBlocks convention.

> **Canonical design:** the [**CRAFT Design**](https://github.com/eBay/HomeBlocks/wiki/CRAFT-Design)
> wiki page is the source of truth for the protocol, and
> [**CRAFT on HomeBlocks**](https://github.com/eBay/HomeBlocks/wiki/CRAFT-on-HomeBlocks) for the
> implementation binding; this file is the C++ API reference. Wire formats are in [rpcs.md](rpcs.md).

---

## Addressing & buffer model

- **Byte-based.** `addr` and `len` are **raw byte offsets/lengths** (not block indices), matching the
  rest of the HomeBlocks public API. They must be aligned to the volume's `lba_size` (returned by
  `login`), else the op fails with `std::errc::invalid_argument`. The byte↔block conversion is confined
  to the server.
- **One buffer type both ways: `sisl::sg_list`** (caller-owned, e.g. iomgr-allocated and long-lived).
  A **write** takes a source buffer; an **empty** `sg_list` (`size == 0`) *is* a zero write
  (WRITE_ZEROES / discard-to-zero) — there is **no `all_zeros` flag**. A **read** fills a caller-provided
  `dest` buffer in place (data → bytes, holes → zeros) and returns a sparse **layout**.
- **Holes are first-class.** A read returns `std::vector<io_extent>` marking which byte sub-ranges were
  data vs holes; the same `hole` concept is what an empty-buffer write produces. A hole is **not**
  `Missing`.

```cpp
// The session + watermark fields the client stamps on EVERY CRAFT IO (write / read / keep_alive).
struct client_hdr {
    uint64_t term{0};              // fences a stale writer (ETERM); every IO incl. keep_alive is checked
    int64_t  commit_lsn{-1};       // advance the frontier toward this, best-effort (-1 = don't). CRAFT's
                                   // commit path: it piggybacks on the IO the client already sends --
                                   // there is NO standalone commit verb.
    int64_t  all_committed_lsn{-1}; // set-wide min commit_lsn; floors journal reclaim (-1 = unknown).
};

// One sub-range of a read's sparse layout, in BYTES. Carries no bytes (they live in the dest buffer).
struct io_extent { uint64_t addr; uint64_t len; bool hole; }; // hole => reads as zeros (unmapped)
```

---

## Per-partition in-memory state

```cpp
struct CraftPartitionState {
    int64_t  commit_lsn       {-1};  // contiguous committed prefix (== Synced)
    int64_t  last_append_lsn  {-1};  // highest appended dLSN (may be uncommitted)
    uint64_t client_token     {0};   // token from last successful InternalLogin
    uint64_t term             {0};   // current session term
};
```

Authoritative in memory; recovered from the journal + superblock on restart (the reference model does
not persist it).

---

## Client-facing API

### `login`

```cpp
struct LoginResult {
    std::vector<replica_endpoint> members;
    int64_t  dLSN;      // starting (per-partition) LSN for new IO
    uint64_t term;
    uint32_t lba_size;  // block size in bytes: the client aligns addr/len to it and presents geometry to the FS
};

async_result<LoginResult> login(volume_handle const& vol, uint64_t client_token);
```

Leader-only. Orchestrates the full login sequence (GetRSCommitLSN broadcast → optional FetchData →
`SyncRSCommitLSN` RAFT → `InternalLogin` RAFT) and returns once both RAFT entries commit. A follower
fails with `craft_error::NOT_LEADER`. Postconditions: all quorum members have
`commit_lsn == rs_commit_lsn`; all reject IO whose `term` != the new session term.

---

### `async_write`

```cpp
async_status async_write(volume_handle const& vol, client_hdr hdr, int64_t dlsn,
                         uint64_t addr, uint64_t len, sisl::sg_list data);
```

Append the client-assigned write at slot `dlsn`. `addr`/`len` are byte offset/length, block-aligned.
**Empty `data`** (`size == 0`) is a **zero write** (WRITE_ZEROES; `len` is the range to unmap; reads
back as a hole); non-empty `data` is a **data write** of exactly `len` bytes (single contiguous
buffer). Both take a `dLSN` and merge by highest-`dLSN`-per-LBA. Does **not** apply to the LBA index;
`hdr.commit_lsn` rides along and advances the contiguous commit frontier best-effort, in `dLSN` order
(this is CRAFT's commit — piggybacked on the writes the client is already broadcasting). Rejected with
`craft_error::STALE_TERM` if `hdr.term` != the session term, or `std::errc::invalid_argument` on
misalignment. ACK fires on journal-append completion.

---

### `async_read`

```cpp
async_result<std::vector<io_extent>>
async_read(volume_handle const& vol, client_hdr hdr, int64_t read_lsn,
           uint64_t addr, uint64_t len, sisl::sg_list dest);
```

`read_lsn` is the read horizon `H`. Returns the latest version ≤ `H` for `[addr, addr+len)` by **filling
the caller-owned `dest` buffer in place** — data sub-ranges get their bytes, holes get zeros — and
returns the sparse **layout** (`io_extent[]`, ascending by `addr`) marking which byte sub-ranges were
data vs holes. Data comes from the LBA index if applied, or straight from the **journal-tail overlay**
if only Appended (an overlay read; no index write on the read path). **Entries above `H` are never
served even if the replica holds them** (the sub-quorum tail). The read never fetches from a peer; the
client routes only to a replica that holds every write ≤ `H` overlapping the range. `hdr.commit_lsn`
piggybacks a frontier advance. Rejects on term mismatch / misalignment.

---

### `keep_alive`

```cpp
async_result<LSNPair> keep_alive(volume_handle const& vol, client_hdr hdr);
```

The dedicated carrier for CRAFT's commit: advance the contiguous commit watermark toward
`hdr.commit_lsn` (applying present entries strictly in `dLSN` order, skipping Empty, reclaiming
superseded blocks) **and reset the client-timeout watchdog**. Returns the **achieved**
`{commit_lsn, last_append_lsn}` — best-effort: `commit_lsn` stalls just below the first Missing hole
(resync fills it; not an error; pauses only apply + reclaim, never reads). **Term-fenced** (`hdr.term`):
a stale client must not be able to reset the watchdog and keep the session alive.
`hdr.all_committed_lsn` lets the replica reclaim journal below
`min(all_committed_lsn, checkpointed apply frontier)`. Sent periodically (even when idle) and after
quorum-acked writes.

> **There is no standalone `commit` verb.** The commit watermark advances via `hdr.commit_lsn`
> piggybacked on `async_write` / `async_read` / `keep_alive`; `keep_alive` is its dedicated carrier and
> also resets the watchdog. Readability is per-write and does not wait for the watermark (overlay reads).

---

### `get_lsns`

```cpp
struct LSNPair { int64_t commit_lsn; int64_t last_append_lsn; };

async_result<LSNPair> get_lsns(volume_handle const& vol);
```

Returns `{commit_lsn, last_append_lsn}` for the local partition. Used by peers via `GetRSCommitLSN`
during login and by the leader during `SyncRSCommitLSN`.

---

## Internal / peer & RAFT-entry API

These are on the `craft_replica` backend, not the public client surface.

### `truncate`

```cpp
async_status truncate(int64_t lsn);
```

Drop all journal entries with `dLSN > lsn` (stale prior-term tail; login truncation after
`InternalLogin` commits).

### `append` (propose SyncRSCommitLSN)

```cpp
async_status append(int64_t sync_to, uint64_t client_token);
```

Propose a `SyncRSCommitLSN` RAFT entry. The leader first resolves every unresolved slot ≤ `sync_to`
(fetch from a holder, or an `Empty` verdict on quorum-lacks evidence) and attaches `empty_slots[]`; it
never proposes past an unresolved slot.

### `fetch_data` (peer resync)

```cpp
async_result<std::vector<JournalSlot>> fetch_data(std::vector<int64_t> lsns);
```

Server-to-server raw journal read during `SyncRSCommitLSN` apply. Each `JournalSlot` is one of: a data
slot, a zero write (`all_zeros=true`, no data), or `Empty` (`is_empty=true`); a requested LSN absent
from the result means not-present-here.

### `get_rs_commit_lsn`

Alias of `get_lsns` exposed to peers during the `GetRSCommitLSN` broadcast.

### RAFT state machine entries

`SyncRSCommitLSN { rs_commit_lsn, client_token, empty_slots[] }` and
`InternalLogin { client_token, term }` — see [rpcs.md](rpcs.md) and the wiki for apply semantics
(unchanged by the client-surface reshaping above).

---

## Replacing the existing API

The CRAFT free functions supersede the legacy byte block ops (now `[[deprecated]]`, kept for reference):

| Old API (deprecated) | CRAFT replacement |
|---|---|
| `async_write(vol, addr, sgs)` | `async_write(vol, hdr, dlsn, addr, len, data)` |
| `async_read(vol, addr, sgs)` | `async_read(vol, hdr, read_lsn, addr, len, dest)` → `io_extent[]` layout |
| `async_unmap(vol, addr, len)` | `async_write(vol, hdr, dlsn, addr, len, /*empty*/ {})` (empty buffer = zero write) |
| — | `login`, `keep_alive` (commit carrier), `get_lsns` |

The commit watermark rides on these calls (`client_hdr::commit_lsn`); there is no separate `commit`
call. Each handle is **one replica device**, never an aggregate / "whole volume" handle: the client
holds one handle per member and advances their commits independently (just like the protocol). A handle
comes from `create_volume` per server in production, or per-replica `create_memory_volume` for the
in-memory reference model (`make_memory_replica_set` is a test harness that loops it and wires the
in-process fabric).
