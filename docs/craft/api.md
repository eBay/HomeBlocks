# HomeBlocks CRAFT C++ API

The CRAFT surface HomeBlocks declares is a set of **free functions over a `volume_handle`** in the
public header `home_blocks.hpp` (parallel to the legacy `async_read`/`async_write` block ops, now
`[[deprecated]]`). **One `volume_handle` == one replica device**: these are the CRAFT ops issued against
a SINGLE replica (login, the client-assigned write, the horizon read, the commit carrier). Each forwards
1:1 to that volume's homestore-backed `CraftReplDev` -- nothing to translate, because `CraftReplDev`'s
client-facing verbs are already the wire's shape: byte-addressed (`addr`/`len` are absolute byte offsets,
block-aligned to `lba_size`) and carrying the wire's `craft::client_hdr` on every op.

## HomeBlocks is the CRAFT *backend* -- the far side of the wire

This is the whole of HomeBlocks' role in CRAFT, and it is worth stating flatly because the alternative is
the natural assumption:

- HomeBlocks **never constructs a CRAFT client.** `make_client` appears nowhere in this repo, and no class
  here implements `craft::craft_replica` -- that interface is the *client's* view of a member and lives on
  the near side of the wire, implemented only by a transport proxy or by the reference model.
- The CRAFT **client** (dLSN assignment, quorum broadcast, read routing) lives in the standalone
  **`craft_client`** package, its ublk driver in **`ublkpp`** (`craft_disk`), and the in-memory reference
  model + reference server in `craft_client`.
- A CRAFT **server** (the planned `CraftConnector`) terminates the wire, looks up the volume the control
  layer told it to expose, enforces admission, and then calls **exactly these free functions** with the
  handle. So this page is the per-replica surface a transport binds to.
- HomeBlocks' only dependency on `craft_client` is `craft_types` -- the header-only vocabulary the wire is
  defined in (`client_hdr`, `lsn_pair`, `read_result`, `io_extent`, `craft_error`, `LoginResult`). Not the
  client, not the `craft_replica` interface, not the codec.

A volume created in CRAFT mode (see [Creating a CRAFT volume](#creating-a-craft-volume)) is backed by a
`CraftReplDev`; on any other volume these functions return `std::errc::not_supported`.

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

// A read reply: the sparse layout PLUS the replica's piggybacked watermarks. Every CRAFT IO response
// carries {commit_lsn, last_append_lsn}, so any round-trip refreshes the client's model of that member.
struct read_result { std::vector<io_extent> extents; lsn_pair lsns; };

// A resolution-round reply: everything <= resolved_upto is resolved; empty_slots are the Empty verdicts,
// every other formerly-unresolved slot was filled from a holder (the failed write completed late).
struct resolution_result { int64_t resolved_upto; std::vector<int64_t> empty_slots; };
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

Authoritative in memory; recovered from the journal + superblock on restart (the in-memory reference
model, now in `craft_client`, does not persist it).

---

## Client-facing API

### `login`

```cpp
struct LoginResult {
    std::vector<replica_endpoint> members;
    int64_t            dLSN;         // starting (per-partition) LSN for new IO
    uint64_t           term;         // 0 == NOT_LEADER redirect; >0 == session term
    uint32_t           lba_size;     // block size in bytes: client aligns addr/len to it
    boost::uuids::uuid leader_hint;  // non-nil iff redirect (term==0); retry login there
};

async_result<LoginResult> login(volume_handle const& vol, uint64_t client_token);
```

Orchestrates the full login sequence (GetRSCommitLSN broadcast → optional FetchData →
`SyncRSCommitLSN` RAFT → `InternalLogin` RAFT) and returns once both RAFT entries commit. **A follower
returns LoginResult with `term==0` and `leader_hint` set to the current leader's id (not an error)**;
the client finds the matching handle by id and retries login there. `NO_QUORUM` / `REPLICA_DOWN` are
returned as errors. Postconditions: all quorum members have `commit_lsn == rs_commit_lsn`; all reject
IO whose `term` != the new session term.

---

### `async_write`

```cpp
async_result<lsn_pair> async_write(volume_handle const& vol, client_hdr hdr, int64_t dlsn,
                                   uint64_t addr, uint64_t len, sisl::sg_list data);
```

Append the client-assigned write at slot `dlsn`. `addr`/`len` are byte offset/length, block-aligned.
**Empty `data`** (`size == 0`) is a **zero write** (WRITE_ZEROES; `len` is the range to unmap; reads
back as a hole); non-empty `data` is a **data write** of exactly `len` bytes (single contiguous
buffer). Both take a `dLSN` and merge by highest-`dLSN`-per-LBA. Does **not** apply to the LBA index;
`hdr.commit_lsn` rides along and advances the contiguous commit frontier best-effort, in `dLSN` order
(this is CRAFT's commit — piggybacked on the writes the client is already broadcasting). Rejected with
`craft_error::STALE_TERM` if `hdr.term` != the session term, or `std::errc::invalid_argument` on
misalignment. ACK fires on journal-append completion and **returns the achieved
`{commit_lsn, last_append_lsn}`** — every CRAFT IO response piggybacks the watermarks, so any
round-trip refreshes the client's per-member model (its reclaim floor and Missing-map recovery) without
a separate `keep_alive`.

---

### `async_read`

```cpp
async_result<read_result>
async_read(volume_handle const& vol, client_hdr hdr, int64_t read_lsn,
           uint64_t addr, uint64_t len, sisl::sg_list dest);
```

`read_lsn` is the read horizon `H`. Returns the latest version ≤ `H` for `[addr, addr+len)` by **filling
the caller-owned `dest` buffer in place** — data sub-ranges get their bytes, holes get zeros — and
returns `read_result`: the sparse **layout** (`io_extent[]`, ascending by `addr`) marking which byte
sub-ranges were data vs holes, plus the replica's piggybacked **`{commit_lsn, last_append_lsn}`**
(snapshotted atomically with the read; see `async_write`). Data comes from the LBA index if applied, or
straight from the **journal-tail overlay** if only Appended (an overlay read; no index write on the
read path). **Entries above `H` are never served even if the replica holds them** (the sub-quorum
tail). The read never fetches from a peer; the client routes only to a replica that holds every write ≤
`H` overlapping the range. `hdr.commit_lsn` piggybacks a frontier advance. Rejects on term mismatch /
misalignment.

---

### `logout`

```cpp
async_status logout(volume_handle const& vol, client_hdr hdr);
```

Explicit session teardown. Term-fenced (`hdr.term` must match the session term). The leader commits an
`InternalLogout` entry, which clears `client_token` and `term` on all live replicas; subsequent IOs from
the old client fail with `craft_error::STALE_TERM`. Returns `craft_error::NOT_LEADER` on a follower.

---

### `keep_alive`

```cpp
async_result<lsn_pair> keep_alive(volume_handle const& vol, client_hdr hdr);
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

### `request_resolution`

```cpp
async_result<resolution_result> request_resolution(volume_handle const& vol, client_hdr hdr, int64_t upto);
```

The **client-requested resolution round** (the design's client-request `SyncRSCommitLSN` trigger), fired
by the client after a **failed (sub-quorum) write** instead of waiting for the watchdog / periodic
cadence. The round is **leader** work — it reuses the same pre-resolution machinery the
`SyncRSCommitLSN` proposer runs (fetch each unresolved slot ≤ `upto` from a holder, or verdict it Empty
on quorum-lacks evidence, then propose the entry carrying the verdicts) — but the client cannot know who
leads mid-session, so it **broadcasts** this to every member with at most one outstanding per member: the
current leader resolves; a follower returns `craft_error::NOT_LEADER` (or, once peer channels exist, may
forward to its leader). On success everything ≤ `resolved_upto` is resolved set-wide: `empty_slots` were
verdicted Empty (permanent no-op holes; a **late write into an Empty-verdicted slot is rejected** —
reconciliation, Empty beats data — so a voided write fails deterministically on its own ack path), and
every other formerly-unresolved slot was filled and is durable (the failed write completed late).
Term-fenced (`STALE_TERM`).

---

## Internal / peer & RAFT-entry API

These are on `CraftReplDev` directly, not on the public `volume_handle` surface: they are server-to-server
verbs and never cross the client wire, so no CRAFT server exposes them.

### `get_lsns` / `get_rs_commit_lsn`

```cpp
async_result<lsn_pair> get_lsns();           // local snapshot of {commit_lsn, last_append_lsn}
async_result<lsn_pair> get_rs_commit_lsn();  // alias; used by the login leader's GetRSCommitLSN broadcast
```

Peer-to-peer; not a client-facing free function. The login leader polls all replicas via
`get_rs_commit_lsn` to compute `rs_commit_lsn = max(quorum.last_append_lsn)`.

### `truncate`

```cpp
async_status truncate(int64_t lsn);
```

Drop all journal entries with `dLSN > lsn` (stale prior-term tail; login truncation after
`InternalLogin` commits).

### `fetch_data` (peer resync)

```cpp
async_result<std::vector<JournalSlot>> fetch_data(std::vector<int64_t> lsns);
```

Server-to-server raw journal read during `SyncRSCommitLSN` apply. Each `JournalSlot` is one of: a data
slot, a zero write (`all_zeros=true`, no data), or `Empty` (`is_empty=true`); a requested LSN absent
from the result means not-present-here.

### RAFT state machine entries

`SyncRSCommitLSN { rs_commit_lsn, client_token, empty_slots[] }` and
`InternalLogin { client_token, term }` — see [rpcs.md](rpcs.md) and the wiki for apply semantics
(unchanged by the client-surface reshaping above). `CreateVolume { id, size, page_size, members[], name }`
is a partition's first entry, applied by the volume layer (below).

---

## Creating a CRAFT volume

An instance runs CRAFT volumes when it is brought up in CRAFT mode, which runs HomeStore's RAFT service (the
replication mode is process-wide):

```cpp
home_blocks_config{
    ...,
    .repl_mode = replication_mode::CRAFT,
    .repl_svc_port = consensus_port,             // this node's RAFT port
    .lookup_peer = [](peer_id_t const& member) { // a member's RAFT {host, port}, e.g. from the OM
        return std::pair< std::string, uint16_t >{host_of(member), port_of(member)};
    },
};
```

A member is identified by its svc id (`on_svc_id`). `lookup_peer` is called off-reactor whenever the RAFT
layer (re)connects to a member, so it may block.

`create_volume` is called on **one** member of the partition (the receiver), with the whole member set:

```cpp
volume_info info{partition_uuid, size_bytes, page_size, name};
info.repl_mode = replication_mode::CRAFT;
info.members = {member_a, member_b, member_c}; // must include the receiver
auto vol = co_await hb->create_volume(std::move(info));
```

1. The receiver forms the partition's RAFT group (group id = volume id): it creates the group, wins its first
   election and adds every other member. The members persist the group's membership.
2. It proposes `CreateVolume`, the group's first entry. Every member creates its replica (data chunks, its
   own journal, index, `CraftReplDev`) when the entry commits.
3. `create_volume` returns once the entry committed and the receiver created its replica; the other members
   create theirs as they apply it. `get_volume` returns each member's replica on that member.

If a create that formed the group fails, or nothing commits within `craft_create_volume_timeout_ms` (default 30s), it
destroys the group again: a member that already created its replica removes it when the group is destroyed. The page
size must be 4096, the node's data block size, and the name at most 99 bytes without NUL; otherwise the create fails
with `invalid_argument`. Retrying a create that succeeded (same id, size, page size and name) returns the volume; any
other create with that id fails with `invalid_argument`. A concurrent create of the same volume fails with
`operation_in_progress`, and so does one made while an earlier RAFT group of the volume is still being destroyed:
HomeStore reaps a destroyed group, and the replicas follow it, `repl_dev_cleanup_interval_sec` (a minute by default)
after its destroy commits. A group that outlived an earlier create with no replica on this member (the member crashed
before the rollback, or its own create failed) is never destroyed by a later create: its leader proposes `CreateVolume`
again, which every member applies once, and any other member fails the create with `operation_in_progress`.

A restart reopens each replica's journal and reattaches its `CraftReplDev` (the partition's CRAFT state itself
is not recovered yet). A replica whose group is gone is removed. The legacy `async_read` / `async_write` /
`async_unmap` and `remove_volume` return `operation_not_supported` on a CRAFT volume.

---

## Replacing the existing API

The CRAFT free functions supersede the legacy byte block ops (now `[[deprecated]]`, kept for reference):

| Old API (deprecated) | CRAFT replacement |
|---|---|
| `async_write(vol, addr, sgs)` | `async_write(vol, hdr, dlsn, addr, len, data)` → achieved `lsn_pair` |
| `async_read(vol, addr, sgs)` | `async_read(vol, hdr, read_lsn, addr, len, dest)` → `read_result` (layout + `lsn_pair`) |
| `async_unmap(vol, addr, len)` | `async_write(vol, hdr, dlsn, addr, len, /*empty*/ {})` (empty buffer = zero write) |
| — | `login`, `keep_alive` (commit carrier), `request_resolution` (failed-write trigger), `get_lsns` |

The commit watermark rides on these calls (`client_hdr::commit_lsn`); there is no separate `commit`
call. Each handle is **one replica device**, never an aggregate / "whole volume" handle. A handle comes
from `create_volume` / `get_volume` on an instance in CRAFT mode. The in-memory
reference model that used to back a handle in-process moved to the `craft_client` package
(`craft::MemCraftReplica`), so there is no in-tree memory volume here anymore.
