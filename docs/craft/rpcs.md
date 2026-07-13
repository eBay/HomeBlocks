# CRAFT RPCs

9 RPCs total. 5 client↔server (Login, Write, Read, KeepAlive, Resolve), 4 server↔server (2 via RAFT,
2 non-RAFT). RAFT internal RPCs (heartbeat, vote, membership) are not listed here.

> **Commit is not a separate RPC.** The commit watermark rides on `client_hdr.commit_lsn`, piggybacked
> on Write / Read / KeepAlive; KeepAlive is its dedicated carrier (commit + watchdog reset). Readability
> is per-write and does not wait for it (journal-tail overlay reads).

> **Every IO response piggybacks the replica's watermarks.** Write / Read / KeepAlive responses all carry
> the achieved `{commit_lsn, last_append_lsn}`, so any round-trip refreshes that member's entry in the
> client's model (its reclaim floor and Missing-map recovery) — the response-side mirror of the
> `client_hdr` watermarks every IO request carries.

> **Canonical design:** the [**CRAFT Design**](https://github.com/eBay/HomeBlocks/wiki/CRAFT-Design)
> wiki page is the source of truth for the protocol, and
> [**CRAFT on HomeBlocks**](https://github.com/eBay/HomeBlocks/wiki/CRAFT-on-HomeBlocks) for the
> implementation binding; this file is the RPC wire-format reference. The C++ API is in [api.md](api.md).

---

## Common header

Every client IO (Write, Read, KeepAlive) carries a `client_hdr`:

```
client_hdr: { term: uint64, commit_lsn: int64, all_committed_lsn: int64 }
```

- `term` — fences a stale writer (rejected `ETERM`); checked on **every** IO including KeepAlive, so a
  deposed client cannot even reset the liveness watchdog.
- `commit_lsn` — advance the contiguous commit frontier toward this, best-effort, in dLSN order
  (`-1` = don't advance). This is CRAFT's commit, piggybacked.
- `all_committed_lsn` — set-wide min commit_lsn; floors journal reclaim (`-1` = unknown).

**Addressing is byte-based:** `addr` / `len` are byte offset/length, block-aligned to the volume's
`lba_size` (from Login), else `EINVAL`.

---

## Client → Server

### 1. Login (Unicast to leader)

```
Request:  { volume_id: uuid, client_token: uint64 }
Response: { members: [endpoint], dLSN: int64, term: uint64, lba_size: uint32 }
```

Client sends to the RAFT leader (a follower replies `NOT_LEADER` + leader endpoint). Leader runs the
full login orchestration (GetRSCommitLSN → optional FetchData → SyncRSCommitLSN RAFT → InternalLogin
RAFT) and responds once both RAFT entries commit. `volume_id` names the volume to establish the session
on (the design's `login(client_token, vol_id)`): a multi-volume server routes by it, and it is the same
id every follow-on connection presents in HELO (see the wire doc in `craft_client`). `dLSN` is the
starting per-partition LSN. `lba_size` is the volume block size in bytes — the client aligns every
addr/len to it and presents the geometry to the filesystem.

HomeBlocks handler: `CraftReplDev::login()`

---

### 2. Write (Broadcast to all replicas)

```
Request:  { hdr: client_hdr, dlsn: int64, addr: uint64, len: uint64, data: bytes }
Response: { status: Status, commit_lsn: int64, last_append_lsn: int64 }
```

Client sends to every replica in the set in parallel at the client-assigned `dlsn`. Each replica
appends the entry to its data journal at slot `dlsn` and ACKs immediately, returning its achieved
`{commit_lsn, last_append_lsn}` (the piggybacked watermarks; see the note at the top). Durable once
quorum ACKs, readable once Appended (served from the journal-tail overlay until applied).

**Empty `data` (length 0) is a zero write** (WRITE_ZEROES / discard-to-zero): the slot is metadata-only,
allocates no blocks, and on apply unmaps its range (`len` is the range). Non-empty `data` is a data
write of exactly `len` bytes. There is **no `all_zeros` flag** — the empty buffer is the signal. Both
take a `dLSN` and merge by highest-`dLSN`-per-LBA. `hdr.commit_lsn` piggybacks a frontier advance.

HomeBlocks handler: `CraftReplDev::write()`

---

### 3. Read (Unicast to chosen replica)

```
Request:  { hdr: client_hdr, read_lsn: int64, addr: uint64, len: uint64 }
Response: { status: Status, commit_lsn: int64, last_append_lsn: int64,
            layout: [{ addr: uint64, len: uint64, hole: bool }], data: bytes }
```

`read_lsn` is the read horizon `H`. The client picks an *eligible* replica (one filled to the login
dLSN `L`, with no Missing slot ≤ `H` overlapping the range). The replica returns the latest version ≤
`H` for `[addr, addr+len)`, from the LBA index if applied or straight from the journal-tail overlay if
only Appended (an **overlay read**; no index write). It **ignores any write above `H` even if it holds
it** (the sub-quorum tail). The read path **never fetches from a peer**; large reads may be split across
replicas.

The response is **sparse**: `layout` marks which byte sub-ranges are **data** vs **holes**
(`hole=true`: never-written, zero-written, or an all-zero region collapsed by a read-time scan — read
as zeros, **not** `Missing`); `data` carries the bytes for the data extents. The client places them
into its caller-owned (iomgr) destination buffer per the layout (holes → zeros). `hdr.commit_lsn`
piggybacks a frontier advance.

HomeBlocks handler: `CraftReplDev::read()`

---

### 4. KeepAlive (Broadcast to all replicas)

```
Request:  { hdr: client_hdr }
Response: { status: Status, commit_lsn: int64, last_append_lsn: int64 }
```

The dedicated commit carrier: advances `commit_lsn` (in-order apply, skipping Empty, reclaiming
superseded blocks) toward `hdr.commit_lsn` **and resets the per-replica client-timeout watchdog** (which
is why it is term-fenced — a stale client must not keep the session alive). The response carries the
**achieved** `{commit_lsn, last_append_lsn}` (best-effort: stalls below the first Missing hole), which
feeds the client's Missing map and the reconfig promotion gate. `hdr.all_committed_lsn` lets the replica
reclaim journal below it. Sent periodically during idle periods and after quorum-acknowledged writes.

HomeBlocks handler: `CraftReplDev::keep_alive()`

---

### 5. Resolve (Broadcast to all replicas; the leader resolves)

```
Request:  { hdr: client_hdr, upto: int64 }
Response: { status: Status, resolved_upto: int64, empty_slots: [int64] }
```

The **client-requested resolution round** (the design's client-request `SyncRSCommitLSN` trigger),
fired after a **failed (sub-quorum) write** instead of waiting for the watchdog / periodic cadence.
The client cannot know who leads mid-session, so it broadcasts the request to every member (at most one
outstanding per member, like its keep_alive drive): the **current leader** resolves every unresolved
slot ≤ `upto` — fetch from any holder, or verdict `Empty` on quorum-lacks evidence, the same
pre-resolution it runs before any `SyncRSCommitLSN` proposal — and proposes the entry carrying the
verdicts; a follower replies `NOT_LEADER` (or forwards to its leader) and MUST NOT resolve anything
itself. The response lists every `Empty` verdict ≤ `resolved_upto` (including earlier rounds', so a
client whose previous reply was lost still learns them); every other formerly-unresolved slot was
filled and is durable — the failed write completed late. A slot verdicted `Empty` is a permanent no-op
hole: a replica **rejects** a late write into it (reconciliation: Empty beats data), so a write the
round voided fails deterministically on its own ack path. Term-fenced.

HomeBlocks handler: `CraftReplDev::request_resolution()`

---

## Server → Server (non-RAFT)

### 6. GetRSCommitLSN (Broadcast, initiated by leader)

```
Request:  { term: uint64, my_commit_lsn: int64, my_last_append_lsn: int64, is_login: bool }
Response: { term: uint64, commit_lsn: int64, last_append_lsn: int64 }
```

Leader polls peers for their LSN state before a `SyncRSCommitLSN` proposal (login and on timeout).
`is_login=true` makes the peer **quiesce prior-session writes** before reporting `last_append` (the
fencing barrier); watchdog/periodic polls carry `is_login=false` and never quiesce. Only `last_append`
feeds the recovery watermark; `commit_lsn` is a contiguity certificate that bounds the leader's
hole-resolution window, seeds `all_committed_lsn`, and carries the reconfig promotion gate.

HomeBlocks handler: `CraftReplDev::get_rs_commit_lsn()` / `get_lsns()`
Dispatched by: `CraftConnector` (inter-node channel, non-RAFT)

---

### 7. FetchData (Unicast, from behind replica to an ahead peer)

```
Request:  { lsns: [int64] }
Response: { slots: [{ lsn: int64, is_empty: bool, all_zeros: bool, lba: uint64, len: uint32, data: bytes }] }
```

Called when a replica discovers it is missing data for certain LSNs after a `SyncRSCommitLSN` entry.
**Four-way per slot:** `is_empty=false, all_zeros=false` carries `data`; `all_zeros=true` is a zero write
(no data, applied as a range unmap); `is_empty=true` means the peer **positively** marked that slot
`Empty` (leader-only verdict); a requested `lsn` **omitted** means *not-present-here*. A peer never
returns `is_empty=true` for a slot it merely lacks.

HomeBlocks handler: `CraftReplDev::fetch_data()`
Dispatched by: `CraftConnector` (inter-node channel, non-RAFT)

---

## Server → Server (RAFT)

### 8. SyncRSCommitLSN (RAFT proposal, from leader)

```
RAFT entry payload: { rs_commit_lsn: int64, client_token: uint64, empty_slots: [int64] }
```

Proposed by the leader via `CraftReplDev::append()` — triggered by login, the watchdog, the periodic
checkpoint, or the client-requested **Resolve** RPC (#5). **Before proposing**, the leader resolves
every unresolved slot ≤ `rs_commit_lsn`: fetch from any holder, or record an `Empty` verdict on
quorum-lacks evidence; it never proposes past an unresolved slot. On RAFT commit each replica: verify
the token, mark `empty_slots` as permanent no-op holes (discarding any local data there), fetch the
remaining missing slots from peers, then advance `commit_lsn`. Replicas never declare `Empty`
unilaterally. This is the primary recovery mechanism — it carries no write data, only the watermark
and verdicts.

---

### 9. InternalLogin (RAFT proposal, from leader during login)

```
RAFT entry payload: { client_token: uint64, term: uint64 }
```

Proposed after `SyncRSCommitLSN` commits. On apply each replica stores `client_token` and `term`,
rejecting any subsequent IO from a different term. Proposed immediately after the `SyncRSCommitLSN`
entry during the login sequence.

---

## RPC Transport

**The client plane is decided; the peer plane is not.** These are two different questions and they were
previously answered as one.

- **Client plane** (a CRAFT client ↔ a replica: LOGIN / WRITE / READ / KEEPALIVE / LOGOUT / RESOLVE).
  Settled by the CRAFT-1 spike: a packed binary wire over TCP/io_uring, specified in
  [craft_client/docs/wire.md](https://github.com/szmyd/craft_client) with opcodes **1–14** allocated and a
  reference client + server that pass it end to end. `CraftConnector` terminates *this* wire.
- **Peer plane** (a replica ↔ a replica: `GetRSCommitLSN` / `fetch_data` / `truncate`, driven as a side effect
  of applying a `SyncRSCommitLSN` or `InternalLogin` RAFT entry). **Not specified.** No opcode is allocated for
  it — `craft::wire::op` stops at 14 — so it is deferred at the *wire*, not merely at the transport. It need not
  ride the same channel as the client plane: these calls are node-to-node, and HomeBlocks may prefer its existing
  inter-node RPC. See `craft_client/docs/peer-plane.md` for the interface (`craft_peer`) and what remains.

Note the asymmetry this creates, because it is easy to get backwards: HomeBlocks is **both ends** of the peer
plane (it initiates on RAFT commit *and* serves peers) but **only the far end** of the client plane (it never
builds a CRAFT client).

Before that lands, the wire is exercised **entirely inside the `craft_client` package**, not here: its
in-memory reference model (`craft::MemCraftReplica`) stands in for a HomeBlocks replica, and its reference
TCP server (`craft::net::craft_tcp_server`) stands in for `CraftConnector` -- so the client, the codec and
the server are all driven end-to-end with no storage engine. `CraftConnector` is the piece that replaces
that reference server: same wire, same handlers, but resolving a `volume_handle` and calling the HomeBlocks
CRAFT free functions ([api.md](api.md)) instead of the reference model.

The model is a stand-in, not a shortcut. It completes every reply on a thread other than the submitter's,
never inline, so the client is exercised against the execution model a real transport imposes: replies
arrive on a foreign thread, out of order, and a write may be acked by a quorum while stragglers are still
in flight.
