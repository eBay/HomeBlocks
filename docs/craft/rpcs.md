# CRAFT RPCs

8 RPCs total. 4 client↔server, 4 server↔server (2 via RAFT, 2 non-RAFT).
RAFT internal RPCs (heartbeat, vote, membership) are not listed here.

---

## Client → Server

### 1. Login (Unicast to leader)

```
Request:  { client_token: string | uint64 }
Response: { members: [endpoint], dLSN: int64, term: uint64, gLSN: int64 }
```

Client sends to the RAFT leader. Leader runs the full login orchestration sequence
(GetRSCommitLSN → optional FetchData → SyncRSCommitLSN RAFT → InternalLogin RAFT)
and responds once both RAFT entries commit.

HomeBlocks handler: `CraftReplDev::login()`

---

### 2. Write (Broadcast to all replicas)

```
Request:  { term: uint64, lsn: int64, glsn: int64, lba: uint64, len: uint32, data: bytes }
Response: { status: Status }
```

Client sends to every replica in the set in parallel. Each replica appends `data` to its
data journal at slot `lsn` and ACKs immediately. Write is durable once quorum ACKs.
Data is **not** readable until committed.

Zero-copy is required: `data` must not be copied during journal append.

HomeBlocks handler: `CraftReplDev::write()`

---

### 3. Read (Unicast to chosen replica)

```
Request:  { term: uint64, min_commit_lsn: int64, lba: uint64, len: uint32 }
Response: { status: Status, data: bytes }
```

Client picks a replica whose Missing set does not overlap `[lba, lba+len)`. If
`min_commit_lsn > replica.commit_lsn`, the replica commits inline first. For large
reads crossing multiple LSNs where no single replica is up-to-date, the client splits
the read across replicas.

HomeBlocks handler: `CraftReplDev::read()`

---

### 4. Commit (Broadcast to all replicas)

```
Request:  { term: uint64, lsn: int64 }
Response: { status: Status }
```

Tells replicas to advance `commit_lsn` to `lsn`. May be piggybacked on the next
Write or KeepAlive instead of sent as a standalone RPC.

HomeBlocks handler: `CraftReplDev::commit()`

---

### 5. KeepAlive (Broadcast to all replicas)

```
Request:  { commit_lsn: int64 }
Response: { status: Status }
```

Advances `commit_lsn` and resets the per-replica client-timeout watchdog. Sent
periodically during idle periods and after every quorum-acknowledged write.

HomeBlocks handler: `CraftReplDev::keep_alive()`

---

## Server → Server (non-RAFT)

### 6. GetRSCommitLSN (Broadcast, initiated by leader)

```
Request:  { term: uint64, my_commit_lsn: int64, my_last_append_lsn: int64 }
Response: { term: uint64, commit_lsn: int64, last_append_lsn: int64 }
```

Leader sends to all peers to collect their current LSN state before a `SyncRSCommitLSN`
RAFT proposal. Used during login and on timeout.

HomeBlocks handler: `CraftReplDev::get_rs_commit_lsn()` / `get_lsns()`
Dispatched by: `CraftConnector` (inter-node channel, non-RAFT)

---

### 7. FetchData (Unicast, from behind replica to an ahead peer)

```
Request:  { lsns: [int64] }
Response: { slots: [{ lsn: int64, lba: uint64, len: uint32, data: bytes }] }
```

Called when a replica discovers it is missing data for certain LSNs after receiving a
`SyncRSCommitLSN` RAFT entry. Targets the peer most likely to have the data.

HomeBlocks handler: `CraftReplDev::fetch_data()`
Dispatched by: `CraftConnector` (inter-node channel, non-RAFT)

---

## Server → Server (RAFT)

### 8. SyncRSCommitLSN (RAFT proposal, from leader)

```
RAFT entry payload: { rs_commit_lsn: int64, client_token: uint64 }
```

Proposed by the leader via `CraftReplDev::append()`. On RAFT commit each replica
applies the entry: fetch missing data if behind, then advance `commit_lsn`. This is the
primary recovery mechanism — it does not carry data itself, only the LSN watermark.

---

### 9. InternalLogin (RAFT proposal, from leader during login)

```
RAFT entry payload: { client_token: uint64, term: uint64 }
```

Proposed by the leader after `SyncRSCommitLSN` commits. On apply each replica stores
`client_token` and `term`, rejecting any subsequent IO from a different token. Proposed
immediately after the `SyncRSCommitLSN` entry during the login sequence.

---

## RPC Transport

The transport layer for NubloxProto RPCs is decided by the **CRAFT-1 spike** (SDSTOR-22297
dependency). `CraftConnector` is transport-agnostic: it will dispatch via whatever channel
CRAFT-1 selects (likely gRPC or a custom framing over TCP). Server-to-server RPCs (6 and 7)
use the same transport.

During development, before CRAFT-1 lands, `CraftConnector` can use direct C++ function
calls or a stub transport for unit/integration testing.
