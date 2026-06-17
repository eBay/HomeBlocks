# CRAFT Protocol

## Overview

Four phases: **Leader Election → Login → IO Phase → Failure/Resync**.

---

## Phase 1: Leader Election

A standard RAFT leader election takes place across all replica set members (S1, S2, S3).
The elected leader handles `login` RPCs from clients. After login, the leader has no special
role in the data path.

---

## Phase 2: Login

The login sequence establishes a new **term**, synchronizes replica state, and returns the
starting LSN to the client. It must complete before any IO is accepted.

```
Client ──login(client_token, vol_id)──► Leader (S1)
                                            │
                                    ┌───────▼────────┐
                                    │ GetRSCommitLSN │  ← non-RAFT broadcast to all peers
                                    └───────┬────────┘
                                    S2: [commit=5, append=7]
                                    S3: [commit=10, append=11]
                                            │
                                    Compute quorum's max dLSN
                                    (e.g., use last_append from quorum)
                                            │
                                    If leader is behind:
                                    ┌───────▼────────┐
                                    │  FetchData(N)  │  ← unicast to ahead peer
                                    └───────┬────────┘
                                    append fetched data to own journal
                                            │
                                    ┌───────▼────────────┐
                                    │ SyncRSCommitLSN(N) │  ← RAFT proposal (rLSN++)
                                    └───────┬────────────┘
                                    On RAFT commit, each follower:
                                      • checks own last_append vs N
                                      • fetches missing data if behind
                                      • advances commit_lsn to N
                                    Replicas with append > N: truncate(N)
                                            │
                                    ┌───────▼──────────────────┐
                                    │ InternalLogin(token, term)│  ← RAFT proposal
                                    └───────┬──────────────────┘
                                    On RAFT commit:
                                      • all replicas store client_token
                                      • reject IOs from any other client
                                            │
                                Client ◄── [members, dLSN=N, term=T, gLSN=G]
```

**Login response fields:**
- `members` — endpoints of all replica set members
- `dLSN` — starting LSN for new IO; minimum for any member to accept reads/writes
- `term` — current term number; client includes this on every IO
- `gLSN` — global LSN (volume-scoped in NuBlox: gLSN = volume LSN, dLSN = partition LSN)

After a successful login, the client opens one queue per replica member and begins IO.

---

## Phase 3: IO Phase

### Write

The client assigns the next dLSN (`++next_lsn`) and broadcasts the write to all replicas.

```
Client ──write(term, lsn, glsn, lba, len, data)──► S1, S2, S3 (broadcast)
  │
  │   Each replica:
  │     1. Validates term == current term
  │     2. Appends data to data journal at dLSN slot
  │     3. Updates last_append_lsn = max(last_append_lsn, lsn)
  │     4. ACKs client
  │
  ├─ Quorum ACKs received → write is durable ("Appended" state)
  │   • Client can ACK the application layer
  │   • Data is NOT yet readable (not committed)
  │
  └─ Commit is sent lazily (via `commit` or piggy-backed on next write / read)
```

**Out-of-order tolerance:** Replicas may receive writes out of order. A write that arrives
before its predecessor is stored but tracked as a "Missing" predecessor (the replica records
the gap). Writes within the same client session are globally ordered by dLSN.

**Overlapping writes:** If two writes overlap LBA ranges, the one with the lower dLSN must
be committed before the other can be read. The client serializes overlapping writes.

### Commit

```
Client ──commit(term, lsn)──► S1, S2, S3 (broadcast)
```

Instructs replicas to advance `commit_lsn` to `lsn` and apply journal entries ≤ lsn
to the state machine (LBA index update, block map finalization). After commit, the data
is readable.

Commit may be piggybacked on the next write or on a keep_alive.

### Read

```
Client ──read(term, min_commit_lsn, lba, len)──► chosen replica (unicast)
```

The client chooses a replica (round-robin, filtered for staleness) and sends the
`min_commit_lsn` it wants the replica to have committed before serving the read.

The replica:
1. If `commit_lsn < min_commit_lsn`: applies journal entries up to `min_commit_lsn` inline.
2. Serves the read from the state machine (LBA index → block read).

**Replica selection:** The client tracks a per-replica "Missing" set (LSNs whose writes were
acknowledged before the read was issued but that the replica has not yet received). If a
replica's Missing set overlaps the read's LBA range, skip to the next replica. For large
reads spanning multiple LSNs, the read may be split across replicas.

### KeepAlive

```
Client ──keep_alive(commit_lsn)──► S1, S2, S3 (broadcast, periodic)
```

Advances `commit_lsn` and resets the client-timeout watchdog on each replica.
If no keepalive is received within the watchdog period, the leader initiates
`SyncRSCommitLSN` (see Failure/Resync below).

---

## Phase 4: Failure / Resync

### SyncRSCommitLSN

Triggered by: login (always), client-timeout watchdog, or periodic checkpoint (every N LSNs,
configurable, default N=128).

```
Leader (S1):
  1. Broadcast GetRSCommitLSN(my_commit, my_last_append) → all peers
  2. Collect [commit_lsn, last_append_lsn] from quorum
  3. Determine rs_commit_lsn = max(quorum's last_append)
     (can use commit_lsn instead for a more conservative choice)
  4. If leader.last_append < rs_commit_lsn:
       FetchData(missing_lsns) from an ahead peer → append to own journal
  5. Propose SyncRSCommitLSN(rs_commit_lsn) via RAFT

Each replica on RAFT apply:
  • If last_append < rs_commit_lsn:
      FetchData(missing_lsns) from a peer → append to local journal
  • commit_lsn = rs_commit_lsn
```

### New Term (client crash / reconnect)

When a new client logs in, a new term T+1 is established.

Replicas with `last_append > dLSN` from the previous term **truncate** those entries:
`truncate(dLSN)` discards journal entries above the agreed starting LSN.

An LSN that was never received by any replica and is not discoverable via FetchData is
marked **Empty** — not an error; it is treated as a hole.

### Periodic RAFT Checkpointing

During IO, the client periodically appends a `SyncRSCommitLSN` entry to the RAFT log
(every N=128 LSNs by default). This gives offline replicas a catch-up anchor without
waiting for the next login.

---

## Invariants

1. A write is **durable** once quorum has appended it to their journals.
2. A write is **readable** only after it has been committed (`commit_lsn ≥ lsn`).
3. `commit_lsn ≤ last_append_lsn` always.
4. Only one client (identified by `client_token`) may issue writes in a given term.
5. A replica will not serve reads from any LBA range that has a Missing predecessor at
   a lower dLSN (stale replica must be synced first).
6. Truncation only removes entries **above** the agreed `dLSN`; entries at or below are
   never discarded.
