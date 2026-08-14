# CRAFT Implementation Sub-tasks

Epic: [SDSTOR-22382](https://jirap.corp.ebay.com/browse/SDSTOR-22382) — CRAFT Server-Side (HB + CraftConnector)

> **Canonical design:** the [**CRAFT Design**](https://github.com/eBay/HomeBlocks/wiki/CRAFT-Design)
> wiki page is the source of truth for the model; this file is the work breakdown.

Two components:
- **Component A**: HomeBlocks CRAFT API (`CraftReplDev` + supporting internals)
- **Component B**: `CraftConnector` (RPC frontend, analogous to `ScstConnector`)

---

## Dependency graph

```
S1 (CraftReplDev foundation)
├── S2 (Write path)
│   └── S3 (Commit + Read path)
├── S4 (Truncate)
├── S6 (Peer data exchange APIs)
│   └── S5 (RAFT state machine entries)
│       └── S7 (Login orchestration)  ← also needs S2, S4
└── S8 (CRAFT volume lifecycle)

S9  (CraftConnector)  ← skeleton early, full handlers need S2/S3/S7
S10 (Reconfiguration / membership change)  ← needs S5, S6, S8
```

Parallel tracks after S1 completes: S2+S3, S4, S6+S5+S7, S8, S9-skeleton.

---

## S1 — CraftReplDev Foundation
**Jira:** [SDSTOR-22383](https://jirap.corp.ebay.com/browse/SDSTOR-22383)
**Blocks:** everything

As an I/O engineer, I want a `CraftReplDev` class in HomeBlocks that wraps HomeStore's
journal/index and participates in RAFT only for `SyncRSCommitLSN` and `InternalLogin`
entries, so that CRAFT-mode volumes store write data directly in the journal rather than
through RAFT log entries.

**Acceptance criteria:**
- `CraftReplDev` exists as a class parallel to (and eventually replacing) `ReplDisk` usage
- Maintains per-partition in-memory state: `commit_lsn`, `last_append_lsn`, `client_token`, `term`
- Provides journal-slot append / read / truncate primitives consumed by all other CRAFT stories
- RAFT group initialized with real member list (not solo); participates in leader election
- RAFT participation limited to: leader election + `SyncRSCommitLSN` entries + `InternalLogin` entries
- Unit-testable with a mock HomeStore journal backend
- All volumes use `CraftReplDev`; the existing solo `ReplDev` is removed

**Key files to create/modify:**
- `src/lib/craft/craft_repl_dev.hpp` / `.cpp` (new)
- `src/lib/homeblks_impl.cpp` — wire up `CraftReplDev` on volume create when craft mode active
- `src/include/homeblks/home_blocks.hpp` — `volume_info` gains `replication_mode` field
- `src/tests/craft/` — mock backend + unit tests

---

## S2 — Write Path
**Blocks:** S3, S7

As an I/O engineer, I want `CraftReplDev::write()` to append client-assigned LSN writes to
the HomeStore data journal (zero-copy, out-of-order tolerant), so that replicas can
independently journal writes broadcast by the client.

**Acceptance criteria:**
- `write(term, lsn, lba, len, data)`: allocate blks up front, write the payload **once** via the data service (zero-copy), then journal the reference record `{term, lsn, lba, len, blkid}` at the `lsn` slot; ACK on journal-append completion (homestore's `HS_DATA_LINKED` pattern; the journal entry is header + key + serialized blkids only, payload never enters the journal). The ACK **returns the achieved `{commit_lsn, last_append_lsn}`** — every CRAFT IO response piggybacks the watermarks (the wire's `write_rsp`)
- **Empty-verdict tombstone:** a slot verdicted `Empty` (S5) is a permanent no-op hole; `write()` REJECTS a late arrival into it (reconciliation: Empty beats data), so a write a resolution round voided fails deterministically on its own ack path
- Truncate (S4) and Empty-discard free the referenced blks; a crash between the data write and the journal append leaves only uncommitted blks, which homestore recovery reclaims
- Rejects with `ETERM` if `term != state.term`
- Updates `last_append_lsn = max(last_append_lsn, lsn)`
- Handles out-of-order LSN arrival without blocking (gaps tracked as Missing); backed by the HomeStore logstore in **out-of-order mode** (`write_async(seq_num)`), not the append-only mode RAFT uses
- Zero-copy: `data` buffer is not copied during the append path
- **Thin (`all_zeros`) write:** `write(term, lsn, lba, len, all_zeros, data)` with `all_zeros=true` (WRITE_ZEROES / discard-to-zero) skips allocation and the data-service write; journal a metadata-only slot `{term, lsn, lba, len, all_zeros}`. Both forms take a dLSN and merge by highest-per-LBA. The client sets `all_zeros` after a client-side zero scan; the server does **not** re-scan a data write
- Does NOT apply data to the LBA index (that is `commit`'s job; a zero write applies as an index delete / range unmap)
- Unit tests cover: in-order writes, out-of-order writes, term rejection, a zero (`all_zeros`) write allocates no blocks and merges over/under a data write by dLSN

---

## S3 — Commit and Read Path
**Blocked by:** S2
**Blocks:** S7, S9 (full)

As an I/O engineer, I want `CraftReplDev::commit()`, `keep_alive()`, and `read()` so that
clients can make writes readable and serve reads without the read path ever fetching from a
peer. See the CRAFT Design wiki page for the model.

**Acceptance criteria:**
- `commit(term, lsn)`: advances the contiguous commit watermark `commit_lsn` (≡ Synced) by applying present entries **strictly in dLSN order** (skipping Empty slots) and reclaiming superseded blocks (a data entry applies as an index insert; a zero `all_zeros` entry as an index delete / range unmap); **returns the achieved `{commit_lsn, last_append_lsn}`** (best-effort: `commit_lsn` stalls just below the first Missing hole, not an error; only apply + reclaim pause, never reads). The index is never written out of order, so no per-entry version checks are needed and the index at any CP is an exact prefix.
- `keep_alive(commit_lsn, all_committed_lsn)`: same as commit + resets the client-timeout watchdog timer; **returns `{commit_lsn, last_append_lsn}`** (feeds the reconfig promotion gate). `all_committed_lsn` (set-wide min, client-computed) lets the replica reclaim journal below `min(all_committed_lsn, checkpointed apply frontier)`.
- `read(term, readLSN, lba, len, dest)`: returns the latest version ≤ `readLSN` (horizon `H`) for the range, from the index if applied or from the **journal-tail overlay** if only Appended (overlay read; **no index write on the read path**). Fills the caller-owned `dest` in place and returns `craft::read_result` — the sparse layout **plus the piggybacked `{commit_lsn, last_append_lsn}`**. No peer fetch on the read path.
- **Thin reads (sparse + holes):** the result is data extents interleaved with **holes** for sub-ranges with no data (never written, a committed `all_zeros` write, or an all-zero region collapsed by a read-time scan); a hole is a marker, never a materialized zero buffer, and is **not** `Missing`. The read-time scan is the only zero-scan on the server (the write path never re-scans).
- **Server-side horizon clamp:** the replica serves only versions ≤ `readLSN`; a sub-quorum tail write above it (held in the overlay) is never returned. Unit test: with a locally-held write at LSN X, a read at `readLSN < X` returns the ≤`readLSN` version, not the held one.
- **Journal-tail overlay:** in-memory map (LBA → highest-dLSN unapplied entry) over `(commit_lsn, last_append]`; consulted by reads, updated on append/apply/truncate.
- **Recovery-aware:** on restart, rebuild the overlay (appended-above-`commit_lsn` set) from the FUA-durable journal (never reclaimed above the checkpointed frontier), so every appended entry is readable again after a crash.
- Read eligibility is enforced by the client (LBA-overlap against its per-replica Missing map, plus `Synced ≥ L`); a lagging member is never sent a read it cannot serve.
- All three reject with `ETERM` if `term != state.term`
- Watchdog timer: if no `keep_alive` or `write` arrives within configurable timeout, trigger `SyncRSCommitLSN` (see S5)
- Unit tests cover: commit-then-read, overlay read of an appended entry above a hole, in-order apply after the hole fills (same stable state as a no-hole run), overlay rebuild on restart, LBA-overlap routing (client picks an eligible replica), watchdog fire, a zero write reads back as a hole, a data write of all-zero bytes also reads back as a hole (read-time scan)

---

## S4 — Truncate Path
**Blocked by:** S1
**Blocks:** S7

As an I/O engineer, I want `CraftReplDev::truncate(lsn)` to drop journal entries above the
given LSN, so that replicas can clean up stale writes from a previous term when a new login
starts.

**Acceptance criteria:**
- `truncate(lsn)` removes all data journal entries with dLSN > `lsn`
- Updates `last_append_lsn = min(last_append_lsn, lsn)`
- Clears any Missing-set entries above `lsn`
- Does not affect entries ≤ `lsn`
- Safe to call concurrently with in-flight reads at committed LSNs ≤ `lsn`
- Unit tests cover: truncate with committed entries below, truncate with missing entries, idempotency

---

## S5 — RAFT State Machine Entries (SyncRSCommitLSN + InternalLogin)
**Blocked by:** S1, S6
**Blocks:** S7

As an I/O engineer, I want `SyncRSCommitLSN` and `InternalLogin` RAFT log entry types
implemented in `CraftReplDev`, so that replicas can converge on a consistent LSN watermark
and enforce single-writer exclusivity without data flowing through the RAFT log.

**Acceptance criteria:**

**SyncRSCommitLSN:**
- RAFT entry carries `{rs_commit_lsn, client_token, empty_slots[]}`
- **Leader pre-resolution:** before proposing `N`, the leader resolves every unresolved slot ≤ `N`: fetch it from any holder, or record an `Empty` verdict on quorum-lacks evidence (leader counts itself; non-responders never count); it must not propose past an unresolved slot
- On apply: verify token; mark `empty_slots` Empty, **discarding any local data held there** (reconciliation); if behind, `fetch_data()` the remaining missing slots from peers; then `commit_lsn = rs_commit_lsn`. **Apply never truncates** and replicas **never declare Empty unilaterally**
- Peer catch-up (`CraftPeerFetcher::fetch_from_peer`) is **timeout-bounded**: every call passes `peer_fetch_timeout_ms` (`home_blks_config.fbs`, default 5000ms); a peer that misses the deadline is treated as a hard failure, same as any other fetch failure (best-effort — `commit_lsn` still advances, unresolved LSNs stay missing). The interface only carries the deadline; enforcing it against a real wire call is S9's (the transport's)
- `append(sync_to, client_token)` proposes this entry via RAFT
- Triggers: periodic every N LSNs (configurable via `home_blks_config.fbs`, default 128), watchdog, login, **client-requested (after a failed sub-quorum write)** — the client's `Resolve` RPC lands on `CraftReplDev::request_resolution(term, upto)`, which runs this same leader pre-resolution and returns the Empty verdicts ≤ `upto` (`craft::resolution_result`). The client broadcasts it to every member (it cannot know the leader mid-session); a follower returns `NOT_LEADER`

**InternalLogin:**
- RAFT entry carries `{client_token, term}`
- On apply: `state.client_token = client_token`, `state.term = term`
- All subsequent IO with a different `term` is rejected
- Proposed immediately after `SyncRSCommitLSN` commits during login

**Unit tests:** mock RAFT; verify apply callbacks for both entry types; verify fetch_data is
called when behind; verify term enforcement after InternalLogin.

---

## S6 — Peer Data Exchange APIs
**Blocked by:** S1
**Blocks:** S5

As an I/O engineer, I want `get_rs_commit_lsn()` / `get_lsns()` and `fetch_data()` on
`CraftReplDev`, so that the leader can poll peers during login/sync and lagging replicas can
pull missing journal data during `SyncRSCommitLSN` apply.

**Acceptance criteria:**
- `get_lsns(vol_id)` / `get_rs_commit_lsn()` returns `{commit_lsn, last_append_lsn}` for the local partition
- `fetch_data(lsns)` reads raw journal data for the requested LSNs and returns it (without applying to state machine)
- These are called server-to-server via `CraftConnector` (see S9); stub the transport for unit tests
- `fetch_data` returns four-way per slot: present+data, **present+zero** (`all_zeros=true`, no data, applied as a range unmap), **omitted** (not-present-here), or `is_empty=true` — the last **only** for a slot the peer has *positively* marked Empty (via a prior verdict), never for one it merely lacks
- The `Empty` **verdict is leader-only** (S5 pre-resolution); `fetch_data` responders report state, they never decide
- Unit tests cover: normal fetch, fetch across commit boundary, fetch of Empty slot

---

## S7 — Login Orchestration (Leader-side)
**Blocked by:** S2, S4, S5, S6
**Blocks:** S9 (full)

As an I/O engineer, I want `CraftReplDev::login()` to orchestrate the full login sequence on
the RAFT leader, so that a new client attachment establishes a consistent starting LSN and
term across the replica set.

**Acceptance criteria:**
- Implements the leader-side login sequence:
  1. Broadcast `GetRSCommitLSN(is_login=true)` to all peers; each **quiesces prior-session writes** before replying (fencing barrier); collect `{commit_lsn, last_append_lsn}`
  2. Compute `rs_commit_lsn` = quorum's max `last_append_lsn` (forced; no `commit_lsn` variant, see the watermark deep-dive in the wiki)
  3. If `self.last_append_lsn < rs_commit_lsn`: `fetch_data(missing)` from ahead peer, append to own journal
  4. Propose `SyncRSCommitLSN(rs_commit_lsn)` via RAFT; wait for commit
  5. Propose `InternalLogin(client_token, term+1)` via RAFT; wait for commit
  6. Replicas with `last_append > rs_commit_lsn` receive `truncate(rs_commit_lsn)` call
  7. Return `{members, dLSN=rs_commit_lsn, term=term+1}`
- **Quiesce release:** if the login aborts (cannot reach quorum) the leader signals release, and replicas also time out the quiesce independently as a backstop, so a failed login cannot wedge the current owner
- **Phase-1b fallback:** a slot ≤ `rs_commit_lsn` absent from the whole responding quorum was never quorum-durable and is marked `Empty`; if quorum is unreachable the login fails and the client retries
- Returns `ENOTLEADER` if called on a follower
- Login is serialized (only one in-flight login per partition at a time)
- Integration test: 3-node mock cluster; login with divergent replica state; verify all replicas converge

---

## S8 — CRAFT Volume Lifecycle
**Blocked by:** S1

As an I/O engineer, I want HomeBlocks to create and recover CRAFT-mode volumes with a
multi-member `CraftReplDev` RAFT group, so that volumes can be provisioned and survive
restarts without losing LSN state.

**Acceptance criteria:**
- The existing solo `ReplDev` creation path in `homeblks_impl.cpp` is replaced with `CraftReplDev` for all volumes
- On `create_volume` with `craft` mode: create a multi-member RAFT group with the provided member endpoints
- `vol_sb_t` persists `commit_lsn` and `last_append_lsn` (or they are recoverable from the journal on restart)
- On `HomeBlocksImpl` restart: `CraftReplDev` recovers `{commit_lsn, last_append_lsn, term}` from the journal/superblock
- Journal reclaim: truncate the data journal below `min(checkpointed apply frontier, all_committed_lsn)` (see the wiki's journal-backing note)
- RAFT-log compaction tied to the reclaim floor (compact below the `SyncRSCommitLSN` entry at the floor), so a member that fell past the retention window is RAFT-log-behind and nuraft's snapshot machinery (S10 callbacks) catches it up — returning voters as well as joiners; no separate trigger
- Member add/remove stubs (the full reconfiguration path is S10)
- Existing volume creation/removal lifecycle tests pass with CRAFT mode

---

## S9 — CraftConnector
**Blocked by (skeleton):** nothing  
**Blocked by (full handlers):** S2, S3, S7

As an I/O engineer, I want a `CraftConnector` class (analogous to `ScstConnector`) that
receives NubloxProto RPCs and translates them 1-to-1 to `CraftReplDev` API calls, so that
the RPC layer and storage layer have a clean boundary with no storage logic in the connector.

**Acceptance criteria:**
- `CraftConnector` class exists; transport is pluggable (CRAFT-1 spike determines final choice)
- Client-facing handlers: `Login` (routes by the request's `volume_id`), `Write`, `Read`, `KeepAlive`,
  `Resolve` (there is no `Commit` verb: the watermark rides on `client_hdr.commit_lsn`, see
  [rpcs.md](rpcs.md))
- Server-to-server handlers: `GetRSCommitLSN`, `FetchData`, `GetLSNs` (used during login and resync)
- Each handler translates NubloxProto types ↔ `CraftReplDev` types with no storage logic
- Leader redirect: if a `Login` arrives at a follower, return leader endpoint
- Term mismatch: return `ETERM` to client
- Blocked on CRAFT-1 for the real transport; initial version uses direct function-call stubs
- Integration test: end-to-end Login + Write + Read + Commit through the connector

---

## S10 — Reconfiguration (Membership Change)
**Blocked by:** S5, S6, S8

As an I/O engineer, I want CRAFT to add / replace / remove a replica-set member by leaning on
HomeStore's `replace_member` machinery, so that the replica set can self-heal and rebalance
without losing durability or read-safety. See the CRAFT Design wiki page for the design.

**Acceptance criteria:**
- Single-member-at-a-time changes; drive HomeStore `replace_member` (learner phase via `flip_learner_flag`); the listener implements `on_start_replace_member` / `on_complete_replace_member` / `on_clean_replace_member_task` / `on_remove_member`.
- Snapshot callbacks (`create_snapshot` / `read_snapshot_obj` / `write_snapshot_obj` / `apply_snapshot`) ship CRAFT's committed index + blocks, sourced from a HomeStore CP boundary (`cp_mgr().trigger_cp_flush`), modeled on homeobject's `ReplicationStateMachine`.
- Catch-up is pull-based: backlog via the snapshot path, tail via `SyncRSCommitLSN` -> `FetchData`.
- Promotion (learner -> voter) gated by CRAFT's `commit_lsn ≥ startLSN` (carried in the keep_alive response), interposed on `complete_replace_member`.
- No removal fence: the live tail across removal is covered by broadcast + at-least-one-surviving-holder + server-side resync; tail loss requires a dual failure (see the wiki's Reconfiguration live-tail note).
- Detached recovery: a committed logout / lease record lets the RAFT leader self-elect to drive resync while no client is attached; it yields on the next committed `InternalLogin`.
- Term-bump / re-login on `HS_CTRL_COMPLETE_REPLACE` so an attached client refreshes its member set.
- Integration test (in-process, multiple `HomeBlkDisk` instances): replace a member, verify catch-up + promotion, no data loss, reads stay safe.

---

## Out of scope for this epic

- API/proto definition and RPC schema → **SDSTOR-22297**
- RPC transport/framing selection → **CRAFT-1 spike**
- Client-side quorum logic, partition management → client-side epic
- CSI / ublk-nublox changes → client-side epic
