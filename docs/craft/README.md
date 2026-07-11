# CRAFT — Client Assisted RAFT

**CRAFT** (Client Assisted RAFT) is the replication protocol for HomeBlocks block volumes. It
separates the data path from the consensus path: clients broadcast writes directly to all
replicas at client-assigned LSNs, while RAFT is used only for leader election, login
synchronization, and recovery bookkeeping. Write data never flows through the RAFT log.

> **Canonical design:** the CRAFT design lives in two wiki pages: the store-agnostic protocol,
> [**CRAFT Design**](https://github.com/eBay/HomeBlocks/wiki/CRAFT-Design), and its HomeStore
> implementation, [**CRAFT on HomeBlocks**](https://github.com/eBay/HomeBlocks/wiki/CRAFT-on-HomeBlocks).
> Together they are the source of truth. This folder holds reference detail they summarize: RPC
> wire formats ([rpcs.md](rpcs.md)), the C++ API ([api.md](api.md)), and the implementation work
> breakdown ([subtasks.md](subtasks.md)).

## Documents

| File | Contents |
|---|---|
| [protocol.md](protocol.md) | Pointer to the canonical [CRAFT Design](https://github.com/eBay/HomeBlocks/wiki/CRAFT-Design) wiki page |
| [api.md](api.md) | HomeBlocks C++ CRAFT API (`CraftReplDev` methods) |
| [rpcs.md](rpcs.md) | All 8 RPCs (client↔server and server↔server) |
| wire.md | On-wire byte encoding of the client RPCs — **moved to the `craft_client` repo** (`docs/wire.md`), which now owns the wire protocol. |
| transport.md | TCP transport binding behavior — **moved to the `craft_client` repo** (`docs/transport.md`). |
| [states.md](states.md) | Pointer to the canonical wiki page (slot states, read eligibility) |
| [subtasks.md](subtasks.md) | Implementation sub-task breakdown (SDSTOR-22382 children) |

## Glossary

| Term | Definition |
|---|---|
| **CRAFT** | Client Assisted RAFT — the HomeBlocks replication protocol |
| **dLSN** | Data LSN — a monotonically increasing sequence number in the **data journal** of a single partition/replica-set. Dense (contiguous) per partition; the only LSN CRAFT itself uses. |
| **rLSN** | RAFT LSN — the index within the RAFT log. Distinct from dLSN. |
| **term** | CRAFT session term stored via `InternalLogin` (distinct from RAFT's internal election term), incremented on every client login. Used by replicas to reject stale IOs. |
| **commit_lsn (≡ Synced)** | The contiguous applied prefix: every dLSN ≤ it is applied to the LBA index, strictly in dLSN order (Empty slots skipped). Readability is per-write and not gated on it: appended entries above it are served from the journal-tail overlay. |
| **last_append_lsn** | Highest dLSN whose data has been written to the data journal (possibly not yet committed). |
| **Replica Set (RS)** | The set of HomeBlocks nodes that hold copies of one partition. Typically 3 nodes. |
| **Partition** | The independent replica-set CRAFT replicates. A volume comprises one or more partitions; a given LBA maps to exactly one partition. |
| **CraftReplDev** | New HomeBlocks replication device class (parallel to `ReplDisk`) that implements CRAFT. |
| **CraftConnector** | New HomeBlocks RPC frontend (parallel to `ScstConnector`) that translates NubloxProto RPCs to `CraftReplDev` API calls. |
| **SyncRSCommitLSN** | A RAFT log entry type. On apply, each replica fetches any missing data up to the encoded dLSN and advances `commit_lsn`. |
| **InternalLogin** | A RAFT log entry type. On apply, stores the new `client_token` and enforces single-writer exclusivity. |
| **Missing** | A dLSN slot that a replica knows about (from a peer or from the RAFT log) but has not yet received data for. |
| **Empty** | A dLSN proven never quorum-durable, declared by the leader; a permanent no-op the commit skips. |
| **zero write (all_zeros)** | A payload-free write naming just a byte range (WRITE_ZEROES / discard-to-zero), signaled by an **empty `sisl::sg_list`** (no `all_zeros` flag). Takes a dLSN and merges like any write; allocates nothing and unmaps its range on apply, reading back as a hole. |
| **hole** | A read sub-range with no data (never written, zero-written, or an all-zero region collapsed at read time). Returned as a marker, read as zeros; **not** the same as `Missing`. |
| **client_hdr** | The session + watermark fields stamped on every client IO: `{term, commit_lsn, all_committed_lsn}` (see the commit note below). |
| **lba_size** | The volume block size in bytes, returned by `login`; the client aligns every byte `addr`/`len` to it and presents the geometry to the filesystem. |
| **io_extent** | One sub-range of a read's sparse layout, in **bytes**: `{addr, len, hole}` — carries no bytes (they are in the caller's buffer). |

## Key design properties

- **Single writer**: only one client at a time owns a partition (enforced by `InternalLogin` RAFT entry).
- **Leaderless data path**: after login, the RAFT leader has no special role for writes or reads.
- **Client drives commit (piggybacked, no standalone verb)**: the client stamps its `commit_lsn` on every write / read / keep_alive (`client_hdr`); replicas apply to the index strictly in dLSN order at the contiguous frontier. `keep_alive` is the dedicated carrier (commit + watchdog reset, term-fenced). Readability is per-write and does not wait: appended entries are served from the journal-tail overlay (no index write on the read path).
- **Byte-based, one buffer type**: `addr`/`len` are byte offsets/lengths (block-aligned to `lba_size`); `sisl::sg_list` is the single caller-owned buffer both ways (an **empty** write buffer is a zero write). A read fills the caller's buffer in place and returns a sparse `io_extent` layout (data vs holes).
- **Client-routed reads**: reads are unicast, chosen by LBA-overlap against the client's per-replica Missing map (plus `Synced ≥ L`, the login dLSN). The read path never fetches from a peer; fetch is resync-only.
- **Merge key, not serialization**: overlapping writes need no ordering; highest dLSN per LBA wins on every replica.
- **Thin from the start**: a write may be `all_zeros` (WRITE_ZEROES) with no payload; reads return sparse results (data extents + holes), and the server collapses all-zero reads to holes, so reads and resync stay thin. A hole is not `Missing`.
- **Reconfiguration leans on HomeStore**: `replace_member` / learner / snapshot / CP (see the wiki and subtasks S10).
- **Server-side resync**: `SyncRSCommitLSN` lets replicas catch up from each other without client involvement.
- **No HomeStore changes needed**: `CraftReplDev` is built entirely on top of existing HomeStore journal/index/block primitives.
- **Full replacement**: `CraftReplDev` replaces the existing solo `ReplDev` for all volumes. There are no non-CRAFT (ReplDisk/solo) volumes in the final design.
