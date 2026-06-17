# CRAFT — Client Assisted RAFT

**CRAFT** (Client Assisted RAFT) is the replication protocol for NuBlox 2.0. It separates the
data path from the consensus path: clients broadcast writes directly to all replicas at
client-assigned LSNs, while RAFT is used only for leader election, login synchronization, and
recovery bookkeeping. Write data never flows through the RAFT log.

## Documents

| File | Contents |
|---|---|
| [protocol.md](protocol.md) | Full protocol — leader election, login, IO phase, failure/resync |
| [api.md](api.md) | HomeBlocks C++ CRAFT API (`CraftReplDev` methods) |
| [rpcs.md](rpcs.md) | All 8 RPCs (client↔server and server↔server) |
| [states.md](states.md) | LSN state machines (client view and replica view) |
| [subtasks.md](subtasks.md) | Implementation sub-task breakdown (SDSTOR-22382 children) |

## Glossary

| Term | Definition |
|---|---|
| **CRAFT** | Client Assisted RAFT — the NuBlox 2.0 replication protocol |
| **dLSN** | Data LSN — a monotonically increasing sequence number in the **data journal** of a single partition/replica-set. Per-volume in NuBlox. |
| **gLSN** | Global LSN — monotonically increasing across all partitions of a volume. |
| **rLSN** | RAFT LSN — the index within the RAFT log. Distinct from dLSN. |
| **term** | RAFT term number, incremented on every new client login. Used by replicas to reject stale IOs. |
| **commit_lsn** | Highest dLSN whose data has been applied to the state machine (index + block map). A committed write is readable. |
| **last_append_lsn** | Highest dLSN whose data has been written to the data journal (possibly not yet committed). |
| **Replica Set (RS)** | The set of HomeBlocks nodes that hold copies of one partition. Typically 3 nodes. |
| **Partition** | A contiguous region of a Volume, replicated across one Replica Set. In NuBlox, partition ≈ volume. |
| **CraftReplDev** | New HomeBlocks replication device class (parallel to `ReplDisk`) that implements CRAFT. |
| **CraftConnector** | New HomeBlocks RPC frontend (parallel to `ScstConnector`) that translates NubloxProto RPCs to `CraftReplDev` API calls. |
| **SyncRSCommitLSN** | A RAFT log entry type. On apply, each replica fetches any missing data up to the encoded dLSN and advances `commit_lsn`. |
| **InternalLogin** | A RAFT log entry type. On apply, stores the new `client_token` and enforces single-writer exclusivity. |
| **Missing** | A dLSN slot that a replica knows about (from a peer or from the RAFT log) but has not yet received data for. |
| **Empty** | A dLSN that was never received by any replica and is not discoverable during resync. Treated as a no-op hole. |

## Key design properties

- **Single writer**: only one client at a time owns a partition (enforced by `InternalLogin` RAFT entry).
- **Leaderless data path**: after login, the RAFT leader has no special role for writes or reads.
- **Client drives commit**: replicas do not commit until told by the client (via `commit` RPC or `min_commit_lsn` in a `read` RPC).
- **Server-side resync**: `SyncRSCommitLSN` lets replicas catch up from each other without client involvement.
- **No HomeStore changes needed**: `CraftReplDev` is built entirely on top of existing HomeStore journal/index/block primitives.
- **Full replacement**: `CraftReplDev` replaces the existing solo `ReplDev` for all volumes. There are no non-CRAFT (ReplDisk/solo) volumes in the final design.
