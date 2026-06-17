# CRAFT LSN State Machines

---

## Client-side write states

The client tracks each write through the following states:

```
Queued/Blocked ──► Pending ──► Appended ──► Committed
```

| State | Meaning |
|---|---|
| **Queued / Blocked** | Received from the application block layer; not yet sent to any replica. May be blocked behind an overlapping in-flight write. |
| **Pending** | Sent to replicas; fewer than quorum have ACKed. |
| **Appended** | Quorum of replicas have ACKed (written to their data journals). The write is durable but **not yet readable**. The client may ACK the application. |
| **Committed** | The client has sent a `commit` (or `keep_alive`) for this LSN and at least one replica has applied it to its state machine. The write is readable. |

Once a write reaches **Committed**, the client drops it from its tracking state.

---

## Replica-side slot states

Each dLSN slot on a replica is in one of these states:

| State | Meaning |
|---|---|
| **Appended** | Data received from the client and written to the data journal. Not yet committed. |
| **Committed** | Journal entry applied to the state machine (LBA index updated, block map finalized). Readable. |
| **Empty** | Slot that was never received and was not found on any peer during resync. Treated as a permanent hole; not an error. |
| **Synced** | All LSNs ≤ this slot are committed. Indicates a clean checkpoint. |
| **Missing** | The replica knows the slot should exist (from context: a higher LSN arrived, or a `SyncRSCommitLSN` entry referenced it) but the data has not yet arrived. The replica must fetch this slot before it can commit past it. |

---

## Transitions

```
              write() RPC received
                       │
                       ▼
                  [Appended]
                       │
          commit()/keep_alive() received
          OR min_commit_lsn in read() ≥ this lsn
                       │
                       ▼
                  [Committed]  ────────────► readable from state machine

              gap detected (higher LSN arrived first,
              or SyncRSCommitLSN references this lsn)
                       │
                       ▼
                  [Missing]
                       │
               fetch_data() completes
                       │
                       ▼
                  [Appended] ───► (then Committed as above)

              SyncRSCommitLSN applied, slot not on any peer
                       │
                       ▼
                  [Empty]  ── permanent hole, skipped in commit advance
```

---

## Per-replica tracking summary

Each replica maintains:
- `commit_lsn` — highest LSN fully committed to state machine
- `last_append_lsn` — highest LSN written to the data journal
- A set of **Missing** LSN slots (gaps between `commit_lsn` and `last_append_lsn`)

The client additionally tracks:
- `next_lsn` — counter for the next write assignment
- Per-replica **Missing** sets (from the client's perspective: writes that reached quorum
  but not yet a specific replica)

---

## Read eligibility

A replica is eligible to serve a read for LBA range `[lba, lba+len)` if:
1. `commit_lsn >= min_commit_lsn` (after inline commit if needed)
2. No **Missing** entry at a dLSN ≤ read's target LSN overlaps the LBA range

If no single replica satisfies (2) for the full range, the client may split the read across
replicas such that each sub-range is served by an eligible member.
