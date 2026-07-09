# craft_client: dLSN tracking, the read horizon, and the split read

This directory holds the reference CRAFT client and the ublk driver above it. The model under
`../model/` is a temporary backing store; the client is the piece meant to graduate to production once a
network shim replaces the in-memory `volume_handle`.

Everything hard about the client lives in one place: **which version of a block may a read see, and how
does that follow from writes that are still in flight?** `dlsn_tracker` answers it. This doc is the map.

Design source: `HomeBlocks.wiki/CRAFT-Design.md`, sections *Write, Commit, Read, and readLSN protection*,
*Two-sided read safety*, and *Split reads*.

---

## The pieces

| | |
|---|---|
| `dlsn_tracker` | The whole state machine. Assigns dLSNs, tracks each slot's fate, derives the commit frontier and the read horizon. |
| `sisl::StreamTracker<dlsn_slot>` | The spine, keyed by dLSN. Two `AtomicBitset`s (created / completed) over a flat slot array. Same primitive `home_log_store::m_records` uses. |
| `resolution_gate` | A re-armable event. Only the pathological read paths ever wait on it. |
| `craft_client` | Broadcast, quorum tally, login/redirect. Owns a `dlsn_tracker`. |

### Three numbers

```
   F   = frontier_       contiguous RESOLVED prefix.  == client_hdr.commit_lsn, == journal reclaim floor
   Ha  = highest_acked_  highest quorum-acked dLSN
   H   = read horizon    per-read (and, when they disagree, per-block)
```

The load-bearing trick is what the tracker's *completed* bit means. It means **resolved**, not "acked":

```
   slot_outcome::acked  ─┐
                         ├──> resolved  ──> completed bit set  ──> F may advance past it
   slot_outcome::empty  ─┘

   slot_outcome::in_flight ─┐
                            ├──> unresolved ──> bit stays clear ──> F PINS here
   slot_outcome::failed    ─┘
```

Which buys the invariant **every slot ≤ F is resolved**. So "is anything unresolved at or below `H`?"
collapses to `F < H`, and the common read is two atomic loads. It also means a failed write pins `F`
automatically — which is exactly the `commit_lsn` the client is supposed to piggyback, since commit must
not pass a slot the leader has not yet filled or Emptied.

---

## A write

```
 craft_client::write(addr, len, data)
        │
        │  ┌──────────────────────────────────────────────────────────────────┐
        ├─►│ tracker.reserve(addr, len)                                       │
        │  │   d = next_dlsn_.fetch_add(1)          ← dense, monotonic        │
        │  │   StreamTracker.create(d, {addr,len,in_flight})                  │
        │  │       payload ─► set active bit  (release; see note below)       │
        │  └──────────────────────────────────────────────────────────────────┘
        │        MUST happen-before the broadcast: any replica that can hold
        │        this slot implies a concurrent read's scan can already see it.
        │
        ├─► async_write(replica[0], hdr{term, F, -1}, d, addr, len, data) ─┐
        ├─► async_write(replica[1], ...)                                   ├─ when_all
        ├─► async_write(replica[2], ...) ─────────────────────────────────┘
        │
        │   tally acks
        │     acks ≥ quorum ......................... outcome = acked
        │     acks < quorum, every replica refused ... outcome = empty   (provably absent set-wide)
        │     acks < quorum, otherwise .............. outcome = failed  (some replica MAY hold it)
        │
        │  ┌──────────────────────────────────────────────────────────────────┐
        └─►│ tracker.resolve(d, outcome)                                      │
           │   StreamTracker.update(d, ...)                                   │
           │       outcome ─► set completed bit if resolved  (release)        │
           │   if acked:  CAS-max highest_acked_         (release; RMW chain) │
           │   if resolved: advance_frontier()                                │
           │       F' = min(completed_upto(F+1), next_dlsn-1);  CAS-max F     │
           │       every kTruncBatch: StreamTracker.truncate(F)               │
           │   gate.signal()                                                  │
           └──────────────────────────────────────────────────────────────────┘
```

Note the resolve ordering: the outcome and the completed bit are published **before** `highest_acked_`.
Because `highest_acked_` is only ever raised by a CAS (never a plain store), every publication of `Ha`
joins one release sequence, so a reader that acquire-loads *any* later `Ha` also sees the outcome of
every slot ≤ `Ha`. That is what lets the read path trust `acked` without further synchronization.

The write path never takes a mutex around an STL container. `StreamTracker`'s `shared_mutex` is taken
*shared* by `create`/`update`/`completed_upto`; only `truncate` (throttled to one per 512 resolves) and
an array resize take it exclusively.

---

## A read

```
 craft_client::read(addr, len, dest)
        │
        │  ┌──────────────────────────────────────────────────────────────────┐
        ├─►│ tracker.plan_read(addr, len)                                     │
        │  ├──────────────────────────────────────────────────────────────────┤
        │  │ (1) F = frontier_ ; Ha = highest_acked_        two atomic loads  │
        │  │     F ≥ Ha ?  ──yes──► ONE segment @ max(F,Ha).  done.           │
        │  │                        nothing unresolved ≤ H exists.            │
        │  │        │no                                                       │
        │  │        ▼                                                         │
        │  │ (2) enumerate the UNRESOLVED slots in (F, Ha]                    │
        │  │       chain completed_upto(h) ─► first non-completed idx ≥ h     │
        │  │       cost is O(#unresolved), NOT O(Ha-F)  ← straggler immunity  │
        │  │     none overlaps [addr,len) ? ──yes──► ONE segment @ Ha. done.  │
        │  │        │no                                                       │
        │  │        ▼                                                         │
        │  │ (3) per-block winner pass, ASCENDING dLSN over [umin, Ha]        │
        │  │       acked slot      ─► clears every block it covers            │
        │  │                          (highest dLSN wins per LBA: it          │
        │  │                           supersedes everything beneath it)      │
        │  │       unresolved slot ─► records itself on blocks still clear    │
        │  │                                                                  │
        │  │     ustar[b] = lowest unresolved slot covering b that sits       │
        │  │                above every acked slot covering b   (or none)     │
        │  │                                                                  │
        │  │       H_b = ustar[b] < 0  ?  Ha  :  ustar[b] - 1                 │
        │  │                                                                  │
        │  │     coalesce equal H_b into contiguous segments                  │
        │  └──────────────────────────────────────────────────────────────────┘
        │
        │   one segment                      several segments (the SPLIT READ)
        │        │                                    │
        │        │                       sisl::sg_iterator carves `dest`
        │        │                                    │
        ├────────┴────────────────────────────────────┼──────────────┐
        ▼                                             ▼              ▼
  async_read(leader, hdr, H,  addr, len, dest)   seg0 @ H_0     seg1 @ H_1   ...  (when_all)
                                                  ▼              ▼
                                             dest[0..n)     dest[n..m)
        │
        │   each replica fills its own slice in place: data extents, holes zero-filled.
        │   nothing to stitch -- only the byte count to return.
        ▼
      len
```

### Why per-block, and why that means no stalling

A replica serves, per LBA, the highest-dLSN version ≤ `H`. So an unresolved slot is only dangerous on
blocks where **no later acked write shadows it**. Reading block `b` at `H_b = ustar[b] - 1`:

* exposes no unresolved write — nothing unresolved remains at or below `ustar[b] - 1` for that block; and
* hides no acked write — the highest acked slot `A_b` covering `b` satisfies `A_b < ustar[b]`, so `A_b`
  is still visible.

Both halves matter, and they are the two violations the design names. Serving an unresolved write breaks
**read stability** (it may later be `Empty`'d, and the next read would regress). Hiding an acked write
breaks **read-after-write** (the app already waited on that ack). A single `H` sometimes cannot do both:

```
     read [block 0, block 1)          dLSN 0: covers blocks 0-1, UNRESOLVED
                                      dLSN 1: covers block 0,    ACKED

     block 0 ── winner at Ha=1 is dLSN 1 (acked)      ─► safe at H = 1
     block 1 ── winner at Ha=1 is dLSN 0 (unresolved) ─► must clamp to H = -1

     H=1  would surface dLSN 0 on block 1   ✗ read stability
     H=-1 would hide    dLSN 1 on block 0   ✗ read-after-write
     ─────────────────────────────────────────────────────────────
     split: [block 0] @ H=1  +  [block 1] @ H=-1     ✓ both hold
```

Because a safe `H_b` always exists (`ustar[b] > F`, so `H_b ≥ F`), **`in_flight` and `failed` fence
identically and neither ever blocks a read.** That is why there is no "wait for the write to resolve"
path in the common case, and why a failed write needs no leader resolution round before reads may
proceed over the range it touched. It only pins `commit_lsn`.

### The only paths that wait

`plan_read` suspends on `resolution_gate` in three pathological cases, and returns `NO_QUORUM` instead if
a `failed` slot is in the blocking set (nothing would ever wake it):

1. an index in `(F, Ha]` whose `create()` is not yet visible — its range is unknown, so it fences
   conservatively; resolves in nanoseconds;
2. `Ha - umin` past the scan cap — a write has hung;
3. the plan would fan out past `k_max_segments` (8 sub-reads).

The gate's `signal()` skips its allocation when nobody is waiting. That fast path is only sound because
`enter()` and `signal()` bracket their flag write and their state read with `seq_cst` fences — a Dekker
interlock. Without it a resolver could read `waiters_ == 0` while a reader reads pre-resolve state, and
the reader would sleep forever.

---

## What is NOT here yet

**Read routing.** Every segment above goes to `replicas_[leader_]`, and that peer's error is surfaced
directly. There is no failover, no eligibility check, and no per-replica **Missing map**.

This is a correctness gap, not just a resilience one. The tracker decides *which version* a read may see;
the Missing map decides *which replica can actually serve it*. The design calls the second half
**route-around** (`CRAFT-Design.md`, "Case (b)"):

```
   what we have                       what is missing
   ────────────                       ───────────────
   H_b  = safe horizon per block      target = a replica that HOLDS the winner at H_b

   if the leader was not in the       ┌─ leader lacks the acked N ─────────────┐
   write quorum for the acked N       │  serves M (unresolved) or older        │
   that wins block b, then at         │  ✗ exposes an unresolved write         │
   H_b = Ha it serves the wrong       │  ✗ hides a durable one                 │
   version and BOTH guarantees        └────────────────────────────────────────┘
   break.  Our tests never catch
   this because the leader is         route-around: pick any member of N's write
   always in the ack set.             quorum -- at least one never received M.
```

The client already knows, per write, which replicas acked, so the raw material for a Missing map exists;
it is just not kept. Until it is, `read()` is correct **only when the leader is in the write quorum of
every winner in the range** — which the model's default configuration always satisfies, and
`force_subquorum` can violate.

Also outstanding, in rough order:

- broadcast `keep_alive` to all replicas → the true set-wide `all_committed_lsn` (today we send `-1`,
  deliberately: our own frontier is an upper bound on the minimum, never the minimum, and sending it
  would let a replica reclaim journal a lagging peer still needs);
- a `keep_alive` timer, so the server watchdog sees the client;
- re-login on `STALE_TERM`, and `resolve_upto()` to retire a `failed` hole after a leader resolution
  round (there is no public verb for one today);
- ublk `DISCARD` / `WRITE_ZEROES` → a CRAFT zero write (empty `sg_list`).

---

## Concurrency notes

* **Keyed on dLSN, never on thread.** `sisl::async::task` resumes on whatever thread completed the last
  replica's RPC, so a shard key captured before a `co_await` is a data race after it.
* **Stale reads only ever fence more.** dLSNs are dense, so every index in `(F, Ha]` was issued before
  `Ha` was published. A scanner may therefore walk that range and treat anything it cannot positively see
  as resolved *as* unresolved. A side index of in-flight ranges would not have this property — it can be
  read as empty while an entry is concurrently being claimed, and nothing orders those two events.
* **The slot bit publishes the payload.** Two `shared_lock` holders establish no ordering between them,
  so a scanner reaches a slot only via its bit. `sisl::AtomicBitset` therefore sets bits with **release**
  and reads them with **acquire** (`safe_bits` in `bitword.hpp`); with relaxed ops the scanner has no
  acquire edge to the `addr`/`len` it just observed, which is a real data race — the stress test reports
  ~12 of them under TSAN if you weaken the ordering. On x86-64 the ordering is free: `fetch_or` is already
  a locked RMW, and the acquire load and release store are plain `mov`s.
  Standalone `atomic_thread_fence` would be equally correct and **equally invisible to TSAN**, which does
  not model fences (GCC warns `-Wtsan`). Fence-based publication reports a false race on every scan, which
  gets suppressed, which then hides the real ones. Keep the ordering on the ops.
* **`outcome` is the only mutable field** in `dlsn_slot`; every access goes through `std::atomic_ref`.
  `addr`/`len` are written once, before the bit is set, so plain reads are safe once the bit is observed.

## Tests

| target | what it pins down |
|---|---|
| `test_craft_dlsn_tracker` | The tracker in isolation: frontier fold, failed-slot pin, fast path, clamp, shadow, split, fan-out stall, straggler, plus a concurrent reader/writer stress. Links **without** `homestore::homestore`. |
| `test_craft_client` | The same rules end to end against the model, including the case where the leader physically holds a sub-quorum write. |

```
ctest --test-dir build/Release -R Craft

# The stress test is the one that needs a sanitizer to say anything:
./build/Sanitized-thread/src/test/craft/test_craft_dlsn_tracker
```
