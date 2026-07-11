# CRAFT transport: kernel and io_uring feature floor

Status: reference. Companion to `transport.md` (connection/session behavior) and `wire.md` (on-wire
encoding), both now in the `craft_client` package under `docs/`. This document records which io_uring
features the TCP-over-io_uring transport depends on, what the current production kernel target permits, and
which optimizations are gated on a later kernel so we can revisit them when a kernel-migration timeline
exists. It is a living checklist, not a design decision: nothing here changes the wire or the session model.

> **Note.** The TCP-over-io_uring transport this document describes now lives in the standalone
> `craft_client` package (its connection code under `net/`, and the companion `transport.md` / `wire.md`
> under `craft_client/docs/`). This kernel/feature checklist stays here with the HomeBlocks CRAFT docs.

## Target and the hazard this guards

- **Production target: Linux 6.8.** Everything the transport ships MUST run on 6.8.
- **Dev machines run newer kernels (6.1x).** That gap is the hazard: a feature that works in dev silently
  fails on the 6.8 target. The rule is to design to the 6.8 ceiling and treat anything newer as a
  runtime-detected optimization, never a hard dependency.
- **liburing is userspace and forward-compatible.** We build against liburing 2.15, which exposes helper
  functions for ops the *running* kernel may not implement; those fail at submit/CQE time with `-EINVAL` or
  `-EOPNOTSUPP`. So the ceiling is always the KERNEL, never the liburing version. See the liburing-version
  section below for how the two axes stay independent.

## Floor: what the transport requires today

Every io_uring op and helper the connection code (`craft_client`'s `net/craft_conn.cpp`) calls:

| Feature | Landed | Use |
|---|---|---|
| `io_uring_queue_init` (flags 0), `queue_exit`, `get_sqe`, `submit`, `wait_cqe`, `cqe_seen` | 5.1 | ring setup + blocking submit-and-wait |
| `IORING_OP_SEND` / `IORING_OP_RECV` (`prep_send` / `prep_recv`) | 5.6 | byte-stream send/recv, `MSG_NOSIGNAL` |

**Effective floor: kernel 5.6**, roughly fifteen releases below the 6.8 target. The blocking P1 slice uses
nothing near the edge.

## Ceiling: features we may adopt (all available on 6.8)

The async transport (concurrent IOs, the request_id landing pad, payload gather/scatter) maps onto features
that all predate 6.8, so the full "real transport" story stays inside the envelope:

| Feature | Landed | Use in the design |
|---|---|---|
| `IORING_OP_SENDMSG` / `RECVMSG` (iovec scatter-gather) | 5.3 | scatter-recv reads into the ublk buffer, gather-send writes |
| `IORING_OP_ACCEPT` | 5.5 | server-side accept on the ring |
| Provided buffer rings (`io_uring_setup_buf_ring`, `IOU_PBUF_RING`) | 5.19 | recv without pre-committing a buffer per idle connection |
| `IORING_OP_MSG_RING` | 5.18 | cross-ring completion posting (already used by the ublk adapter) |
| `IORING_REGISTER_RING_FDS`, direct/fixed descriptors (`OP_SOCKET`, accept-direct) | 5.18 / 5.19 | drop per-op fd refcounting |
| Multishot accept (`IORING_ACCEPT_MULTISHOT`) | 5.19 | one SQE arms a stream of accepts |
| Multishot recv (`IORING_RECV_MULTISHOT`) | 6.0 | one SQE arms a stream of recvs on a connection |
| `IORING_OP_SEND_ZC` / `SENDMSG_ZC` (zero-copy send, incl. registered buffers) | 6.0 / 6.1 | write-payload gather-send without a copy |
| `IORING_SETUP_DEFER_TASKRUN` + `IORING_SETUP_SINGLE_ISSUER` | 6.1 | the low-latency config for our per-queue single-issuer ring |
| `IORING_SETUP_COOP_TASKRUN`, `IORING_SETUP_SUBMIT_ALL` | 5.19 / 5.18 | batching / task-run cost reduction |

The newest thing we would lean on is `SEND_ZC` (6.0), and even that is optional. The intended latency config,
`DEFER_TASKRUN | SINGLE_ISSUER` (6.1), is comfortably available.

## Above the ceiling: revisit at kernel migration

These work on dev boxes but are NOT available on 6.8. Do not build a hard dependency on any of them. When a
migration timeline exists, revisit the "unlocks" column:

| Feature | Needs | Unlocks |
|---|---|---|
| **NAPI busy-poll registration** (`IORING_REGISTER_NAPI`) | **6.9** | kernel-side busy polling on the ring, the headline low-latency networking knob. This is the one worth the most on migration. |
| Send/recv **bundles** (`IORING_RECVSEND_BUNDLE`) | 6.10 | multiple buffers drained/filled in one send/recv op, fewer SQEs per burst |
| `IORING_OP_BIND` / `IORING_OP_LISTEN` | 6.11 | bind/listen on the ring (we do these blocking today, so no real loss) |
| Incremental provided-buffer consumption (`IOU_PBUF_RING_INC`) | 6.11 | partial consumption of a provided buffer, tighter buffer accounting |

## liburing version (independent of the kernel)

liburing is versioned separately from the kernel, and the two are independent axes:

- **Build-time liburing version** = which helpers we can call in C++.
- **Kernel target** = which ops actually succeed at runtime.

A given liburing is backward-compatible to any kernel that has io_uring at all (the only hard floor liburing
imposes is io_uring's existence, 5.1). A NEWER liburing on an OLDER kernel does not fail to load or link: its
helpers for newer ops still compile, and using an unsupported op degrades to a runtime `-EINVAL` /
`-EOPNOTSUPP` (or a failed `queue_init` for an unsupported setup flag). So building against the newest
liburing is safe even for the 6.8 target, PROVIDED op usage is gated by the kernel probe (below), never by
"the helper compiled."

The linking nuance that decides how we depend on it: liburing helpers split two ways.

- **`static inline`** (most `io_uring_prep_*`, `buf_ring_add`, ...): inlined into our binary at build time. No
  runtime `.so` version coupling at all; only the kernel matters.
- **Real versioned `.so` symbols** (`io_uring_queue_init`, `io_uring_register_napi`,
  `io_uring_setup_buf_ring`, ...): tagged `LIBURING_2.x` via liburing's version script. If we build against
  2.15 and use a symbol introduced in a later minor than the TARGET's runtime `liburing.so.2`, the loader
  fails to resolve it at load time. Dynamic linking therefore adds a SECOND runtime variable (the target's
  liburing) on top of the kernel.

Guidance:

1. **Statically link a conan-pinned liburing.** That freezes the version into the binary, so the only runtime
   variable is the kernel (which the probe already guards) and the build is reproducible. This matches how
   craft_client pins its other deps. The transport currently links the SYSTEM liburing via `-luring`
   (`liburing.so.2.15`), which is spike-grade: fine for the loopback bring-up, to be pinned before it ships.
2. **Build-time floor: roughly 2.4.** That exposes every helper in the `<= 6.8` ceiling table (`send_zc`,
   buffer rings, `SINGLE_ISSUER` / `DEFER_TASKRUN`, multishot). Bump to ~2.6 only if/when NAPI busy-poll is
   adopted post-6.9. Building against 2.15 today is fine.
3. **Never infer kernel support from helper availability.** The probe guardrail below is what enforces this,
   and it is why building against a liburing newer than the target kernel is safe.

## Guardrail

Because dev (6.1x) is far ahead of the target (6.8), add a one-time capability check at ring setup
(`io_uring_get_probe` / `IORING_REGISTER_PROBE`) that asserts the ops the transport actually depends on and
refuses to start otherwise. That turns "works in dev, broken on 6.8" into a loud failure in CI rather than a
production surprise, and it documents the dependency set in code. Any feature from the table above is opted
into only behind a positive probe, with the ceiling-level path as the fallback (same back-pocket posture as
Homa / RDMA in `craft_client`'s `transport.md`).

## Revisit checklist (when a kernel-migration timeline lands)

1. Re-baseline the floor: is the new minimum >= 6.9? If so, promote NAPI busy-poll from "detected" to a
   first-class latency path and measure it against `DEFER_TASKRUN | SINGLE_ISSUER` alone.
2. Evaluate send/recv bundles (6.10) for the write fan-out and multishot-recv drain paths.
3. Re-run the probe assertion against the new minimum and prune any now-unconditional feature checks.
