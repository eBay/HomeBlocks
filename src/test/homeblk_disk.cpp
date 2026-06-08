/*********************************************************************************
 * Modifications Copyright 2026 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#include "homeblk_disk.hpp"

extern "C" {
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>
}

#include <algorithm>
#include <bit>
#include <cerrno>
#include <coroutine>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <utility>

#include <iomgr/iomgr.hpp> // iomanager.run_on_forget / iomgr::reactor_regex -- submit homestore IO on a reactor
#include <sisl/logging/logging.h>
#include <ublksrv.h>

#include <ublkpp/lib/cqe_state.hpp> // build_cqe_state_data / next_sqe + sisl::async managed-user-data helpers

#include "coro_helpers.hpp" // homeblocks::detail::detach (src/lib, on the include path)

namespace homeblocks::ublk {

// ublk speaks in 512-byte sectors regardless of the device's logical block size; ublkpp keeps this
// constant in an un-installed internal header, so we redefine the one value we need.
static constexpr uint32_t k_sector_shift = 9;

// =============================================================================
// Per-queue completion bridge
//
// Lives for the queue's lifetime: created in prepare() on the queue thread, destroyed by the
// HomeBlkDisk destructor (which runs only after every queue thread has joined). Owns the eventfd, the
// suspended service-loop coroutine frame, and the cross-thread completion list.
// =============================================================================
struct queue_bridge {
    int evfd{-1};
    bool armed{false};                     // the service loop is started lazily on the first IO (see async_iov)
    ublkpp::cqe_state poll_state{};        // the service loop awaits this; resumed by run_queue_loop's CQE dispatch
    std::coroutine_handle<> service_handle{};

    std::mutex mtx;
    std::vector< ublkpp::cqe_state* > completed; // pushed by reactor threads (post), drained on the queue thread

    queue_bridge() = default;
    queue_bridge(queue_bridge const&) = delete;
    queue_bridge& operator=(queue_bridge const&) = delete;

    ~queue_bridge() {
        // Destroy the suspended service-loop frame first. Safe: del_dev is umount-gated and the kernel
        // cancels the in-flight POLL_ADD when ublksrv_queue_deinit tears the io_uring down before this runs.
        if (service_handle) service_handle.destroy();
        if (evfd >= 0) ::close(evfd);
    }

    // Called from a homeblocks reactor thread when an IO completes. Publishes the result into the per-IO
    // cqe_state and kicks the eventfd so the service loop resumes the awaiting coroutine on the queue
    // thread. The result write is ordered before the queue thread's read by the mutex push/pop pair.
    void post(ublkpp::cqe_state* st, int result) {
        st->_result = result;
        {
            std::lock_guard< std::mutex > lk(mtx);
            completed.push_back(st);
        }
        uint64_t const one = 1;
        if (auto r = ::write(evfd, &one, sizeof(one)); sizeof(one) != static_cast< size_t >(r)) {
            LOGERROR("homeblk_disk: eventfd kick returned {}", r);
        }
    }
};

// Service loop: one per queue, pinned to the queue thread. Arms an io_uring POLL_ADD on the bridge's
// eventfd, suspends on poll_state, and -- when woken by run_queue_loop dispatching that CQE -- drains the
// completion list and resumes each per-IO coroutine here (the only thread allowed to call
// ublksrv_complete_io). Never returns; its frame is owned by queue_bridge::service_handle.
static ublkpp::disk_task< int > service_loop(ublksrv_queue const* q, queue_bridge* br) {
    while (true) {
        br->poll_state._result = 0;
        br->poll_state._result_ready = false;
        br->poll_state._waiter = {};

        // The ring is sized with room for this one persistent poll, so next_sqe should always succeed; but
        // never dereference a null SQE -- if the SQ is momentarily full, flush it to the kernel and retry.
        auto* sqe = ublkpp::next_sqe(q);
        while (sqe == nullptr) [[unlikely]] {
            LOGTRACE("homeblk_disk: SQ full arming eventfd poll on q{}, flushing", q->q_id);
            io_uring_submit(q->ring_ptr);
            sqe = ublkpp::next_sqe(q);
        }
        io_uring_prep_poll_add(sqe, br->evfd, POLLIN);
        // Encode the managed-user-data the same way build_cqe_state_data does, so run_queue_loop routes
        // this CQE back to poll_state's waiter (this coroutine) instead of delegating it to ublksrv.
        sqe->user_data = sisl::async::encode_managed_user_data(&br->poll_state);

        co_await br->poll_state;

        uint64_t v;
        if (auto r = ::read(br->evfd, &v, sizeof(v)); sizeof(v) != static_cast< size_t >(r)) {
            LOGTRACE("homeblk_disk: eventfd drain returned {}", r);
        }

        std::vector< ublkpp::cqe_state* > batch;
        {
            std::lock_guard< std::mutex > lk(br->mtx);
            batch.swap(br->completed);
        }
        for (auto* st : batch) {
            st->_result_ready = true;
            if (auto h = std::exchange(st->_waiter, {})) h.resume();
        }
    }
}

// Detached homeblocks IO: started inline on the queue thread, it suspends into homeblocks' reactors and
// resumes there on completion, where it posts the result back to the queue's bridge. Parameters are taken
// by value so they live in the coroutine frame (queue-thread captures would dangle after the first await).
static async_status run_hb_io(volume_handle vol, uint64_t addr, sisl::sg_list sgs, bool is_read, queue_bridge* br,
                              ublkpp::cqe_state* state) {
    auto const res =
        is_read ? co_await async_read(vol, addr, std::move(sgs)) : co_await async_write(vol, addr, std::move(sgs));
    br->post(state, res.has_value() ? static_cast< int >(res.value()) : -EIO);
    co_return ok();
}

HomeBlkDisk::HomeBlkDisk(std::shared_ptr< home_blocks > instance, volume_handle vol, volume_id_t vol_id,
                         uint64_t capacity, uint32_t page_size, uint32_t max_tx) :
        ublkpp::ublk_disk(),
        _instance(std::move(instance)),
        _vol(std::move(vol)),
        _vol_id(vol_id),
        _capacity(capacity),
        _page_size(page_size),
        _id_str(boost::uuids::to_string(vol_id)),
        _bridges(MAX_NR_HW_QUEUES) {
    if (!_vol) throw std::runtime_error("HomeBlkDisk: null volume handle");
    if (page_size == 0 || (page_size & (page_size - 1)) != 0)
        throw std::runtime_error("HomeBlkDisk: page_size must be a power of two");
    if (max_tx < k_sector_shift) throw std::runtime_error("HomeBlkDisk: max_tx too small");

    // homeblocks reads/writes through its own datapath, never the kernel page cache.
    _direct_io = true;

    auto const bs_shift = static_cast< uint8_t >(std::countr_zero(page_size));
    auto& p = *params();
    p.basic.logical_bs_shift = bs_shift;
    p.basic.physical_bs_shift = bs_shift;
    // Leave max_sectors at the base default (DEF_BUF_SIZE >> 9), which tracks the ublk per-tag IO buffer
    // (ublkpp's --max_io_size, default 512 KiB): a basic.max_sectors larger than that buffer is rejected by
    // the kernel with EINVAL. Only clamp it DOWN if homeblocks' own max transfer (max_tx) is smaller, so we
    // never hand homeblocks an IO bigger than it accepts. (Raising --max_io_size above 512 KiB won't widen
    // our transfers -- harmless for a test adapter; lower it below our max_sectors and the kernel will EINVAL.)
    p.basic.max_sectors = std::min(p.basic.max_sectors, static_cast< uint32_t >(max_tx >> k_sector_shift));
    p.basic.dev_sectors = capacity >> k_sector_shift;
    // The kernel requires the device size to be a whole multiple of max_sectors.
    p.basic.dev_sectors -= (p.basic.dev_sectors % p.basic.max_sectors);

    // DISCARD/WRITE_ZEROES map cleanly onto homeblocks async_unmap, but enabling them means getting the
    // ublk_param_discard geometry exactly right or the device fails to come up; left disabled for now (the
    // op is also rejected defensively in async_iov). See TODO there.
    p.types &= ~UBLK_PARAM_TYPE_DISCARD;

    LOGINFO("HomeBlkDisk [vol={}] sectors={} lbs={} max_sectors={}", _id_str, p.basic.dev_sectors, page_size,
            p.basic.max_sectors);
}

HomeBlkDisk::~HomeBlkDisk() = default;

HomeBlkDisk::prepare_result HomeBlkDisk::prepare(ublksrv_queue const* q, int const) {
    // The target calls prepare(nullptr) once at tgt_init just to learn the SQE ceiling (to size the
    // io_uring); it must not touch q or build any per-queue state. The real per-queue call (with a live q)
    // happens later from init_queue. Each user IO stages exactly one POLL_ADD via the service loop.
    if (q == nullptr) return {.max_sqes_per_io = 1};

    if (q->q_id < 0 || static_cast< size_t >(q->q_id) >= _bridges.size())
        throw std::runtime_error(fmt::format("HomeBlkDisk: q_id {} out of range (max {})", q->q_id, _bridges.size()));

    auto br = std::make_unique< queue_bridge >();
    br->evfd = ::eventfd(0, EFD_NONBLOCK);
    if (br->evfd < 0) throw std::runtime_error(fmt::format("HomeBlkDisk: eventfd failed: {}", strerror(errno)));

    // Build the service-loop coroutine but DO NOT start it here. init_queue's exact io_uring readiness is a
    // ublksrv-internal detail, so we never touch q->ring_ptr from this path; the loop is armed lazily from the
    // first async_iov instead, which is unambiguously inside the running queue loop with a live ring. Steal the
    // coroutine handle so the disk_task destructor becomes a no-op; the bridge now owns the frame.
    auto task = service_loop(q, br.get());
    br->service_handle = std::exchange(task._coro, {});

    _bridges[q->q_id] = std::move(br);
    // Each user IO allocates exactly one per-IO cqe_state via build_cqe_state_data; reserve one pool slot.
    return {.max_sqes_per_io = 1};
}

ublkpp::disk_task< int > HomeBlkDisk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs,
                                                uint32_t nr_vecs, uint64_t addr) {
    auto const op = ublksrv_get_op(data->iod);
    if (op == UBLK_IO_OP_FLUSH) co_return 0; // homeblocks IO is durable on completion; nothing to flush
    if (op == UBLK_IO_OP_DISCARD || op == UBLK_IO_OP_WRITE_ZEROES) co_return -ENOTSUP; // TODO: async_unmap

    auto* br = _bridges[q->q_id].get();
    if (!br) co_return -EIO; // prepare() must have populated this slot on the queue thread

    // Arm the completion service loop on first use. We're inside the queue loop here, so q->ring_ptr is live;
    // resume() runs the loop to its first co_await, staging the eventfd POLL_ADD. Done before the first IO is
    // dispatched, so its completion can never race ahead of the loop being ready to drain it.
    if (!br->armed) {
        br->armed = true;
        br->service_handle.resume();
    }

    bool const is_read = (op == UBLK_IO_OP_READ);

    // Copy the iovec descriptors into an sg_list (moved into run_hb_io's frame) before the first co_await.
    // iov_base points into the kernel-mapped ublk IO buffer and stays valid until ublksrv_complete_io, so
    // homeblocks reads/writes it in place -- only the descriptors are copied, not the data.
    sisl::sg_list sgs;
    sgs.size = 0;
    for (uint32_t i = 0; i < nr_vecs; ++i) {
        sgs.iovs.push_back(iovecs[i]);
        sgs.size += iovecs[i].iov_len;
    }

    // Per-IO cqe_state in the slot's pool. We resume it ourselves from the service loop (homeblocks, not
    // io_uring, completes the IO), so the encoded user_data is unused -- we only need the state pointer.
    auto [state, _] = ublkpp::build_cqe_state_data(data);

    // Fire the homeblocks IO ON A WORKER REACTOR -- NOT inline on this ublk queue thread. homestore is
    // reactor-affine: the data service's per-reactor io_uring, the shared index b-tree write-back, and the
    // checkpoint's dirty-state tracking all assume reactor context. Driving that chain from a foreign
    // (queue) thread under deep concurrency corrupts the index (verify mismatches) and the completion
    // notification fails to wake the owning reactor (observed as multi-second lost-wakeup stalls that a
    // stray fd event "unsticks"). run_on_forget lands run_hb_io on a random worker; the homeblocks op then
    // runs and completes on reactors as designed, and its completion still bounces back to THIS queue
    // thread via br->post()'s eventfd kick, where the service loop resumes the cqe_state we await below.
    iomanager.run_on_forget(iomgr::reactor_regex::random_worker,
                            [vol = _vol, addr, sgs = std::move(sgs), is_read, br, state]() {
                                detail::detach(run_hb_io(vol, addr, sgs, is_read, br, state));
                            });

    co_return co_await *state;
}

} // namespace homeblocks::ublk
