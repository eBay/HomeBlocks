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

#include <algorithm>
#include <bit>
#include <cerrno>
#include <stdexcept>
#include <utility>

#include <iomgr/iomgr.hpp> // iomanager.run_on_forget / iomanager.post_msg_ring / iomgr::reactor_regex
#include <sisl/logging/logging.h>
#include <ublksrv.h>

#include <ublkpp/lib/cqe_state.hpp> // build_cqe_state_data + sisl::async managed-user-data helpers

#include "coro_helpers.hpp" // homeblocks::detail::detach (src/lib, on the include path)

namespace homeblocks::ublk {

// ublk speaks in 512-byte sectors regardless of the device's logical block size; ublkpp keeps this constant in an
// un-installed internal header, so we redefine the one value we need.
static constexpr uint32_t k_sector_shift = 9;

// The per-IO worker: started on a worker reactor (so homestore I/O runs on a reactor, as it requires), it
// co_awaits the homeblocks op and -- resuming on the journal-commit reactor when it completes -- hands the result
// straight to the ublk queue's io_uring via IORING_OP_MSG_RING. `state_ud` is the managed-encoded per-IO
// cqe_state pointer; it lands as the target CQE's user_data, and `result` as its res, which is exactly what the
// queue's run_queue_loop reads to resume the per-IO coroutine and call ublksrv_complete_io. Parameters are taken
// by value so they live in the coroutine frame (a worker-reactor closure's captures would dangle across awaits).
static async_status run_hb_io(volume_handle vol, uint64_t addr, sisl::sg_list sgs, bool is_read, int queue_ring_fd,
                              uint64_t state_ud) {
    auto const res =
        is_read ? co_await async_read(vol, addr, std::move(sgs)) : co_await async_write(vol, addr, std::move(sgs));
    int const result = res.has_value() ? static_cast< int >(res.value()) : -EIO;

    // Post the completion CQE directly onto the queue's ring. We're on the commit reactor (uring-capable), so this
    // is one queued SQE that the reactor's poll_once batches out -- no eventfd, no lock, no service loop.
    if (auto const r = iomanager.post_msg_ring(queue_ring_fd, state_ud, result); r != 0) {
        // Rare: not on a uring reactor (-ENODEV) or SQ full even after a flush (-EAGAIN). The completion MUST
        // reach the queue ring or the IO stalls, so retry from a worker reactor (guaranteed uring-capable).
        LOGWARN("homeblk_disk: direct msg_ring post returned {}; retrying via a worker reactor", r);
        iomanager.run_on_forget(iomgr::reactor_regex::random_worker, [queue_ring_fd, state_ud, result]() {
            if (auto const r2 = iomanager.post_msg_ring(queue_ring_fd, state_ud, result); r2 != 0) {
                LOGERROR("homeblk_disk: msg_ring post failed on worker reactor ({}); IO {:#x} may stall", r2, state_ud);
            }
        });
    }
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
        _id_str(boost::uuids::to_string(vol_id)) {
    if (!_vol) throw std::runtime_error("HomeBlkDisk: null volume handle");
    if (page_size == 0 || (page_size & (page_size - 1)) != 0)
        throw std::runtime_error("HomeBlkDisk: page_size must be a power of two");
    if (max_tx < (1u << k_sector_shift)) throw std::runtime_error("HomeBlkDisk: max_tx too small");

    // homeblocks reads/writes through its own datapath, never the kernel page cache.
    _direct_io = true;

    auto const bs_shift = static_cast< uint8_t >(std::countr_zero(page_size));
    auto& p = *params();
    p.basic.logical_bs_shift = bs_shift;
    p.basic.physical_bs_shift = bs_shift;
    // Leave max_sectors at the base default (DEF_BUF_SIZE >> 9), which tracks the ublk per-tag IO buffer
    // (ublkpp's --max_io_size, default 512 KiB): a basic.max_sectors larger than that buffer is rejected by the
    // kernel with EINVAL. Only clamp it DOWN if homeblocks' own max transfer (max_tx) is smaller, so we never
    // hand homeblocks an IO bigger than it accepts.
    p.basic.max_sectors = std::min(p.basic.max_sectors, static_cast< uint32_t >(max_tx >> k_sector_shift));
    p.basic.dev_sectors = capacity >> k_sector_shift;
    // The kernel requires the device size to be a whole multiple of max_sectors.
    p.basic.dev_sectors -= (p.basic.dev_sectors % p.basic.max_sectors);

    // DISCARD/WRITE_ZEROES map cleanly onto homeblocks async_unmap, but enabling them means getting the
    // ublk_param_discard geometry exactly right or the device fails to come up; left disabled for now (the op is
    // also rejected defensively in async_iov).
    p.types &= ~UBLK_PARAM_TYPE_DISCARD;

    LOGINFO("HomeBlkDisk [vol={}] sectors={} lbs={} max_sectors={}", _id_str, p.basic.dev_sectors, page_size,
            p.basic.max_sectors);
}

HomeBlkDisk::~HomeBlkDisk() = default;

HomeBlkDisk::prepare_result HomeBlkDisk::prepare(ublksrv_queue const*, int const) {
    // No per-queue state: completions arrive via msg_ring straight onto the queue's own ring, dispatched by
    // run_queue_loop. We only need one cqe_state pool slot per tag for the per-IO state build_cqe_state_data
    // allocates (the state run_queue_loop resumes on completion).
    return {.max_sqes_per_io = 1};
}

ublkpp::disk_task< int > HomeBlkDisk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs,
                                                uint32_t nr_vecs, uint64_t addr) {
    auto const op = ublksrv_get_op(data->iod);
    if (op == UBLK_IO_OP_FLUSH) co_return 0; // homeblocks IO is durable on completion; nothing to flush
    if (op == UBLK_IO_OP_DISCARD || op == UBLK_IO_OP_WRITE_ZEROES) co_return -ENOTSUP; // TODO: async_unmap

    bool const is_read = (op == UBLK_IO_OP_READ);

    // Copy the iovec descriptors into an sg_list (moved into run_hb_io's frame) before the first co_await. iov_base
    // points into the kernel-mapped ublk IO buffer and stays valid until ublksrv_complete_io, so homeblocks
    // reads/writes it in place -- only the descriptors are copied, not the data.
    sisl::sg_list sgs;
    sgs.size = 0;
    for (uint32_t i = 0; i < nr_vecs; ++i) {
        sgs.iovs.push_back(iovecs[i]);
        sgs.size += iovecs[i].iov_len;
    }

    // Per-IO cqe_state in the tag's pool. `state_ud` is its managed-encoded pointer; the homeblocks completion
    // carries it back via msg_ring, and run_queue_loop decodes it to resume `state` (writing state->_result =
    // cqe->res, i.e. our IO result) -- all on this queue thread. We touch `state` only here and there: no lock.
    auto const sc = ublkpp::build_cqe_state_data(data);
    auto* const state = sc.first;
    uint64_t const state_ud = sc.second;
    int const queue_ring_fd = q->ring_ptr->ring_fd;

    // Launch the homeblocks IO ON a worker reactor -- homestore is reactor-affine; driving it inline on this ublk
    // queue thread corrupts the index and loses wakeups. The op runs and completes on reactors, then posts its
    // result straight back to THIS queue's ring (see run_hb_io).
    iomanager.run_on_forget(iomgr::reactor_regex::random_worker,
                            [vol = _vol, addr, sgs = std::move(sgs), is_read, queue_ring_fd, state_ud]() {
                                detail::detach(run_hb_io(vol, addr, sgs, is_read, queue_ring_fd, state_ud));
                            });

    co_return co_await *state;
}

} // namespace homeblocks::ublk
