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
#pragma once

// HomeBlkDisk: a ublkpp leaf disk (ublkpp::ublk_disk) that exposes a single homeblocks volume as a
// ublk block device (/dev/ublkbN). It bridges two independent async runtimes:
//
//   - ublkpp drives each ublk queue on its own pthread with a SINGLE_ISSUER io_uring; a driver's
//     async_iov coroutine runs inline on that queue thread and a per-IO ublkpp::cqe_state must be
//     resumed ON THE QUEUE THREAD (only it may touch the queue's io_uring / call ublksrv_complete_io).
//   - homeblocks runs its own iomgr reactors; async_read/async_write complete on a reactor thread.
//
// A naive `co_await homeblocks::async_read(...)` inside async_iov would therefore resume the
// completion on a homeblocks reactor thread and call ublksrv_complete_io off the single-issuer ring.
// Instead each queue owns an eventfd + a service-loop coroutine (see homeblk_disk.cpp): the homeblocks
// completion (reactor thread) publishes its result and kicks the eventfd; the service loop -- woken by
// an io_uring POLL_ADD CQE on the queue thread -- drains the completions and resumes the per-IO
// coroutines there. This is the coroutine-era rebirth of the old eventfd/send_event homeblk adapter.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <ublkpp/lib/ublk_disk.hpp>

#include <homeblks/home_blocks.hpp>

struct iovec;
struct ublk_io_data;
struct ublksrv_queue;

namespace homeblocks::ublk {

// Per-queue completion bridge (eventfd + service-loop coroutine + completion list). Defined in the .cpp
// because it is an implementation detail of the queue<->reactor handoff.
struct queue_bridge;

class HomeBlkDisk : public ublkpp::ublk_disk {
public:
    // `instance` keeps homeblocks alive for the disk's lifetime; `vol` is the volume to expose (its
    // geometry -- capacity/page_size -- is supplied explicitly because the public API exposes no way to
    // introspect a volume_handle). `max_tx` is the largest single transfer (home_blocks::max_vol_io_size()).
    HomeBlkDisk(std::shared_ptr< home_blocks > instance, volume_handle vol, volume_id_t vol_id, uint64_t capacity,
                uint32_t page_size, uint32_t max_tx);
    ~HomeBlkDisk() override;

    std::string id() const noexcept override { return _id_str; }

    prepare_result prepare(ublksrv_queue const* q, int const iouring_device_start) override;
    ublkpp::disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs,
                                       uint32_t nr_vecs, uint64_t addr) override;

private:
    std::shared_ptr< home_blocks > _instance;
    volume_handle _vol;
    volume_id_t _vol_id;
    uint64_t _capacity;
    uint32_t _page_size;
    std::string _id_str;

    // Indexed by q->q_id; sized to MAX_NR_HW_QUEUES at construction and populated by prepare() (each on
    // its own queue thread, exactly once per q_id) so async_iov reads its slot lock-free.
    std::vector< std::unique_ptr< queue_bridge > > _bridges;
};

} // namespace homeblocks::ublk
