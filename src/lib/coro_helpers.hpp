/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
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

// homeblocks coroutine bridge helpers -- the two places we cross between synchronous, non-coroutine code and
// the stdexec/sisl::async::task world
//   - sync_get(task) : block the caller until the task completes and return its value. Safe only OFF a reactor (test
//                      mains, sync control-plane); blocking a reactor here would park its iomgr loop (see
//                      sync-get-on-reactor-deadlock note).
//   - detach(task)   : fire-and-forget a coroutine from a non-coroutine context (e.g. a void on_commit/timer
//                      callback). exec::task is lazy, so an un-awaited task never runs -- this starts it.
//
// homestore keeps an equivalent (internal, unexported) src/lib/common/coro_helpers.hpp; we keep our own minimal
// copy rather than depend on its internals.

#include <coroutine>
#include <exception>
#include <functional>
#include <mutex>
#include <vector>
#include <tuple>
#include <utility>

#include <stdexec/execution.hpp>
#include <exec/inline_scheduler.hpp>

#include <sisl/async/task.hpp>
#include <sisl/logging/logging.h>

namespace homeblocks::detail {

// Block the calling thread until the task completes and return its value (void for task<void>). The task is
// fulfilled by other (reactor) threads; sync_wait drains a run_loop here. Do NOT call on an iomgr reactor.
template < typename Task >
inline auto sync_get(Task&& task) {
    auto result = stdexec::sync_wait(std::forward< Task >(task)).value();
    if constexpr (std::tuple_size_v< decltype(result) > == 0) {
        return;
    } else {
        return std::get< 0 >(std::move(result));
    }
}

// Fire-and-forget a coroutine whose result we don't need. The task is taken by value (copied into the
// self-owning wrapper frame); the wrapper swallows exceptions so a throwing body can't reach start_detached's
// receiver (which would std::terminate) -- tasks normally complete errors-as-values, so this is a backstop.
// write_env injects an inline scheduler so the sticky-affinity exec::task can start without an enclosing
// scheduler (it resumes inline on whatever thread completes its awaited work) -- same idiom as sisl's when_all.
template < typename T >
inline void detach(sisl::async::task< T > task) {
    auto wrapper = [](sisl::async::task< T > t) -> sisl::async::task< void > {
        try {
            co_await std::move(t);
        } catch (const std::exception& e) { LOGERROR("Detached task threw, swallowing: {}", e.what()); } catch (...) {
            LOGERROR("Detached task threw an unknown exception, swallowing");
        }
    }(std::move(task));
    stdexec::start_detached(
        stdexec::write_env(std::move(wrapper), stdexec::prop{stdexec::get_scheduler, exec::inline_scheduler{}}));
}

// A list of coroutines waiting for a condition, resumed by notify_all(). wait_until(pred) suspends only if
// pred() is false, and the check-then-park is atomic with respect to notify_all() (both take mu_), so a
// notification can never be missed. Waiters resume inline on the notifying thread on EVERY notify_all(),
// whether or not pred() now holds, so callers must loop: `while (!pred()) co_await wl.wait_until(pred);`.
// pred() may take other locks; lock order is wait_list::mu_ first.
class wait_list {
public:
    struct awaiter {
        wait_list& wl;
        std::function< bool() > pred;
        bool await_ready() { return pred(); }
        bool await_suspend(std::coroutine_handle<> h) {
            std::lock_guard lk{wl.mu_};
            if (pred()) return false;
            wl.waiters_.push_back(h);
            return true;
        }
        void await_resume() noexcept {}
    };
    awaiter wait_until(std::function< bool() > pred) { return awaiter{*this, std::move(pred)}; }
    void notify_all() {
        std::vector< std::coroutine_handle<> > to_resume;
        {
            std::lock_guard lk{mu_};
            to_resume.swap(waiters_);
        }
        for (auto h : to_resume)
            h.resume();
    }

private:
    std::mutex mu_;
    std::vector< std::coroutine_handle<> > waiters_;
};

} // namespace homeblocks::detail
