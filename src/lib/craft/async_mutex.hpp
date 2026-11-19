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

#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

#include <sisl/async/task.hpp>
#include <sisl/async/value_awaitable.hpp>

namespace homeblocks {

// A mutex a coroutine can wait on without blocking its thread. One holder at a time: try_lock() gives up if it's held,
// and lock() queues. A releasing holder hands the mutex straight to the longest waiter, so a try_lock() can't slip in
// ahead of one. The next holder resumes inline, on the releasing thread. A waiter whose frame is destroyed before its
// turn (its caller gave up) leaves the queue; destroying it while another thread hands it the mutex is a race the
// caller must avoid, as for any coroutine destroyed mid-await.
class AsyncMutex {
public:
    // Holds the mutex until it goes out of scope or reset(). Move-only.
    class Lock {
    public:
        Lock() = default;
        Lock(Lock&& o) noexcept : mu_{std::exchange(o.mu_, nullptr)} {}
        Lock& operator=(Lock&& o) noexcept {
            if (this != &o) {
                reset();
                mu_ = std::exchange(o.mu_, nullptr);
            }
            return *this;
        }
        ~Lock() { reset(); }

        void reset() {
            if (mu_) std::exchange(mu_, nullptr)->release();
        }
        explicit operator bool() const { return mu_ != nullptr; }

    private:
        friend class AsyncMutex;
        explicit Lock(AsyncMutex* mu) : mu_{mu} {}
        AsyncMutex* mu_{nullptr};
    };

    AsyncMutex() = default;
    AsyncMutex(AsyncMutex const&) = delete;
    AsyncMutex& operator=(AsyncMutex const&) = delete;

    std::optional< Lock > try_lock() {
        std::lock_guard lk{mu_};
        if (locked_) return std::nullopt;
        locked_ = true;
        return Lock{this};
    }

    sisl::async::task< Lock > lock() {
        auto waiter = std::make_shared< Waiter >();
        {
            std::lock_guard lk{mu_};
            if (!locked_) {
                locked_ = true;
                co_return Lock{this};
            }
            waiters_.push_back(waiter);
        }
        // If this frame is destroyed while it waits (its caller gave up), the guard leaves the queue, or passes the
        // mutex on if it was already handed here: else the mutex would stay locked with no holder.
        AbandonGuard guard{this, waiter};
        co_await waiter->turn; // handed over: locked_ stayed true, and the mutex is now ours
        guard.waiter.reset();
        co_return Lock{this};
    }

    bool locked() const {
        std::lock_guard lk{mu_};
        return locked_;
    }

private:
    struct Waiter {
        sisl::async::value_awaitable< bool > turn;
        bool handed{false}; // guarded by mu_: release() gave this waiter the mutex
    };

    struct AbandonGuard {
        AsyncMutex* mu;
        std::shared_ptr< Waiter > waiter; // reset once the waiter holds the mutex
        ~AbandonGuard() {
            if (waiter) mu->abandon(waiter);
        }
    };

    void release() {
        std::shared_ptr< Waiter > next;
        {
            std::lock_guard lk{mu_};
            if (waiters_.empty()) {
                locked_ = false;
                return;
            }
            next = std::move(waiters_.front());
            waiters_.pop_front();
            next->handed = true;
        }
        next->turn.complete(true); // outside the lock: resumes that waiter, which now holds the mutex
    }

    // A waiting lock() frame was destroyed before its turn.
    void abandon(std::shared_ptr< Waiter > const& waiter) {
        {
            std::lock_guard lk{mu_};
            if (!waiter->handed) {
                std::erase(waiters_, waiter);
                return;
            }
        }
        release(); // it was handed the mutex but will never hold it: pass it on
    }

    mutable std::mutex mu_;
    bool locked_{false};
    std::deque< std::shared_ptr< Waiter > > waiters_;
};

} // namespace homeblocks
