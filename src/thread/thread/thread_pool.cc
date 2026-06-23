// Copyright 2026 The Action Engine Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "thread/thread_pool.h"

#include <cstddef>
#include <cstdint>

#include <absl/base/call_once.h>
#include <boost/fiber/algo/shared_work.hpp>
#include <boost/fiber/context.hpp>

#include "thread/fiber.h"

namespace thread {

WorkerThreadPool::~WorkerThreadPool() {
  thread::NewTree({}, [this]() {
    act::concurrency::impl::MutexLock lock(&mu_);
    shutdown_ = true;
    cv_.SignalAll();
    for (auto& worker : workers_) {
      worker.thread.join();
    }
  })->Join();
}

void WorkerThreadPool::Start(size_t num_threads) {
  for (size_t idx = 0; idx < num_threads; ++idx) {
    workers_.push_back({
        .thread = std::thread([this, idx] {
          EnsureThreadHasScheduler<boost::fibers::algo::shared_work>(
              /*suspend=*/true);

          act::concurrency::impl::MutexLock lock(&mu_);
          while (!shutdown_) {
            while (!work_queue_.empty()) {
              boost::intrusive_ptr<boost::fibers::context> ctx =
                  work_queue_.front();
              work_queue_.pop_front();

              // We are a worker thread, so we have a scheduler.
              // We attach the fiber to our scheduler and then schedule it.
              // Because we use shared_work, other workers can pick it up too.
              boost::fibers::context* active_ctx =
                  boost::fibers::context::active();

              active_ctx->attach(ctx.get());
              active_ctx->schedule(ctx.get());
            }
            if (shutdown_) {
              break;
            }
            cv_.WaitWithTimeout(&mu_, absl::Milliseconds(50));
            // Release the lock to allow the scheduler to run and process fibers
            // from the shared pool.
            mu_.unlock();
            boost::this_fiber::yield();
            mu_.lock();
          }
          DLOG(INFO) << absl::StrFormat("Worker %zu exiting.", idx);
        }),
    });
  }
}

void WorkerThreadPool::Schedule(
    boost::intrusive_ptr<boost::fibers::context> ctx) {
  act::concurrency::impl::MutexLock lock(&mu_);
  CHECK(ctx->get_scheduler() == nullptr)
      << "Cannot schedule an already scheduled context.";
  work_queue_.push_back(std::move(ctx));
  cv_.Signal();
}

WorkerThreadPool& WorkerThreadPool::Instance() {
  static WorkerThreadPool* instance = [] {
    auto* pool = new WorkerThreadPool();
    pool->Start();
    return pool;
  }();

  return *instance;
}

static absl::once_flag kInitWorkerThreadPoolFlag;

void EnsureWorkerThreadPool() {
  absl::call_once(kInitWorkerThreadPoolFlag, WorkerThreadPool::Instance);
}
}  // namespace thread