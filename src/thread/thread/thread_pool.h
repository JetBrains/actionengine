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

#ifndef THREAD_FIBER_THREAD_POOL_H_
#define THREAD_FIBER_THREAD_POOL_H_

#include <atomic>
#include <thread>

#include <boost/context/pooled_fixedsize_stack.hpp>

#include "thread/boost_primitives.h"

namespace thread {

template <typename Algo, typename... Args>
static void EnsureThreadHasScheduler(Args&&... args) {
  thread_local bool kThreadHasScheduler = false;
  if (kThreadHasScheduler) {
    return;
  }

  boost::fibers::use_scheduling_algorithm<Algo>(std::forward<Args>(args)...);
  kThreadHasScheduler = true;
}

class WorkerThreadPool {
 public:
  explicit WorkerThreadPool() = default;

  ~WorkerThreadPool();

  void Start(size_t num_threads = std::thread::hardware_concurrency());

  void Schedule(boost::intrusive_ptr<boost::fibers::context> ctx);

  static WorkerThreadPool& Instance();

  boost::context::pooled_fixedsize_stack& Allocator() {
    thread_local boost::context::pooled_fixedsize_stack alloc;
    return alloc;
  }

 private:
  struct Worker {
    std::thread thread;
  };

  act::concurrency::impl::Mutex mu_{};
  act::concurrency::impl::CondVar cv_ ABSL_GUARDED_BY(mu_);
  std::atomic<size_t> worker_idx_{0};
  absl::InlinedVector<Worker, 16> workers_{};
  std::deque<boost::intrusive_ptr<boost::fibers::context>> work_queue_
      ABSL_GUARDED_BY(mu_);
  bool shutdown_ ABSL_GUARDED_BY(mu_) = false;
};

void EnsureWorkerThreadPool();
}  // namespace thread

#endif  // THREAD_FIBER_THREAD_POOL_H_