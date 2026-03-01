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

#ifndef ACTIONENGINE_NET_HTTP_PROXYGEN_FOLLY_UTILS_H_
#define ACTIONENGINE_NET_HTTP_PROXYGEN_FOLLY_UTILS_H_

#include "actionengine/concurrency/concurrency.h"

namespace folly {
class EventBase;
template <class Duration>
class HHWheelTimerBase;
using HHWheelTimer = HHWheelTimerBase<std::chrono::milliseconds>;
using HHWheelTimerHighRes = HHWheelTimerBase<std::chrono::microseconds>;
class SSLContext;
}  // namespace folly

namespace act::util {

struct HHWheelTimerImpl;

struct EvbWorker {
  EvbWorker();
  EvbWorker(EvbWorker&& other) noexcept;
  EvbWorker(const EvbWorker& other) = delete;
  EvbWorker& operator=(const EvbWorker& other) = delete;
  ~EvbWorker();

  folly::HHWheelTimer* timer() const;

  std::unique_ptr<folly::EventBase> evb;
  std::unique_ptr<HHWheelTimerImpl> timer_impl;
  std::unique_ptr<std::thread> thread;
};

class EvbWorkerPool {
 public:
  explicit EvbWorkerPool(size_t size);

  ~EvbWorkerPool();

  const EvbWorker& GetWorker() const;

 private:
  mutable act::Mutex mu_;
  mutable act::CondVar cv_;

  std::vector<EvbWorker> workers_;

  mutable size_t next_worker_ = 0;
};

}  // namespace act::util

namespace act::net::http {

folly::SSLContext* GetGlobalSSLContext();

}  // namespace act::net::http

#endif  // ACTIONENGINE_NET_HTTP_PROXYGEN_FOLLY_UTILS_H_