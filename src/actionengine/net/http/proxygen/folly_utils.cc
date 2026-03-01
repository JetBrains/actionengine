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

#include "actionengine/net/http/proxygen/folly_utils.h"

#include <absl/base/call_once.h>
#include <absl/base/nullability.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/io/async/SSLContext.h>
#include <folly/io/async/SSLOptions.h>

namespace act::util {

struct HHWheelTimerImpl {
  folly::HHWheelTimer::UniquePtr timer;
};

EvbWorker::EvbWorker() = default;

EvbWorker::EvbWorker(EvbWorker&& other) noexcept {
  evb = std::move(other.evb);
  timer_impl = std::move(other.timer_impl);
  thread = std::move(other.thread);
}

EvbWorker::~EvbWorker() {}

folly::HHWheelTimer* EvbWorker::timer() const {
  if (timer_impl == nullptr) {
    return nullptr;
  }
  return timer_impl->timer.get();
}

EvbWorkerPool::EvbWorkerPool(size_t size) {
  act::MutexLock lock(&mu_);
  workers_.reserve(size);
  for (size_t idx = 0; idx < size; ++idx) {
    workers_.push_back({});
    workers_.back().thread =
        std::make_unique<std::thread>([this, self = &workers_[idx]] {
          self->evb = std::make_unique<folly::EventBase>();
          folly::EventBaseManager::get()->setEventBase(self->evb.get(),
                                                       /*takeOwnership=*/true);
          self->timer_impl =
              std::make_unique<HHWheelTimerImpl>(HHWheelTimerImpl{
                  .timer = folly::HHWheelTimer::newTimer(
                      self->evb.get(),
                      std::chrono::milliseconds(
                          folly::HHWheelTimer::DEFAULT_TICK_INTERVAL),
                      folly::AsyncTimeout::InternalEnum::NORMAL,
                      std::chrono::milliseconds(5000))});

          // Signal that evb and timer are ready
          cv_.SignalAll();

          self->evb->loopForever();
        });
  }

  // Wait until all workers have initialised
  for (const auto& worker : workers_) {
    while (worker.evb == nullptr || worker.timer() == nullptr) {
      cv_.Wait(&mu_);
    }
  }
}

EvbWorkerPool::~EvbWorkerPool() {
  DLOG(INFO) << "EvbWorkerPool::~EvbWorkerPool";
  act::MutexLock lock(&mu_);
  for (const auto& worker : workers_) {
    worker.evb->terminateLoopSoon();
    worker.thread->join();
  }
}

const EvbWorker& EvbWorkerPool::GetWorker() const {
  act::MutexLock lock(&mu_);
  const size_t next_worker = next_worker_++;
  if (next_worker_ >= workers_.size()) {
    next_worker_ = 0;
  }
  return workers_[next_worker];
}

}  // namespace act::util

namespace act::net::http {

static absl::once_flag ssl_init_flag;

void InitSSLContext(folly::SSLContext* absl_nonnull ssl_ctx) {
  // ssl_ctx->setOptions(SSL_OP_NO_COMPRESSION);
  folly::ssl::setCipherSuites<folly::ssl::SSLCommonOptions>(*ssl_ctx);
  // ssl_ctx->loadTrustedCertificates("");
  std::list<std::string> next_protos;
  folly::splitTo<std::string>(',', "http/1.1",
                              std::inserter(next_protos, next_protos.begin()));
  // folly::splitTo<std::string>(',', "h2,h2-14,spdy/3.1,spdy/3,http/1.1",
  //                             std::inserter(next_protos, next_protos.begin()));
  ssl_ctx->setAdvertisedNextProtocols(next_protos);
}

folly::SSLContext* GetGlobalSSLContext() {
  static auto ctx = new folly::SSLContext();
  absl::call_once(ssl_init_flag, InitSSLContext, ctx);
  return ctx;
}

}  // namespace act::net::http
