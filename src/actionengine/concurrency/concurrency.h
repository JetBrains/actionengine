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

/**
 * @file
 * @brief
 *   Concurrency utilities for ActionEngine.
 *
 * This file provides a set of concurrency utilities for use in ActionEngine.
 * It includes classes and functions for managing fibers, channels, and
 * synchronization primitives.
 */

#ifndef ACTIONENGINE_CONCURRENCY_CONCURRENCY_H_
#define ACTIONENGINE_CONCURRENCY_CONCURRENCY_H_

#include <any>

#include <absl/base/attributes.h>
#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/status/statusor.h>
#include <absl/time/time.h>

#include "absl/container/flat_hash_map.h"
#include "thread/concurrency.h"

// IWYU pragma: begin_exports
#if GOOGLE3
#include "actionengine/google3_concurrency_headers.h"
#else
#include "thread/concurrency.h"
#endif  // GOOGLE3
// IWYU pragma: end_exports

/** @file
 *  @brief
 *    Concurrency utilities for ActionEngine.
 *
 *  This file provides a set of concurrency utilities for use in ActionEngine.
 *  That includes classes and functions for managing fibers, channels, and
 *  basic synchronization primitives: mutexes and condition variables.
 *
 *  This implementation is based on Boost::fiber, but it is designed to mimic
 *  closely the `thread::Fiber` and `thread::Channel` interfaces used
 *  internally at Google. A fully compatible implementation is neither a
 *  guarantee nor a goal, but the API is designed to be similar enough
 *  to be used interchangeably in most cases. The `Mutex` and `CondVar` classes
 *  provide the same basic functionality as their counterparts in Abseil.
 *
 *  @headerfile actionengine/concurrency/concurrency.h
 */

/** @private */
namespace act::concurrency {

#if GOOGLE3
using CondVar = absl::CondVar;
using Mutex = absl::Mutex;
using MutexLock = absl::MutexLock;

inline void SleepFor(absl::Duration duration) {
  absl::SleepFor(duration);
}
#else
using CondVar = impl::CondVar;
using Mutex = impl::Mutex;
using MutexLock = impl::MutexLock;

void SleepFor(absl::Duration duration);
#endif  // GOOGLE3

/** @brief
 *    A lock that locks two mutexes at once.
 *
 *  This class is used to lock two mutexes at once, ensuring that they are
 *  locked in a deadlock-free manner. It is useful for situations where two
 *  mutexes need to be locked together, such as when accessing shared data
 *  structures that are protected by multiple mutexes, or in move constructors
 *  where two mutexes need to be held to ensure thread safety.
 *
 *  There is no sophisticated deadlock prevention logic in this class, but
 *  rather a simple ordering of mutexes based on their addresses.
 */
class ABSL_SCOPED_LOCKABLE TwoMutexLock {
 public:
  explicit TwoMutexLock(Mutex* absl_nonnull mu1, Mutex* absl_nonnull mu2)
      ABSL_EXCLUSIVE_LOCK_FUNCTION(mu1, mu2);

  // This class is not copyable or movable.
  TwoMutexLock(const TwoMutexLock&) = delete;  // NOLINT(runtime/mutex)
  TwoMutexLock& operator=(const TwoMutexLock&) = delete;

  ~TwoMutexLock() ABSL_UNLOCK_FUNCTION();

 private:
  Mutex* absl_nonnull const mu1_;
  Mutex* absl_nonnull const mu2_;
};
}  // namespace act::concurrency

namespace act {

using concurrency::CondVar;
using concurrency::Mutex;
using concurrency::MutexLock;

/** @brief
 *    Sleeps for the given duration, allowing other fibers to proceed on the
 *    rest of the thread's quantum, and other threads to run.
 *
 * @param duration
 *   The duration to sleep for.
 */
inline void SleepFor(absl::Duration duration) {
  concurrency::SleepFor(duration);
}

template <typename T>
class FutureState {
 public:
  friend class FutureT;

  FutureState() = default;

  FutureState(const FutureState&) = delete;
  FutureState& operator=(const FutureState&) = delete;

  ~FutureState() {
    act::MutexLock lock(&mu_);
    if (pending_waiters_ > 0) {
      if (!result_has_been_set_) {
        if (const absl::Status set_error_status =
                SetError_(absl::AbortedError("Future is being destroyed."));
            !set_error_status.ok()) {
          DLOG(WARNING) << "Could not set error while destroying a future.";
          return;
        }
      }

      // We let the waiter(s) deal with the result before destroying.
      while (pending_waiters_ > 0) {
        cv_.Wait(&mu_);
      }
    }
  }

  void AddCallback(
      absl::AnyInvocable<void(FutureState<T>* absl_nonnull)> callback) {
    act::MutexLock lock(&mu_);
    if (result_has_been_set_) {
      mu_.unlock();
      std::move(callback)(this);
      mu_.lock();
      return;
    }
    callbacks_.push_back(std::move(callback));
  }

  absl::Status SetValue(T value) {
    act::MutexLock lock(&mu_);
    return SetValue_(std::move(value));
  }

  absl::Status SetError(absl::Status error) {
    act::MutexLock lock(&mu_);
    return SetError_(std::move(error));
  }

  void Cancel() {
    act::MutexLock lock(&mu_);
    Cancel_();
  }

  bool Cancelled() const {
    act::MutexLock lock(&mu_);
    return cancelled_;
  }

  bool Expired() const {
    act::MutexLock lock(&mu_);
    return absl::Now() >= deadline_;
  }

  absl::Time deadline() const {
    act::MutexLock lock(&mu_);
    return deadline_;
  }

  void SetDeadline(absl::Time deadline) {
    act::MutexLock lock(&mu_);
    deadline_ = deadline;
    cv_.SignalAll();
  }

  absl::StatusOr<T> WaitForValueOrErrorUntil(
      absl::Time deadline = absl::InfiniteFuture()) {
    act::MutexLock lock(&mu_);
    ++pending_waiters_;
    absl::StatusOr<T> result = WaitForValueOrErrorUntil_(deadline);
    --pending_waiters_;
    cv_.SignalAll();
    return result;
  }

  bool HasValueOrError() const {
    act::MutexLock lock(&mu_);
    return result_has_been_set_;
  }

 private:
  void ExecuteCallbacks_() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    for (auto& callback : callbacks_) {
      mu_.unlock();
      callback(this);
      mu_.lock();
    }
    callbacks_.clear();
  }

  void Cancel_() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (result_has_been_set_) {
      return;
    }
    cancelled_ = true;
    SetError_(absl::CancelledError("Cancelled.")).IgnoreError();
  }

  absl::StatusOr<T> WaitForValueOrErrorUntil_(
      absl::Time desired_deadline = absl::InfiniteFuture())
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    while (!result_has_been_set_ && !result_has_been_awaited_) {
      const absl::Time now = absl::Now();
      if (now >= desired_deadline || now >= deadline_) {
        break;
      }
      cv_.WaitWithDeadline(
          &mu_, desired_deadline < deadline_ ? desired_deadline : deadline_);
    }

    // It would be a mistake to make several WaitForValueOrErrorUntil calls,
    // but check for that anyway.
    if (result_has_been_awaited_) {
      return absl::FailedPreconditionError("Result has already been awaited.");
    }

    // Optimistic: if there is a result, return it even if past the deadline
    if (result_has_been_set_) {
      absl::StatusOr<T> result = std::move(result_);

      // Clear the state
      result_ = absl::StatusOr<T>();
      result_has_been_awaited_ = true;
      cv_.SignalAll();

      return result;
    }

    return absl::DeadlineExceededError(
        "Deadline exceeded waiting for Future result.");
  }

  absl::Status SetValue_(T value) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (result_has_been_set_) {
      return absl::FailedPreconditionError("Cannot set result twice.");
    }

    if (cancelled_) {
      return SetError_(absl::CancelledError("Cancelled."));
    }

    if (absl::Now() > deadline_) {
      return SetError_(absl::DeadlineExceededError("Deadline exceeded"));
    }

    result_ = std::move(value);
    result_has_been_set_ = true;
    ExecuteCallbacks_();
    cv_.SignalAll();

    return absl::OkStatus();
  }

  absl::Status SetError_(absl::Status error)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (error.ok()) {
      return absl::InvalidArgumentError(
          "SetError must be called with a non-ok status.");
    }
    if (result_has_been_set_) {
      return absl::FailedPreconditionError(
          "FutureState already has been set with a result or an error.");
    }

    result_has_been_set_ = true;
    result_ = std::move(error);
    ExecuteCallbacks_();
    cv_.SignalAll();

    return absl::OkStatus();
  }

  mutable act::Mutex mu_;
  act::CondVar cv_ ABSL_GUARDED_BY(mu_);
  absl::StatusOr<T> result_ ABSL_GUARDED_BY(mu_);

  bool cancelled_ ABSL_GUARDED_BY(mu_);
  bool result_has_been_set_ ABSL_GUARDED_BY(mu_) = false;
  bool result_has_been_awaited_ ABSL_GUARDED_BY(mu_) = false;
  uint8_t pending_waiters_ ABSL_GUARDED_BY(mu_) = 0;
  absl::Time deadline_ ABSL_GUARDED_BY(mu_) = absl::InfiniteFuture();
  std::vector<absl::AnyInvocable<void(FutureState<T>* absl_nonnull)>> callbacks_
      ABSL_GUARDED_BY(mu_);
};

template <typename T>
class Future {
 public:
  using State = FutureState<T>;

  Future() : state_(std::make_unique<FutureState<T>>()) {}

  Future(Future&& other) noexcept : state_(std::move(other.state_)) {}

  Future& operator=(Future&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    state_ = std::move(other.state_);
    return *this;
  }

  absl::Status SetValue(T value) const {
    return state_->SetValue(std::move(value));
  }

  absl::Status SetError(absl::Status error) const {
    return state_->SetError(std::move(error));
  }

  absl::StatusOr<T> WaitUntil(absl::Time deadline = absl::InfiniteFuture()) {
    return state_->WaitForValueOrErrorUntil(deadline);
  }

  std::shared_ptr<State> state() { return state_; }

 private:
  std::shared_ptr<State> state_;
};

}  // namespace act

#endif  // ACTIONENGINE_CONCURRENCY_CONCURRENCY_H_
