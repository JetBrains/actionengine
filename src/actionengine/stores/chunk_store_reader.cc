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

#include "actionengine/stores/chunk_store_reader.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <tuple>
#include <utility>

#include <absl/base/nullability.h>
#include <absl/base/thread_annotations.h>
#include <absl/log/check.h>
#include <absl/log/log.h>
#include <absl/status/status.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/data/conversion.h"
#include "actionengine/data/types.h"
#include "actionengine/stores/chunk_store.h"
#include "actionengine/util/global_settings.h"
#include "actionengine/util/status_macros.h"

namespace act {

bool ChunkStoreReaderOptions::ordered_or_default() const {
  if (ordered) {
    return *ordered;
  }
  return GetGlobalSettings().readers_read_in_order;
}

bool ChunkStoreReaderOptions::remove_chunks_or_default() const {
  if (remove_chunks) {
    return *remove_chunks;
  }
  return GetGlobalSettings().readers_remove_read_chunks;
}

size_t ChunkStoreReaderOptions::n_chunks_to_buffer_or_default() const {
  if (n_chunks_to_buffer) {
    return *n_chunks_to_buffer;
  }
  return GetGlobalSettings().readers_buffer_size;
}

absl::Duration ChunkStoreReaderOptions::timeout_or_default() const {
  if (timeout) {
    return *timeout;
  }
  return GetGlobalSettings().readers_timeout;
}

ChunkStoreReader::ChunkStoreReader(ChunkStore* absl_nonnull chunk_store,
                                   ChunkStoreReaderOptions options)
    : chunk_store_(chunk_store), options_(std::move(options)) {}

ChunkStoreReader::~ChunkStoreReader() {
  act::MutexLock lock(&mu_);
  if (fiber_ == nullptr) {
    return;
  }

  const std::unique_ptr<thread::Fiber> fiber = std::move(fiber_);
  fiber_ = nullptr;

  fiber->Cancel();

  mu_.unlock();
  fiber->Join();
  mu_.lock();

  while (pending_ops_ > 0) {
    cv_.Wait(&mu_);
  }
}

void ChunkStoreReader::Cancel() const {
  act::MutexLock lock(&mu_);
  if (fiber_ != nullptr) {
    fiber_->Cancel();
    chunk_store_->Notify();
  }
}

void ChunkStoreReader::SetOptions(const ChunkStoreReaderOptions& options) {
  act::MutexLock lock(&mu_);
  options_ = options;
  if (fiber_ != nullptr) {
    LOG(WARNING) << "Reader options changed while prefetching is in progress. "
                    "Reader state is very likely inconsistent.";
  }
}

absl::StatusOr<std::optional<Chunk>> ChunkStoreReader::Next(
    std::optional<absl::Duration> timeout) {
  act::MutexLock lock(&mu_);
  ASSIGN_OR_RETURN(
      std::optional<Chunk> chunk,
      GetNextChunkFromBuffer(timeout.value_or(options_.timeout_or_default())));
  if (!chunk || chunk->IsNull()) {
    return std::nullopt;
  }
  if (chunk->metadata && chunk->metadata->mimetype == "__status__") {
    absl::StatusOr<absl::Status> status = ConvertTo<absl::Status>(*chunk);
    if (!status.ok()) {
      return status.status();
    }
  }
  return chunk;
}

absl::StatusOr<std::optional<NodeFragment>> ChunkStoreReader::NextFragment(
    std::optional<absl::Duration> timeout) {
  act::MutexLock lock(&mu_);
  absl::StatusOr<std::optional<std::pair<int, Chunk>>> seq_and_chunk =
      GetNextSeqAndChunkFromBuffer(
          timeout.value_or(options_.timeout_or_default()));
  RETURN_IF_ERROR(seq_and_chunk.status());
  ASSIGN_OR_RETURN(const int64_t final_seq, chunk_store_->GetFinalSeq());
  if (!seq_and_chunk->has_value()) {
    return std::nullopt;
  }
  if (seq_and_chunk->value().second.IsNull()) {
    // If the chunk is null, it means that the stream has ended.
    // TODO: this logic is not ideal, as it does not allow to distinguish
    //   between an empty chunk and the end of the stream. We should rethink it.
    return std::nullopt;
  }
  auto& chunk = seq_and_chunk->value().second;
  const int seq = seq_and_chunk->value().first;
  return NodeFragment{std::string(chunk_store_->GetId()), std::move(chunk), seq,
                      final_seq == -1 || seq != final_seq};
}

absl::StatusOr<act::Future<std::optional<NodeFragment>>>
ChunkStoreReader::NextNodeFragmentFuture() {
  act::MutexLock lock(&mu_);
  EnsurePrefetchIsRunningOrHasCompleted();
  if (!status_.ok()) {
    return status_;
  }

  act::Future<std::optional<NodeFragment>> future;
  waiters_.push_back(future.state());
  // If there is something in the buffer already, populate the future now:
  RETURN_IF_ERROR(FanoutBufferToWaiters());
  if (!status_.ok()) {
    return status_;
  }

  // If not populated and there are no values to come, set to nullopt.
  if (buffer_->length() == 0 && !loop_started_and_running_ &&
      !future.state()->HasValueOrError()) {
    mu_.unlock();
    const absl::Status status = future.SetValue(std::nullopt);
    mu_.lock();
    RETURN_IF_ERROR(status);
  }

  return future;
}

template <>
absl::StatusOr<std::optional<std::pair<int, Chunk>>> ChunkStoreReader::Next(
    std::optional<absl::Duration> timeout) {
  act::MutexLock lock(&mu_);
  return GetNextSeqAndChunkFromBuffer(
      timeout.value_or(options_.timeout_or_default()));
}

absl::StatusOr<std::optional<std::pair<int, Chunk>>>
ChunkStoreReader::GetNextSeqAndChunkFromBuffer(absl::Duration timeout)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  EnsurePrefetchIsRunningOrHasCompleted();

  std::optional<std::pair<int, Chunk>> seq_and_chunk;
  bool ok;
  ++pending_ops_;
  mu_.unlock();
  const int selected = thread::SelectUntil(
      absl::Now() + timeout,
      {buffer_->reader()->OnRead(&seq_and_chunk, &ok), thread::OnCancel()});
  mu_.lock();
  --pending_ops_;
  cv_.SignalAll();

  if (selected == -1) {
    return absl::DeadlineExceededError("Timed out waiting for chunk.");
  }
  if (thread::Cancelled()) {
    return absl::CancelledError("Cancelled waiting for chunk.");
  }

  if (!ok) {
    // If the prefetcher finished with an error, return the error.
    RETURN_IF_ERROR(status_);
    // Otherwise it simply finished reading.
    return std::nullopt;
  }
  if (seq_and_chunk) {
    if (const Chunk& chunk = seq_and_chunk->second;
        chunk.metadata && chunk.metadata->mimetype == "__status__") {
      if (absl::StatusOr<absl::Status> status = ConvertTo<absl::Status>(chunk);
          !status.ok()) {
        return status.status();
      }
    }
  }
  return seq_and_chunk;
}

absl::StatusOr<std::optional<std::pair<int, Chunk>>>
ChunkStoreReader::GetNextUnorderedSeqAndChunkFromStore() const
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {

  const int next_read_offset = total_chunks_read_;

  // if (const int final_seq = chunk_store_->GetFinalSeq();
  //     final_seq != -1 && next_read_offset > final_seq) {
  //   return std::nullopt;
  // }

  mu_.unlock();
  auto chunk_or_status = chunk_store_->GetByArrivalOrder(
      next_read_offset, absl::InfiniteDuration());
  mu_.lock();

  if (!chunk_or_status.ok()) {
    return chunk_or_status.status();
  }

  const Chunk& chunk = *chunk_or_status;
  ASSIGN_OR_RETURN(const int seq,
                   chunk_store_->GetSeqForArrivalOffset(next_read_offset));
  if (chunk.IsNull()) {
    mu_.unlock();
    absl::Status pop_status = chunk_store_->Pop(seq).status();
    mu_.lock();
    if (!pop_status.ok()) {
      DLOG(ERROR) << "Failed to pop chunk at seq " << seq << ": " << pop_status;
      return pop_status;
    }
    return std::nullopt;
  }

  return std::pair(seq, chunk);
}

absl::Status ChunkStoreReader::RunPrefetchLoop()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  absl::Status status;
  total_chunks_read_ = options_.start_seq_or_offset;

  while (!thread::Cancelled()) {
    if (const absl::StatusOr<int64_t> final_seq = chunk_store_->GetFinalSeq();
        !final_seq.ok()) {
      status = final_seq.status();
      break;
    } else if (*final_seq >= 0 && total_chunks_read_ > *final_seq) {
      // If we have read all chunks, we can stop.
      status = absl::OkStatus();
      break;
    }

    // Either branch of the following code will read the next chunk and seq
    // into these variables.
    Chunk next_chunk;
    int next_seq = -1;

    if (options_.ordered_or_default()) {
      mu_.unlock();
      auto chunk =
          chunk_store_->Get(total_chunks_read_, absl::InfiniteDuration());
      mu_.lock();
      if (!chunk.ok()) {
        status = chunk.status();
        break;
      }
      next_chunk = *chunk;
      next_seq = total_chunks_read_;
    } else {
      auto next_unordered_seq_and_chunk =
          GetNextUnorderedSeqAndChunkFromStore();
      if (!next_unordered_seq_and_chunk.ok()) {
        status = next_unordered_seq_and_chunk.status();
        break;
      }
      if (!next_unordered_seq_and_chunk->has_value()) {
        // No more chunks to read.
        status = absl::OkStatus();
        break;
      }
      if (auto next_seq_and_chunk = next_unordered_seq_and_chunk.value();
          next_seq_and_chunk.has_value()) {
        std::tie(next_seq, next_chunk) = *std::move(next_seq_and_chunk);
        if (next_seq == -1) {
          next_seq = 0;
        }
      }
    }

    if (options_.remove_chunks_or_default() && next_seq >= 0) {
      mu_.unlock();
      absl::Status pop_status = chunk_store_->Pop(next_seq).status();
      mu_.lock();
      if (!pop_status.ok()) {
        status = pop_status;
        DLOG(ERROR) << "Failed to pop chunk: " << pop_status;
        break;
      }
    }

    ++total_chunks_read_;

    std::pair<int, Chunk> next_seq_and_chunk =
        std::make_pair(next_seq, std::move(next_chunk));

    // It is crucial to unlock the mutex as buffer_ is finite, so readers
    // should be able to proceed if the maximum number of chunks has been
    // buffered:
    mu_.unlock();
    buffer_->writer()->Write(std::move(next_seq_and_chunk));
    mu_.lock();

    if (absl::Status fanout_status = FanoutBufferToWaiters();
        !fanout_status.ok()) {
      status = fanout_status;
      break;
    }
  }

  buffer_->writer()->Close();
  RETURN_IF_ERROR(FanoutBufferToWaiters());

  if (thread::Cancelled()) {
    status.Update(absl::CancelledError("Prefetcher fiber was cancelled."));
  }
  return status;
}

absl::Status ChunkStoreReader::FanoutBufferToWaiters() {
  while (!waiters_.empty() && buffer_->length() > 0) {
    const std::shared_ptr<NodeFragmentFuture::State> waiter = waiters_.front();
    waiters_.pop_front();

    // TODO: it should not be the reader's responsibility to set these
    //       statuses.
    if (waiter->Cancelled()) {
      mu_.unlock();
      waiter->SetError(absl::CancelledError("Cancelled.")).IgnoreError();
      mu_.lock();
      continue;
    }
    if (absl::Now() >= waiter->deadline()) {
      mu_.unlock();
      waiter->SetError(absl::DeadlineExceededError("Deadline exceeded."))
          .IgnoreError();
      mu_.lock();
      continue;
    }

    std::optional<std::pair<int, Chunk>> value_from_buffer;
    buffer_->reader()->Read(&value_from_buffer);

    absl::Status populate_status;

    if (!value_from_buffer || value_from_buffer->second.IsNull()) {
      mu_.unlock();
      populate_status = waiter->SetValue(std::nullopt);
      mu_.lock();
      RETURN_IF_ERROR(populate_status);
      continue;
    }
    std::optional<NodeFragment> fragment_from_buffer;
    if (value_from_buffer) {
      fragment_from_buffer.emplace();
      ASSIGN_OR_RETURN(const int64_t final_seq, chunk_store_->GetFinalSeq());
      int seq = value_from_buffer->first;
      fragment_from_buffer->seq = seq;
      fragment_from_buffer->data = std::move(value_from_buffer->second);
      fragment_from_buffer->continued = final_seq == -1 || seq != final_seq;
    }
    // gracefully indicate that NodeRef is not supported yet
    if (fragment_from_buffer &&
        fragment_from_buffer->GetNodeRef().status().ok()) {
      return absl::UnimplementedError(
          "NodeRef support is not implemented yet.");
    }

    // Resolve futures with errors in case of error statuses
    if (fragment_from_buffer) {
      ASSIGN_OR_RETURN(Chunk & chunk, fragment_from_buffer->GetChunk());
      if (chunk.metadata && chunk.metadata->mimetype == "__status__") {
        absl::StatusOr<absl::Status> status = ConvertTo<absl::Status>(chunk);
        if (!status.ok()) {
          {
            mu_.unlock();
            populate_status = waiter->SetError(status.status());
            mu_.lock();
            RETURN_IF_ERROR(populate_status);
          }
          return status.status();
        }
        {
          mu_.unlock();
          if (!status->ok()) {
            populate_status = waiter->SetError(*status);
          } else {
            populate_status = waiter->SetValue(std::move(fragment_from_buffer));
          }
          mu_.lock();
          RETURN_IF_ERROR(populate_status);
        }
        continue;
      }
    }

    mu_.unlock();
    populate_status = waiter->SetValue(std::move(fragment_from_buffer));
    mu_.lock();
    RETURN_IF_ERROR(populate_status);
  }

  // For consistency, if any non-Future reads are pending, let them consume
  // data (even though this can delay prefetching ever so slightly)
  while (pending_ops_ > 0 && buffer_->length() > 0) {
    cv_.Wait(&mu_);
  }

  return absl::OkStatus();
}

template <>
absl::StatusOr<std::optional<absl::Status>>
ChunkStoreReader::Next<absl::Status>(std::optional<absl::Duration> timeout) {
  ASSIGN_OR_RETURN(std::optional<Chunk> chunk, Next(timeout));
  if (!chunk) {
    return std::nullopt;
  }
  if (chunk->metadata && chunk->metadata->mimetype == "__status__") {
    absl::StatusOr<std::optional<absl::Status>> retval;
    absl::Status next_status;
    const absl::Status conversion_status =
        Assign(*std::move(chunk), &next_status);
    if (!conversion_status.ok()) {
      retval.AssignStatus(conversion_status);
      return retval;
    }
    retval.emplace() = std::move(next_status);
    return retval;
  }
  return absl::InvalidArgumentError(
      "Expected a status chunk, but got a regular chunk.");
}

absl::Status ChunkStoreReader::GetStatus() const {
  act::MutexLock lock(&mu_);
  return status_;
}

void ChunkStoreReader::EnsurePrefetchIsRunningOrHasCompleted()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (fiber_ != nullptr || buffer_ != nullptr) {
    return;
  }

  loop_started_and_running_ = true;
  buffer_ =
      std::make_unique<thread::Channel<std::optional<std::pair<int, Chunk>>>>(
          options_.n_chunks_to_buffer_or_default());

  status_ = absl::OkStatus();
  fiber_ = thread::NewTree({}, [this] {
    act::MutexLock lock(&mu_);
    status_ = RunPrefetchLoop();
    loop_started_and_running_ = false;
    // Any waiters left at this point will NOT receive any value; populate them
    // with errors.
    while (!waiters_.empty()) {
      const std::shared_ptr<NodeFragmentFuture::State> waiter =
          waiters_.front();
      waiters_.pop_front();
      if (!status_.ok()) {
        mu_.unlock();
        waiter->SetError(status_).IgnoreError();
        mu_.lock();
        continue;
      }
      mu_.unlock();
      waiter->SetValue(std::nullopt).IgnoreError();
      mu_.lock();
    }
    cv_.SignalAll();
  });
}

absl::StatusOr<std::optional<Chunk>> ChunkStoreReader::GetNextChunkFromBuffer(
    absl::Duration timeout) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  absl::StatusOr<std::optional<std::pair<int, Chunk>>> seq_and_chunk =
      GetNextSeqAndChunkFromBuffer(timeout);
  RETURN_IF_ERROR(seq_and_chunk.status());

  if (!seq_and_chunk->has_value()) {
    return std::nullopt;
  }
  if (seq_and_chunk->value().second.IsNull()) {
    // If the chunk is null, it means that the stream has ended.
    // TODO: this logic is not ideal, as it does not allow to distinguish
    //   between an empty chunk and the end of the stream. We should rethink it.
    return std::nullopt;
  }
  return std::move((*seq_and_chunk)->second);
}

}  // namespace act