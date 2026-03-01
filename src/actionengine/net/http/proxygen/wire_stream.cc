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

#include "actionengine/net/http/proxygen/wire_stream.h"

#include <string>

#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/status/status.h>

#include "actionengine/data/types.h"
#include "actionengine/net/http/ws_common.h"
#include "actionengine/stores/byte_chunking.h"
#include "actionengine/util/random.h"
#include "actionengine/util/status_macros.h"
#include "cppack/msgpack.h"

namespace act::net::http {

absl::StatusOr<std::unique_ptr<WebsocketWireStream>>
WebsocketWireStream::Connect(std::string_view url, std::string_view id) {
  ASSIGN_OR_RETURN(auto ws_stream, WebsocketClientStream::Connect(url));
  return std::make_unique<WebsocketWireStream>(
      std::move(ws_stream), id.empty() ? GenerateUUID4() : id);
}

WebsocketWireStream::WebsocketWireStream(
    std::shared_ptr<net::http::WebsocketStream> stream, std::string_view id)
    : stream_(std::move(stream)), id_(id) {
  CHECK(stream_ != nullptr);
  CHECK(!id_.empty());

  codec_ = std::make_unique<act::data::ByteSplittingCodec>(
      [this](const uint8_t* data, size_t size) {
        return stream_->SendBytes(data, size);
      },
      65536);

  stream_->SetFrameCallback([this](net::http::WebsocketStream* stream,
                                   net::http::WebsocketFrame* frame) {
    act::MutexLock lock(&mu_);
    if (frame == nullptr) {
      if (!writer_closed_) {
        recv_channel_.writer()->Close();
        writer_closed_ = true;
      }
      return;
    }
    if (writer_closed_) {
      // no data will ever be read, no point in parsing the frame
      return;
    }

    std::vector<uint8_t> message_data;
    if (codec_ != nullptr) {
      mu_.unlock();
      absl::StatusOr<std::optional<std::vector<uint8_t>>> maybe_full_message =
          codec_->FeedIncomingPacket(frame->unmasked_payload.data(),
                                     frame->unmasked_payload.size());
      mu_.lock();

      if (!maybe_full_message.ok()) {
        mu_.unlock();
        stream->Abort(
            absl::InternalError(maybe_full_message.status().message()));
        mu_.lock();
        return;
      }

      if (!*maybe_full_message) {
        return;
      }
      message_data = **std::move(maybe_full_message);
    } else {
      message_data = std::vector(frame->unmasked_payload.begin(),
                                 frame->unmasked_payload.end());
    }

    mu_.unlock();
    absl::StatusOr<WireMessage> unpacked =
        cppack::Unpack<WireMessage>(message_data);
    mu_.lock();

    if (!unpacked.ok()) {
      mu_.unlock();
      stream->Abort(absl::InternalError(
          absl::StrFormat("Unpack failed: %s", unpacked.status().message())));
      mu_.lock();
      return;
    }

    recv_channel_.writer()->WriteUnlessCancelled(*std::move(unpacked));
  });
  stream_->SetDoneCallback([this](net::http::WebsocketStream*) {
    act::MutexLock lock(&mu_);
    if (!writer_closed_) {
      recv_channel_.writer()->Close();
      writer_closed_ = true;
    }
  });
}

WebsocketWireStream::~WebsocketWireStream() {
  act::MutexLock lock(&mu_);
  // stream_->SetFrameCallback(nullptr);
  // stream_->SetDoneCallback(nullptr);
  mu_.unlock();
  stream_.reset();
  mu_.lock();
}

absl::Status WebsocketWireStream::Send(WireMessage message) {
  act::MutexLock lock(&mu_);
  return Send_(std::move(message));
}

absl::StatusOr<std::optional<WireMessage>> WebsocketWireStream::Receive(
    absl::Duration timeout) {
  const absl::Time now = absl::Now();
  act::MutexLock lock(&mu_);
  const absl::Time deadline = now + timeout;

  WireMessage message;
  bool ok;

  mu_.unlock();
  const int selected = thread::SelectUntil(
      deadline, {recv_channel_.reader()->OnRead(&message, &ok)});
  mu_.lock();

  if (selected == 0 && !ok) {
    if (absl::Status status = stream_->GetStatus(); !status.ok()) {
      return status;
    }
    return std::nullopt;
  }

  if (selected == -1) {
    return absl::DeadlineExceededError(
        "Receive timed out while waiting for a message.");
  }

  if (IsHalfCloseMessage(message)) {
    return std::nullopt;
  }

  if (auto [is_abort, abort_status] = GetReasonIfIsAbortMessage(message);
      is_abort) {
    stream_->Abort(abort_status);
    return abort_status;
  }

  return message;
}

absl::Status WebsocketWireStream::Accept() {
  act::MutexLock lock(&mu_);
  stream_->Accept();
  return absl::OkStatus();
}

absl::Status WebsocketWireStream::Start() {
  act::MutexLock lock(&mu_);
  stream_->Start();
  return absl::OkStatus();
}

void WebsocketWireStream::HalfClose() {
  act::MutexLock lock(&mu_);
  HalfClose_();
}

void WebsocketWireStream::Abort(absl::Status status) {
  act::MutexLock lock(&mu_);
  Abort_(std::move(status));
}

absl::Status WebsocketWireStream::GetStatus() const {
  act::MutexLock lock(&mu_);
  return stream_->GetStatus();
}

std::string WebsocketWireStream::GetId() const {
  return id_;
}

const void* WebsocketWireStream::GetImpl() const {
  return stream_.get();
}

void WebsocketWireStream::HalfClose_() {
  if (stream_->HalfClosed()) {
    return;
  }
  Send_(MakeHalfCloseMessage()).IgnoreError();
  stream_->HalfClose(1000, "");
}

void WebsocketWireStream::Abort_(absl::Status status) {
  if (stream_->HalfClosed()) {
    return;
  }
  Send_(MakeAbortMessage(status)).IgnoreError();
  stream_->Abort(std::move(status));
}

absl::Status WebsocketWireStream::Send_(WireMessage message) {
  if (stream_->HalfClosed()) {
    return absl::FailedPreconditionError("Stream is closed");
  }

  if (codec_ == nullptr) {
    mu_.unlock();
    const std::vector<uint8_t> message_uint8_t =
        cppack::Pack(std::move(message));
    absl::Status status =
        stream_->SendBytes(message_uint8_t.data(), message_uint8_t.size());
    mu_.lock();
    return status;
  }

  mu_.unlock();
  const std::vector<uint8_t> message_uint8_t = cppack::Pack(std::move(message));
  absl::Status status =
      codec_->Write(message_uint8_t.data(), message_uint8_t.size());
  mu_.lock();
  return status;
}

}  // namespace act::net::http