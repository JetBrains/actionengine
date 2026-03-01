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

#include "actionengine/net/webrtc/signalling_client.h"

#include <absl/container/flat_hash_map.h>
#include <absl/log/log.h>
#include <absl/strings/str_format.h>
#include <absl/time/time.h>
#include <boost/json/parse.hpp>
#include <proxygen/lib/http/HTTPMessage.h>

#include "actionengine/util/status_macros.h"

namespace act::net {

SignallingClient::SignallingClient(std::string_view address, uint16_t port,
                                   bool use_ssl)
    : address_(address), port_(port), use_ssl_(use_ssl) {}

SignallingClient::~SignallingClient() {
  act::MutexLock lock(&mu_);
  CancelInternal();
  on_offer_ = nullptr;
  on_candidate_ = nullptr;
  on_answer_ = nullptr;
  mu_.unlock();
  stream_.reset();
  mu_.lock();
}

void SignallingClient::ResetCallbacks() {
  act::MutexLock lock(&mu_);
  on_offer_ = nullptr;
  on_candidate_ = nullptr;
  on_answer_ = nullptr;
}

void SignallingClient::OnOffer(PeerJsonHandler on_offer) {
  act::MutexLock lock(&mu_);
  on_offer_ = std::move(on_offer);
}

void SignallingClient::OnCandidate(PeerJsonHandler on_candidate) {
  act::MutexLock lock(&mu_);
  on_candidate_ = std::move(on_candidate);
}

void SignallingClient::OnAnswer(PeerJsonHandler on_answer) {
  act::MutexLock lock(&mu_);
  on_answer_ = std::move(on_answer);
}

thread::Case SignallingClient::OnError() const {
  return error_event_.OnEvent();
}

absl::Status SignallingClient::GetStatus() const {
  act::MutexLock lock(&mu_);
  return loop_status_;
}

absl::Status SignallingClient::ConnectWithIdentity(
    std::string_view identity,
    const absl::flat_hash_map<std::string, std::string>& headers) {
  act::MutexLock l(&mu_);

  if (!on_answer_ && !on_offer_ && !on_candidate_) {
    return absl::FailedPreconditionError(
        "WebsocketActionEngineServer no handlers set: connecting in this "
        "state would lose messages");
  }

  identity_ = std::string(identity);

  std::string url = absl::StrFormat(
      "%s://%s:%d/%s", use_ssl_ ? "https" : "http", address_, port_, identity_);

  auto stream_or = http::WebsocketClientStream::Connect(url);
  if (!stream_or.ok()) {
    loop_status_ = stream_or.status();
    return loop_status_;
  }
  stream_ = std::move(stream_or).value();

  proxygen::HTTPMessage proxygen_headers;
  for (const auto& [key, value] : headers) {
    proxygen_headers.getHeaders().add(key, value);
  }
  stream_->SetRequestHeaders(proxygen_headers);

  stream_->SetFrameCallback(
      [this](http::WebsocketStream* s, http::WebsocketFrame* f) {
        HandleFrame(s, f);
      });
  stream_->SetDoneCallback([this](http::WebsocketStream* s) { HandleDone(s); });

  stream_->Start();

  return absl::OkStatus();
}

absl::Status SignallingClient::Send(const std::string& message) const {
  act::MutexLock lock(&mu_);
  if (!stream_) {
    return absl::FailedPreconditionError("Not connected");
  }
  return stream_->SendText(message);
}

void SignallingClient::Cancel() {
  act::MutexLock lock(&mu_);
  CancelInternal();
}

void SignallingClient::Join() {
  // WebsocketClientStream is asynchronous, so we don't have a thread to join.
}

void SignallingClient::CancelInternal() {
  if (stream_ != nullptr) {
    stream_->HalfClose(1000, "SignallingClient cancelled");
  }
  loop_status_ = absl::CancelledError("WebsocketActionEngineServer cancelled");
}

void SignallingClient::HandleFrame(http::WebsocketStream* stream,
                                   http::WebsocketFrame* frame) {
  if (frame == nullptr) {
    return;
  }

  if (frame->opcode != http::WSOpcode::kText) {
    return;
  }

  std::string message = frame->GetUnmaskedDataString();

  boost::system::error_code error;
  boost::json::value parsed_message = boost::json::parse(message, error);
  if (error) {
    LOG(ERROR) << "WebsocketActionEngineServer parse() failed: "
               << error.message();
    return;
  }

  std::string client_id;
  if (const auto id_ptr = parsed_message.find_pointer("/id", error);
      id_ptr == nullptr || error) {
    LOG(ERROR) << "WebsocketActionEngineServer no 'id' field in message: "
               << message;
    return;
  } else {
    client_id = id_ptr->as_string().c_str();
  }

  std::string type;
  if (const auto type_ptr = parsed_message.find_pointer("/type", error);
      type_ptr == nullptr || error) {
    LOG(ERROR) << "WebsocketActionEngineServer no 'type' field in message: "
               << message;
    return;
  } else {
    type = type_ptr->as_string().c_str();
  }

  if (type != "offer" && type != "candidate" && type != "answer") {
    LOG(ERROR) << "WebsocketActionEngineServer unknown message type: " << type
               << " in message: " << message;
    return;
  }

  PeerJsonHandler handler = nullptr;
  {
    act::MutexLock lock(&mu_);
    if (type == "offer") {
      handler = on_offer_;
    } else if (type == "candidate") {
      handler = on_candidate_;
    } else if (type == "answer") {
      handler = on_answer_;
    }
  }

  if (handler) {
    handler(client_id, std::move(parsed_message));
  }
}

void SignallingClient::HandleDone(http::WebsocketStream* stream) {
  act::MutexLock lock(&mu_);
  loop_status_ = stream->GetStatus();
  if (!loop_status_.ok()) {
    on_answer_ = nullptr;
    on_candidate_ = nullptr;
    on_offer_ = nullptr;
    error_event_.Notify();
  }
}
}  // namespace act::net
