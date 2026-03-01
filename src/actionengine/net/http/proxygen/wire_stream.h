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

#ifndef ACTIONENGINE_NET_HTTP_PROXYGEN_WIRE_STREAM_H_
#define ACTIONENGINE_NET_HTTP_PROXYGEN_WIRE_STREAM_H_

#include <string>

#include <absl/flags/flag.h>
#include <absl/status/status.h>

#include "actionengine/data/types.h"
#include "actionengine/net/http/proxygen/ws_client_stream.h"
#include "actionengine/net/http/ws_common.h"
#include "actionengine/net/stream.h"
#include "actionengine/stores/byte_chunking.h"

namespace act::net::http {

class WebsocketWireStream final : public act::WireStream {
 public:
  static constexpr int kBufferSize = 256;

  static absl::StatusOr<std::unique_ptr<WebsocketWireStream>> Connect(
      std::string_view url, std::string_view id = "");

  explicit WebsocketWireStream(
      std::shared_ptr<net::http::WebsocketStream> stream, std::string_view id);

  ~WebsocketWireStream() override;

  absl::Status Send(WireMessage message) override;

  absl::StatusOr<std::optional<WireMessage>> Receive(
      absl::Duration timeout) override;

  absl::Status Accept() override;

  absl::Status Start() override;

  void HalfClose() override;

  void Abort(absl::Status status) override;

  absl::Status GetStatus() const override;

  std::string GetId() const override;

  const void* absl_nonnull GetImpl() const override;

 private:
  void HalfClose_() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void Abort_(absl::Status status) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  absl::Status Send_(WireMessage message) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable act::Mutex mu_;

  std::shared_ptr<net::http::WebsocketStream> stream_;
  std::string id_;

  thread::Channel<WireMessage> recv_channel_{kBufferSize};
  bool writer_closed_ ABSL_GUARDED_BY(mu_) = false;
  std::unique_ptr<act::data::ByteSplittingCodec> codec_;
};

}  // namespace act::net::http

#endif  // ACTIONENGINE_NET_HTTP_PROXYGEN_WIRE_STREAM_H_