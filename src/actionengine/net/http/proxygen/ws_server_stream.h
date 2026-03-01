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

#ifndef ACTIONENGINE_NET_HTTP_PROXYGEN_WS_SERVER_STREAM_H_
#define ACTIONENGINE_NET_HTTP_PROXYGEN_WS_SERVER_STREAM_H_

#include <memory>
#include <optional>

#include <absl/functional/any_invocable.h>
#include <absl/status/statusor.h>

#include "actionengine/net/http/ws_common.h"
#include "actionengine/net/stream.h"

namespace folly {
class EventBase;
template <class Duration>
class HHWheelTimerBase;
using HHWheelTimer = HHWheelTimerBase<std::chrono::milliseconds>;
using HHWheelTimerHighRes = HHWheelTimerBase<std::chrono::microseconds>;
}  // namespace folly

namespace proxygen {
class HTTPMessage;
class RequestHandler;
}  // namespace proxygen

namespace act::net::http {

class WebsocketServerStreamImpl;

class WebsocketServerStream final : public WebsocketStream {
 public:
  friend class WebsocketServerStreamFactory;

  explicit WebsocketServerStream();
  ~WebsocketServerStream() override;

  void SetResponseHeaders(const proxygen::HTTPMessage& headers) const;
  void SetResponseHeaders(std::unique_ptr<proxygen::HTTPMessage> headers) const;
  absl::StatusOr<std::reference_wrapper<const proxygen::HTTPMessage>>
  GetRequestHeaders(absl::Duration timeout) const;

  absl::Status SendBytes(const uint8_t* absl_nonnull data,
                         size_t size) override;
  absl::Status SendFrame(const WebsocketFrame& frame) override;
  absl::Status SendText(std::string_view text) override;

  void Accept() override;
  void Start() override;
  absl::Status GetStatus() const override;

  void HalfClose(uint16_t code, std::string_view reason) override;
  void Abort(absl::Status status) override;
  [[nodiscard]] bool HalfClosed() const override;

  void SetFrameCallback(FrameCallback on_frame) override;
  void SetDoneCallback(DoneCallback on_done) override;
  void SetRequestHeadersCallback(RequestHeadersCallback on_headers);

  proxygen::RequestHandler* absl_nonnull GetRequestHandler() const;

 private:
  std::unique_ptr<WebsocketServerStreamImpl> impl_;
};

}  // namespace act::net::http

#endif  // ACTIONENGINE_NET_HTTP_PROXYGEN_WS_SERVER_STREAM_H_
