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

#ifndef ACTIONENGINE_NET_HTTP_WS_COMMON_H_
#define ACTIONENGINE_NET_HTTP_WS_COMMON_H_

#include <array>
#include <string_view>

#include <absl/container/inlined_vector.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_cat.h>

namespace act::net::http {

static constexpr std::string_view kWSKeyHeader = "Sec-WebSocket-Key";
static constexpr std::string_view kWSProtocolHeader = "Sec-WebSocket-Protocol";
static constexpr std::string_view kWSExtensionsHeader =
    "Sec-WebSocket-Extensions";
static constexpr std::string_view kWSAcceptHeader = "Sec-WebSocket-Accept";
static constexpr std::string_view kWSVersionHeader = "Sec-WebSocket-Version";

static constexpr std::string_view kWSVersion = "13";
static constexpr std::string_view kUpgradeTo = "websocket";

enum class WSOpcode : uint8_t {
  kContinuation = 0x0,
  kText = 0x1,
  kBinary = 0x2,
  kClose = 0x8,
  kPing = 0x9,
  kPong = 0xA,
};

inline bool IsValidWSOpcode(WSOpcode opcode) {
  return opcode == WSOpcode::kContinuation || opcode == WSOpcode::kText ||
         opcode == WSOpcode::kBinary || opcode == WSOpcode::kClose ||
         opcode == WSOpcode::kPing || opcode == WSOpcode::kPong;
}

template <typename Sink>
void AbslStringify(Sink& sink, WSOpcode opcode) {
  if (opcode == WSOpcode::kContinuation) {
    sink.Append("0x0 <continuation>");
  } else if (opcode == WSOpcode::kText) {
    sink.Append("0x1 <text>");
  } else if (opcode == WSOpcode::kBinary) {
    sink.Append("0x2 <binary>");
  } else if (opcode == WSOpcode::kClose) {
    sink.Append("0x8 <close>");
  } else if (opcode == WSOpcode::kPing) {
    sink.Append("0x9 <ping>");
  } else if (opcode == WSOpcode::kPong) {
    sink.Append("0xA <pong>");
  } else {
    sink.Append("0x? <unknown>");
  }
}

struct WebsocketFrame {
  static absl::StatusOr<WebsocketFrame> MakeCloseFrame(uint16_t code,
                                                       std::string_view reason);

  static absl::StatusOr<WebsocketFrame> ParseFromBuffer(
      const uint8_t* absl_nonnull data, size_t size,
      size_t* absl_nullable next_offset = nullptr);

  [[nodiscard]] std::string GetUnmaskedDataString() const;
  [[nodiscard]] std::string_view GetUnmaskedDataView() const;

  [[nodiscard]] std::string Serialize() const;

  void SetMask(uint32_t new_mask);

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const WebsocketFrame& frame);

  bool fin = true;
  bool rsv1 = false;
  bool rsv2 = false;
  bool rsv3 = false;
  WSOpcode opcode = WSOpcode::kBinary;

  bool masked = false;
  std::array<uint8_t, 4> mask;

  absl::InlinedVector<uint8_t, 128> unmasked_payload;
};

template <typename Sink>
void AbslStringify(Sink& sink, const WebsocketFrame& frame) {
  sink.Append(absl::StrCat("fin: ", frame.fin, ", opcode: ", frame.opcode,
                           ", masked: ", frame.masked,
                           ", data length: ", frame.unmasked_payload.size()));
}

absl::StatusOr<uint16_t> GetCloseCode(const WebsocketFrame& frame);
absl::StatusOr<std::string_view> GetCloseReason(const WebsocketFrame& frame);

class WebsocketStream {
 public:
  using FrameCallback = std::function<void(WebsocketStream* absl_nonnull,
                                           WebsocketFrame* absl_nullable)>;
  using DoneCallback = std::function<void(WebsocketStream* absl_nonnull)>;

  virtual ~WebsocketStream() = default;

  virtual absl::Status SendBytes(std::string_view data) {
    return this->SendBytes(reinterpret_cast<const uint8_t*>(data.data()),
                           data.size());
  }

  virtual absl::Status SendBytes(const uint8_t* absl_nonnull data,
                                 size_t size) = 0;
  virtual absl::Status SendFrame(const WebsocketFrame& frame) = 0;
  virtual absl::Status SendText(std::string_view text) = 0;

  virtual void Accept() = 0;
  virtual void Start() = 0;
  virtual absl::Status GetStatus() const = 0;

  virtual void HalfClose(uint16_t code, std::string_view reason) = 0;
  virtual void Abort(absl::Status status) = 0;
  [[nodiscard]] virtual bool HalfClosed() const = 0;

  virtual void SetFrameCallback(FrameCallback on_frame) = 0;
  virtual void SetDoneCallback(DoneCallback on_done) = 0;
};

using RequestHeadersCallback =
    std::function<void(std::shared_ptr<WebsocketStream>)>;

}  // namespace act::net::http

namespace act::net {

struct WsUrl {
  std::string scheme;
  std::string host;
  uint16_t port;
  std::string target;

  static absl::StatusOr<WsUrl> FromString(std::string_view url_str);

  static WsUrl FromStringOrDie(std::string_view url_str);
};

}  // namespace act::net

#endif  // ACTIONENGINE_NET_HTTP_WS_COMMON_H_