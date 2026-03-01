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

#include "actionengine/net/http/ws_common.h"

#include <array>
#include <string>
#include <string_view>

#include <absl/log/log.h>
#include <absl/status/statusor.h>

#include "actionengine/stores/byte_chunking.h"

namespace act::net::http {

static std::array<uint8_t, 4> EnsureBigEndian(uint32_t value) {
  return {static_cast<uint8_t>((value >> 24) & 0xFF),
          static_cast<uint8_t>((value >> 16) & 0xFF),
          static_cast<uint8_t>((value >> 8) & 0xFF),
          static_cast<uint8_t>(value & 0xFF)};
}

absl::StatusOr<WebsocketFrame> WebsocketFrame::MakeCloseFrame(
    uint16_t code, std::string_view reason) {
  if (reason.size() > 123) {
    return absl::InvalidArgumentError(
        "Close reason too long: must not exceed 123 characters");
  }
  WebsocketFrame frame;
  frame.opcode = WSOpcode::kClose;
  frame.unmasked_payload.push_back(code >> 8);
  frame.unmasked_payload.push_back(code & 0xFF);
  frame.unmasked_payload.insert(frame.unmasked_payload.end(), reason.begin(),
                                reason.end());
  return frame;
}

absl::StatusOr<WebsocketFrame> WebsocketFrame::ParseFromBuffer(
    const uint8_t* data, size_t size, size_t* absl_nullable next_offset) {
  if (size < 2) {
    return absl::InvalidArgumentError(
        "Frame too short, at least 2 bytes required.");
  }

  std::string_view data_view(reinterpret_cast<const char*>(data), size);

  WebsocketFrame frame;
  const uint8_t first_byte = data[0];
  frame.fin = (first_byte & 0b10000000) != 0;
  frame.rsv1 = (first_byte & 0b01000000) != 0;
  frame.rsv2 = (first_byte & 0b00100000) != 0;
  frame.rsv3 = (first_byte & 0b00010000) != 0;
  frame.opcode = static_cast<WSOpcode>(first_byte & 0b00001111);

  if (!IsValidWSOpcode(frame.opcode)) {
    return absl::InvalidArgumentError("Invalid opcode.");
  }

  const uint8_t second_byte = data[1];
  frame.masked = (second_byte & 0b10000000) != 0;
  size_t payload_size = second_byte & 0b01111111;

  size_t offset = 2;

  // Magic number for 16-bit payload size
  if (payload_size == 126) {
    if (size < offset + 2) {
      return absl::InvalidArgumentError(
          "Frame too short, at least 2 bytes required for payload size.");
    }
    payload_size = (data[offset] << 8) | data[offset + 1];
    offset += 2;
  }

  // Magic number for 64-bit payload size
  if (payload_size == 127) {
    if (size < offset + 8) {
      return absl::InvalidArgumentError(
          "Frame too short, at least 8 bytes required for payload size.");
    }
    payload_size = 0;
    for (size_t i = 0; i < 8; ++i) {
      payload_size <<= 8;
      payload_size |= data[offset + i];
    }
    offset += 8;
  }

  if (frame.masked) {
    if (size < offset + 4) {
      return absl::InvalidArgumentError(
          "Frame too short, at least 4 bytes required for mask.");
    }
    frame.mask[0] = data[offset];
    frame.mask[1] = data[offset + 1];
    frame.mask[2] = data[offset + 2];
    frame.mask[3] = data[offset + 3];
    offset += 4;
  }

  if (size < offset + payload_size) {
    DLOG(INFO) << frame;
    return absl::InvalidArgumentError(absl::StrCat(
        "Not enough bytes left in frame to match payload size: size=", size,
        " offset=", offset, " payload_size=", payload_size, "."));
  }

  frame.unmasked_payload.clear();
  frame.unmasked_payload.reserve(payload_size);
  if (frame.masked) {
    size_t bytes_processed = 0;
    for (size_t i = offset; i < offset + payload_size; ++i) {
      frame.unmasked_payload.push_back(data[i] ^
                                       frame.mask[bytes_processed % 4]);
      ++bytes_processed;
    }
  } else {
    frame.unmasked_payload.insert(frame.unmasked_payload.end(), data + offset,
                                  data + offset + payload_size);
  }

  if (next_offset != nullptr) {
    *next_offset = *next_offset + offset + payload_size;
  }

  return frame;
}

std::string WebsocketFrame::GetUnmaskedDataString() const {
  std::string result;
  result.reserve(unmasked_payload.size());
  for (const auto& byte : unmasked_payload) {
    result.push_back(byte);
  }
  return result;
}

std::string WebsocketFrame::Serialize() const {
  std::string result;
  result.reserve(unmasked_payload.size() + 10);

  uint8_t first_byte = fin ? 0b10000000 : 0;
  first_byte |= rsv1 ? 0b01000000 : 0;
  first_byte |= rsv2 ? 0b00100000 : 0;
  first_byte |= rsv3 ? 0b00010000 : 0;
  first_byte |= static_cast<uint8_t>(opcode);
  result.push_back(first_byte);

  size_t unmasked_payload_size = unmasked_payload.size();

  uint8_t second_byte = masked ? 0b10000000 : 0;
  if (unmasked_payload_size > 65535) {
    second_byte |= 127;
  } else if (unmasked_payload_size > 125) {
    second_byte |= 126;
  } else {
    second_byte |= unmasked_payload_size;
  }
  result.push_back(second_byte);

  if (unmasked_payload_size > 65535) {
    result.push_back((unmasked_payload_size & 0xFF00000000000000) >> 56);
    result.push_back((unmasked_payload_size & 0x00FF000000000000) >> 48);
    result.push_back((unmasked_payload_size & 0x0000FF0000000000) >> 40);
    result.push_back((unmasked_payload_size & 0x000000FF00000000) >> 32);
    result.push_back((unmasked_payload_size & 0x00000000FF000000) >> 24);
    result.push_back((unmasked_payload_size & 0x0000000000FF0000) >> 16);
    result.push_back((unmasked_payload_size & 0x000000000000FF00) >> 8);
    result.push_back((unmasked_payload_size & 0x00000000000000FF));
  }
  if (unmasked_payload_size > 125) {
    result.push_back((unmasked_payload_size & 0x000000000000FF00) >> 8);
    result.push_back((unmasked_payload_size & 0x00000000000000FF));
  }

  if (masked) {
    result.push_back(mask[0]);
    result.push_back(mask[1]);
    result.push_back(mask[2]);
    result.push_back(mask[3]);
  }

  if (masked) {
    size_t bytes_processed = 0;
    for (const auto& byte : unmasked_payload) {
      result.push_back(byte ^ mask[bytes_processed % 4]);
      ++bytes_processed;
    }
  } else {
    result.insert(result.end(), unmasked_payload.begin(),
                  unmasked_payload.end());
  }

  return result;
}

void WebsocketFrame::SetMask(uint32_t new_mask) {
  mask = EnsureBigEndian(new_mask);
}

absl::StatusOr<uint16_t> GetCloseCode(const WebsocketFrame& frame) {
  if (frame.opcode != WSOpcode::kClose) {
    return absl::InvalidArgumentError("Frame is not a close frame.");
  }
  return (frame.unmasked_payload[0] << 8) | frame.unmasked_payload[1];
}

absl::StatusOr<std::string_view> GetCloseReason(const WebsocketFrame& frame) {
  if (frame.opcode != WSOpcode::kClose) {
    return absl::InvalidArgumentError("Frame is not a close frame.");
  }
  return frame.GetUnmaskedDataView().substr(2);
}

std::string_view WebsocketFrame::GetUnmaskedDataView() const {
  return std::string_view(
      reinterpret_cast<char*>(const_cast<uint8_t*>(unmasked_payload.data())),
      unmasked_payload.size());
}

}  // namespace act::net::http

absl::StatusOr<act::net::WsUrl> act::net::WsUrl::FromString(
    std::string_view url_str) {
  bool use_ssl = false;
  bool explicit_no_ssl = false;

  if (url_str.substr(0, 6) == "wss://") {
    use_ssl = true;
    url_str = url_str.substr(6);
  } else if (url_str.substr(0, 5) == "ws://") {
    url_str = url_str.substr(5);
    explicit_no_ssl = true;
  }

  std::string_view address;
  std::string_view target = "/";
  uint16_t port;
  if (const size_t colon_pos = url_str.find(':');
      colon_pos == std::string_view::npos) {
    address = url_str;
    port = use_ssl ? 443 : 80;
  } else {
    address = url_str.substr(0, colon_pos);
    const size_t port_start = colon_pos + 1;
    const size_t slash_pos = url_str.find('/', port_start);

    std::string_view port_str;
    if (slash_pos == std::string_view::npos) {
      port_str = url_str.substr(port_start);
    } else {
      port_str = url_str.substr(port_start, slash_pos - port_start);
    }

    if (!absl::SimpleAtoi(port_str, &port)) {
      LOG(ERROR)
          << "Failed to parse port from signalling server URL: address is "
          << address << " port string is '" << port_str << "'";
      return absl::InvalidArgumentError(absl::StrCat(
          "Signalling server URL contains an invalid port: '", port_str, "'"));
    }

    if (slash_pos != std::string_view::npos) {
      target = url_str.substr(slash_pos);
    }
  }

  if (address.empty()) {
    return absl::InvalidArgumentError(
        "Signalling server URL must contain a hostname");
  }

  if (target.empty() || target[0] != '/') {
    return absl::InvalidArgumentError(
        "Signalling server URL target must start with '/'");
  }

  if (port == 0) {
    return absl::InvalidArgumentError(
        "Signalling server URL port must be greater than 0");
  }

  if (port == 443 && !explicit_no_ssl) {
    use_ssl = true;
  }

  return WsUrl{.scheme = use_ssl ? "wss" : "ws",
               .host = std::string(address),
               .port = port,
               .target = std::string(target)};
}

act::net::WsUrl act::net::WsUrl::FromStringOrDie(std::string_view url_str) {
  auto result = FromString(url_str);
  CHECK(result.ok()) << "Failed to parse WsUrl from string: "
                     << result.status();
  return *std::move(result);
}