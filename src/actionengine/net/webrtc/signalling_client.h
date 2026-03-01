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

#ifndef ACTIONENGINE_NET_WEBRTC_SIGNALLING_CLIENT_H_
#define ACTIONENGINE_NET_WEBRTC_SIGNALLING_CLIENT_H_

#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/status/status.h>
#include <boost/json/value.hpp>

#include "actionengine/concurrency/concurrency.h"
#include "actionengine/net/http/proxygen/ws_client_stream.h"

/**
 * @file
 * Provides the `SignallingClient` class for WebRTC signalling over WebSocket.
 */
namespace act::net {

// void(peer_id, message)
using PeerJsonHandler =
    std::function<void(std::string_view, boost::json::value)>;

/**
 * A client for WebRTC signalling using WebSocket.
 *
 * This class handles the connection to a WebSocket server for WebRTC
 * signalling, allowing the client to send and receive offers, answers, and ICE
 * candidates. It provides methods to set callbacks for handling these messages
 * and to connect with a specific identity.
 *
 * @headerfile actionengine/net/webrtc/signalling_client.h
 */
class SignallingClient {
 public:
  explicit SignallingClient(std::string_view address = "localhost",
                            uint16_t port = 80, bool use_ssl = false);

  // This class is not copyable or movable
  SignallingClient(const SignallingClient&) = delete;
  SignallingClient& operator=(const SignallingClient&) = delete;

  ~SignallingClient();

  void ResetCallbacks();

  void OnOffer(PeerJsonHandler on_offer);

  void OnCandidate(PeerJsonHandler on_candidate);

  void OnAnswer(PeerJsonHandler on_answer);

  thread::Case OnError() const;

  absl::Status GetStatus() const;

  absl::Status ConnectWithIdentity(
      std::string_view identity,
      const absl::flat_hash_map<std::string, std::string>& headers = {});

  absl::Status Send(const std::string& message) const;

  void Cancel();

  void Join();

 private:
  void CancelInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void HandleFrame(http::WebsocketStream* stream,
                   http::WebsocketFrame* frame);

  void HandleDone(http::WebsocketStream* stream);

  std::string identity_;
  const std::string address_;
  const uint16_t port_;
  const bool use_ssl_;

  PeerJsonHandler on_offer_ ABSL_GUARDED_BY(mu_);
  PeerJsonHandler on_candidate_ ABSL_GUARDED_BY(mu_);
  PeerJsonHandler on_answer_ ABSL_GUARDED_BY(mu_);

  std::unique_ptr<http::WebsocketClientStream> stream_ ABSL_GUARDED_BY(mu_);
  absl::Status loop_status_ ABSL_GUARDED_BY(mu_);
  mutable act::Mutex mu_;
  thread::PermanentEvent error_event_;
};

}  // namespace act::net

#endif  // ACTIONENGINE_NET_WEBRTC_SIGNALLING_CLIENT_H_