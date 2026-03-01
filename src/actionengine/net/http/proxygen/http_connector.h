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

#ifndef ACTIONENGINE_NET_HTTP_PROXYGEN_HTTP_CONNECTOR_H_
#define ACTIONENGINE_NET_HTTP_PROXYGEN_HTTP_CONNECTOR_H_

#include <absl/base/thread_annotations.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_cat.h>

#include "actionengine/concurrency/concurrency.h"

namespace folly {
class EventBase;
template <class Duration>
class HHWheelTimerBase;
using HHWheelTimer = HHWheelTimerBase<std::chrono::milliseconds>;
using HHWheelTimerHighRes = HHWheelTimerBase<std::chrono::microseconds>;
class SSLContext;
}  // namespace folly

namespace proxygen {
class HTTPConnector;
class HTTPUpstreamSession;
}  // namespace proxygen

namespace act::net::http {

class HTTPConnectorImpl;

void SslHandshakeFollowup(proxygen::HTTPUpstreamSession* absl_nonnull session);

class HTTPConnector {
 public:
  HTTPConnector(folly::EventBase* absl_nonnull evb,
                folly::HHWheelTimer* absl_nonnull timer);
  ~HTTPConnector();

  absl::StatusOr<proxygen::HTTPUpstreamSession* absl_nonnull> Connect(
      std::string_view url, absl::Duration timeout = absl::InfiniteDuration(),
      std::shared_ptr<folly::SSLContext> ssl_context = nullptr);

 private:
  std::unique_ptr<HTTPConnectorImpl> impl_;
};

}  // namespace act::net::http

#endif  // ACTIONENGINE_NET_HTTP_PROXYGEN_HTTP_CONNECTOR_H_