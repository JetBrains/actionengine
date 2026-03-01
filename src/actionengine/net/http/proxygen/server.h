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

#ifndef ACTIONENGINE_NET_HTTP_PROXYGEN_SERVER_H_
#define ACTIONENGINE_NET_HTTP_PROXYGEN_SERVER_H_

#include <string>

#include <absl/flags/flag.h>
#include <absl/status/status.h>

#include "actionengine/data/types.h"
#include "actionengine/net/http/ws_common.h"
#include "actionengine/service/service.h"

namespace folly {
class SocketAddress;
}

namespace proxygen {
class HTTPServer;
}

namespace act::net::http {

class WebsocketServer {
 public:
  explicit WebsocketServer(act::Service* absl_nonnull service,
                           std::string_view address = "0.0.0.0",
                           uint16_t port = 20000);

  ~WebsocketServer();

  void Run();
  void StopListening();

  void Cancel();

  absl::Status GetStatus() const;
  absl::Status Join();

 private:
  void StopListeningInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  void CancelInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
  absl::Status JoinInternal() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable act::Mutex mu_;
  mutable act::CondVar cv_ ABSL_GUARDED_BY(mu_);

  act::Service* absl_nonnull const service_;
  std::unique_ptr<proxygen::HTTPServer> server_;
  const std::string address_;
  const uint16_t port_;

  std::unique_ptr<std::thread> main_loop_;
  std::optional<absl::Status> status_ ABSL_GUARDED_BY(mu_);

  bool prep_done_ ABSL_GUARDED_BY(mu_) = false;
  bool listening_stopped_ ABSL_GUARDED_BY(mu_) = false;
  bool stopped_ ABSL_GUARDED_BY(mu_) = false;
  bool joining_ ABSL_GUARDED_BY(mu_) = false;
};

}  // namespace act::net::http

#endif  // ACTIONENGINE_NET_HTTP_PROXYGEN_SERVER_H_