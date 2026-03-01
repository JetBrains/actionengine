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

#ifndef ACTIONENGINE_NET_HTTP_PROXYGEN_WS_SERVER_STREAM_HANDLER_FACTORY_H_
#define ACTIONENGINE_NET_HTTP_PROXYGEN_WS_SERVER_STREAM_HANDLER_FACTORY_H_

#include "actionengine/net/http/proxygen/ws_server_stream.h"
#include "actionengine/net/http/ws_common.h"

namespace proxygen {
class RequestHandlerFactory;
}

namespace act::net::http {

class WebsocketServerStreamFactoryImpl;

class WebsocketServerStreamFactory {
 public:
  explicit WebsocketServerStreamFactory(
      RequestHeadersCallback on_request_headers = nullptr);

  ~WebsocketServerStreamFactory();

  void SetNewStreamCallback(
      std::function<void(std::unique_ptr<WebsocketServerStream>)> callback);
  std::unique_ptr<proxygen::RequestHandlerFactory> ExtractFactory();

 private:
  std::unique_ptr<WebsocketServerStreamFactoryImpl> impl_;
};

}  // namespace act::net::http

#endif  // ACTIONENGINE_NET_HTTP_PROXYGEN_WS_SERVER_STREAM_HANDLER_FACTORY_H_