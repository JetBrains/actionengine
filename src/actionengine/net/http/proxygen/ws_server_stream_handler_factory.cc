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

#include "actionengine/net/http/proxygen/ws_server_stream_handler_factory.h"

#include <proxygen/httpserver/RequestHandlerFactory.h>

namespace act::net::http {

class WebsocketServerStreamFactoryImpl
    : public proxygen::RequestHandlerFactory {
 public:
  explicit WebsocketServerStreamFactoryImpl(
      RequestHeadersCallback on_request_headers = nullptr)
      : on_request_headers_(std::move(on_request_headers)) {}

  ~WebsocketServerStreamFactoryImpl() override = default;

  void SetNewStreamCallback(
      std::function<void(std::unique_ptr<WebsocketServerStream>)> callback) {
    on_new_stream_ = std::move(callback);
  }

  void onServerStart(folly::EventBase* /*evb*/) noexcept override {
    CHECK(on_new_stream_ != nullptr);
  }

  void onServerStop() noexcept override { on_new_stream_(nullptr); }

  proxygen::RequestHandler* onRequest(proxygen::RequestHandler*,
                                      proxygen::HTTPMessage*) noexcept override;

 private:
  WebsocketStream::FrameCallback on_frame_;
  WebsocketStream::DoneCallback on_done_;
  RequestHeadersCallback on_request_headers_;
  std::function<void(std::unique_ptr<WebsocketServerStream>)> on_new_stream_;
};

WebsocketServerStreamFactory::WebsocketServerStreamFactory(
    RequestHeadersCallback on_request_headers) {
  impl_ = std::make_unique<WebsocketServerStreamFactoryImpl>(
      std::move(on_request_headers));
}

WebsocketServerStreamFactory::~WebsocketServerStreamFactory() {}

proxygen::RequestHandler* WebsocketServerStreamFactoryImpl::onRequest(
    proxygen::RequestHandler*, proxygen::HTTPMessage*) noexcept {
  auto stream = std::make_unique<WebsocketServerStream>();
  proxygen::RequestHandler* stream_ptr = stream->GetRequestHandler();
  on_new_stream_(std::move(stream));
  return stream_ptr;
}

void WebsocketServerStreamFactory::SetNewStreamCallback(
    std::function<void(std::unique_ptr<WebsocketServerStream>)> callback) {
  impl_->SetNewStreamCallback(std::move(callback));
}

std::unique_ptr<proxygen::RequestHandlerFactory>
WebsocketServerStreamFactory::ExtractFactory() {
  return std::move(impl_);
}

}  // namespace act::net::http