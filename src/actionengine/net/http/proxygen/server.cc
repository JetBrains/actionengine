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

#include "actionengine/net/http/proxygen/server.h"

#pragma push_macro("RETURN_IF_ERROR")
#include <proxygen/httpserver/HTTPServer.h>
#pragma pop_macro("RETURN_IF_ERROR")

#include "actionengine/net/http/proxygen/wire_stream.h"
#include "actionengine/net/http/proxygen/ws_server_stream_handler_factory.h"
#include "actionengine/util/random.h"

namespace act::net::http {

WebsocketServer::WebsocketServer(act::Service* service,
                                 std::string_view address, uint16_t port)
    : service_(service), address_(address), port_(port) {}

WebsocketServer::~WebsocketServer() {
  act::MutexLock lock(&mu_);
  StopListeningInternal();
  CancelInternal();
  JoinInternal().IgnoreError();
}

void WebsocketServer::Run() {
  act::MutexLock l(&mu_);
  if (status_) {
    return;
  }
  main_loop_ = std::make_unique<std::thread>([this]() {
    act::MutexLock lock(&mu_);
    const std::vector<proxygen::HTTPServer::IPConfig> ips = {
        {folly::SocketAddress(address_, port_, true),
         proxygen::HTTPServer::Protocol::HTTP},
    };

    auto factory = std::make_unique<WebsocketServerStreamFactory>();
    factory->SetNewStreamCallback(
        [this](std::unique_ptr<WebsocketServerStream> new_stream) {
          if (new_stream == nullptr) {
            return;
          }

          const absl::Status status = service_->StartStreamHandler(
              std::make_shared<WebsocketWireStream>(std::move(new_stream),
                                                    GenerateUUID4()));
          if (!status.ok()) {
            LOG(ERROR) << "WebsocketServer EstablishConnection failed: "
                       << status;
          }
        });
    DLOG(INFO) << "WebsocketServer listening on " << address_ << ":" << port_;

    proxygen::HTTPServerOptions options;
    options.threads = static_cast<size_t>(8);
    options.h2cEnabled = false;
    options.idleTimeout = std::chrono::milliseconds(600000);
    options.shutdownOn = {SIGINT, SIGTERM};
    options.enableContentCompression = false;
    options.handlerFactories =
        proxygen::RequestHandlerChain()
            .addThen(std::move(factory)->ExtractFactory())
            .build();
    // Increase the default flow control to 1MB/10MB
    options.initialReceiveWindow = static_cast<uint32_t>(1 << 20);
    options.receiveStreamWindowSize = static_cast<uint32_t>(1 << 20);
    options.receiveSessionWindowSize = 10 * (1 << 20);
    options.supportsConnect = false;

    server_ = std::make_unique<proxygen::HTTPServer>(std::move(options));
    server_->bind(ips);

    mu_.unlock();
    DLOG(INFO) << "WebsocketServer starting";
    server_->start(/*onSuccess=*/
                   [this]() {
                     act::MutexLock lock(&mu_);
                     prep_done_ = true;
                     cv_.SignalAll();
                   },
                   /*onError=*/
                   [this](std::exception_ptr exc_ptr) {
                     try {
                       std::rethrow_exception(std::move(exc_ptr));
                     } catch (const std::exception& exc) {
                       act::MutexLock lock(&mu_);
                       DLOG(ERROR) << exc.what();
                       status_ = absl::InternalError(exc.what());
                     }
                   },
                   /*getAcceptorFactory=*/nullptr,
                   /*ioExecutor=*/
                   std::make_shared<folly::IOThreadPoolExecutor>(
                       8, std::make_shared<folly::NamedThreadFactory>(
                              "WebSocketServerIOThread")));
    mu_.lock();
    status_ = absl::OkStatus();
  });

  while (!prep_done_ && !status_) {
    cv_.Wait(&mu_);
  }
}

void WebsocketServer::StopListening() {
  act::MutexLock lock(&mu_);
  StopListeningInternal();
}

void WebsocketServer::Cancel() {
  act::MutexLock lock(&mu_);
  CancelInternal();
}

absl::Status WebsocketServer::GetStatus() const {
  act::MutexLock lock(&mu_);
  if (!status_) {
    return absl::OkStatus();
  }
  return *status_;
}

absl::Status WebsocketServer::Join() {
  act::MutexLock lock(&mu_);
  return JoinInternal();
}

void WebsocketServer::CancelInternal() {
  if (stopped_) {
    return;
  }
  server_->stop();
  stopped_ = true;
}

absl::Status WebsocketServer::JoinInternal() {
  if (main_loop_ == nullptr) {
    DCHECK(status_);
    return *status_;
  }
  if (joining_) {
    while (joining_) {
      cv_.Wait(&mu_);
    }
    return *status_;
  }
  joining_ = true;
  mu_.unlock();
  main_loop_->join();
  mu_.lock();
  joining_ = false;
  main_loop_ = nullptr;
  cv_.SignalAll();
  DCHECK(status_);
  return *status_;
}

void WebsocketServer::StopListeningInternal() {
  if (listening_stopped_) {
    return;
  }
  server_->stopListening();
  listening_stopped_ = true;
}

}  // namespace act::net::http
