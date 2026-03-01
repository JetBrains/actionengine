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

#include "actionengine/net/http/proxygen/http_connector.h"

#pragma push_macro("RETURN_IF_ERROR")
#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/AsyncSocket.h>
#include <proxygen/lib/http/HTTPConnector.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>
#include <proxygen/lib/utils/URL.h>
#pragma pop_macro("RETURN_IF_ERROR")

#include "actionengine/net/http/proxygen/folly_utils.h"

namespace act::net::http {

void SslHandshakeFollowup(proxygen::HTTPUpstreamSession* session) {
  const auto* ssl_socket =
      dynamic_cast<folly::AsyncSSLSocket*>(session->getTransport());

  const unsigned char* next_proto = nullptr;
  unsigned next_proto_length = 0;
  ssl_socket->getSelectedNextProtocol(&next_proto, &next_proto_length);
  if (next_proto) {
    DLOG(INFO) << "Client selected next protocol "
               << std::string(reinterpret_cast<const char*>(next_proto),
                              next_proto_length);
  } else {
    // DLOG(INFO) << "Client did not select a next protocol";
  }
}

HTTPConnector::HTTPConnector(folly::EventBase* evb, folly::HHWheelTimer* timer)
    : impl_(std::make_unique<HTTPConnectorImpl>(evb, timer)) {}

HTTPConnector::~HTTPConnector() {}

class HTTPConnectorImpl : public proxygen::HTTPConnector::Callback {
 public:
  HTTPConnectorImpl(folly::EventBase* absl_nonnull evb,
                    folly::HHWheelTimer* absl_nonnull timer);

  ~HTTPConnectorImpl() override;

  absl::StatusOr<proxygen::HTTPUpstreamSession* absl_nonnull> Connect(
      std::string_view url, absl::Duration timeout = absl::InfiniteDuration(),
      std::shared_ptr<folly::SSLContext> ssl_context = nullptr);

  void connectSuccess(
      proxygen::HTTPUpstreamSession* absl_nonnull session) override;

  void connectError(const folly::AsyncSocketException& ex) override;

 private:
  folly::EventBase* const absl_nonnull evb_;
  std::unique_ptr<proxygen::HTTPConnector> connector_;

  mutable act::Mutex mu_;
  mutable act::CondVar cv_;

  bool connecting_ ABSL_GUARDED_BY(mu_) = false;
  bool secure_ = false;
  absl::StatusOr<proxygen::HTTPUpstreamSession* absl_nonnull> connect_result_
      ABSL_GUARDED_BY(mu_);
};

absl::StatusOr<proxygen::HTTPUpstreamSession*> HTTPConnector::Connect(
    std::string_view url, absl::Duration timeout,
    std::shared_ptr<folly::SSLContext> ssl_context) {
  return impl_->Connect(url, timeout, std::move(ssl_context));
}

HTTPConnectorImpl::HTTPConnectorImpl(folly::EventBase* evb,
                                     folly::HHWheelTimer* timer)
    : evb_(evb),
      connector_(std::make_unique<proxygen::HTTPConnector>(this, timer)) {}

HTTPConnectorImpl::~HTTPConnectorImpl() {
  act::MutexLock lock(&mu_);
  if (connecting_) {
    connector_->reset(/*invokeCallbacks=*/true);
    while (connecting_) {
      cv_.Wait(&mu_);
    }
  }
}

absl::StatusOr<proxygen::HTTPUpstreamSession*> HTTPConnectorImpl::Connect(
    std::string_view url, absl::Duration timeout,
    std::shared_ptr<folly::SSLContext> ssl_context) {
  const absl::Time deadline = absl::Now() + timeout;

  act::MutexLock lock(&mu_);
  if (connecting_) {
    return absl::FailedPreconditionError("Already connecting to another url.");
  }
  const proxygen::URL parsed_url(url);
  if (!parsed_url.isValid()) {
    return absl::InvalidArgumentError(absl::StrCat("Invalid URL: ", url));
  }

  connecting_ = true;
  secure_ = parsed_url.isSecure();
  evb_->runInEventBaseThreadAlwaysEnqueue(
      [this, parsed_url, timeout, ssl_context]() mutable {
        const folly::SocketOptionMap opts{
            {{.level = SOL_SOCKET, .optname = SO_REUSEADDR}, 1}};

        if (!secure_) {
          connector_->connect(
              evb_,
              folly::SocketAddress(parsed_url.getHost(), parsed_url.getPort(),
                                   /*allowNameLookup=*/true),
              absl::ToChronoMilliseconds(timeout), opts);
        } else {
          if (ssl_context == nullptr) {
            ssl_context = std::shared_ptr<folly::SSLContext>(
                GetGlobalSSLContext(), [](folly::SSLContext*) {});
          }
          connector_->connectSSL(
              evb_,
              folly::SocketAddress(parsed_url.getHost(), parsed_url.getPort(),
                                   /*allowNameLookup=*/true),
              ssl_context,
              /*session=*/nullptr, absl::ToChronoMilliseconds(timeout), opts,
              folly::AsyncSocket::anyAddress(), parsed_url.getHost());
        }
      });

  while (absl::IsUnknown(connect_result_.status())) {
    cv_.WaitWithDeadline(&mu_, deadline);
  }
  connecting_ = false;
  cv_.SignalAll();

  if (connect_result_.ok()) {
    return *connect_result_;
  }
  if (absl::Now() > deadline) {
    return absl::DeadlineExceededError("Timed out waiting for connection.");
  }
  return connect_result_.status();
}

void HTTPConnectorImpl::connectSuccess(proxygen::HTTPUpstreamSession* session) {
  if (secure_) {
    SslHandshakeFollowup(session);
  }
  act::MutexLock lock(&mu_);
  connect_result_ = session;
  cv_.SignalAll();
}

void HTTPConnectorImpl::connectError(const folly::AsyncSocketException& ex) {
  act::MutexLock lock(&mu_);
  connect_result_ = absl::InternalError(ex.what());
  cv_.SignalAll();
}

}  // namespace act::net::http
