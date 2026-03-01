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

#include "actionengine/net/http/proxygen/ws_server_stream.h"

#pragma push_macro("RETURN_IF_ERROR")
#include <folly/io/async/EventBaseManager.h>
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/httpserver/ResponseHandler.h>
#pragma pop_macro("RETURN_IF_ERROR")

#include "actionengine/net/http/proxygen/ws_server_stream_handler_factory.h"
#include "actionengine/util/status_macros.h"

namespace act::net::http {

class WebsocketServerStreamImpl final : public WebsocketStream,
                                        public proxygen::RequestHandler {
 public:
  explicit WebsocketServerStreamImpl(WebsocketServerStream* absl_nonnull owner)
      : owner_(owner) {}

  ~WebsocketServerStreamImpl() override {
    act::MutexLock lock(&mu_);
    if (!half_closed_) {
      LOG(WARNING) << "Stream was not closed before destruction.";
    }

    if (evb_ != nullptr) {
      if (!status_) {
        Abort_(absl::ResourceExhaustedError("Stream is being destroyed."));
      } else if (!eom_sent_) {
        eom_sent_ = true;
        evb_->runImmediatelyOrRunInEventBaseThreadAndWait(
            [downstream = downstream_] { downstream->sendEOM(); });
      }
    }

    // Wait until it's safe to destroy the object
    while (!request_complete_) {
      cv_.Wait(&mu_);
    }
  }

  void SetResponseHeaders(const proxygen::HTTPMessage& headers) {
    act::MutexLock lock(&mu_);
    response_headers_ = std::make_unique<proxygen::HTTPMessage>(headers);
  }

  void SetResponseHeaders(std::unique_ptr<proxygen::HTTPMessage> headers) {
    act::MutexLock lock(&mu_);
    response_headers_ = std::move(headers);
  }

  absl::StatusOr<std::reference_wrapper<const proxygen::HTTPMessage>>
  GetRequestHeaders(absl::Duration timeout) const {
    const absl::Time deadline = absl::Now() + timeout;

    act::MutexLock lock(&mu_);
    while (!request_ && !aborted_) {
      cv_.WaitWithDeadline(&mu_, deadline);
    }
    if (request_) {
      std::cref(*request_);
    }
    if (aborted_) {
      return absl::CancelledError("Stream aborted.");
    }
    if (absl::Now() > deadline) {
      return absl::DeadlineExceededError(
          "Timed out waiting for request headers.");
    }
    return absl::UnknownError("Should not reach here.");
  }

  absl::Status SendBytes(const uint8_t* data, size_t size) override {
    WebsocketFrame frame;
    frame.fin = true;
    frame.opcode = WSOpcode::kBinary;
    frame.SetMask(0);
    frame.masked = false;
    frame.unmasked_payload.reserve(size);
    frame.unmasked_payload.assign(data, data + size);
    return SendFrame(frame);
  }

  absl::Status SendFrame(const WebsocketFrame& frame) override {
    act::MutexLock lock(&mu_);
    return SendFrame_(frame);
  }

  absl::Status SendText(std::string_view text) override {
    WebsocketFrame frame;
    frame.fin = true;
    frame.opcode = WSOpcode::kText;
    frame.SetMask(0);
    frame.masked = false;
    frame.unmasked_payload.reserve(text.size());
    for (const char c : text) {
      frame.unmasked_payload.push_back(c);
    }
    return SendFrame(frame);
  }

  void Accept() override {
    act::MutexLock lock(&mu_);
    while (request_ == nullptr && !status_) {
      cv_.Wait(&mu_);
    }
    if (status_) {
      return;
    }

    DCHECK(request_ != nullptr);

    const std::string path = request_->getPath();

    // Check if Upgrade and Connection headers are present.
    if (!request_->getHeaders().exists(proxygen::HTTP_HEADER_UPGRADE) ||
        !request_->getHeaders().exists(proxygen::HTTP_HEADER_CONNECTION)) {
      evb_->runImmediatelyOrRunInEventBaseThreadAndWait([this] {
        proxygen::ResponseBuilder(downstream_).rejectUpgradeRequest();
      });

      MarkNoFurtherIngressWithStatus(
          absl::UnavailableError("Missing Upgrade/Connection header"));
      return;
    }

    // Make sure we are requesting an upgrade to.
    const std::string& proto =
        request_->getHeaders().getSingleOrEmpty(proxygen::HTTP_HEADER_UPGRADE);
    if (!proxygen::caseInsensitiveEqual(proto, kUpgradeTo)) {
      evb_->runImmediatelyOrRunInEventBaseThreadAndWait([this] {
        proxygen::ResponseBuilder(downstream_).rejectUpgradeRequest();
      });
      MarkNoFurtherIngressWithStatus(absl::InvalidArgumentError(
          absl::StrCat("Provided upgrade protocol: '", proto, "', expected: '",
                       kUpgradeTo, "'")));
      return;
    }
    mu_.unlock();
    evb_->runImmediatelyOrRunInEventBaseThreadAndWait([this] {
      act::MutexLock lock(&mu_);
      // Build the upgrade response.
      proxygen::ResponseBuilder response(downstream_);
      std::unique_ptr<proxygen::HTTPMessage> response_headers =
          std::move(response_headers_);
      if (response_headers == nullptr) {
        response_headers = std::make_unique<proxygen::HTTPMessage>();
        response_headers->setStatusCode(101);
        response_headers->setStatusMessage("Switching Protocols");
        response_headers->getHeaders().add(kWSVersionHeader, kWSVersion);
      }
      response.status(response_headers->getStatusCode(),
                      response_headers->getStatusMessage());
      response.setEgressWebsocketHeaders();
      response_headers->getHeaders().forEach(
          [&response](const std::string& key, const std::string& value) {
            response.header(key, value);
          });
      response.send();
      // TODO: synchronise on the write actually happening
    });
    mu_.lock();

    upgraded_ = true;
    cv_.SignalAll();
  }

  void Start() override {
    CHECK(false) << "Server streams should not be started.";
  }

  absl::Status GetStatus() const override {
    act::MutexLock lock(&mu_);
    if (!status_) {
      return absl::OkStatus();
    }
    return *status_;
  }

  void HalfClose(uint16_t code, std::string_view reason) override {
    act::MutexLock lock(&mu_);
    if (half_closed_) {
      return;
    }

    if (const absl::Status status = SendCloseFrame_(code, reason);
        !status.ok()) {
      DLOG(WARNING) << "Could not send a Close frame: " << status;
    }
    half_closed_ = true;
  }

  void Abort(absl::Status status) override {
    act::MutexLock lock(&mu_);
    Abort_(std::move(status));
  }

  [[nodiscard]] bool HalfClosed() const override {
    act::MutexLock lock(&mu_);
    return half_closed_;
  }

  void SetFrameCallback(FrameCallback on_frame) override {
    act::MutexLock lock(&mu_);
    on_frame_ = std::move(on_frame);
    cv_.SignalAll();
  }

  void SetDoneCallback(DoneCallback on_done) override {
    act::MutexLock lock(&mu_);
    on_done_ = std::move(on_done);
  }

  void SetRequestHeadersCallback(RequestHeadersCallback on_headers) {
    act::MutexLock lock(&mu_);
    on_request_headers_ = std::move(on_headers);
  }

  void onRequest(
      std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override {
    act::MutexLock lock(&mu_);
    DCHECK(request_ == nullptr);
    request_ = std::move(headers);
    evb_ = folly::EventBaseManager::get()->getExistingEventBase();
    DCHECK(evb_ != nullptr);
    cv_.SignalAll();

    if (on_request_headers_) {
      const RequestHeadersCallback on_request_headers =
          std::move(on_request_headers_);
      mu_.unlock();
      on_request_headers(std::shared_ptr<WebsocketServerStream>(
          owner_, [](WebsocketServerStream*) {}));
      mu_.lock();
    } else {
      mu_.unlock();
      Accept();
      mu_.lock();
    }
  }

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override {
    act::MutexLock lock(&mu_);
    // if (!upgrade_status_) {
    //   Abort_(absl::FailedPreconditionError("Received data before upgrade."));
    // }

    folly::fbstring buf = body->moveToFbString();
    size_t next_offset = 0;
    while (next_offset < buf.size()) {
      absl::StatusOr<WebsocketFrame> frame = WebsocketFrame::ParseFromBuffer(
          reinterpret_cast<uint8_t*>(buf.data() + next_offset),
          buf.size() - next_offset, &next_offset);
      if (close_frame_ != nullptr) {
        Abort_(
            absl::FailedPreconditionError("Receiving data past CLOSE frame."));
        return;
      }

      if (!frame.ok()) {
        Abort_(absl::InternalError(
            absl::StrCat("Could not parse frame: ", frame.status())));
        return;
      }

      if (frame->opcode == WSOpcode::kPing) {
        WebsocketFrame response;
        response.fin = true;
        response.opcode = WSOpcode::kPong;
        response.unmasked_payload = frame->unmasked_payload;
        SendFrame_(std::move(response)).IgnoreError();
        continue;
      }

      const FrameCallback* absl_nullable on_frame = &on_frame_;
      if (frame->opcode == WSOpcode::kClose) {
        absl::StatusOr<uint16_t> close_code = GetCloseCode(*frame);
        absl::StatusOr<std::string_view> close_reason = GetCloseReason(*frame);
        if (!close_code.ok() || !close_reason.ok()) {
          Abort_(absl::InternalError(
              absl::StrCat("Could not parse close frame: ", close_code.status(),
                           ", ", close_reason.status())));
          return;
        }

        status_ = *close_code == 1000
                      ? absl::OkStatus()
                      : absl::CancelledError(
                            absl::StrCat("Received close code: ", *close_code));
        close_frame_ = std::make_unique<WebsocketFrame>(std::move(*frame));
        cv_.SignalAll();

        if (on_frame) {
          mu_.unlock();
          (*on_frame)(owner_, nullptr);
          mu_.lock();
        }

        if (*close_code != 1000) {
          half_closed_ = true;
          if (!SendCloseFrame_(*close_code, "ACK").ok()) {
            Abort_(
                absl::ResourceExhaustedError("Could not send a Close frame."));
          }
          // MarkDoneWithStatus(absl::ResourceExhaustedError(absl::StrCat(
          //     "The other end closed the socket with code ", *close_code, ".")));
          return;
        }
        MarkNoFurtherIngressWithStatus(absl::OkStatus());
        return;
      }

      // Defer the processing of the body until on_frame_ is set
      while (!status_ && !on_frame_) {
        cv_.Wait(&mu_);
      }
      if (status_) {
        DLOG(WARNING) << "onBody() called after status_ has been set.";
        return;
      }
      DCHECK(on_frame_);
      on_frame = &on_frame_;
      mu_.unlock();
      (*on_frame)(owner_, &*frame);
      mu_.lock();
    }
  }

  void onUpgrade(proxygen::UpgradeProtocol prot) noexcept override {}

  void onEOM() noexcept override {
    act::MutexLock lock(&mu_);
    if (close_frame_ == nullptr) {
      // Forbid sending, as a no-close frame EOM is incorrect
      Abort_(absl::ResourceExhaustedError(
          "Received an EOM without a close frame."));
    }
    if (!eom_sent_ && !aborted_) {
      eom_sent_ = true;
      evb_->runImmediatelyOrRunInEventBaseThreadAndWait(
          [downstream = downstream_] {
            proxygen::ResponseBuilder(downstream).sendWithEOM();
          });
    }
  }

  void requestComplete() noexcept override {
    WebsocketStream::DoneCallback on_done;
    {
      act::MutexLock lock(&mu_);
      if (on_done_) {
        on_done = std::move(on_done_);
      }
      request_complete_ = true;
      cv_.SignalAll();
    }
    if (on_done) {
      on_done(owner_);
    }
  }

  void onError(proxygen::ProxygenError err) noexcept override {
    LOG(ERROR) << "onError(): " << err;
    WebsocketStream::DoneCallback on_done;
    {
      act::MutexLock lock(&mu_);
      if (on_done_) {
        on_done = std::move(on_done_);
      }
      Abort_(absl::InternalError(absl::StrCat("Proxygen error code: ", err)));
      request_complete_ = true;
      cv_.SignalAll();
    }
    if (on_done) {
      on_done(owner_);
    }
  }

 private:
  void Abort_(absl::Status status) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (!half_closed_) {
      half_closed_ = true;
    }

    if (!status_) {
      MarkNoFurtherIngressWithStatus(std::move(status));
    }

    if (!request_complete_ && !aborted_) {
      aborted_ = true;
      mu_.unlock();
      evb_->runImmediatelyOrRunInEventBaseThreadAndWait(
          [downstream = downstream_] { downstream->sendAbort(); });
      mu_.lock();
    }
  }

  absl::Status SendCloseFrame_(uint16_t code, std::string_view reason)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    ASSIGN_OR_RETURN(WebsocketFrame frame,
                     WebsocketFrame::MakeCloseFrame(code, reason));
    frame.SetMask(0);
    frame.masked = false;
    RETURN_IF_ERROR(SendFrame_(std::move(frame)));
    return absl::OkStatus();
  }

  void MarkNoFurtherIngressWithStatus(absl::Status status)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    // DCHECK(!status_);  // Status must only be set by this method
    status_ = std::move(status);
    cv_.SignalAll();
  }

  absl::Status SendFrame_(const WebsocketFrame& frame)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (half_closed_) {
      return absl::FailedPreconditionError(
          "Trying to send a frame after the stream was half-closed.");
    }
    if (evb_ == nullptr) {
      return absl::FailedPreconditionError(
          "Trying to send a frame before a request was taken.");
    }
    while (!upgraded_ && !status_) {
      cv_.Wait(&mu_);
    }
    if (status_ && !status_->ok()) {
      return absl::FailedPreconditionError(absl::StrCat(
          "Local internal error or abnormal closure by other end: ",
          status_->message()));
    }

    std::unique_ptr<folly::IOBuf> serialized =
        folly::IOBuf::fromString(frame.Serialize());

    evb_->runImmediatelyOrRunInEventBaseThread(
        [downstream = downstream_,
         serialized = std::move(serialized)]() mutable {
          proxygen::ResponseBuilder response(downstream);
          response.body(std::move(serialized));
          response.send();
        });

    return absl::OkStatus();
  }

  mutable act::Mutex mu_;
  mutable act::CondVar cv_ ABSL_GUARDED_BY(mu_);

  WebsocketServerStream* const absl_nonnull owner_;

  folly::EventBase* absl_nullable evb_ = nullptr;
  bool upgraded_ ABSL_GUARDED_BY(mu_) = false;
  bool half_closed_ ABSL_GUARDED_BY(mu_) = false;
  bool request_complete_ ABSL_GUARDED_BY(mu_) = false;
  bool aborted_ ABSL_GUARDED_BY(mu_) = false;
  bool eom_sent_ ABSL_GUARDED_BY(mu_) = false;

  std::optional<absl::Status> status_ ABSL_GUARDED_BY(mu_);

  std::unique_ptr<proxygen::HTTPMessage> request_ ABSL_GUARDED_BY(mu_);
  std::unique_ptr<proxygen::HTTPMessage> response_headers_ ABSL_GUARDED_BY(mu_);
  std::unique_ptr<WebsocketFrame> close_frame_ ABSL_GUARDED_BY(mu_);

  FrameCallback on_frame_ ABSL_GUARDED_BY(mu_);
  DoneCallback on_done_ ABSL_GUARDED_BY(mu_);
  RequestHeadersCallback on_request_headers_ ABSL_GUARDED_BY(mu_);
};

WebsocketServerStream::WebsocketServerStream()
    : impl_(std::make_unique<WebsocketServerStreamImpl>(this)) {}

WebsocketServerStream::~WebsocketServerStream() = default;

void WebsocketServerStream::SetResponseHeaders(
    const proxygen::HTTPMessage& headers) const {
  impl_->SetResponseHeaders(headers);
}

void WebsocketServerStream::SetResponseHeaders(
    std::unique_ptr<proxygen::HTTPMessage> headers) const {
  impl_->SetResponseHeaders(std::move(headers));
}

absl::StatusOr<std::reference_wrapper<const proxygen::HTTPMessage>>
WebsocketServerStream::GetRequestHeaders(absl::Duration timeout) const {
  return impl_->GetRequestHeaders(timeout);
}

absl::Status WebsocketServerStream::SendBytes(const uint8_t* data,
                                              size_t size) {
  return impl_->SendBytes(data, size);
}

absl::Status WebsocketServerStream::SendFrame(const WebsocketFrame& frame) {
  return impl_->SendFrame(frame);
}

absl::Status WebsocketServerStream::SendText(std::string_view text) {
  return impl_->SendText(text);
}

void WebsocketServerStream::Accept() {
  impl_->Accept();
}

void WebsocketServerStream::Start() {
  impl_->Start();
}

absl::Status WebsocketServerStream::GetStatus() const {
  return impl_->GetStatus();
}

void WebsocketServerStream::HalfClose(uint16_t code, std::string_view reason) {
  impl_->HalfClose(code, reason);
}

void WebsocketServerStream::Abort(absl::Status status) {
  impl_->Abort(std::move(status));
}

bool WebsocketServerStream::HalfClosed() const {
  return impl_->HalfClosed();
}

void WebsocketServerStream::SetFrameCallback(FrameCallback on_frame) {
  impl_->SetFrameCallback(std::move(on_frame));
}

void WebsocketServerStream::SetDoneCallback(DoneCallback on_done) {
  impl_->SetDoneCallback(std::move(on_done));
}

void WebsocketServerStream::SetRequestHeadersCallback(
    RequestHeadersCallback on_headers) {
  impl_->SetRequestHeadersCallback(std::move(on_headers));
}

proxygen::RequestHandler* WebsocketServerStream::GetRequestHandler() const {
  return impl_.get();
}

}  // namespace act::net::http