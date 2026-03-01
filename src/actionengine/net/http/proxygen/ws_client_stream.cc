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

#include "actionengine/net/http/proxygen/ws_client_stream.h"

#include <memory>
#include <string>
#include <string_view>

#include <absl/status/status.h>
#pragma push_macro("RETURN_IF_ERROR")
#include <folly/io/async/EventBaseManager.h>
#include <folly/io/async/SSLContext.h>
#include <proxygen/lib/http/HTTPConnector.h>
#include <proxygen/lib/http/coro/client/HTTPCoroConnector.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>
#include <proxygen/lib/utils/URL.h>
#pragma pop_macro("RETURN_IF_ERROR")

#include "actionengine/net/http/proxygen/folly_utils.h"
#include "actionengine/net/http/proxygen/http_connector.h"
#include "actionengine/net/http/ws_common.h"
#include "actionengine/util/status_macros.h"

namespace act::net::http {

static act::util::EvbWorkerPool* GetWorkerPool() {
  static auto pool = new act::util::EvbWorkerPool(4);
  return pool;
}

class WebsocketClientStreamImpl final
    : public WebsocketStream,
      public proxygen::HTTPTransactionHandler {
 public:
  friend class WebsocketClientStream;

  explicit WebsocketClientStreamImpl(WebsocketClientStream* absl_nonnull owner,
                                     proxygen::URL url)
      : owner_(owner), evb_(nullptr), url_(std::move(url)) {}

  ~WebsocketClientStreamImpl() override {
    act::MutexLock lock(&mu_);
    if (!half_closed_) {
      LOG(WARNING) << "Stream was not closed before destruction.";
    }

    // If started and not finished by the time of destruction, we need to abort.
    if (txn_ != nullptr && !status_) {
      Abort_(absl::ResourceExhaustedError("Stream is being destroyed."));
    }

    if (!eom_sent_ && !txn_aborted_ && txn_ != nullptr) {
      eom_sent_ = true;
      mu_.unlock();
      evb_->runImmediatelyOrRunInEventBaseThreadAndWait(
          [txn = txn_] { txn->sendEOM(); });
      mu_.lock();
    }

    // Transaction must end and done callback must have run for clean destruction.
    while (txn_ != nullptr || on_done_ != nullptr) {
      DLOG(INFO) << "Waiting for transaction to end.";
      cv_.Wait(&mu_);
    }
  }

  void SetRequestHeaders(std::unique_ptr<proxygen::HTTPMessage> headers) {
    act::MutexLock lock(&mu_);
    request_headers_ = std::move(headers);
  }

  absl::StatusOr<std::reference_wrapper<const proxygen::HTTPMessage>>
  GetResponseHeaders(absl::Duration timeout) const {
    const absl::Time deadline = absl::Now() + timeout;
    act::MutexLock lock(&mu_);
    while (response_headers_ == nullptr && !status_.has_value() &&
           deadline > absl::Now()) {
      cv_.WaitWithDeadline(&mu_, deadline);
    }
    if (response_headers_ != nullptr) {
      return std::cref(*response_headers_);
    }
    if (status_) {
      // For an OK status, headers must have been set at some point
      DCHECK(!status_->ok());
      return *status_;
    }
    if (absl::Now() > deadline) {
      return absl::DeadlineExceededError(
          "Timed out waiting for response headers.");
    }

    return absl::UnknownError("Should not reach here.");
  }

  absl::Status SendBytes(const uint8_t* data, size_t size) override {
    WebsocketFrame frame;
    frame.fin = true;
    frame.opcode = WSOpcode::kBinary;
    const uint32_t mask = absl::Uniform<uint32_t>(
        bit_gen_, 0, std::numeric_limits<uint32_t>::max());
    frame.SetMask(mask);
    frame.masked = true;
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
    const uint32_t mask = absl::Uniform<uint32_t>(
        bit_gen_, 0, std::numeric_limits<uint32_t>::max());
    frame.SetMask(mask);
    frame.masked = true;
    frame.unmasked_payload.reserve(text.size());
    for (const char c : text) {
      frame.unmasked_payload.push_back(c);
    }
    return SendFrame(frame);
  }

  void Accept() override {
    CHECK(false) << "Accept() must not be called on client streams.";
  }

  void Start() override {
    act::MutexLock lock(&mu_);
    while (txn_ == nullptr && !status_) {
      cv_.Wait(&mu_);
    }
    if (status_) {
      return;
    }
    DCHECK(txn_ != nullptr && evb_ != nullptr);
    if (txn_ == nullptr || evb_ == nullptr) {
      MarkNoFurtherIngressWithStatus(absl::FailedPreconditionError(
          "Trying to start a stream with no associated HTTP transaction or "
          "event loop."));
    }
    txn_->setIdleTimeout(std::chrono::milliseconds(120000));

    std::unique_ptr<proxygen::HTTPMessage> request =
        std::move(request_headers_);
    if (request == nullptr) {
      request = std::make_unique<proxygen::HTTPMessage>();
    }
    request->getHeaders().add(proxygen::HTTP_HEADER_USER_AGENT,
                              "Action Engine Proxygen Client");
    request->getHeaders().add(proxygen::HTTP_HEADER_HOST,
                              url_.getHostAndPort());
    request->setMethod(proxygen::HTTPMethod::GET);
    request->setSecure(url_.isSecure());
    request->setWantsKeepalive(true);
    request->setURL(url_.makeRelativeURL());
    request->getHeaders().add("Connection", "Upgrade");
    request->getHeaders().add("Sec-WebSocket-Version", "13");
    request->getHeaders().add("Accept", "*/*");
    request->setEgressWebsocketUpgrade();

    mu_.unlock();
    evb_->runInEventBaseThreadAndWait(
        [txn = txn_, request = std::move(request)]() mutable {
          txn->sendHeaders(*request);
        });
    mu_.lock();

    while (!upgrade_status_ && !status_) {
      cv_.Wait(&mu_);
    }
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

  void setTransaction(
      proxygen::HTTPTransaction* absl_nonnull txn) noexcept override {
    act::MutexLock lock(&mu_);
    DCHECK(txn_ == nullptr);
    txn_ = txn;
    evb_ = folly::EventBaseManager::get()->getExistingEventBase();
    DCHECK(evb_ != nullptr);
  }

  void detachTransaction() noexcept override {
    act::MutexLock lock(&mu_);
    DCHECK(txn_ != nullptr);
    txn_ = nullptr;
    cv_.SignalAll();
  }

  void onHeadersComplete(
      std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override {
    act::MutexLock lock(&mu_);
    DCHECK(response_headers_ == nullptr);
    response_headers_ = std::move(msg);
    cv_.SignalAll();
  }

  void onBody(std::unique_ptr<folly::IOBuf> chain) noexcept override {
    act::MutexLock lock(&mu_);

    folly::fbstring buf = chain->moveToFbString();
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
        status_ = absl::OkStatus();
        cv_.SignalAll();

        if (on_frame) {
          mu_.unlock();
          (*on_frame)(owner_, nullptr);
          mu_.lock();
        }

        MarkNoFurtherIngressWithStatus(absl::OkStatus());

        absl::StatusOr<uint16_t> close_code = GetCloseCode(*frame);
        absl::StatusOr<std::string_view> close_reason = GetCloseReason(*frame);
        if (!close_code.ok() || !close_reason.ok()) {
          Abort_(absl::InternalError(
              absl::StrCat("Could not parse close frame: ", close_code.status(),
                           ", ", close_reason.status())));
          return;
        }
        close_frame_ = std::make_unique<WebsocketFrame>(std::move(*frame));
        if (*close_code != 1000) {
          half_closed_ = true;
          if (!SendCloseFrame_(*close_code, "ACK").ok()) {
            Abort_(
                absl::ResourceExhaustedError("Could not send a Close frame."));
          }
          return;
        }

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

  void onTrailers(
      std::unique_ptr<proxygen::HTTPHeaders> trailers) noexcept override {
    act::MutexLock lock(&mu_);
    Abort_(absl::FailedPreconditionError("The other end sent HTTP trailers."));
  }

  void onEOM() noexcept override {
    act::MutexLock lock(&mu_);
    if (close_frame_ == nullptr) {
      // Forbid sending, as a no-close frame EOM is incorrect
      Abort_(absl::ResourceExhaustedError(
          "Received a EOM without a close frame."));
    }
    if (!eom_sent_) {
      eom_sent_ = true;
      evb_->runImmediatelyOrRunInEventBaseThreadAndWait(
          [txn = txn_] { txn->sendEOM(); });
    }
  }

  void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override {
    act::MutexLock lock(&mu_);
    upgrade_status_ = absl::OkStatus();
    cv_.SignalAll();
  }

  void onError(const proxygen::HTTPException& error) noexcept override {
    DLOG(INFO) << "client onError: " << error.what();
    act::MutexLock lock(&mu_);
    Abort_(absl::InternalError(error.what()));
  }

  void onEgressPaused() noexcept override {}

  void onEgressResumed() noexcept override {}

 private:
  void Abort_(absl::Status status) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (!half_closed_) {
      half_closed_ = true;
    }

    if (!status_) {
      MarkNoFurtherIngressWithStatus(std::move(status));
    }

    if (txn_ != nullptr && !txn_aborted_) {
      txn_aborted_ = true;
      mu_.unlock();
      evb_->runImmediatelyOrRunInEventBaseThreadAndWait(
          [txn = txn_] { txn->sendAbort(); });
      mu_.lock();
    }
  }

  absl::Status SendCloseFrame_(uint16_t code, std::string_view reason)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    ASSIGN_OR_RETURN(WebsocketFrame frame,
                     WebsocketFrame::MakeCloseFrame(code, reason));
    const uint32_t mask = absl::Uniform<uint32_t>(
        bit_gen_, 0, std::numeric_limits<uint32_t>::max());
    frame.SetMask(mask);
    frame.masked = true;
    RETURN_IF_ERROR(SendFrame_(std::move(frame)));
    return absl::OkStatus();
  }

  void MarkNoFurtherIngressWithStatus(absl::Status status)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    // DCHECK(!status_);  // Status must only be set by this method

    if (!upgrade_status_) {
      DCHECK(!status.ok()) << "Overall status can be OK only if an upgrade "
                              "happened at some point.";
      upgrade_status_ = absl::ResourceExhaustedError(absl::StrCat(
          "Could not upgrade before the stream was exhausted: ", status));
    }
    status_ = std::move(status);

    if (on_done_) {
      const WebsocketStream::DoneCallback* on_done = &on_done_;
      mu_.unlock();
      (*on_done)(owner_);
      mu_.lock();
      on_done_ = nullptr;
    }
    cv_.SignalAll();
  }

  absl::Status SendFrame_(const WebsocketFrame& frame)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    if (half_closed_) {
      return absl::FailedPreconditionError(
          "Trying to send a frame after the stream was half-closed.");
    }
    if (evb_ == nullptr || txn_ == nullptr) {
      return absl::FailedPreconditionError(
          "Trying to send a frame before the stream was started.");
    }
    if (status_ && !status_->ok()) {
      return absl::FailedPreconditionError(absl::StrCat(
          "Local internal error or abnormal closure by other end: ",
          status_->message()));
    }

    std::unique_ptr<folly::IOBuf> serialized =
        folly::IOBuf::fromString(frame.Serialize());

    evb_->runImmediatelyOrRunInEventBaseThread(
        [txn = txn_, serialized = std::move(serialized)]() mutable {
          txn->sendChunkHeader(serialized->length());
          txn->sendBody(std::move(serialized));
          txn->sendChunkTerminator();
        });

    return absl::OkStatus();
  }

  mutable act::Mutex mu_;
  mutable act::CondVar cv_ ABSL_GUARDED_BY(mu_);

  WebsocketClientStream* owner_;

  mutable absl::BitGen bit_gen_;

  folly::EventBase* absl_nonnull evb_;
  proxygen::HTTPTransaction* txn_ = nullptr;
  proxygen::URL url_;

  bool half_closed_ ABSL_GUARDED_BY(mu_) = false;
  bool txn_aborted_ ABSL_GUARDED_BY(mu_) = false;
  bool eom_sent_ ABSL_GUARDED_BY(mu_) = false;
  std::optional<absl::Status> status_ ABSL_GUARDED_BY(mu_);
  std::optional<absl::Status> upgrade_status_ ABSL_GUARDED_BY(mu_);

  std::unique_ptr<proxygen::HTTPMessage> request_headers_ ABSL_GUARDED_BY(mu_);
  std::unique_ptr<proxygen::HTTPMessage> response_headers_ ABSL_GUARDED_BY(mu_);
  std::unique_ptr<WebsocketFrame> close_frame_ ABSL_GUARDED_BY(mu_);

  FrameCallback on_frame_ ABSL_GUARDED_BY(mu_);
  DoneCallback on_done_ ABSL_GUARDED_BY(mu_);
};

absl::StatusOr<std::unique_ptr<WebsocketClientStream>>
WebsocketClientStream::Connect(std::string_view url, folly::EventBase* evb,
                               folly::HHWheelTimer* timer) {
  proxygen::URL parsed_url(url);
  if (!parsed_url.isValid()) {
    return absl::InvalidArgumentError(absl::StrCat("Invalid URL: ", url));
  }
  if (evb == nullptr) {
    DCHECK(timer == nullptr);
    const act::util::EvbWorker& worker = GetWorkerPool()->GetWorker();
    evb = worker.evb.get();
    timer = worker.timer();
  }

  HTTPConnector connector(evb, timer);
  ASSIGN_OR_RETURN(proxygen::HTTPUpstreamSession * session,
                   connector.Connect(url));

  auto stream = std::make_unique<WebsocketClientStream>(url);
  evb->runInEventBaseThreadAndWait(
      [session, impl = stream->impl_.get()]()
          ABSL_NO_THREAD_SAFETY_ANALYSIS { session->newTransaction(impl); });

  return stream;
}

WebsocketClientStream::WebsocketClientStream(std::string_view url)
    : impl_(std::make_unique<WebsocketClientStreamImpl>(this,
                                                        proxygen::URL(url))) {}

WebsocketClientStream::~WebsocketClientStream() {
  impl_->Abort(absl::ResourceExhaustedError("Stream is being destroyed."));
  impl_.reset();
}

void WebsocketClientStream::SetRequestHeaders(
    const proxygen::HTTPMessage& headers) const {
  impl_->SetRequestHeaders(std::make_unique<proxygen::HTTPMessage>(headers));
}

void WebsocketClientStream::SetRequestHeaders(
    std::unique_ptr<proxygen::HTTPMessage> headers) const {
  impl_->SetRequestHeaders(std::move(headers));
}

absl::StatusOr<std::reference_wrapper<const proxygen::HTTPMessage>>
WebsocketClientStream::GetResponseHeaders(absl::Duration timeout) const {
  return impl_->GetResponseHeaders(timeout);
}

absl::Status WebsocketClientStream::SendBytes(const uint8_t* data,
                                              size_t size) {
  return impl_->SendBytes(data, size);
}

absl::Status WebsocketClientStream::SendFrame(const WebsocketFrame& frame) {
  return impl_->SendFrame(frame);
}

absl::Status WebsocketClientStream::SendText(std::string_view text) {
  return impl_->SendText(text);
}

void WebsocketClientStream::Accept() {
  impl_->Accept();
}

void WebsocketClientStream::Start() {
  impl_->Start();
}

absl::Status WebsocketClientStream::GetStatus() const {
  return impl_->GetStatus();
}

void WebsocketClientStream::HalfClose(uint16_t code, std::string_view reason) {
  impl_->HalfClose(code, reason);
}

void WebsocketClientStream::Abort(absl::Status status) {
  impl_->Abort(std::move(status));
}

bool WebsocketClientStream::HalfClosed() const {
  return impl_->HalfClosed();
}

void WebsocketClientStream::SetFrameCallback(FrameCallback on_frame) {
  impl_->SetFrameCallback(std::move(on_frame));
}

void WebsocketClientStream::SetDoneCallback(DoneCallback on_done) {
  impl_->SetDoneCallback(std::move(on_done));
}

}  // namespace act::net::http