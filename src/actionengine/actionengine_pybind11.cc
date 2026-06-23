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

#include <string_view>

#include <Python.h>
#include <absl/debugging/failure_signal_handler.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/tracer_provider.h>
#include <pybind11/cast.h>
#include <pybind11/detail/common.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11_abseil/check_status_module_imported.h>

#include "actionengine/actions/actions_pybind11.h"
#include "actionengine/data/data_pybind11.h"
#include "actionengine/net/http/websockets_pybind11.h"
#include "actionengine/net/webrtc/webrtc_pybind11.h"
#include "actionengine/nodes/nodes_pybind11.h"
#include "actionengine/redis/chunk_store_pybind11.h"
#include "actionengine/service/service_pybind11.h"
#include "actionengine/stores/chunk_store_pybind11.h"
#include "actionengine/util/global_settings_pybind11.h"
#include "actionengine/util/telemetry.h"
#include "actionengine/util/utils_pybind11.h"

namespace pybind11::detail {
template <>
class type_caster<std::unique_ptr<act::ChunkStore>>
    : public type_caster_base<std::unique_ptr<act::ChunkStore>> {};
}  // namespace pybind11::detail

namespace act {

static absl::StatusOr<
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>>
InitializeOTelProvider() {
  std::string otel_endpoint;
  std::string otel_auth_header;

  if (const char* absl_nullable otel_endpoint_env =
          std::getenv("ACTIONENGINE_OTEL_ENDPOINT")) {
    otel_endpoint = otel_endpoint_env;
  }

  if (!otel_endpoint.empty()) {
    if (const char* absl_nullable otel_auth_header_env =
            std::getenv("ACTIONENGINE_OTEL_AUTH_HEADER")) {
      otel_auth_header = otel_auth_header_env;
    }
    if (otel_auth_header.empty()) {
      LOG(WARNING) << "ACTIONENGINE_OTEL_AUTH_HEADER is not set, "
                   << "OTLP exporter will not be initialized.";
      return opentelemetry::nostd::shared_ptr<
          opentelemetry::trace::TracerProvider>(nullptr);
    }
  }

  if (const char* absl_nullable langfuse_base_url_env =
          std::getenv("LANGFUSE_BASE_URL")) {
    otel_endpoint = langfuse_base_url_env;
  }
  if (otel_endpoint.empty()) {
    LOG(WARNING)
        << "Neither ACTIONENGINE_OTEL_AUTH_HEADER, nor "
           "LANGFUSE_BASE_URL is set, OTLP exporter will not be initialized.";
    return opentelemetry::nostd::shared_ptr<
        opentelemetry::trace::TracerProvider>(nullptr);
  }
  otel_endpoint += "/api/public/otel/v1/traces";
  std::string langfuse_public_key;
  if (const char* absl_nullable langfuse_public_key_env =
          std::getenv("LANGFUSE_PUBLIC_KEY")) {
    langfuse_public_key = langfuse_public_key_env;
  }
  if (langfuse_public_key.empty()) {
    LOG(WARNING) << "LANGFUSE_PUBLIC_KEY is not set, "
                 << "OTLP exporter will not be initialized.";
    return opentelemetry::nostd::shared_ptr<
        opentelemetry::trace::TracerProvider>(nullptr);
  }

  std::string langfuse_secret_key;
  if (const char* absl_nullable langfuse_secret_key_env =
          std::getenv("LANGFUSE_SECRET_KEY")) {
    langfuse_secret_key = langfuse_secret_key_env;
  }
  if (langfuse_secret_key.empty()) {
    LOG(WARNING) << "LANGFUSE_SECRET_KEY is not set, "
                 << "OTLP exporter will not be initialized.";
    return opentelemetry::nostd::shared_ptr<
        opentelemetry::trace::TracerProvider>(nullptr);
  }

  otel_auth_header =
      "Basic " + absl::Base64Escape(absl::StrCat(langfuse_public_key, ":",
                                                 langfuse_secret_key));
  return act::telemetry::GetHttpTracerProvider(otel_endpoint, otel_auth_header);
}

PYBIND11_MODULE(_C, m) {
  absl::InstallFailureSignalHandler({});
  absl::StatusOr<
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>>
      http_tracer_provider = InitializeOTelProvider();
  if (!http_tracer_provider.ok()) {
    LOG(WARNING) << "Failed to initialize HTTP tracer provider: "
                 << http_tracer_provider.status();
  } else if (*http_tracer_provider) {
    act::telemetry::SetGlobalTracerProvider(*http_tracer_provider);
  }
  if (!pybind11::google::internal::IsStatusModuleImported()) {
    py::module_::import("actionengine.status");
    // importing under a custom path/name, so just in case check that the
    // library understands our import.
    py::google::internal::CheckStatusModuleImported();
  }

  py::bind_map<absl::flat_hash_map<std::string, std::string>>(m,
                                                              "AbslStringMap");

  py::module_ data = pybindings::MakeDataModule(m, "data");

  py::module_ chunk_store = pybindings::MakeChunkStoreModule(m, "chunk_store");

  py::module_ actions = pybindings::MakeActionsModule(m, "actions");
  const py::module_ nodes = m.def_submodule(
      "nodes", "ActionEngine interfaces for AsyncNode and NodeMap.");
  pybindings::BindNodeMap(nodes, "NodeMap");
  pybindings::BindAsyncNode(nodes, "AsyncNode");
  py::module_ redis = pybindings::MakeRedisModule(m, "redis");
  py::module_ service = pybindings::MakeServiceModule(m, "service");
  py::module_ webrtc = pybindings::MakeWebRtcModule(m, "webrtc");
  py::module_ websockets = pybindings::MakeWebsocketsModule(m, "websockets");

  pybindings::BindGlobalSettings(m, "GlobalSettings");
  pybindings::BindGetGlobalSettingsFunction(m, "get_global_settings");

  m.def("save_event_loop_globally", &pybindings::SaveEventLoopGlobally,
        py::arg_v("loop", py::none()),
        "Saves the provided event loop globally for later use. If no loop is "
        "provided, attempts to get the currently running event loop.");

  m.def("run_threadsafe_if_coroutine", &pybindings::RunThreadsafeIfCoroutine,
        py::arg("function_call_result"), py::arg_v("loop", py::none()),
        pybindings::keep_event_loop_memo());

  m.def("save_first_encountered_event_loop",
        &pybindings::SaveFirstEncounteredEventLoop,
        "Saves the first encountered event loop globally for later use.");
}

}  // namespace act
