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

#include "actionengine/net/http/websockets_pybind11.h"

#include <pybind11/stl.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/net/http/proxygen/server.h"
#include "actionengine/net/http/proxygen/wire_stream.h"
#include "actionengine/net/stream.h"
#include "actionengine/util/status_macros.h"
#include "actionengine/util/utils_pybind11.h"

namespace act::pybindings {

namespace py = ::pybind11;

void BindWebsocketWireStream(py::handle scope, std::string_view name) {
  py::classh<net::http::WebsocketWireStream, WireStream>(
      scope, std::string(name).c_str(),
      py::release_gil_before_calling_cpp_dtor())
      .def("send", &net::http::WebsocketWireStream::Send,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "receive",
          [](const std::shared_ptr<net::http::WebsocketWireStream>& self,
             double timeout) {
            const absl::Duration timeout_duration =
                timeout < 0.0 ? absl::InfiniteDuration()
                              : absl::Seconds(timeout);
            return self->Receive(timeout_duration);
          },
          py::arg_v("timeout", -1.0), py::call_guard<py::gil_scoped_release>())
      .def("accept", &net::http::WebsocketWireStream::Accept,
           py::call_guard<py::gil_scoped_release>())
      .def("start", &net::http::WebsocketWireStream::Start,
           py::call_guard<py::gil_scoped_release>())
      .def("half_close", &net::http::WebsocketWireStream::HalfClose,
           py::call_guard<py::gil_scoped_release>())
      .def("get_status", &net::http::WebsocketWireStream::GetStatus)
      .def("get_id", &net::http::WebsocketWireStream::GetId)
      .def("__repr__",
           [](const std::shared_ptr<net::http::WebsocketWireStream>& self) {
             return absl::StrFormat("WebsocketWireStream %p", &self);
           })
      .doc() = "A WebsocketWireStream interface.";
}

void BindWebsocketServer(py::handle scope, std::string_view name) {
  py::classh<net::http::WebsocketServer>(
      scope, std::string(name).c_str(), "A WebsocketServer interface.",
      py::release_gil_before_calling_cpp_dtor())
      .def(py::init([](Service* absl_nonnull service, std::string_view address,
                       uint16_t port) {
             return std::make_shared<net::http::WebsocketServer>(service,
                                                                 address, port);
           }),
           py::arg("service"), py::arg_v("address", "0.0.0.0"),
           py::arg_v("port", 20000), pybindings::keep_event_loop_memo())
      .def("run", &net::http::WebsocketServer::Run,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "cancel",
          [](const std::shared_ptr<net::http::WebsocketServer>& self) {
            return self->Cancel();
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "join",
          [](const std::shared_ptr<net::http::WebsocketServer>& self) {
            return self->Join();
          },
          py::call_guard<py::gil_scoped_release>())
      .doc() = "A WebsocketServer interface.";
}

py::module_ MakeWebsocketsModule(py::module_ scope,
                                 std::string_view module_name) {
  pybind11::module_ websockets = scope.def_submodule(
      std::string(module_name).c_str(), "ActionEngine Websocket interface.");

  BindWebsocketWireStream(websockets, "WebsocketWireStream");
  BindWebsocketServer(websockets, "WebsocketServer");

  websockets.def(
      "make_websocket_stream",
      [](std::string_view url)
          -> absl::StatusOr<std::unique_ptr<net::http::WebsocketWireStream>> {
        return net::http::WebsocketWireStream::Connect(url);
      },
      py::arg_v("url", "http://localhost:20000/"),
      py::call_guard<py::gil_scoped_release>());

  return websockets;
}

}  // namespace act::pybindings