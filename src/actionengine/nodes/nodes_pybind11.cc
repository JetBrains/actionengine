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

#include "actionengine/nodes/nodes_pybind11.h"

#include <memory>
#include <string>
#include <string_view>

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/nodes/node_map.h"
#include "actionengine/stores/chunk_store.h"
#include "actionengine/stores/chunk_store_pybind11.h"
#include "actionengine/stores/chunk_store_reader.h"
#include "actionengine/util/utils_pybind11.h"

namespace act {}  // namespace act

namespace act::pybindings {

void BindNodeMap(py::handle scope, std::string_view name) {
  py::classh<NodeMap>(scope, std::string(name).c_str(),
                      py::release_gil_before_calling_cpp_dtor())
      .def(MakeSameObjectRefConstructor<NodeMap>())
      .def(py::init([](const ChunkStoreFactory& factory = {}) {
             return std::make_shared<NodeMap>(factory);
           }),
           py::arg_v("chunk_store_factory", py::none()))
      .def(
          "get",
          [](const std::shared_ptr<NodeMap>& self, std::string_view id) {
            return ShareWithNoDeleter(self->Get(id));
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "borrow",
          [](const std::shared_ptr<NodeMap>& self, std::string_view id) {
            return self->Borrow(id);
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "extract",
          [](const std::shared_ptr<NodeMap>& self,
             std::string_view id) -> std::optional<std::shared_ptr<AsyncNode>> {
            std::shared_ptr<AsyncNode> node = self->Extract(id);
            if (node == nullptr) {
              return std::nullopt;
            }
            return node;
          },
          py::call_guard<py::gil_scoped_release>())
      .def(
          "contains",
          [](const std::shared_ptr<NodeMap>& self, std::string_view id) {
            return self->contains(id);
          },
          py::call_guard<py::gil_scoped_release>());
}

void BindAsyncNode(py::handle scope, std::string_view name) {
  py::classh<AsyncNode>(scope, std::string(name).c_str(),
                        py::release_gil_before_calling_cpp_dtor())
      .def(py::init<>(), py::call_guard<py::gil_scoped_release>())
      .def(MakeSameObjectRefConstructor<AsyncNode>())
      .def(py::init([](const std::string& id, NodeMap* node_map,
                       std::unique_ptr<PyChunkStore> chunk_store = nullptr) {
             return std::make_shared<AsyncNode>(id, node_map,
                                                std::move(chunk_store));
           }),
           py::arg_v("id", ""), py::arg_v("node_map", nullptr),
           py::arg_v("chunk_store", nullptr),
           py::call_guard<py::gil_scoped_release>())
      // it is not possible to pass a std::unique_ptr to pybind11, so we pass
      // the factory function instead.
      .def(py::init([](const std::string& id, NodeMap* node_map,
                       const ChunkStoreFactory& chunk_store_factory = {}) {
             std::unique_ptr<ChunkStore> chunk_store(nullptr);
             if (chunk_store_factory) {
               chunk_store = chunk_store_factory(id);
             }
             return std::make_shared<AsyncNode>(id, node_map,
                                                std::move(chunk_store));
           }),
           py::arg_v("id", ""), py::arg_v("node_map", nullptr),
           py::arg_v("chunk_store_factory", py::none()))
      .def(
          "put_fragment",
          [](const std::shared_ptr<AsyncNode>& self, NodeFragment fragment,
             int seq = -1) { return self->Put(std::move(fragment), seq); },
          py::arg_v("fragment", NodeFragment()), py::arg_v("seq", -1))
      .def(
          "put_chunk",
          [](const std::shared_ptr<AsyncNode>& self, Chunk chunk, int seq = -1,
             bool final = false) {
            return self->Put(std::move(chunk), seq, final);
          },
          py::arg_v("chunk", Chunk()), py::arg_v("seq", -1),
          py::arg_v("final", false))
      .def(
          "bind_stream",
          [](const std::shared_ptr<AsyncNode>& self,
             const std::shared_ptr<WireStream>& stream) {
            absl::flat_hash_map<std::string, WireStream*> peers;
            peers[stream->GetId()] = stream.get();
            self->GetWriter().BindPeers(std::move(peers));
          },
          py::arg("stream"), py::call_guard<py::gil_scoped_release>())
      .def(
          "next_fragment",
          [](const std::shared_ptr<AsyncNode>& self, double timeout = -1.0,
             py::handle asyncio_future = py::none()) -> absl::Status {
            const absl::Duration timeout_duration =
                timeout < 0 ? absl::InfiniteDuration() : absl::Seconds(timeout);

            ASSIGN_OR_RETURN(
                act::Future<std::optional<NodeFragment>> node_fragment_future,
                self->GetReader().NextNodeFragmentFuture());
            node_fragment_future.state()->SetDeadline(absl::Now() +
                                                      timeout_duration);

            absl::AnyInvocable<void(
                act::Future<std::optional<NodeFragment>>::State* absl_nonnull)>
                callback;
            {
              py::gil_scoped_acquire gil;
              // on cancellation from Python, cancel the C++ future
              asyncio_future.attr("add_done_callback")(
                  py::cpp_function([cpp_future = node_fragment_future.state()](
                                       py::handle future) {
                    if (!future.attr("cancelled")().cast<bool>()) {
                      return;
                    }
                    cpp_future->Cancel();
                  }));

              // on result, resolve Python future
              // ReSharper disable once CppTooWideScope
              // (the scope is correct as this callback must be created with
              // GIL held)
              callback = [asyncio_future =
                              py::cast<py::object>(asyncio_future)](
                             act::Future<std::optional<NodeFragment>>::
                                 State* absl_nonnull cpp_future) mutable {
                absl::StatusOr<std::optional<NodeFragment>> result =
                    cpp_future->WaitForValueOrErrorUntil(absl::InfinitePast());

                if (!result.ok()) {
                  {
                    py::gil_scoped_acquire gil;
                    if (const absl::Status status = SetAsyncioFutureResult(
                            asyncio_future, result.status());
                        !status.ok()) {
                      DLOG(ERROR) << status;
                    }
                    asyncio_future = py::object();
                  }
                  return;
                }

                {
                  py::gil_scoped_acquire gil;
                  if (const absl::Status status = SetAsyncioFutureResult(
                          asyncio_future, *std::move(result));
                      !status.ok()) {
                    DLOG(ERROR) << status;
                  }
                  asyncio_future = py::object();
                }
              };
            }
            node_fragment_future.state()->AddCallback(std::move(callback));
            return absl::OkStatus();
          },
          py::arg_v("timeout", -1.0), py::arg_v("future", py::none()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "next_chunk",
          [](const std::shared_ptr<AsyncNode>& self, double timeout = -1.0,
             py::handle asyncio_future = py::none()) -> absl::Status {
            const absl::Duration timeout_duration =
                timeout < 0 ? absl::InfiniteDuration() : absl::Seconds(timeout);

            ASSIGN_OR_RETURN(
                act::Future<std::optional<NodeFragment>> node_fragment_future,
                self->GetReader().NextNodeFragmentFuture());
            node_fragment_future.state()->SetDeadline(absl::Now() +
                                                      timeout_duration);

            absl::AnyInvocable<void(
                act::Future<std::optional<NodeFragment>>::State* absl_nonnull)>
                callback;
            {
              py::gil_scoped_acquire gil;
              // on cancellation from Python, cancel the C++ future
              asyncio_future.attr("add_done_callback")(
                  py::cpp_function([cpp_future = node_fragment_future.state()](
                                       py::handle future) {
                    if (!future.attr("cancelled")().cast<bool>()) {
                      return;
                    }
                    cpp_future->Cancel();
                  }));

              // on result, resolve Python future
              // ReSharper disable once CppTooWideScope
              // (the scope is correct as this callback must be created with
              // GIL held)
              callback = [asyncio_future =
                              py::cast<py::object>(asyncio_future)](
                             act::Future<std::optional<NodeFragment>>::
                                 State* absl_nonnull cpp_future) mutable {
                absl::StatusOr<std::optional<NodeFragment>> result =
                    cpp_future->WaitForValueOrErrorUntil(absl::InfinitePast());

                if (!result.ok()) {
                  {
                    py::gil_scoped_acquire gil;
                    if (const absl::Status status = SetAsyncioFutureResult(
                            asyncio_future, result.status());
                        !status.ok()) {
                      DLOG(ERROR) << status;
                    }
                    asyncio_future = py::object();
                    return;
                  }
                }

                std::optional<Chunk> chunk;
                if (*result) {
                  if (absl::StatusOr<std::reference_wrapper<Chunk>>
                          result_as_chunk = (*result)->GetChunk();
                      result_as_chunk.ok()) {
                    chunk = std::move(result_as_chunk.value());
                  } else {
                    {
                      py::gil_scoped_acquire gil;
                      if (const absl::Status status = SetAsyncioFutureResult(
                              asyncio_future, result_as_chunk.status());
                          !status.ok()) {
                        DLOG(ERROR) << status;
                      }
                      asyncio_future = py::object();
                    }
                    return;
                  }
                }

                {
                  py::gil_scoped_acquire gil;
                  if (const absl::Status status = SetAsyncioFutureResult(
                          asyncio_future, std::move(chunk));
                      !status.ok()) {
                    DLOG(ERROR) << status;
                  }
                  asyncio_future = py::object();
                }
              };
            }

            node_fragment_future.state()->AddCallback(std::move(callback));
            return absl::OkStatus();
          },
          py::arg_v("timeout", -1.0), py::arg_v("future", py::none()),
          py::call_guard<py::gil_scoped_release>())
      .def("get_id",
           [](const std::shared_ptr<AsyncNode>& self) { return self->GetId(); })
      .def(
          "make_reader",
          [](const std::shared_ptr<AsyncNode>& self,
             const ChunkStoreReaderOptions& options) {
            return std::shared_ptr(self->MakeReader(options));
          },
          py::arg_v("options", ChunkStoreReaderOptions()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "set_reader_options",
          [](const std::shared_ptr<AsyncNode>& self,
             std::optional<bool> ordered = std::nullopt,
             std::optional<bool> remove_chunks = std::nullopt,
             std::optional<int> n_chunks_to_buffer = std::nullopt,
             std::optional<double> timeout = std::nullopt,
             int start_seq_or_offset = -1) {
            ChunkStoreReaderOptions options;
            options.ordered = ordered;
            options.remove_chunks = remove_chunks;
            options.n_chunks_to_buffer = n_chunks_to_buffer;
            options.start_seq_or_offset = start_seq_or_offset;

            std::optional<absl::Duration> timeout_duration = std::nullopt;
            if (timeout.has_value()) {
              timeout_duration = *timeout < 0 ? absl::InfiniteDuration()
                                              : absl::Seconds(*timeout);
            }
            options.timeout = timeout_duration;
            self->SetReaderOptions(options);
            return self;
          },
          py::arg_v("ordered", py::none()),
          py::arg_v("remove_chunks", py::none()),
          py::arg_v("n_chunks_to_buffer", py::none()),
          py::arg_v("timeout", py::none()), py::arg_v("start_seq_or_offset", 0),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "set_reader_options",
          [](const std::shared_ptr<AsyncNode>& self,
             const ChunkStoreReaderOptions& options) {
            self->SetReaderOptions(options);
            return self;
          },
          py::arg_v("options", ChunkStoreReaderOptions()),
          py::call_guard<py::gil_scoped_release>())
      .def("get_chunk_store", [](const std::shared_ptr<AsyncNode>& self) {
        return ShareWithNoDeleter(self->GetChunkStore());
      });
}

}  // namespace act::pybindings
