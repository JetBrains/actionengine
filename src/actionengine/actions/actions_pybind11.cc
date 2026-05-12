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

#include "actionengine/actions/actions_pybind11.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <pybind11/attr.h>
#include <pybind11/cast.h>
#include <pybind11/eval.h>
#include <pybind11/functional.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/import_status_module.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

#include "actionengine/actions/action.h"
#include "actionengine/data/serialization.h"
#include "actionengine/nodes/node_map.h"
#include "actionengine/service/session.h"
#include "actionengine/util/random.h"
#include "actionengine/util/status_macros.h"
#include "actionengine/util/utils_pybind11.h"
#include "boost/json/value.hpp"
#include "boost/json/value_from.hpp"

namespace act::pybindings {

namespace py = ::pybind11;

struct PyUserData {
  ~PyUserData() {
    py::gil_scoped_acquire gil;

    // no deletion unless fully run or cancelled
    if (action->HasBeenRun()) {
      DCHECK(concurrent_future.attr("done")().cast<bool>());
    }

    asyncio_future = py::object();
    concurrent_future = py::object();
  }

  Action* absl_nonnull action = nullptr;
  thread::PermanentEvent done;

  py::object concurrent_future;
  py::object asyncio_future;
};

static std::shared_ptr<PyUserData> EnsurePyUserData(
    Action* absl_nonnull action) {
  if (absl::StatusOr<std::shared_ptr<void>> future_ptr =
          action->GetUserData("_py_user_data");
      future_ptr.ok()) {
    return std::static_pointer_cast<PyUserData>(future_ptr.value());
  }
  auto user_data = std::make_shared<PyUserData>();
  user_data->action = action;
  user_data->concurrent_future = py::none();
  user_data->asyncio_future =
      GetGloballySavedEventLoop().attr("create_future")();
  action->SetUserData("_py_user_data", user_data);
  return user_data;
}

class DestroyUnderGilGuard {
 public:
  explicit DestroyUnderGilGuard(py::handle obj) {
    obj_ = py::cast<py::object>(obj);
  }

  DestroyUnderGilGuard(const DestroyUnderGilGuard& other) {
    py::gil_scoped_acquire gil;
    obj_ = other.obj_;
  }

  DestroyUnderGilGuard(DestroyUnderGilGuard&& other) noexcept {
    py::gil_scoped_acquire gil;
    obj_ = std::move(other.obj_);
    other.obj_ = py::object();
  }

  DestroyUnderGilGuard& operator=(const DestroyUnderGilGuard& other) {
    py::gil_scoped_acquire gil;
    if (this == &other) {
      return *this;
    }
    obj_ = other.obj_;
    return *this;
  }

  DestroyUnderGilGuard& operator=(DestroyUnderGilGuard&& other) noexcept {
    py::gil_scoped_acquire gil;
    if (this == &other) {
      return *this;
    }
    obj_ = std::move(other.obj_);
    other.obj_ = py::object();
    return *this;
  }

  explicit DestroyUnderGilGuard(py::object obj) { obj_ = std::move(obj); }

  ~DestroyUnderGilGuard() {
    py::gil_scoped_acquire gil;
    obj_ = py::object();
  }

  py::handle Get() const { return obj_; }

 private:
  py::object obj_;
};

absl::StatusOr<ActionHandler> MakeSimpleActionHandler(py::handle py_handler) {
  try {
    if (const bool is_coroutine_function =
            py::module::import("inspect")
                .attr("iscoroutinefunction")(py_handler)
                .cast<bool>();
        !is_coroutine_function) {
      return absl::InvalidArgumentError(
          "Handler must be a coroutine function.");
    }
  } catch (const py::error_already_set& e) {
    return PyExceptionToStatus(e);
  }

  if (py_handler.is_none()) {
    return absl::InvalidArgumentError("Handler must not be None.");
  }
  if (py_handler.ptr() == nullptr) {
    return absl::InvalidArgumentError("Handler must not be None.");
  }

  return [py_handler = DestroyUnderGilGuard(py_handler)](
             const std::shared_ptr<Action>& action) mutable -> absl::Status {
    py::gil_scoped_acquire gil;
    std::shared_ptr<PyUserData> user_data = EnsurePyUserData(action.get());

    absl::StatusOr<py::object> concurrent_future_or = RunThreadsafeIfCoroutine(
        py_handler.Get()(action), GetGloballySavedEventLoop());
    if (!concurrent_future_or.ok()) {
      return concurrent_future_or.status();
    }

    user_data->concurrent_future = *std::move(concurrent_future_or);
    *concurrent_future_or = py::object();

    // If an action is cancelled from C++ side,
    action->SetOnCancelled([user_data]() {
      py::gil_scoped_acquire gil;
      auto _ = user_data->asyncio_future.attr("cancel")();
    });

    user_data->concurrent_future.attr("add_done_callback")(
        py::cpp_function([user_data](py::handle) {
          if (!user_data->done.HasBeenNotified()) {
            user_data->done.Notify();
          }
        }));

    user_data->asyncio_future.attr("add_done_callback")(
        py::cpp_function([user_data](py::handle future) {
          if (future.attr("cancelled")().cast<bool>()) {
            if (!user_data->concurrent_future.attr("done")().cast<bool>()) {
              auto _ = user_data->concurrent_future.attr("cancel")();
            }
          }
        }));

    {
      py::gil_scoped_release release_gil;
      thread::Select({user_data->done.OnEvent()});
    }

    absl::StatusOr<py::object> result;
    try {
      result.emplace() = user_data->concurrent_future.attr("result")();
    } catch (py::error_already_set& e) {
      result.AssignStatus(PyExceptionToStatus(e));
      DLOG(ERROR) << "[" << action->schema().name << " " << action->id()
                  << "] ended with an exception: \033[91m" << result.status()
                  << "\033[0m";
    }

    absl::Status raw_status = result.status();
    RETURN_IF_ERROR(
        SetAsyncioFutureResult(user_data->asyncio_future, std::move(result)));
    return raw_status;
  };
}

static absl::Status SetBoostJsonObjectFromPyObject(
    boost::json::object* absl_nonnull object, py::dict dict) {
  try {
    const auto dumps = py::module::import("json").attr("dumps");
    const auto json_str = dumps(dict).cast<std::string>();

    ASSIGN_OR_RETURN(boost::json::value value, act::ParseJson(json_str));
    auto value_ptr_as_object = value.try_as_object();
    if (value_ptr_as_object.has_error()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Failed to parse json: ", value_ptr_as_object.error().message()));
    }
    *object = std::move(*value_ptr_as_object);
  } catch (const py::cast_error& e) {
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to cast dict to json: ", e.what()));
  }
  return absl::OkStatus();
}

static absl::StatusOr<py::object> ToPyObject(const boost::json::value& value) {
  if (value.is_null()) {
    return py::none();
  }
  if (value.is_bool()) {
    return py::bool_(value.as_bool());
  }
  if (value.is_int64()) {
    return py::int_(value.as_int64());
  }
  if (value.is_double()) {
    return py::float_(value.as_double());
  }
  if (value.is_string()) {
    return py::str(std::string(value.as_string()));
  }
  if (value.is_uint64()) {
    return py::int_(value.as_uint64());
  }
  if (value.is_array()) {
    py::list list;
    for (const auto& item : value.as_array()) {
      ASSIGN_OR_RETURN(py::object item_obj, ToPyObject(item));
      list.append(item_obj);
    }
    return list;
  }
  if (value.is_object()) {
    py::dict dict;
    for (const auto& [key, item] : value.as_object()) {
      ASSIGN_OR_RETURN(py::object item_obj, ToPyObject(item));
      dict[py::str(key)] = item_obj;
    }
    return dict;
  }
  return absl::InvalidArgumentError(
      "Cannot load boost::json::value into a Python object");
}

void BindActionPortSchema(py::handle scope, std::string_view name) {
  // py::bind_map<absl::flat_hash_map<std::string, ActionPortSchema>>(
  //     scope, "ActionPortSchemaMap");
  py::class_<ActionPortSchema>(scope, std::string(name).c_str())
      .def(py::init<>())
      .def(py::init([](std::string_view name, std::string_view mimetype,
                       std::string_view description = "",
                       bool required = false) {
             return ActionPortSchema{std::string(name), std::string(mimetype),
                                     std::string(description), required};
           }),
           py::arg("name"), py::arg("mimetype"), py::arg_v("description", ""),
           py::arg_v("required", false))
      .def(
          "copy",
          [](const ActionPortSchema& self,
             bool clear_autofills = false) -> ActionPortSchema {
            ActionPortSchema copy(self);
            if (clear_autofills) {
              copy.autofills = std::nullopt;
            }
            return copy;
          },
          py::arg_v("clear_autofills", false))
      .def(
          "autofill_with",
          [](ActionPortSchema& self, py::object objects,
             std::string_view mimetype = "",
             act::SerializerRegistry* registry = nullptr) -> ActionPortSchema& {
            if (objects.is_none()) {
              objects = py::list();
            }
            const auto object_list = py::cast<py::list>(objects);
            const auto to_chunk =
                py::module_::import("actionengine.data").attr("to_chunk");

            py::list chunks;
            for (const auto& obj : object_list) {
              py::object chunk = to_chunk(obj, py::str(mimetype), registry);
              chunks.append(chunk);
            }
            self.autofills.emplace();
            self.autofills->reserve(chunks.size());
            for (auto& chunk : chunks) {
              self.autofills->push_back(py::cast<Chunk>(std::move(chunk)));
            }
            return self;
          },
          py::arg("chunks"), py::arg_v("mimetype", ""),
          py::arg_v("registry", py::none()), py::return_value_policy::reference)
      .def_property_readonly("autofills",
                             [](const ActionPortSchema& self)
                                 -> const std::optional<std::vector<Chunk>>& {
                               return self.autofills;
                             })
      .def_property(
          "json_schema",
          [](const ActionPortSchema& self) -> absl::StatusOr<py::dict> {
            ASSIGN_OR_RETURN(py::dict dict, ToPyObject(self.json_schema));
            if (!dict.contains("title")) {
              dict["title"] = py::str(self.name);
            }
            if (!dict.contains("description")) {
              dict["description"] = py::str(self.description);
            }
            return dict;
          },
          [](ActionPortSchema& self, py::dict schema) -> absl::Status {
            RETURN_IF_ERROR(SetBoostJsonObjectFromPyObject(&self.json_schema,
                                                           std::move(schema)));
            return absl::OkStatus();
          })
      .def_readwrite("name", &ActionPortSchema::name)
      .def_readwrite("type", &ActionPortSchema::type)
      .def_readwrite("description", &ActionPortSchema::description)
      .def_readwrite("unary", &ActionPortSchema::unary)
      .def_readwrite("required", &ActionPortSchema::required)
      // .def("__repr__",
      //      [](const ActionPortSchema& def) { return absl::StrCat(def); })
      .doc() = "A port of an action schema.";
}

void BindActionSchema(py::handle scope, std::string_view name) {
  py::class_<ActionSchema>(scope, std::string(name).c_str())
      .def(py::init([]() {
        ActionSchema schema;
        schema.user_data = std::make_shared<DestroyUnderGilGuard>(py::dict());
        return schema;
      }))
      .def(py::init([](std::string_view action_name,
                       const std::vector<ActionPortSchema>& inputs,
                       const std::vector<ActionPortSchema>& outputs,
                       std::string_view description = "") {
             absl::flat_hash_map<std::string, ActionPortSchema> input_ports;
             input_ports.reserve(inputs.size());
             for (const auto& port : inputs) {
               input_ports[port.name] = port;
             }
             absl::flat_hash_map<std::string, ActionPortSchema> output_ports;
             output_ports.reserve(outputs.size());
             for (const auto& port : outputs) {
               output_ports[port.name] = port;
             }
             auto schema = ActionSchema(
                 std::string(action_name), std::move(input_ports),
                 std::move(output_ports), std::string(description));
             if (schema.user_data == nullptr) {
               schema.user_data =
                   std::make_shared<DestroyUnderGilGuard>(py::dict());
             }
             return schema;
           }),
           py::kw_only(), py::arg("name"), py::arg_v("inputs", py::none()),
           py::arg_v("outputs", py::none()), py::arg_v("description", ""))
      .def(
          "copy",
          [](const ActionSchema& self, bool clear_autofills = false) {
            ActionSchema copy = self;
            if (clear_autofills) {
              for (auto& [_, port] : copy.inputs) {
                port.autofills = std::nullopt;
              }
              for (auto& [_, port] : copy.outputs) {
                port.autofills = std::nullopt;
              }
            }
            return copy;
          },
          py::arg_v("clear_autofills", false))
      .def(
          "map_outputs_to_json_fields",
          [](ActionSchema& self, py::handle map_or_none = py::none())
              -> absl::StatusOr<std::reference_wrapper<ActionSchema>> {
            try {
              py::dict map;
              if (!map_or_none.is_none()) {
                map = py::cast<py::dict>(map_or_none);
              }
              for (const auto& [output_name, json_field] : map) {
                RETURN_IF_ERROR(
                    self.MapOutputToJsonField(output_name.cast<std::string>(),
                                              json_field.cast<std::string>()));
              }
              return std::ref(self);
            } catch (const py::cast_error& e) {
              return absl::InvalidArgumentError(absl::StrCat(
                  "Failed to cast map to Dict[str, str]: ", e.what()));
            }
          },
          py::arg_v("map", py::none()))
      .def(
          "get_json_field_for_output",
          [](const ActionSchema& self,
             std::string_view output_name) -> std::optional<std::string> {
            return self.GetJsonFieldForOutput(output_name);
          },
          py::arg("output_name"))
      .def("clear_json_field_mapping", &ActionSchema::ClearJsonFieldMapping)
      .def(
          "set_python_type_for_port",
          [](ActionSchema& self, std::string_view port, py::type type) {
            if (self.user_data == nullptr) {
              self.user_data =
                  std::make_shared<DestroyUnderGilGuard>(py::dict());
            }
            const DestroyUnderGilGuard* dict =
                static_cast<DestroyUnderGilGuard*>(self.user_data.get());
            dict->Get()[py::str(std::string(port))] = type;
          },
          py::arg("port"), py::arg("type"))
      .def("get_python_type_for_port",
           [](const ActionSchema& self, std::string_view port) -> py::object {
             if (self.user_data == nullptr) {
               return py::none();
             }
             const DestroyUnderGilGuard* dict =
                 static_cast<DestroyUnderGilGuard*>(self.user_data.get());
             if (dict->Get().contains(py::str(std::string(port)))) {
               return dict->Get()[py::str(std::string(port))];
             }
             return py::none();
           })
      .def("__getitem__",
           [](ActionSchema& self, std::string_view key)
               -> absl::StatusOr<std::reference_wrapper<ActionPortSchema>> {
             if (self.HasInput(key)) {
               return std::ref(self.inputs[key]);
             }
             if (self.HasOutput(key)) {
               return std::ref(self.outputs[key]);
             }
             return absl::NotFoundError(
                 absl::StrCat("Port '", key, "' not found."));
           })
      .def_readwrite("name", &ActionSchema::name)
      .def(
          "input",
          [](ActionSchema& self, std::string_view name)
              -> absl::StatusOr<std::reference_wrapper<ActionPortSchema>> {
            const auto it = self.inputs.find(name);
            if (it == self.inputs.end()) {
              return absl::NotFoundError(
                  absl::StrCat("Input port '", name, "' not found."));
            }
            return std::ref(it->second);
          },
          py::return_value_policy::reference_internal)
      .def("inputs",
           [](const ActionSchema& self) -> py::list {
             py::list list;
             for (const auto& [key, value] : self.inputs) {
               list.append(py::str(key));
             }
             return list;
           })
      .def(
          "output",
          [](ActionSchema& self, std::string_view name)
              -> absl::StatusOr<std::reference_wrapper<ActionPortSchema>> {
            const auto it = self.outputs.find(name);
            if (it == self.outputs.end()) {
              return absl::NotFoundError(
                  absl::StrCat("Output port '", name, "' not found."));
            }
            return std::ref(it->second);
          },
          py::return_value_policy::reference_internal)
      .def("outputs",
           [](const ActionSchema& self) -> py::list {
             py::list list;
             for (const auto& [key, value] : self.outputs) {
               list.append(py::str(key));
             }
             return list;
           })
      .def_readwrite("description", &ActionSchema::description)
      .def("__repr__",
           [](const ActionSchema& def) { return absl::StrCat(def); })
      .doc() = "An action schema.";
}

void BindActionRegistry(py::handle scope, std::string_view name) {
  py::classh<ActionRegistry>(scope, std::string(name).c_str())
      .def(py::init([]() { return std::make_shared<ActionRegistry>(); }))
      .def(MakeSameObjectRefConstructor<ActionRegistry>())
      .def(
          "copy",
          [](std::shared_ptr<ActionRegistry>& self, bool clear_autofills) {
            auto copy = std::make_shared<ActionRegistry>();
            const std::vector<std::string> registered_actions =
                self->ListRegisteredActions();
            for (const auto& action_name : registered_actions) {
              absl::StatusOr<std::reference_wrapper<ActionSchema>> schema_ref =
                  self->GetSchema(action_name);
              absl::StatusOr<std::reference_wrapper<ActionHandler>>
                  handler_ref = self->GetHandler(action_name);

              ActionSchema schema;
              ActionHandler handler;
              if (schema_ref.ok()) {
                schema = *schema_ref;
                if (clear_autofills) {
                  for (auto& [_, port] : schema.inputs) {
                    port.autofills = std::nullopt;
                  }
                  for (auto& [_, port] : schema.outputs) {
                    port.autofills = std::nullopt;
                  }
                }
              }
              if (handler_ref.ok()) {
                handler = handler_ref.value();
              }
              copy->Register(action_name, std::move(schema),
                             std::move(handler));
            }
            return copy;
          },
          py::arg_v("clear_autofills", true))
      .def(
          "register",
          [](const std::shared_ptr<ActionRegistry>& self, std::string_view name,
             const ActionSchema& def, py::handle handler) -> absl::Status {
            ASSIGN_OR_RETURN(ActionHandler cpp_handler,
                             MakeSimpleActionHandler(handler));
            self->Register(name, def, std::move(cpp_handler));
            return absl::OkStatus();
          },
          py::arg("name"), py::arg("definition"), py::arg("handler"))
      .def(
          "make_action_message",
          [](const std::shared_ptr<ActionRegistry>& self, std::string_view name,
             std::string_view action_id) -> absl::StatusOr<ActionMessage> {
            return self->MakeActionMessage(name, action_id);
          },
          py::arg("name"), py::arg("action_id"),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "make_action",
          [](const std::shared_ptr<ActionRegistry>& self, std::string_view name,
             std::string_view id, const std::shared_ptr<NodeMap>& node_map,
             const std::shared_ptr<WireStream>& stream,
             const std::shared_ptr<Session>& session)
              -> absl::StatusOr<std::shared_ptr<Action>> {
            ASSIGN_OR_RETURN(std::unique_ptr<Action> action,
                             self->MakeAction(name, id));
            action->mutable_bound_resources()->set_node_map(node_map);
            action->mutable_bound_resources()->set_stream(stream);
            action->mutable_bound_resources()->set_session(session);
            return std::shared_ptr(std::move(action));
          },
          py::arg("name"), py::arg_v("action_id", ""),
          py::arg_v("node_map", nullptr), py::arg_v("stream", nullptr),
          py::arg_v("session", nullptr), py::keep_alive<0, 4>(),
          py::keep_alive<0, 5>(), py::keep_alive<0, 6>(),
          pybindings::keep_event_loop_memo())
      .def(
          "is_registered",
          [](const std::shared_ptr<ActionRegistry>& self,
             std::string_view action_name) {
            return self->IsRegistered(action_name);
          },
          py::arg("name"))
      .def(
          "get_schema",
          [](const std::shared_ptr<ActionRegistry>& self,
             std::string_view action_name)
              -> absl::StatusOr<std::reference_wrapper<ActionSchema>> {
            ASSIGN_OR_RETURN(ActionSchema & schema,
                             self->GetSchema(action_name));
            return schema;
          },
          py::arg("name"), py::return_value_policy::reference)
      .def("list_registered_actions",
           [](const std::shared_ptr<ActionRegistry>& self) {
             return self->ListRegisteredActions();
           })
      .doc() = "Registry for action schemas and handlers.";
}

void BindAction(py::handle scope, std::string_view name) {
  auto cls =
      py::classh<Action>(scope, std::string(name).c_str(),
                         py::release_gil_before_calling_cpp_dtor())
          .def(py::init(
                   [](const std::shared_ptr<Action>& other) { return other; }),
               py::keep_alive<0, 1>())
          .def(py::init([](ActionSchema schema, std::string_view id = "") {
                 auto action = std::make_shared<Action>(
                     !id.empty() ? id : GenerateUUID4());
                 action->set_schema(std::move(schema));
                 action->mutable_bound_resources()->set_node_map(
                     std::make_shared<NodeMap>());
                 return action;
               }),
               py::arg(), py::arg_v("id", ""));
  cls.def(
         "run",
         [](const std::shared_ptr<Action>& action,
            double timeout = -1.0) -> absl::StatusOr<std::shared_ptr<Action>> {
           absl::Duration timeout_duration = absl::InfiniteDuration();
           if (timeout >= 0.0) {
             timeout_duration = absl::Seconds(timeout);
           }
           RETURN_IF_ERROR(action->Run(timeout_duration));
           return action;
         },
         pybindings::keep_event_loop_memo())
      .def(
          "run_in_background",
          [](const std::shared_ptr<Action>& action)
              -> absl::StatusOr<std::shared_ptr<Action>> {
            RETURN_IF_ERROR(action->RunInBackground(/*detach=*/true));
            return action;
          },
          pybindings::keep_event_loop_memo())
      .def(
          "call",
          [](const std::shared_ptr<Action>& action, py::handle headers_obj) {
            absl::flat_hash_map<std::string, std::string> headers;
            if (!headers_obj.is_none()) {
              try {
                headers =
                    py::cast<absl::flat_hash_map<std::string, std::string>>(
                        headers_obj);
              } catch (const py::cast_error& e) {
                return absl::InvalidArgumentError(
                    absl::StrCat("Failed to cast headers to "
                                 "Dict[str, str]: ",
                                 e.what()));
              }
            }
            {
              py::gil_scoped_release release;
              return action->Call(std::move(headers));
            }
          },
          py::arg("headers") = py::none())
      .def(
          "call_and_wait_for_dispatch_status",
          [](const std::shared_ptr<Action>& action,
             py::handle headers_obj) -> absl::StatusOr<absl::Status> {
            return absl::UnimplementedError("not implemented.");
            absl::flat_hash_map<std::string, std::string> headers;
            if (!headers_obj.is_none()) {
              try {
                headers =
                    py::cast<absl::flat_hash_map<std::string, std::string>>(
                        headers_obj);
              } catch (const py::cast_error& e) {
                return absl::InvalidArgumentError(
                    absl::StrCat("Failed to cast headers to "
                                 "Dict[str, str]: ",
                                 e.what()));
              }
            }
            absl::StatusOr<absl::Status> dispatch_status_or;
            {
              py::gil_scoped_release release;
              // dispatch_status_or =
              //     action->CallAndWaitForDispatchStatus(std::move(headers));
            }
            if (!dispatch_status_or.ok()) {
              return dispatch_status_or.status();
            }
            return *dispatch_status_or;
          },
          py::arg("headers") = py::none())
      .def("get_future",
           [](const std::shared_ptr<Action>& action) {
             const std::shared_ptr<PyUserData> user_data =
                 EnsurePyUserData(action.get());
             return user_data->asyncio_future;
           })
      .def(
          "clear_inputs_after_run",
          [](const std::shared_ptr<Action>& self, bool clear) {
            self->mutable_settings()->clear_inputs_after_run = clear;
          },
          py::arg_v("clear", true))
      .def(
          "clear_outputs_after_run",
          [](const std::shared_ptr<Action>& self, bool clear) {
            self->mutable_settings()->clear_outputs_after_run = clear;
          },
          py::arg_v("clear", true))
      .def("cancel",
           [](const std::shared_ptr<Action>& self) {
             const auto user_data = EnsurePyUserData(self.get());
             py::gil_scoped_release release_gil;
             return self->Cancel();
           })
      .def("cancelled", &Action::HasBeenCancelled,
           py::call_guard<py::gil_scoped_release>())
      .def(
          "get_action_message",
          [](const std::shared_ptr<Action>& action)
              -> absl::StatusOr<ActionMessage> {
            return action->GetActionMessage();
          },
          py::call_guard<py::gil_scoped_release>())
      .def("get_registry",
           [](const std::shared_ptr<Action>& action) {
             return action->bound_resources().borrow_registry();
           })
      .def("get_session",
           [](const std::shared_ptr<Action>& action) {
             return action->bound_resources().borrow_session();
           })
      .def("get_node_map",
           [](const std::shared_ptr<Action>& action) {
             return action->bound_resources().borrow_node_map();
           })
      .def("get_stream",
           [](const std::shared_ptr<Action>& action) {
             return action->bound_resources().borrow_stream();
           })
      .def("get_id", &Action::id)
      .def(
          "get_schema",
          [](const std::shared_ptr<Action>& action) -> ActionSchema {
            return action->schema();
          },
          py::return_value_policy::copy)
      .def(
          "get_node",
          [](const std::shared_ptr<Action>& action, std::string_view id) {
            return ShareWithNoDeleter<AsyncNode>(nullptr);
          },
          py::arg("node_id"), py::call_guard<py::gil_scoped_release>())
      .def(
          "get_input",
          [](const std::shared_ptr<Action>& action, std::string_view id,
             const std::optional<bool>& bind_stream) {
            return ShareWithNoDeleter(action->GetInput(id, bind_stream));
          },
          py::arg("name"), py::arg_v("bind_stream", py::none()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "get_output",
          [](const std::shared_ptr<Action>& action, std::string_view id,
             const std::optional<bool>& bind_stream) {
            return ShareWithNoDeleter(action->GetOutput(id, bind_stream));
          },
          py::arg("name"), py::arg_v("bind_stream", py::none()),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "make_action_in_same_session",
          [](const std::shared_ptr<Action>& action, std::string_view name,
             std::string_view id) -> absl::StatusOr<std::unique_ptr<Action>> {
            return action->MakeActionInSameSession(name, id);
          },
          py::arg("name"), py::arg_v("action_id", ""),
          py::call_guard<py::gil_scoped_release>())
      .def(
          "bind_handler",
          [](const std::shared_ptr<Action>& self,
             py::function handler) -> absl::Status {
            ASSIGN_OR_RETURN(auto cpp_handler,
                             MakeSimpleActionHandler(handler));
            self->set_handler(std::move(cpp_handler));
            return absl::OkStatus();
          },
          py::arg("handler"))
      .def(
          "bind_streams_on_inputs_by_default",
          [](const std::shared_ptr<Action>& self, bool bind) {
            self->mutable_settings()->bind_streams_on_inputs_by_default = bind;
          },
          py::arg("bind"))
      .def(
          "bind_streams_on_outputs_by_default",
          [](const std::shared_ptr<Action>& self, bool bind) {
            self->mutable_settings()->bind_streams_on_outputs_by_default = bind;
          },
          py::arg("bind"))
      .def(
          "bind_node_map",
          [](const std::shared_ptr<Action>& self,
             std::shared_ptr<NodeMap> node_map) {
            self->mutable_bound_resources()->set_node_map(std::move(node_map));
          },
          py::arg("node_map"))
      .def("bind_registry",
           [](const std::shared_ptr<Action>& self,
              const std::shared_ptr<ActionRegistry>& registry) {
             self->mutable_bound_resources()->set_registry(registry);
           })
      .def("headers",
           [](const std::shared_ptr<Action>& self) {
             return py::make_key_iterator(self->headers().begin(),
                                          self->headers().end());
           })
      .def(
          "get_header",
          [](const std::shared_ptr<Action>& self, std::string_view key,
             bool decode = false) -> std::optional<py::object> {
            const std::optional<std::string> header = self->get_header(key);
            if (!header) {
              return std::nullopt;
            }

            py::object value = py::bytes(std::string(*header));
            if (decode) {
              value =
                  py::cast<py::str>(value.attr("decode")("utf-8", "strict"));
            }
            return value;
          },
          py::arg("key"), py::arg_v("decode", false))
      .def(
          "set_header",
          [](const std::shared_ptr<Action>& self, py::handle py_key,
             py::handle py_value) -> absl::StatusOr<std::shared_ptr<Action>> {
            std::string key, value;
            try {
              const auto py_key_str = py::cast<py::str>(py_key);
              key = py_key_str.attr("encode")("utf-8").cast<std::string>();
            } catch (const py::cast_error& e) {
              return absl::InvalidArgumentError(
                  absl::StrCat("Failed to cast header key to str: ", e.what()));
            }

            py::bytes py_value_bytes;
            if (py::isinstance<py::bytes>(py_value)) {
              py_value_bytes = py::cast<py::bytes>(py_value);
            } else if (py::isinstance<py::str>(py_value)) {
              const auto py_value_str = py::cast<py::str>(py_value);
              py_value_bytes = py::bytes(
                  py_value_str.attr("encode")("utf-8").cast<std::string>());
            } else {
              return absl::InvalidArgumentError(
                  "Header value must be either bytes or str.");
            }
            value = std::string(py_value_bytes);

            self->set_header(key, value);
            return self;
          },
          py::arg("key"), py::arg("value"))
      .def(
          "remove_header",
          [](const std::shared_ptr<Action>& self, std::string_view key) {
            return self->remove_header(key);
          },
          py::arg("key"));
}

py::module_ MakeActionsModule(py::module_ scope, std::string_view module_name) {
  py::module_ actions = scope.def_submodule(std::string(module_name).c_str(),
                                            "ActionEngine Actions interface.");

  BindActionPortSchema(actions, "ActionPortSchema");
  BindActionSchema(actions, "ActionSchema");
  BindActionRegistry(actions, "ActionRegistry");
  BindAction(actions, "Action");

  return actions;
}

}  // namespace act::pybindings
