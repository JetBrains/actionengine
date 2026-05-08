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

#include "actionengine/util/utils_pybind11.h"

#include <string_view>
#include <utility>

#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <pybind11/cast.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11_abseil/absl_casters.h>
#include <pybind11_abseil/status_caster.h>
#include <pybind11_abseil/statusor_caster.h>

namespace act::pybindings {
namespace py = ::pybind11;

py::object& GetGloballySavedEventLoop() {
  static py::object* global_event_loop_object = new py::none();
  return *global_event_loop_object;
}

void SaveEventLoopGlobally(const py::object& loop) {
  py::object current_loop = loop;
  if (current_loop.is_none()) {
    try {
      current_loop = py::module_::import("asyncio").attr("get_running_loop")();
    } catch (py::error_already_set&) {
      // No event loop was found.
    }
  }
  GetGloballySavedEventLoop() = current_loop;
}

void SaveFirstEncounteredEventLoop() {
  if (py::object& global_event_loop_object = GetGloballySavedEventLoop();
      global_event_loop_object.is_none()) {
    try {
      global_event_loop_object =
          py::module_::import("asyncio").attr("get_running_loop")();
    } catch (py::error_already_set&) {
      // No event loop was found.
    }
  }
}

absl::StatusOr<py::object> RunThreadsafeIfCoroutine(
    py::object function_call_result, py::object loop, bool return_future) {
  if (const py::function iscoroutine =
          py::module_::import("inspect").attr("iscoroutine");
      !iscoroutine(function_call_result)) {
    return function_call_result;
  }

  py::object resolved_loop = std::move(loop);

  if (resolved_loop.is_none()) {
    resolved_loop = GetGloballySavedEventLoop();
  }

  if (resolved_loop.is_none()) {
    return absl::FailedPreconditionError(
        "No asyncio loop was explicitly provided or could be deduced from "
        "previous library calls. Please provide an asyncio loop explicitly.");
  }

  py::object running_loop = py::none();
  try {
    running_loop = py::module_::import("asyncio").attr("get_running_loop")();
    if (const py::function equals = py::module_::import("operator").attr("eq");
        equals(running_loop, resolved_loop)) {
      return absl::FailedPreconditionError(
          "Target event loop resolves to the current thread's loop, which is "
          "not an intended use for asyncio.run_coroutine_threadsafe. Please "
          "provide a different event loop if you intend to use this "
          "function from an async context, or use it from a sync context.");
    }
  } catch (py::error_already_set&) {}

  const py::function run_coroutine_threadsafe =
      py::module_::import("asyncio").attr("run_coroutine_threadsafe");
  const py::object future =
      run_coroutine_threadsafe(function_call_result, resolved_loop);
  if (return_future) {
    return future;
  }

  try {
    return future.attr("result")();
  } catch (py::error_already_set& e) {
    return absl::InternalError(absl::StrCat(e.what()));
  }
}

static PyObject* MakePyException(PyObject* exception_type,
                                 absl::string_view message) {
  return PyObject_CallFunction(exception_type, "s#", message.data(),
                               static_cast<Py_ssize_t>(message.size()));
}

static bool MatchesImportedException(const py::error_already_set& exc,
                                     std::string_view module_name,
                                     std::string_view exception_name) {
  try {
    py::object exception_type =
        py::module_::import(std::string(module_name).c_str())
            .attr(std::string(exception_name).c_str());
    return exc.matches(exception_type.ptr());
  } catch (const py::error_already_set&) {
    return false;
  }
}

PyObject* StatusToPyException(const absl::Status& status) {
  if (status.ok()) {
    return py::none().release().ptr();
  }

  switch (status.code()) {
    case absl::StatusCode::kOk:
      return py::none().release().ptr();

    case absl::StatusCode::kCancelled:
      return MakePyException(PyExc_KeyboardInterrupt, status.message());

    case absl::StatusCode::kUnknown:
      return MakePyException(PyExc_Exception, status.message());

    case absl::StatusCode::kInvalidArgument:
      return MakePyException(PyExc_ValueError, status.message());

    case absl::StatusCode::kDeadlineExceeded:
      return MakePyException(PyExc_TimeoutError, status.message());

    case absl::StatusCode::kNotFound:
      return MakePyException(PyExc_LookupError, status.message());

    case absl::StatusCode::kAlreadyExists:
      return MakePyException(PyExc_FileExistsError, status.message());

    case absl::StatusCode::kPermissionDenied:
      return MakePyException(PyExc_PermissionError, status.message());

    case absl::StatusCode::kResourceExhausted:
      return MakePyException(PyExc_MemoryError, status.message());

    case absl::StatusCode::kFailedPrecondition:
      return MakePyException(PyExc_RuntimeError, status.message());

    case absl::StatusCode::kAborted:
      return MakePyException(PyExc_InterruptedError, status.message());

    case absl::StatusCode::kOutOfRange:
      return MakePyException(PyExc_IndexError, status.message());

    case absl::StatusCode::kUnimplemented:
      return MakePyException(PyExc_NotImplementedError, status.message());

    case absl::StatusCode::kInternal:
      return MakePyException(PyExc_RuntimeError, status.message());

    case absl::StatusCode::kUnavailable:
      return MakePyException(PyExc_ConnectionError, status.message());

    case absl::StatusCode::kDataLoss:
      return MakePyException(PyExc_EOFError, status.message());

    case absl::StatusCode::kUnauthenticated:
      return MakePyException(PyExc_PermissionError, status.message());

    default:
      return MakePyException(PyExc_RuntimeError, status.message());
  }

  return MakePyException(PyExc_RuntimeError, status.message());
}

absl::Status PyExceptionToStatus(const py::error_already_set& exc) {
  const std::string message = exc.what();

  if (MatchesImportedException(exc, "asyncio", "CancelledError") ||
      exc.matches(PyExc_KeyboardInterrupt)) {
    return absl::CancelledError(message);
  }

  if (exc.matches(PyExc_TimeoutError)) {
    return absl::DeadlineExceededError(message);
  }

  if (exc.matches(PyExc_FileNotFoundError) ||
      exc.matches(PyExc_ModuleNotFoundError) ||
      exc.matches(PyExc_ProcessLookupError)) {
    return absl::NotFoundError(message);
  }

  if (exc.matches(PyExc_KeyError) || exc.matches(PyExc_IndexError)) {
    return absl::NotFoundError(message);
  }

  if (exc.matches(PyExc_FileExistsError)) {
    return absl::AlreadyExistsError(message);
  }

  if (exc.matches(PyExc_PermissionError)) {
    return absl::PermissionDeniedError(message);
  }

  if (exc.matches(PyExc_MemoryError) || exc.matches(PyExc_BufferError) ||
      exc.matches(PyExc_OverflowError) || exc.matches(PyExc_RecursionError)) {
    return absl::ResourceExhaustedError(message);
  }

  if (exc.matches(PyExc_ValueError) || exc.matches(PyExc_TypeError) ||
      exc.matches(PyExc_AttributeError) || exc.matches(PyExc_SyntaxError) ||
      exc.matches(PyExc_IndentationError) || exc.matches(PyExc_TabError)) {
    return absl::InvalidArgumentError(message);
  }

  if (exc.matches(PyExc_NotImplementedError)) {
    return absl::UnimplementedError(message);
  }

  if (exc.matches(PyExc_StopIteration) ||
      exc.matches(PyExc_StopAsyncIteration)) {
    return absl::OutOfRangeError(message);
  }

  if (exc.matches(PyExc_BlockingIOError) ||
      exc.matches(PyExc_IsADirectoryError) ||
      exc.matches(PyExc_NotADirectoryError) ||
      exc.matches(PyExc_AssertionError)) {
    return absl::FailedPreconditionError(message);
  }

  if (exc.matches(PyExc_InterruptedError) ||
      exc.matches(PyExc_ChildProcessError)) {
    return absl::AbortedError(message);
  }

  if (exc.matches(PyExc_ConnectionError) ||
      exc.matches(PyExc_BrokenPipeError) ||
      exc.matches(PyExc_ConnectionAbortedError) ||
      exc.matches(PyExc_ConnectionRefusedError) ||
      exc.matches(PyExc_ConnectionResetError)) {
    return absl::UnavailableError(message);
  }

  if (exc.matches(PyExc_EOFError) || exc.matches(PyExc_UnicodeError) ||
      exc.matches(PyExc_UnicodeDecodeError) ||
      exc.matches(PyExc_UnicodeEncodeError) ||
      exc.matches(PyExc_UnicodeTranslateError)) {
    return absl::DataLossError(message);
  }

  if (exc.matches(PyExc_SystemExit) || exc.matches(PyExc_GeneratorExit)) {
    return absl::CancelledError(message);
  }

  if (exc.matches(PyExc_ImportError)) {
    return absl::NotFoundError(message);
  }

  if (exc.matches(PyExc_NameError) || exc.matches(PyExc_UnboundLocalError)) {
    return absl::FailedPreconditionError(message);
  }

  if (exc.matches(PyExc_OSError)) {
    return absl::InternalError(message);
  }

  return absl::InternalError(message);
}

absl::Status SetAsyncioFutureResult(py::handle future, absl::Status status) {
  try {
    future.attr("get_loop")().attr("call_soon_threadsafe")(
        py::cpp_function([status = std::move(status),
                          future = py::cast<py::object>(future)]() mutable {
          if (future.attr("done")().cast<bool>()) {
            future = py::object();
            return;
          }
          if (!status.ok()) {
            future.attr("set_exception")(
                StatusToPyException(std::move(status)));
            future = py::object();
            return;
          }
          future.attr("set_result")(py::none());
          future = py::object();
        }));
    return absl::OkStatus();
  } catch (const py::error_already_set& exc) {
    return PyExceptionToStatus(exc);
  }
}

}  // namespace act::pybindings
