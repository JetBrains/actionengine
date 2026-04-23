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

#include "actionengine/actions/schema.h"

#include <absl/log/check.h>
#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_cat.h>

#include "actionengine/data/types.h"
#include "boost/json/array.hpp"
#include "boost/json/object.hpp"
#include "boost/json/parse.hpp"
#include "boost/json/string.hpp"
#include "boost/json/value.hpp"

namespace act {

absl::StatusOr<boost::json::value> ParseJson(std::string_view serialized_json) {
  boost::system::error_code ec;
  boost::json::value json_value = boost::json::parse(serialized_json, ec);
  if (ec) {
    return absl::InternalError(
        absl::StrCat("Failed to parse JSON: ", ec.message()));
  }
  return json_value;
}

absl::StatusOr<ActionMessage> ActionSchema::GetActionMessage(
    std::string_view action_id) const {
  if (action_id.empty()) {
    return absl::InvalidArgumentError(
        "Action ID cannot be empty to create a message");
  }

  std::vector<Port> input_parameters;
  input_parameters.reserve(inputs.size());
  for (const auto& [input_name, _] : inputs) {
    input_parameters.push_back(Port{
        .name = input_name,
        .id = absl::StrCat(action_id, "#", input_name),
    });
  }

  std::vector<Port> output_parameters;
  output_parameters.reserve(outputs.size());
  for (const auto& [output_name, _] : outputs) {
    output_parameters.push_back(Port{
        .name = output_name,
        .id = absl::StrCat(action_id, "#", output_name),
    });
  }

  return ActionMessage{
      .id = std::string(action_id),
      .name = name,
      .inputs = input_parameters,
      .outputs = output_parameters,
  };
}

absl::Status ActionSchema::MapOutputToJsonField(std::string_view output_name,
                                                std::string_view json_field) {
  if (!HasOutput(output_name)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Output '%s' does not exist in the action schema.", output_name));
  }
  if (json_field == "$") {
    for (const auto& [output, field] : output_to_json_field) {
      if (field == "$" && output != output_name) {
        return absl::InvalidArgumentError(
            absl::StrFormat("Cannot map output '%s' to '$' because '%s' "
                            "already maps to the whole body.",
                            output_name, output));
      }
    }
  }
  output_to_json_field[output_name] = json_field;
  return absl::OkStatus();
}

std::optional<std::string> ActionSchema::GetJsonFieldForOutput(
    std::string_view output_name) const {
  // Map single-output ports to the whole body
  if (outputs.size() == 1 && output_to_json_field.empty() &&
      HasOutput(output_name)) {
    return "$";
  }
  if (const auto it = output_to_json_field.find(output_name);
      it != output_to_json_field.end()) {
    return it->second;
  }
  return std::nullopt;
}

}  // namespace act