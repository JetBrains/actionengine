# Copyright 2026 The Action Engine Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
from collections import defaultdict
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

import actionengine.data  # noqa
import actionengine.logging
from actionengine.actions import (
    ActionSchema,
    ActionRegistry,
)
from actionengine.async_node import AsyncNode

ALLOWED_TOOLS_HEADER = "x-ae-allowed-tools"
LLM_HEADER = "x-ae-llm"
LLM_API_KEY_HEADER = "x-ae-llm-api-key"

_LOGGER = actionengine.logging.get_logger()


class LLMToolInputProperty(BaseModel):
    type: str = Field(description="The type of the property.")
    description: str = Field(
        default="", description="A description of the property."
    )
    enum: list[str] | None = Field(
        default=None,
        description="A list of allowed values for the property.",
        exclude_if=lambda x: x is None,
    )
    properties: dict[str, "LLMToolInputProperty"] | None = Field(
        default=None,
        description="A dictionary of nested properties.",
        exclude_if=lambda x: x is None,
    )


class LLMToolInputSchema(BaseModel):
    type: Literal["object"] = Field(
        default="object", description="The type of the input schema."
    )
    properties: dict[str, LLMToolInputProperty] = Field(
        description="A dictionary of properties and their types.",
        default_factory=dict,
    )


def _ae_type_to_json_schema_type(
    mimetype: str, ae_type: str | type = None
) -> str:
    if mimetype == "" and ae_type is None:
        return "null"

    if ae_type is None:
        if mimetype.startswith("text/"):
            return "string"
        if mimetype.startswith("image/"):
            return "string"
        if mimetype.startswith("application/json"):
            return "object"
        if mimetype.startswith("application/octet-stream"):
            return "string"
        if mimetype.startswith("application/x-msgpack"):
            return "object"
        if mimetype.startswith("application/x-protobuf"):
            return "object"
        return "string"

    if ae_type is dict:
        return "object"

    if ae_type is list or ae_type is tuple:
        return "array"

    if ae_type is str:
        return "string"

    if ae_type is int:
        return "integer"

    if ae_type is float:
        return "number"

    if ae_type is bool:
        return "boolean"

    if isinstance(ae_type, type) and issubclass(ae_type, BaseModel):
        return "object"

    if not isinstance(ae_type, str):
        return "object"

    return "object"


class LLMToolSchema(BaseModel):
    @staticmethod
    def from_action_schema(action_schema: ActionSchema):
        name = action_schema.name
        description = action_schema.description
        input_schema = {"type": "object", "properties": dict()}
        required = []

        for input_name in action_schema.inputs():
            port = action_schema.input(input_name)
            if port.autofills is not None:
                continue

            if port.required:
                required.append(input_name)
            python_type = action_schema.get_python_type_for_port(input_name)
            input_schema["properties"][input_name] = {
                "type": "array",
                "description": port.description,
            }
            if port.unary:
                input_schema["properties"][input_name]["maxItems"] = 1
            if port.required:
                input_schema["properties"][input_name]["minItems"] = 1
            if isinstance(python_type, type) and issubclass(
                python_type, BaseModel
            ):
                input_schema["properties"][input_name][
                    "items"
                ] = python_type.model_json_schema()

            if port.type.startswith("text/"):
                input_schema["properties"][input_name]["items"] = {
                    "type": "string",
                    "format": "text",
                }

        return LLMToolSchema(
            name=name,
            description=description,
            input_schema=input_schema,
            required=required,
        )

    name: str = Field(description="The name of the tool.")
    description: str = Field(description="A description of the tool.")
    eager_input_streaming: bool = Field(
        default=True,
        description="Whether the tool supports eager input streaming.",
    )
    input_schema: dict[str, Any] = Field(
        description="The input schema for the tool."
    )
    required: list[str] = Field(
        default_factory=list,
        description="A list of required fields in the input schema.",
        exclude_if=lambda x: not x,
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)


class LLMTool:
    _action_schema: ActionSchema
    _tool_schema: LLMToolSchema

    def __init__(self, schema: ActionSchema):
        self._action_schema = schema
        self._tool_schema = LLMToolSchema.from_action_schema(schema)

    def get_schema(self) -> LLMToolSchema:
        schema = self._tool_schema.model_copy(deep=True)
        schema.description = self._action_schema.description
        for input_name in self._action_schema.inputs():
            input_port = self._action_schema.input(input_name)
            # if autofills are set, the input should be excluded from the
            # tool schema.
            if input_port.autofills is not None:
                if input_name in schema.input_schema["properties"]:
                    schema.input_schema["properties"].pop(input_name)
        return schema

    def map_output_to_field(self, output_name: str, field_name: str):
        self._action_schema.map_outputs_to_json_fields(
            {output_name: field_name}
        )

    def _reduce_chunked_output(
        self, output_name: str, chunked_output: list[Any]
    ):
        if output_name not in self._action_schema.outputs():
            raise ValueError(
                f"Output {output_name} not found in action schema."
            )

        if len(chunked_output) == 0:
            return []

        if len(chunked_output) == 1:
            return chunked_output[0]

        output_type = self._action_schema.get_python_type_for_port(output_name)
        if (
            output_type is str
            or isinstance(output_type, str)
            and output_type.startswith("text/")
        ):
            return "".join(chunked_output)
        if (
            output_type is bytes
            or isinstance(output_type, str)
            and output_type.startswith("application/octet-stream")
        ):
            return b"".join(chunked_output)
        return chunked_output

    @staticmethod
    async def _read_output(
        node: AsyncNode,
        destination: list[Any],
        timeout: float = -1.0,
    ):
        node.set_reader_options(timeout=timeout)
        async for piece in node:
            destination.append(piece)

    async def run(
        self,
        input_dict: dict[str, Any] | None = None,
        registry: ActionRegistry | None = None,
        timeout: float = -1.0,
        headers: dict[str, str | bytes] = None,
    ):
        input_dict = input_dict or dict()
        try:
            return await self._run(input_dict, registry, timeout, headers)
        finally:
            pass

    async def _run(
        self,
        input_dict: dict[str, Any],
        registry: ActionRegistry,
        timeout: float = -1.0,
        headers: dict[str, str | bytes] = None,
    ):
        started_at = time.perf_counter()

        all_input_names = set(self._action_schema.inputs())

        for provided_name in input_dict:
            if provided_name not in all_input_names:
                raise ValueError(
                    f"Input field {provided_name} not found in action schema."
                )
            if self._action_schema.input(provided_name).autofills is not None:
                raise ValueError(
                    f"The input field {provided_name} is not allowed to be filled."
                )

        resolvable_input_names = set(input_dict.keys())
        for input_name in all_input_names:
            input_port = self._action_schema.input(input_name)
            if input_port.autofills is not None:
                resolvable_input_names.add(input_name)
        non_resolvable_input_names = all_input_names - resolvable_input_names
        if non_resolvable_input_names:
            raise ValueError(
                f"The input fields {non_resolvable_input_names} are not "
                f"resolvable: neither found in the input dict nor in the "
                f"action schema autofills."
            )
        if len(resolvable_input_names) > len(all_input_names):
            excess_input_names = resolvable_input_names - all_input_names
            raise ValueError(f"Excess input fields: {excess_input_names}.")

        action = registry.make_action(self._action_schema.name).bind_registry(
            registry
        )

        headers = headers or dict()
        for header_name, header_value in headers.items():
            action.set_header(header_name, header_value)
        action.run()

        for input_name in self._action_schema.inputs():
            input_port = self._action_schema.input(input_name)
            if input_port.autofills is not None:
                for chunk in input_port.autofills:
                    await action.get_input(input_name).put(chunk)
                await action.get_input(input_name).finalize()
            else:
                if not isinstance(input_dict[input_name], (list, tuple)):
                    input_dict[input_name] = [input_dict[input_name]]
                for chunk in input_dict[input_name]:
                    await action.get_input(input_name).put(chunk)
                await action.get_input(input_name).finalize()

        output_lists = defaultdict(list)

        timeout_left = (
            -1.0
            if timeout == -1.0
            else timeout - (time.perf_counter() - started_at)
        )
        await action.wait_until_complete(timeout_left)

        at_least_one_output_mapped = False
        for output_name in self._action_schema.outputs():
            field_name = self._action_schema.get_json_field_for_output(
                output_name
            )
            if field_name is not None:
                at_least_one_output_mapped = True
                break

        for output_name in self._action_schema.outputs():
            field_name = self._action_schema.get_json_field_for_output(
                output_name
            )
            # if no mapping is specified, outputs will be mapped to fields
            # with the same name as the output.
            if field_name is None:
                if at_least_one_output_mapped:
                    continue
                else:
                    field_name = output_name

            timeout_left = (
                -1.0
                if timeout == -1.0
                else timeout - (time.perf_counter() - started_at)
            )
            await LLMTool._read_output(
                action.get_output(output_name),
                output_lists[field_name],
                timeout_left,
            )

        output_values = dict()
        for output_name in self._action_schema.outputs():
            field_name = self._action_schema.get_json_field_for_output(
                output_name
            )
            if field_name is None:
                if at_least_one_output_mapped:
                    continue
                else:
                    field_name = output_name

            if field_name == "$":
                return self._reduce_chunked_output(
                    output_name, output_lists[field_name]
                )
            output_values[field_name] = self._reduce_chunked_output(
                output_name, output_lists[field_name]
            )

        return output_values
