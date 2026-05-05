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

import asyncio
from typing import Sequence

from actionengine import actions
from actionengine import async_node
from actionengine import data
from actionengine.logging import get_logger
from actionengine.sdk import llm_tool

_LOGGER = get_logger()


def set_allowed_tools(action: actions.Action, tool_names: Sequence[str]):
    """
    Set the allowed tool names in the action headers.

    Note: tools must be registered in the action registry bound to the action.

    :param action: The action object.
    :param tool_names: A list of allowed tool names.
    """
    tool_names = [tool.strip() for tool in tool_names]
    action.set_header(llm_tool.ALLOWED_TOOLS_HEADER, ",".join(tool_names))


def get_allowed_tools(
    action: actions.Action,
) -> list[str]:
    """
    Get the allowed tool names from the action headers.

    :param action: The action object.
    :return: A list of allowed tool names.
    """

    header = action.get_header(llm_tool.ALLOWED_TOOLS_HEADER, decode=True)
    if header is None:
        return []
    return [tool.strip() for tool in header.split(",")]


def make_tools(
    registry: actions.ActionRegistry, names: Sequence[str]
) -> dict[str, llm_tool.LLMTool]:
    """
    Create a dictionary of LLMTools from a sequence of tool names, using the
    action registry to ensure tools are registered.

    :param registry: The action registry to use.
    :param names: A list of tool names.
    :return: A dictionary of LLMTools."""

    names = list(names)
    if (
        registry.is_registered("submit_response__")
        and "submit_response__" not in names
    ):
        names.append("submit_response__")

    for tool_name in names:
        if not registry.is_registered(tool_name):
            raise ValueError(
                f"Tool `{tool_name}` is not registered in the action registry."
            )

    tools = dict()
    for tool_name in names:
        schema = registry.get_schema(tool_name)
        tools[tool_name] = llm_tool.LLMTool(schema)

    return tools


def get_llm_and_api_key(action: actions.Action):
    """
    Get the LLM and API key from the action headers.

    :param action: The action object.
    :return: A tuple of (LLM, API key).
    """

    llm = action.get_header(llm_tool.LLM_PROVIDER_HEADER, decode=True)
    api_key = action.get_header(llm_tool.LLM_API_KEY_HEADER, decode=True)

    if not llm:
        raise ValueError(f"LLM header not set on {action.get_schema().name}")
    if api_key is None:
        api_key = ""

    return llm, api_key


def forward_headers(
    src: actions.Action,
    dst: actions.Action,
    headers: Sequence[str] = None,
):
    """
    Forward tool headers from `src` to `dst`.

    :param src: The source action.
    :param dst: The destination action.
    :param headers: Sequence of headers to forward. If None, forwards all
        headers.
    """

    headers = headers or list(src.headers())
    for header in headers:
        dst.set_header(header, src.get_header(header))


def forward_tool_headers(
    src: actions.Action,
    dst: actions.Action,
):
    """
    Forward tool headers from `src` to `dst`, specifically for LLM-related headers.

    :param src: The source action.
    :param dst: The destination action.
    """
    forward_headers(
        src,
        dst,
        headers=(
            llm_tool.LLM_PROVIDER_HEADER,
            llm_tool.LLM_API_KEY_HEADER,
            llm_tool.ALLOWED_TOOLS_HEADER,
        ),
    )


TOOL_RUNNER_SCHEMA = actions.ActionSchema(
    name="_handle_tool_calls_",
    description="Runs tools from input_dicts.",
    inputs=[
        (
            "calls",
            "application/json",
            "Tool calls in the form of LLM-supplied dictionaries.",
        ),
    ],
    outputs=[
        (
            "outputs",
            "application/json",
            "The results of the tool calls.",
        ),
    ],
)


async def _run_tool(
    tools: dict[str, llm_tool.LLMTool],
    registry: actions.ActionRegistry,
    input_dict_chunk: data.Chunk,
    output_node: async_node.AsyncNode,
    result_idx: int,
    headers: dict[str, str | bytes] = None,
):
    headers = headers or dict()

    try:
        input_dict = await asyncio.to_thread(
            input_dict_chunk.deserialize, "application/json"
        )
        if not isinstance(input_dict, dict):
            raise ValueError("Input dict chunk did not contain a valid JSON.")

        tool = tools[input_dict["name"]]
        _LOGGER.info(f"{input_dict["id"]} {input_dict['name']}")

        allowed_tool_names = headers.get(
            llm_tool.ALLOWED_TOOLS_HEADER, ""
        ).split(",")
        allowed_tool_names = [
            name.strip() for name in allowed_tool_names if name.strip()
        ]
        if input_dict["name"] not in allowed_tool_names:
            raise ValueError(f"Tool {input_dict['name']} is not allowed.")

        result = await tool.run(input_dict["params"], registry, headers=headers)
        if result is None:
            raise ValueError(f"Tool {input_dict['name']} returned None.")
    except Exception as exc:
        await output_node.put(
            {
                "__error__": True,
                "error": str(exc.with_traceback(None)).splitlines()[0],
            },
            seq=result_idx,
            mimetype="application/json",
        )
    else:
        await output_node.put(
            result,
            seq=result_idx,
            mimetype="application/json",
        )


def make_llm_tool_runner():
    async def _runner(action: actions.Action):
        headers = dict()

        llm, api_key = get_llm_and_api_key(action)
        headers[llm_tool.LLM_PROVIDER_HEADER] = llm
        headers[llm_tool.LLM_API_KEY_HEADER] = api_key
        allowed_tools = get_allowed_tools(action)
        allowed_tools += ["submit_response__"]
        headers[llm_tool.ALLOWED_TOOLS_HEADER] = ",".join(allowed_tools)

        tools = make_tools(action.get_registry(), allowed_tools)

        try:
            async with asyncio.TaskGroup() as tg:
                # start all tools as soon as possible, but preserve call order
                tool_call_idx = 0
                while True:
                    chunk: data.Chunk | None = await action[
                        "calls"
                    ].next_chunk()
                    if chunk is None:
                        break

                    tg.create_task(
                        _run_tool(
                            tools,
                            action.get_registry(),
                            chunk,
                            action["outputs"],
                            tool_call_idx,
                            headers=headers,
                        )
                    )
                    tool_call_idx += 1
        finally:
            await action["outputs"].finalize()

    return _runner


def enable_llm_tool_runner(
    registry: actions.ActionRegistry,
):
    registry.register(
        TOOL_RUNNER_SCHEMA.name,
        TOOL_RUNNER_SCHEMA,
        make_llm_tool_runner(),
    )
