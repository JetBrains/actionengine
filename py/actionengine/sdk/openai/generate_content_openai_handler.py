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
import base64
import copy
import json
import os
from typing import Any

import actionengine.logging
from actionengine.actions import Action
from actionengine.node_map import NodeMap
from actionengine.sdk import interaction
from actionengine.sdk import llm_tool
from actionengine.sdk.llm_tool_runner import (
    TOOL_RUNNER_SCHEMA,
    set_allowed_tools,
)
from actionengine.sdk.openai.client import get_openai_client
from actionengine.sdk.openai.generate_content_openai import (
    CreateResponseConfig,
)
from actionengine.sdk.rehydrate_interaction import REHYDRATE_INTERACTION_SCHEMA

TextColor = actionengine.logging.TextColor

_LOGGER = actionengine.logging.get_logger()


def _event_value(event: Any, name: str, default: Any = None) -> Any:
    if isinstance(event, dict):
        return event.get(name, default)
    return getattr(event, name, default)


def _to_plain_item(item: Any) -> dict[str, Any]:
    if isinstance(item, dict):
        return copy.deepcopy(item)
    if hasattr(item, "model_dump"):
        return item.model_dump(exclude_none=True)
    if hasattr(item, "dict"):
        return item.dict(exclude_none=True)
    return dict(item)


def _to_openai_tool(tool: dict[str, Any]) -> dict[str, Any]:
    parameters = copy.deepcopy(tool.get("input_schema", {}))
    parameters.setdefault("type", "object")
    parameters.setdefault("properties", {})
    if tool.get("required") and "required" not in parameters:
        parameters["required"] = tool["required"]
    parameters.setdefault("additionalProperties", False)

    return {
        "type": "function",
        "name": tool["name"],
        "description": tool.get("description", ""),
        "parameters": parameters,
    }


def _tool_result_content(output: Any) -> tuple[str, bool]:
    is_error = isinstance(output, dict) and output.get("__error__")
    if is_error:
        return output.get("error", ""), True
    return json.dumps(output), False


async def _run_tool_calls(
    action: Action,
    api_key: str,
    tools: list[dict[str, Any]],
    tool_calls: list[dict[str, Any]],
    logger,
) -> list[dict[str, Any]]:
    registry = action.get_registry()
    if not registry or not registry.is_registered(TOOL_RUNNER_SCHEMA.name):
        raise RuntimeError(
            f"Tool runner not registered for action "
            f"`{action.get_schema().name}` {action.get_id()}"
        )

    run_tools = registry.make_action(
        TOOL_RUNNER_SCHEMA.name,
        stream=None,
        node_map=NodeMap(),
    )
    run_tools.set_header(llm_tool.LLM_PROVIDER_HEADER, "openai")
    run_tools.set_header(llm_tool.LLM_API_KEY_HEADER, api_key)
    set_allowed_tools(run_tools, list(tool["name"] for tool in tools))
    run_tools.run()

    for tool_call in tool_calls:
        logger.debug(
            f"tool call: \x1b[33;20m{tool_call['name']} "
            f"{tool_call['id']}\x1b[0m "
            f"\x1b[38;5;242m{tool_call['params']}\x1b[0m"
        )
        await run_tools["calls"].put(tool_call)
    await run_tools["calls"].finalize()

    tool_outputs = []
    call_idx = 0
    async for output in run_tools["outputs"]:
        content, is_error = _tool_result_content(output)
        if len(content) > 2000:
            logger.debug(
                f"\x1b[38;5;242m{content[:2000]}... <truncated>\x1b[0m"
            )
        else:
            logger.debug(f"\x1b[38;5;242m{content}\x1b[0m")

        tool_output = {
            "type": "function_call_output",
            "call_id": tool_calls[call_idx]["call_id"],
            "output": content,
        }
        if is_error:
            tool_output["output"] = TextColor.red("ERROR: " + content).replace(
                "\\n", "\n"
            )
        tool_outputs.append(tool_output)
        call_idx += 1

    await run_tools.wait_until_complete()
    return tool_outputs


async def generate_content_openai(action: Action):
    logger = actionengine.logging.get_prefixed_logger(
        _LOGGER,
        TextColor.dimmed_blue(f"generate_content_openai[{action.get_id()}]"),
        first_time_prefix=TextColor.blue(
            f"generate_content_openai[{action.get_id()}]"
        ),
    )
    logger.debug("started.")
    input_timeout = 60.0

    config: CreateResponseConfig = await action["config"].consume(
        timeout=input_timeout, allow_none=True
    )
    config = config or CreateResponseConfig()

    interaction_token, api_key = await asyncio.gather(
        action["interaction_token"].consume(timeout=input_timeout),
        action["api_key"].consume(timeout=input_timeout),
    )

    interaction_id, next_output_seq, next_thought_seq = (
        await interaction.resolve_token_to_id_and_seqs(interaction_token)
    )
    if interaction_id is None:
        interaction_id = base64.urlsafe_b64encode(os.urandom(6)).decode("utf-8")

    rehydrate = Action.from_schema(REHYDRATE_INTERACTION_SCHEMA).bind_handler(
        interaction.rehydrate_interaction
    )
    await rehydrate["interaction_token"].put_and_finalize(interaction_token)
    rehydrate.run()

    input_items: list[dict[str, Any]] = []
    message_idx = 0
    async for message in rehydrate["previous_messages"]:
        input_items.append(
            {
                "role": "user" if message_idx % 2 == 0 else "assistant",
                "content": message,
            }
        )
        message_idx += 1
    await rehydrate.wait_until_complete()

    chat_input = await action["chat_input"].consume(timeout=input_timeout)
    input_items.append({"role": "user", "content": chat_input})

    system_prompt = ""
    async for instruction in action["system_instructions"]:
        system_prompt += instruction

    tools = []
    async for tool in action["tools"]:
        tools.append(tool)
    openai_tools = [_to_openai_tool(tool) for tool in tools]

    client = get_openai_client(api_key)

    new_message = ""
    new_thought = ""
    thoughts_finalized = False

    try:
        while True:
            create_kwargs = {
                "model": config.model,
                "input": input_items,
                "max_output_tokens": config.max_output_tokens,
                "stream": True,
            }
            if system_prompt:
                create_kwargs["instructions"] = system_prompt
            if openai_tools:
                create_kwargs["tools"] = openai_tools
                create_kwargs["tool_choice"] = "auto"
            if config.reasoning_effort is not None:
                create_kwargs["reasoning"] = {
                    "effort": config.reasoning_effort,
                }

            stream = await client.responses.create(**create_kwargs)

            output_items: dict[int, dict[str, Any]] = {}
            tool_calls_by_index: dict[int, dict[str, Any]] = {}

            async for event in stream:
                event_type = _event_value(event, "type", "")

                if event_type == "response.output_text.delta":
                    delta = _event_value(event, "delta", "")
                    await action["output"].put(delta)
                    new_message += delta
                    continue

                if event_type == "response.reasoning_summary_text.delta":
                    delta = _event_value(event, "delta", "")
                    await action["thoughts"].put(delta)
                    new_thought += delta
                    continue

                if event_type == "response.output_item.added":
                    output_index = _event_value(event, "output_index")
                    item = _to_plain_item(_event_value(event, "item"))
                    output_items[output_index] = item
                    if item.get("type") == "function_call":
                        tool_calls_by_index[output_index] = {
                            "id": item.get("id", ""),
                            "call_id": item.get("call_id", ""),
                            "name": item.get("name", ""),
                            "arguments": item.get("arguments", ""),
                        }
                    continue

                if event_type == "response.function_call_arguments.delta":
                    output_index = _event_value(event, "output_index")
                    delta = _event_value(event, "delta", "")
                    tool_calls_by_index.setdefault(
                        output_index,
                        {
                            "id": _event_value(event, "item_id", ""),
                            "call_id": _event_value(event, "call_id", ""),
                            "name": "",
                            "arguments": "",
                        },
                    )
                    tool_calls_by_index[output_index]["arguments"] += delta
                    continue

                if event_type == "response.function_call_arguments.done":
                    output_index = _event_value(event, "output_index")
                    tool_call = tool_calls_by_index.setdefault(
                        output_index,
                        {
                            "id": _event_value(event, "item_id", ""),
                            "call_id": _event_value(event, "call_id", ""),
                            "name": _event_value(event, "name", ""),
                            "arguments": "",
                        },
                    )
                    tool_call["arguments"] = _event_value(
                        event, "arguments", tool_call["arguments"]
                    )
                    tool_call["name"] = _event_value(
                        event, "name", tool_call["name"]
                    )
                    call_id = _event_value(event, "call_id", "")
                    if call_id:
                        tool_call["call_id"] = call_id
                    continue

                if event_type == "response.output_item.done":
                    output_index = _event_value(event, "output_index")
                    item = _to_plain_item(_event_value(event, "item"))
                    output_items[output_index] = item
                    if item.get("type") == "function_call":
                        tool_calls_by_index[output_index] = {
                            "id": item.get("id", ""),
                            "call_id": item.get("call_id", ""),
                            "name": item.get("name", ""),
                            "arguments": item.get("arguments", ""),
                        }
                    continue

                if event_type == "error":
                    raise RuntimeError(str(_event_value(event, "error", event)))

                if event_type == "response.failed":
                    response = _event_value(event, "response", {})
                    error = _event_value(response, "error", response)
                    raise RuntimeError(str(error))

            tool_calls = []
            for output_index in sorted(tool_calls_by_index):
                tool_call = tool_calls_by_index[output_index]
                arguments = tool_call.get("arguments") or "{}"
                parsed_arguments = await asyncio.to_thread(
                    json.loads, arguments
                )
                tool_calls.append(
                    {
                        "id": tool_call.get("id") or tool_call["call_id"],
                        "call_id": tool_call["call_id"],
                        "name": tool_call["name"],
                        "params": parsed_arguments,
                    }
                )

            if not tool_calls:
                break

            for output_index in sorted(output_items):
                input_items.append(output_items[output_index])

            tool_outputs = await _run_tool_calls(
                action, api_key, tools, tool_calls, logger
            )
            input_items.extend(tool_outputs)

    finally:
        await action["output"].finalize()
        if not thoughts_finalized:
            await action["thoughts"].finalize()

        if new_message:
            interaction_token = await interaction.save_turn(
                interaction_id,
                chat_input,
                new_message,
                new_thought,
                next_output_seq,
                next_thought_seq,
            )
        await action["new_interaction_token"].put_and_finalize(
            interaction_token
        )
        logger.debug("finished.")
