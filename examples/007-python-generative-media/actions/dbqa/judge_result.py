import asyncio
import json
import secrets
from pathlib import Path

import actionengine
import jinja2
from actionengine.logging import get_prefixed_logger, TextColor
from actionengine.sdk.anthropic.generate_content_claude import (
    CreateMessageConfig,
)
from actionengine.sdk.llm.generate_content import GENERATE_CONTENT_SCHEMA
from actionengine.sdk.llm.generate_content_handler import generate_content
from actionengine.sdk.llm_tool import LLM_PROVIDER_HEADER
from actionengine.sdk.llm_tool_runner import (
    get_allowed_tools,
    get_llm_and_api_key,
    make_tools,
)

from .logging import get_logger

_LOGGER = get_logger()


_SYSTEM_INSTRUCTIONS_TEMPLATE_PATH = (
    Path(__file__).parent / "judge_result_system_instructions.j2"
)
SYSTEM_INSTRUCTIONS_TEMPLATE = jinja2.Template(
    _SYSTEM_INSTRUCTIONS_TEMPLATE_PATH.read_text()
)


_SUBMIT_RESPONSE_SCHEMA = actionengine.ActionSchema(
    name="submit_response__",
    inputs=[
        ("correct", "text/plain"),
        ("mistakes", "text/plain"),
        ("improvements", "text/plain"),
    ],
    outputs=[("status", "text/plain")],
    description=(
        "A tool that you need to call in the very end of your analysis to "
        "submit your final response. For the tool response, you will get "
        '{"status": "success"} or {"status": "<error message>"}. You MUST '
        "call this tool at the end of your analysis."
    ),
)

_SUBMIT_RESPONSE_SCHEMA["correct"].required = True
_SUBMIT_RESPONSE_SCHEMA["correct"].unary = True
_SUBMIT_RESPONSE_SCHEMA["correct"].description = (
    "Whether the answer is correct w.r.t. the question or not."
)

_SUBMIT_RESPONSE_SCHEMA["mistakes"].required = False
_SUBMIT_RESPONSE_SCHEMA["mistakes"].unary = False
_SUBMIT_RESPONSE_SCHEMA["mistakes"].description = (
    "The mistakes made in the answer that need to be corrected."
)

_SUBMIT_RESPONSE_SCHEMA["improvements"].required = False
_SUBMIT_RESPONSE_SCHEMA["improvements"].unary = False
_SUBMIT_RESPONSE_SCHEMA["improvements"].description = (
    "The improvements that can be made to the answer to make it more accurate."
)


def _make_submit_response_handler(
    outer_action: actionengine.Action,
    submitted: asyncio.Event,
):
    async def submit_response(action: actionengine.Action):
        correct = await action["correct"].consume() == "true"
        mistakes = [mistake async for mistake in action["mistakes"]]
        improvements = [
            improvement async for improvement in action["improvements"]
        ]

        await outer_action["response"].put_and_finalize(
            {
                "correct": correct,
                "mistakes": mistakes,
                "improvements": improvements,
            }
        )

        await action["status"].put_and_finalize({"status": "success"})
        submitted.set()

    return submit_response


async def judge_result(action: actionengine.Action):
    logger = get_prefixed_logger(
        _LOGGER,
        TextColor.dimmed_blue("judge_result"),
        first_time_prefix=TextColor.blue("judge_result"),
    )
    logger.info("started.")

    allowed_tool_names = get_allowed_tools(action)
    if "execute_query" not in allowed_tool_names:
        raise ValueError(
            "`execute_query` tool is required to answer questions, but is not "
            "set in the `allowed_tools` header."
        )
    if not action.get_registry().is_registered("execute_query"):
        raise ValueError(
            "The registry must have an `execute_query` tool bound to answer "
            "questions about databases."
        )
    # disallow recursive calls
    if "judge_result" in allowed_tool_names:
        allowed_tool_names = [
            tool_name
            for tool_name in allowed_tool_names
            if tool_name != "judge_result"
        ]

    logger.info(f"allowed tools: {allowed_tool_names}")

    input_timeout = 3000.0
    db_url, question, answer, query, reasoning = await asyncio.gather(
        action["db_url"].consume(timeout=input_timeout),
        action["question"].consume(timeout=input_timeout),
        action["answer"].consume(timeout=input_timeout),
        action["query"].consume(timeout=input_timeout),
        action["reasoning"].consume(timeout=input_timeout),
    )

    chat_input_dict = {
        "question": question,
        "answer": answer,
        "query": query,
        "reasoning": reasoning,
    }

    logger.debug(TextColor.green(f"chat_input_dict: {chat_input_dict}"))

    chat_input = await asyncio.to_thread(json.dumps, chat_input_dict)

    logger.debug(f"db_url: {TextColor.gray(db_url)}")
    logger.debug(f"input: {TextColor.gray(chat_input)}")

    answer_submitted = asyncio.Event()
    registry = action.get_registry().copy(clear_autofills=False)
    registry.register(
        "submit_response__",
        _SUBMIT_RESPONSE_SCHEMA,
        _make_submit_response_handler(action, answer_submitted),
    )
    for tool_name in ("execute_query", "describe_db"):
        if (
            registry.is_registered(tool_name)
            and tool_name in allowed_tool_names
        ):
            tool_action_schema = registry.get_schema(tool_name)
            tool_action_schema["db_url"].autofill_with([str(db_url)])

    llm, api_key = get_llm_and_api_key(action)

    # create the generation action
    generate = (
        action.make_nested(GENERATE_CONTENT_SCHEMA, propagate_io=True)
        .bind_handler(generate_content)
        .bind_registry(registry)
    )
    generate.run()

    tools = make_tools(registry, allowed_tool_names)
    for tool_name, tool in tools.items():
        schema_dict = tool.get_schema().model_dump()
        if "required" in schema_dict:
            del schema_dict["required"]
        await generate["tools"].put(
            schema_dict,
            mimetype="application/json",
        )
    await generate["tools"].finalize()
    await asyncio.gather(
        generate["api_key"].put_and_finalize(llm),
        generate["chat_input"].put_and_finalize(chat_input),
        generate["system_instructions"].put_and_finalize(
            await asyncio.to_thread(SYSTEM_INSTRUCTIONS_TEMPLATE.render)
        ),
        generate["interaction_token"].put_and_finalize(""),
    )

    config = None
    if action.get_header(LLM_PROVIDER_HEADER, decode=True) == "claude":
        config = CreateMessageConfig(model="claude-sonnet-4-6")
    if config is None:
        await generate["config"].finalize()
    else:
        await generate["config"].put_and_finalize(config)

    await generate.wait_until_complete()
    if not answer_submitted.is_set():
        raise ValueError(
            "No answer was submitted through the submit_response__ tool."
        )
