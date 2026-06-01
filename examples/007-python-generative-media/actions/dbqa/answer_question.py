import asyncio
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
from actionengine.sdk.llm_tool_runner import (
    get_allowed_tools,
    get_llm_and_api_key,
    make_tools,
)

from .logging import get_logger

_LOGGER = get_logger()


_SYSTEM_INSTRUCTIONS_TEMPLATE_PATH = (
    Path(__file__).parent / "answer_question_system_instructions.j2"
)
SYSTEM_INSTRUCTIONS_TEMPLATE = jinja2.Template(
    _SYSTEM_INSTRUCTIONS_TEMPLATE_PATH.read_text()
)


_SUBMIT_RESPONSE_SCHEMA = actionengine.ActionSchema(
    name="submit_response__",
    inputs=[
        ("query", "text/plain"),
        ("reasoning", "text/plain"),
        ("answer", "text/plain"),
    ],
    outputs=[("status", "text/plain")],
    description=(
        "A tool that you need to call in the very end of your analysis to "
        "tell your final response. As this tool's own response, you will get "
        '{"status": "success"} or {"status": "<error message>"}. You MUST '
        "call this tool at the end of your analysis."
    ),
)


def _make_submit_response_handler(
    outer_action: actionengine.Action,
    submitted: asyncio.Event,
):
    async def submit_response(action: actionengine.Action):
        query = await action["query"].consume(allow_none=True)
        if query is not None:
            await outer_action["query"].put(
                query,
            )

        reasoning = await action["reasoning"].consume()
        await outer_action["reasoning"].put(reasoning)

        answer = await action["answer"].consume()
        await outer_action["answer"].put(answer)

        await action["status"].put_and_finalize({"status": "success"})
        submitted.set()

    return submit_response


_SUBMIT_RESPONSE_SCHEMA["query"].required = False
_SUBMIT_RESPONSE_SCHEMA["query"].unary = True
_SUBMIT_RESPONSE_SCHEMA["query"].description = (
    "The query that generates the complete and accurate data used to answer "
    "the question. "
)

_SUBMIT_RESPONSE_SCHEMA["reasoning"].required = False
_SUBMIT_RESPONSE_SCHEMA["reasoning"].unary = True
_SUBMIT_RESPONSE_SCHEMA["reasoning"].description = (
    "The reasoning that explains the steps you took to answer the question."
)

_SUBMIT_RESPONSE_SCHEMA["answer"].required = True
_SUBMIT_RESPONSE_SCHEMA["answer"].unary = True
_SUBMIT_RESPONSE_SCHEMA["answer"].description = (
    "The final answer to the question."
)


async def answer_question(action: actionengine.Action):
    logger = get_prefixed_logger(
        _LOGGER,
        TextColor.dimmed_blue("answer_question"),
        first_time_prefix=TextColor.blue("answer_question"),
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

    logger.info(f"allowed tools: {allowed_tool_names}")

    input_timeout = 3000.0
    db_url, question = await asyncio.gather(
        action["db_url"].consume(timeout=input_timeout),
        action["question"].consume(timeout=input_timeout),
    )
    if not db_url.endswith(".sqlite") and not db_url.endswith(".db"):
        raise ValueError(f"Invalid or inaccessible database URL.")

    additional_inquiries = [
        f"<inquiry>{inquiry}<inquiry>"
        async for inquiry in action["additional_inquiries"].set_reader_options(
            timeout=input_timeout
        )
    ]
    if additional_inquiries:
        question += "You should also process the following inquiries:\n\n"
        question += "\n".join(additional_inquiries)

    logger.debug(f"db_url: {TextColor.gray(db_url)}")
    logger.debug(f"question: {TextColor.gray(question)}")

    answer_submitted = asyncio.Event()
    registry = action.get_registry().copy(clear_autofills=False)
    registry.register(
        "submit_response__",
        _SUBMIT_RESPONSE_SCHEMA,
        _make_submit_response_handler(action, answer_submitted),
    )
    for tool_name in ("execute_query", "describe_db", "judge_result"):
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

    model = (
        "claude-opus-4-7"
        if "describe_db" in allowed_tool_names
        else "claude-sonnet-4-6"
    )

    tools = make_tools(registry, allowed_tool_names)
    for tool_name, tool in tools.items():
        schema_dict = tool.get_schema().model_dump()
        print(f"tool: {tool_name} {schema_dict}")
        if "required" in schema_dict:
            del schema_dict["required"]
        await generate["tools"].put(
            schema_dict,
            mimetype="application/json",
        )
    await generate["tools"].finalize()
    await asyncio.gather(
        generate["api_key"].put_and_finalize(llm),
        generate["chat_input"].put_and_finalize(question),
        generate["system_instructions"].put_and_finalize(
            await asyncio.to_thread(
                SYSTEM_INSTRUCTIONS_TEMPLATE.render,
                {
                    "submit_response_as_tool_call": True,
                    "judge_result_available": "judge_result"
                    in allowed_tool_names,
                },
            )
        ),
        generate["interaction_token"].put_and_finalize(""),
        generate["config"].finalize(),
        # generate["config"].put_and_finalize(CreateMessageConfig(model=model)),
    )

    await generate.wait_until_complete()
    if not answer_submitted.is_set():
        answer = ""
        async for piece in generate["output"]:
            answer += piece
        await action["answer"].put(answer)

        reasoning = ""
        async for piece in generate["thoughts"]:
            reasoning += piece
        await action["reasoning"].put(reasoning)

    await asyncio.gather(
        action["answer"].finalize(),
        action["query"].finalize(),
        action["reasoning"].finalize(),
    )
