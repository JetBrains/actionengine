import asyncio
import hmac
import json
import os
import secrets
import traceback
from pathlib import Path

import jinja2

import actionengine
from actionengine.logging import get_prefixed_logger, TextColor
from actionengine.sdk.anthropic.generate_content_claude import (
    CreateMessageConfig,
)
from actionengine.sdk.llm.generate_content import (
    GENERATE_CONTENT_SCHEMA,
)
from actionengine.sdk.llm.generate_content_handler import (
    generate_content,
)
from actionengine.sdk.llm_tool import LLM_PROVIDER_HEADER, LLM_API_KEY_HEADER
from actionengine.sdk.llm_tool_runner import (
    get_llm_and_api_key,
)

from .logging import get_logger

_LOGGER = get_logger()

_SYSTEM_INSTRUCTIONS_TEMPLATE_PATH = (
    Path(__file__).parent / "validate_query_system_instructions.j2"
)
SYSTEM_INSTRUCTIONS_TEMPLATE = jinja2.Template(
    _SYSTEM_INSTRUCTIONS_TEMPLATE_PATH.read_text()
)


def sign_query(query: str, signing_key: str = "") -> str:
    signing_key = signing_key or os.environ.get("VALIDATE_QUERY_SIGNING_KEY")
    if not signing_key:
        raise ValueError(
            "No signing key supplied and VALIDATE_QUERY_SIGNING_KEY "
            "environment variable is not set."
        )
    return hmac.new(
        signing_key.encode(),
        query.encode(),
        "sha256",
    ).hexdigest()[:24]


async def _iterate_streaming_jsonl(node: actionengine.AsyncNode):
    accumulated = ""
    async for chunk in node:
        try:
            accumulated += chunk
        except TypeError:
            print(TextColor.red(traceback.format_exc()))
            raise
        accumulated = accumulated.replace("```json", "")
        accumulated = accumulated.replace("```\n", "\n")
        lines = accumulated.split("\n")
        if len(lines) == 1:
            continue

        for line in lines[:-1]:
            if not line.strip():
                continue
            yield await asyncio.to_thread(json.loads, line)
        accumulated = lines[-1]

    accumulated = accumulated.strip()
    accumulated = accumulated.replace("```json", "")
    accumulated = accumulated.replace("```\n", "")

    if accumulated:
        yield await asyncio.to_thread(json.loads, accumulated)


def _validate_line_dict(line_dict: dict):
    kind = line_dict.get("kind")
    if kind is None:
        raise ValueError(
            f"Invalid JSON from LLM: {line_dict}. `kind` field is missing."
        )
    if kind not in ("verdict", "violation", "validated_query"):
        raise ValueError(
            f"Invalid JSON from LLM: {line_dict}. `kind` field is not valid."
        )

    if kind == "validated_query":
        if len(line_dict) != 2 or not isinstance(line_dict.get("query"), str):
            raise ValueError(
                f"Invalid JSON from LLM: {line_dict}. `validated_query` must "
                f"have 'query' field with string value."
            )

    if kind == "verdict":
        if len(line_dict) != 2 or line_dict.get("valid") not in (
            True,
            False,
        ):
            raise ValueError(
                f"Invalid JSON from LLM: {line_dict}. Verdict must have "
                f"'valid' field with boolean value."
            )

    if kind == "violation":
        if (
            len(line_dict) != 3
            or "kind" not in line_dict
            or "message" not in line_dict
            or "span" not in line_dict
        ):
            raise ValueError(
                f"Invalid JSON from LLM: {line_dict}. `kind`, `message`, and "
                f"`span` fields are required for violations."
            )
        if (
            line_dict["span"].get("start") is None
            or line_dict["span"].get("end") is None
        ):
            raise ValueError(
                f"Invalid JSON from LLM: {line_dict}. Span must have start and end fields."
            )
        if not isinstance(line_dict["span"]["start"], int):
            raise ValueError(
                f"Invalid JSON from LLM: {line_dict}. Span start must be an integer."
            )
        if not isinstance(line_dict["span"]["end"], int):
            raise ValueError(
                f"Invalid JSON from LLM: {line_dict}. Span end must be an integer."
            )
        if line_dict["span"]["start"] >= line_dict["span"]["end"]:
            raise ValueError(
                f"Invalid JSON from LLM: {line_dict}. Span start must be less than span end."
            )


async def validate_query(action: actionengine.Action):
    logger = get_prefixed_logger(
        _LOGGER,
        TextColor.dimmed_blue("validate_query"),
        first_time_prefix=TextColor.blue("validate_query"),
    )
    logger.debug("started.")

    input_timeout = 3000.0

    try:
        llm, api_key = get_llm_and_api_key(action)
    except ValueError as e:
        llm = "claude"
        api_key = os.environ.get("ANTHROPIC_API_KEY")

    # no remark support so far
    await action["remarks"].finalize()

    query = await action["query"].consume(timeout=input_timeout)
    logger.debug(f"query: {TextColor.gray(query)}")
    query = query.strip()

    normalized_query = (
        query.casefold().replace("\r\n", " ").replace("\n", " ").strip()
    )
    while "  " in normalized_query:
        normalized_query = normalized_query.replace("  ", " ")
    if "select *" in normalized_query:
        await asyncio.gather(
            action["signature"].finalize(),
            action["violations"].put_and_finalize(
                "SELECT * queries are not allowed."
            ),
            action["validated_query"].finalize(),
        )
        return

    raw_rules = [
        rule
        async for rule in action["rules"].set_reader_options(
            timeout=input_timeout
        )
    ]

    rules = []
    for rule in raw_rules:
        rule = rule.replace("\r\n", "")
        rule = rule.replace("\n", "")
        rule = f"- {rule}"
        rules.append(rule)

    system_instructions = await asyncio.to_thread(
        SYSTEM_INSTRUCTIONS_TEMPLATE.render,
        dialect="SQLite",
        rules="\n".join(rules),
    )

    generate = action.make_nested(
        GENERATE_CONTENT_SCHEMA, propagate_io=True
    ).bind_handler(generate_content)
    generate.set_header(LLM_PROVIDER_HEADER, llm)
    generate.set_header(LLM_API_KEY_HEADER, api_key)
    generate.run()

    await asyncio.gather(
        generate["tools"].finalize(),  # no tools
        generate["api_key"].put_and_finalize(llm),
        generate["chat_input"].put_and_finalize(query),
        generate["system_instructions"].put_and_finalize(system_instructions),
        generate["interaction_token"].put_and_finalize(""),
    )

    config = None
    if action.get_header(LLM_PROVIDER_HEADER, decode=True) == "claude":
        config = CreateMessageConfig(model="claude-haiku-4-5")
    if config is None:
        await generate["config"].finalize()
    else:
        await generate["config"].put_and_finalize(config)

    is_valid = None
    validated_query_from_llm = None
    async for line_dict in _iterate_streaming_jsonl(generate["output"]):
        try:
            _validate_line_dict(line_dict)
            match line_dict["kind"]:
                case "verdict":
                    if is_valid is not None:
                        raise ValueError(
                            f"Multiple verdicts received: {line_dict}"
                        )
                    is_valid = line_dict["valid"]
                    if is_valid:
                        # do this early to avoid waiting for violations in clients
                        await action["violations"].finalize()
                case "validated_query":
                    if validated_query_from_llm is not None:
                        raise ValueError(
                            f"Multiple validated_query received: {line_dict}"
                        )
                    validated_query_from_llm = line_dict["query"]
                case "violation":
                    if is_valid is None:
                        raise ValueError(
                            f"Violation received before verdict: {line_dict}"
                        )
                    if is_valid:
                        raise ValueError(
                            f"Violation received after a positive verdict: {line_dict}"
                        )
                    await action["violations"].put(line_dict)
        except Exception:
            traceback.print_exc()
            raise

    if is_valid is None:
        raise ValueError(f"No verdict received.")

    if not is_valid:
        await asyncio.gather(
            action["signature"].finalize(),
            action["violations"].finalize(),
            action["validated_query"].finalize(),
        )
        return

    signing_key = os.environ.get("VALIDATE_QUERY_SIGNING_KEY")
    if signing_key is None:
        raise ValueError(
            "VALIDATE_QUERY_SIGNING_KEY environment variable is not set."
        )

    validated_query = validated_query_from_llm or query
    signature = await asyncio.to_thread(
        sign_query, validated_query, signing_key
    )

    await asyncio.gather(
        action["validated_query"].put_and_finalize(validated_query),
        action["signature"].put_and_finalize(signature),
    )
    await generate.wait_until_complete()
