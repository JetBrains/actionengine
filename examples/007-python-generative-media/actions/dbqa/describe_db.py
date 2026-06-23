import asyncio
import secrets

import actionengine
import actionengine.logging
from actionengine.logging import get_prefixed_logger, TextColor
from actionengine.sdk.llm_tool import LLM_PROVIDER_HEADER, LLM_API_KEY_HEADER
from actionengine.sdk.llm_tool_runner import (
    set_allowed_tools,
)
from bao.memory.api.types import ActionName as MemoryActionName

from .answer_question_schema import ANSWER_QUESTION_SCHEMA
from .answer_question import answer_question
from .logging import get_logger

_LOGGER = get_logger()
_CACHED_DB_DESCRIPTIONS: dict[str, str] = {}


async def describe_db(action: actionengine.Action):
    logger = get_prefixed_logger(
        _LOGGER,
        TextColor.dimmed_blue("describe_db"),
        first_time_prefix=TextColor.blue("describe_db"),
    )
    logger.info("started.")

    input_timeout = 3000.0

    if not hasattr(describe_db, "lock"):
        describe_db.lock = asyncio.Lock()

    db_url = await action["db_url"].consume(input_timeout)

    async with describe_db.lock:
        if db_url in _CACHED_DB_DESCRIPTIONS:
            await action["description"].put_and_finalize(
                _CACHED_DB_DESCRIPTIONS[db_url]
            )
            return

        question = (
            "The purpose, structure, content highlights of the database, "
            "and the least obvious names and relations. Indexes and "
            "constraints are also of interest. Your report will be used by "
            "an AI, so it should be concise, easy to understand, and "
            "minimize token usage. Include some statistics about the columns. "
            "For categorical columns, include the top 5 most frequent values. "
            "Take note of any indirections in the data that may occur, such "
            "as translations, self-references, anonymized data and any other "
            "complicated patterns."
        )

        registry = action.get_registry().copy()
        if registry.is_registered("execute_query"):
            registry.get_schema("execute_query").input("db_url").autofill_with(
                [str(db_url)]
            )

        produce_answer = (
            action.make_nested(ANSWER_QUESTION_SCHEMA, propagate_io=True)
            .bind_handler(answer_question)
            .bind_registry(action.get_registry())
        )
        set_allowed_tools(
            produce_answer,
            (
                "execute_query",
                "validate_query",
                MemoryActionName.MEMORIES_CREATE,
                MemoryActionName.MEMORIES_SEARCH,
                MemoryActionName.SCHEMAS_SEARCH,
                MemoryActionName.SCHEMAS_GET,
            ),
        )
        if span_id := action.get_header("span_id", decode=True):
            produce_answer.set_header("parent_span_id", span_id)
        produce_answer.run()

        await produce_answer["db_url"].put_and_finalize(str(db_url))
        await produce_answer["question"].put_and_finalize(question)
        await produce_answer["additional_inquiries"].finalize()

        timeout = 3000.0
        answer, query, reasoning = await asyncio.gather(
            produce_answer["answer"].consume(timeout),
            # not every answer will have a query, so allow None
            produce_answer["query"].consume(timeout=timeout, allow_none=True),
            produce_answer["reasoning"].consume(timeout=timeout),
        )
        _CACHED_DB_DESCRIPTIONS[db_url] = answer

        await action["description"].put_and_finalize(answer)
        await produce_answer.wait_until_complete()
