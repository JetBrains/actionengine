import asyncio
import sqlite3
import os
import time
from pathlib import Path

import actionengine
import ormsgpack
from actionengine.sdk.llm_tool import (
    LLM_PROVIDER_HEADER,
    LLM_API_KEY_HEADER,
)
from actionengine.logging import get_prefixed_logger, TextColor

from ..logging import get_logger
from ..validate_query_schema import VALIDATE_QUERY_SCHEMA
from ..validate_query import sign_query, validate_query

_LOGGER = get_logger()


def _make_timeout_handler(started_at: float, timeout: float = 15.0):
    def progress_handler():
        if time.perf_counter() - started_at > timeout:
            return 1
        return 0

    return progress_handler


def _select_and_enqueue_rows(
    db_url: str,
    query: str,
    queue: asyncio.Queue,
    loop: asyncio.AbstractEventLoop | None,
):
    started_at = time.perf_counter()

    query = query.strip()
    if not query.endswith(";"):
        query += ";"

    filename_parts = Path(db_url).name.split("?")
    if len(filename_parts) > 1:
        raise ValueError(
            "Invalid database path. Query parameters are not allowed."
        )
    db_url = f"file:{db_url}?mode=ro"

    conn = sqlite3.connect(db_url)
    conn.execute("PRAGMA busy_timeout = 15000")
    conn.commit()

    conn.set_progress_handler(
        _make_timeout_handler(started_at, timeout=15.0), 1
    )
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            loop.call_soon_threadsafe(queue.put_nowait, row)
            if time.perf_counter() - started_at > 15.0:
                raise TimeoutError(
                    "Query took too long to execute (>15 seconds)."
                )

    except Exception as e:
        loop.call_soon_threadsafe(queue.put_nowait, e)
    finally:
        cursor.close()
        loop.call_soon_threadsafe(queue.put_nowait, None)
        conn.close()


async def execute_query(action: actionengine.Action):
    logger = get_prefixed_logger(
        _LOGGER,
        TextColor.dimmed_blue("execute_query"),
        first_time_prefix=TextColor.blue("execute_query"),
    )
    logger.debug("started.")

    input_timeout = 3000.0

    query = await action["query"].consume(timeout=input_timeout)
    signature = await action["signature"].consume(
        timeout=input_timeout, allow_none=True
    )
    db_url = await action["db_url"].consume(timeout=input_timeout)

    if not signature:
        for _ in range(3):
            logger.debug(
                f"empty query signature, calling "
                f"{TextColor.yellow('validate_query')}."
            )
            validate = action.make_nested(
                VALIDATE_QUERY_SCHEMA, propagate_io=True
            ).bind_handler(validate_query)

            llm_provider = (
                action.get_header(LLM_PROVIDER_HEADER, decode=True) or "claude"
            )
            llm_api_key = action.get_header(
                LLM_API_KEY_HEADER, decode=True
            ) or os.environ.get("ANTHROPIC_API_KEY")

            validate.set_header(LLM_PROVIDER_HEADER, llm_provider)
            if llm_api_key:
                validate.set_header(LLM_API_KEY_HEADER, llm_api_key)

            validate.run()

            await asyncio.gather(
                validate["query"].put_and_finalize(query),
                validate["rules"].finalize(),
            )

            violations = [
                violation async for violation in validate["violations"]
            ]
            if violations:
                raise ValueError(f"Query validation failed: {violations}")

            try:
                await validate.wait_until_complete()
                signature = await validate["signature"].consume()
                break
            except Exception:
                raise ValueError(
                    "Query validation failed: `validate_query` failed to produce a signature."
                )

    if not query:
        raise ValueError("Query is required.")

    signing_key = os.environ.get("VALIDATE_QUERY_SIGNING_KEY")
    if signing_key is None:
        raise ValueError(
            "VALIDATE_QUERY_SIGNING_KEY environment variable is not set."
        )
    query = query.strip()

    expected_signature = await asyncio.to_thread(sign_query, query, signing_key)
    if signature != expected_signature:
        raise ValueError("Invalid query signature.")

    queue = asyncio.Queue()
    task = asyncio.create_task(
        asyncio.to_thread(
            _select_and_enqueue_rows,
            db_url,
            query,
            queue,
            asyncio.get_running_loop(),
        )
    )

    try:
        while True:
            row = await queue.get()
            if row is None:
                break
            if isinstance(row, Exception):
                raise row
            serialized = await asyncio.to_thread(ormsgpack.packb, row)
            chunk = actionengine.Chunk()
            chunk.data = serialized
            chunk.metadata = actionengine.ChunkMetadata(
                mimetype="application/x-msgpack"
            )
            await action["rows"].put(chunk)
    except Exception:
        raise
    else:
        await action["rows"].finalize()
    finally:
        await task
        logger.debug("finished.")
