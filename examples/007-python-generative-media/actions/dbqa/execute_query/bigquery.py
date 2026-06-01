import asyncio
import os

import actionengine
from google.cloud import bigquery


from ..logging import get_logger
from ..validate_query_schema import VALIDATE_QUERY_SCHEMA
from ..validate_query import sign_query, validate_query

_LOGGER = get_logger()


def _get_client(credentials_path: str = ""):
    credentials_path = credentials_path or os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS"
    )
    return bigquery.Client.from_service_account_json(credentials_path)


def _iterate_rows(
    query_job: bigquery.QueryJob,
    output_queue: asyncio.Queue[bigquery.Row | Exception | None],
    cancelled: asyncio.Event,
    page_size: int = 10,
    start_index: int = 0,
    timeout: float = 15.0,
):
    try:
        rows = query_job.result(
            page_size=page_size,
            start_index=start_index,
            timeout=timeout,
        )
        row: bigquery.Row
        for row in rows:
            output_queue.put_nowait(row)
            if cancelled.is_set():
                break

        if cancelled.is_set():
            raise asyncio.CancelledError

    except asyncio.CancelledError:
        query_job.cancel()

    except Exception as exc:
        output_queue.put_nowait(exc)

    finally:
        output_queue.put_nowait(None)


async def iterate_rows(query: str, timeout: float = 15.0):
    client = _get_client()

    query_job = client.query(query)
    queue = asyncio.Queue()
    cancelled = asyncio.Event()

    task = asyncio.create_task(
        asyncio.to_thread(
            _iterate_rows,
            query_job,
            queue,
            cancelled,
            timeout=timeout,
        )
    )

    try:
        while True:
            row = await queue.get()
            if row is None:
                break
            if isinstance(row, Exception):
                raise row
            yield row
    except asyncio.CancelledError:
        cancelled.set()
    finally:
        await task


async def execute_query(query: str, timeout: float = 15.0):
    query = sign_query(query)
    validate_query(query, VALIDATE_QUERY_SCHEMA)
