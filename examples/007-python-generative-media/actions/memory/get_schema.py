import asyncio

import actionengine
import httpx

from . import api_utils
from . import status
from .data_types import SearchSchemasRequest, SchemaOut

Status = status.Status
StatusCode = status.StatusCode


async def get_schema(action: actionengine.Action):
    input_timeout = 30.0

    request: SearchSchemasRequest
    api_url, authorization, name = await asyncio.gather(
        action["api_url"].consume(timeout=input_timeout),
        action["authorization"].consume(timeout=input_timeout),
        action["name"].consume(timeout=input_timeout),
    )

    api_url = api_utils.validate_api_url(api_url)
    authorization = api_utils.validate_authorization(authorization)

    headers = {
        "Authorization": f"Bearer {authorization}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(base_url=api_url) as ac:
        async with ac.stream(
            "GET",
            f"/memory/schemas/{name}",
            headers=headers,
        ) as response:
            if response.status_code != 200:
                api_status: Status
                api_status = await api_utils.parse_json_response(
                    (await response.aread()).decode("utf-8"), [Status]
                )
                raise api_status.to_exception()

            schema = await api_utils.parse_json_response(
                await response.aread(), [SchemaOut]
            )
            await action["schema"].put_and_finalize(schema)
