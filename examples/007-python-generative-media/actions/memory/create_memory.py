import asyncio

import actionengine
import httpx
from bao import status
from bao.memory.api.types import CreateMemoryRequest, ObjectMemoryOut

from . import api_utils

Status = status.Status
StatusCode = status.StatusCode


async def create_memory(action: actionengine.Action):
    input_timeout = 30.0

    request: dict
    api_url, authorization, request = await asyncio.gather(
        action["api_url"].consume(timeout=input_timeout),
        action["authorization"].consume(timeout=input_timeout),
        action["memory"].consume(timeout=input_timeout),
    )
    request = CreateMemoryRequest.model_validate(request)

    api_url = api_utils.validate_api_url(api_url)
    authorization = api_utils.validate_authorization(authorization)

    headers = {
        "Authorization": f"Bearer {authorization}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(base_url=api_url) as ac:
        response = await ac.post(
            "/memory/memories",
            content=await asyncio.to_thread(request.model_dump_json),
            headers=headers,
        )

    if 200 <= response.status_code < 300:
        out: ObjectMemoryOut
        out = await asyncio.to_thread(
            ObjectMemoryOut.model_validate_json, response.content
        )
        await action["uid"].put_and_finalize(str(out.uid))
        return

    try:
        api_status: Status
        api_status = await asyncio.to_thread(
            Status.model_validate_json, response.content
        )
    except Exception:
        raise Status(
            code=StatusCode.INVALID_ARGUMENT,
            message="Invalid response from server: status code not OK, but body is not a Status object.",
        ).to_exception()

    raise api_status.to_exception()
