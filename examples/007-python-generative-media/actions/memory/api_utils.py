import asyncio
from typing import Sequence

import httpx
import pydantic_core
from pydantic import BaseModel, ValidationError

from bao import status

Status = status.Status
StatusCode = status.StatusCode


def validate_api_url(url: str):
    if not url.startswith("http"):
        raise Status(
            code=StatusCode.INVALID_ARGUMENT,
            message="Invalid API URL.",
        ).to_exception()

    if url.endswith("/"):
        url = url[:-1]

    return url


def validate_authorization(authorization: str):
    if not authorization:
        raise Status(
            code=StatusCode.UNAUTHENTICATED,
            message="Authorization token is required.",
        ).to_exception()
    return authorization


async def parse_json_response(
    text: str, allowed_models: Sequence[type[BaseModel]] | None = None
):
    try:
        obj = await asyncio.to_thread(pydantic_core.from_json, text)
    except ValueError:
        raise Status(
            code=StatusCode.INVALID_ARGUMENT,
            message="Invalid JSON.",
        ).to_exception()

    if allowed_models is None:
        return obj

    errors_per_model = []
    for model in allowed_models:
        try:
            return await asyncio.to_thread(model.model_validate, obj)
        except ValidationError as e:
            errors_per_model.append(e)

    raise Status(
        code=StatusCode.INVALID_ARGUMENT,
        message="Could not parse JSON.",
        details=errors_per_model,
    ).to_exception()


async def iterate_ndjson(
    response: httpx.Response,
    allowed_models: Sequence[type[BaseModel]] | None = None,
):
    async for line in response.aiter_lines():
        line = line.strip()
        if not line:
            continue

        yield await parse_json_response(line, allowed_models=allowed_models)
