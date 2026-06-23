import asyncio

import actionengine
import httpx

from . import api_utils
from . import status
from .data_types import SearchSchemasRequest, SchemaOut, QueryParameters

Status = status.Status
StatusCode = status.StatusCode


async def search_schemas(action: actionengine.Action):
    input_timeout = 30.0

    request: SearchSchemasRequest | dict
    api_url, authorization, request, query_params = await asyncio.gather(
        action["api_url"].consume(timeout=input_timeout),
        action["authorization"].consume(timeout=input_timeout),
        action["request"].consume(timeout=input_timeout),
        action["query_params"].consume(timeout=input_timeout, allow_none=True),
    )

    if isinstance(request, dict):
        request = SearchSchemasRequest.model_validate(request)
    api_url = api_utils.validate_api_url(api_url)
    authorization = api_utils.validate_authorization(authorization)
    query_params = query_params or QueryParameters()
    if isinstance(query_params, dict):
        query_params = QueryParameters.model_validate(query_params)

    headers = {
        "Authorization": f"Bearer {authorization}",
        "Content-Type": "application/json",
    }

    schemas = []
    async with httpx.AsyncClient(base_url=api_url) as ac:
        async with ac.stream(
            "POST",
            "/memory/schemas/search",
            params=query_params.model_dump(exclude_none=True),
            content=await asyncio.to_thread(request.model_dump_json),
            headers=headers,
        ) as response:
            if response.status_code != 200:
                api_status: Status
                api_status = await api_utils.parse_json_response(
                    (await response.aread()).decode("utf-8"), [Status]
                )
                raise api_status.to_exception()

            async for status_or_schema in api_utils.iterate_ndjson(
                response, [SchemaOut, Status]
            ):
                if isinstance(status_or_schema, Status):
                    raise status_or_schema.to_exception()

                status_or_schema: SchemaOut
                # Remove the definition from the schema output, as it is too large
                # to pass down all LLM calls.
                status_or_schema.definition = None
                schemas.append(status_or_schema)
                await action["schemas"].put(status_or_schema)

            await action["schemas"].finalize()
