import asyncio
import datetime
import json

import actionengine
from absl import logging
from actionengine.sdk.llm import LLMHeaders
from pydantic import BaseModel, Field


class DBSummary(BaseModel):
    model_config = {
        "json_schema_extra": {
            "description": "A summary of a database, including its name, "
            "description, and tables. The summary has an `inspected_at` "
            "field that indicates when the database was last inspected. "
            "In general, if inspection happened more than 24 hours ago, "
            "you should re-inspect the database."
        }
    }
    name: str = Field(description="The name of the database.")
    description: str = Field(description="A brief description of the database.")
    tables: list[str] = Field(description="A list of tables in the database.")

    indexes: list[str] | None = Field(
        description="A list of indexes in the database.",
        exclude_if=lambda x: not x,
    )
    relations: list[str] | None = Field(
        description="A list of relations between tables in the database.",
        exclude_if=lambda x: not x,
    )
    caveats: list[str] | None = Field(
        description="A list of caveats about the database.",
        exclude_if=lambda x: not x,
    )

    inspected_at: datetime.datetime = Field(
        description="The date and time when the database was last inspected.",
        default_factory=datetime.datetime.now,
    )


from actions.memory.create_memory import create_memory
from actions.memory.create_memory_schema import CREATE_MEMORY_SCHEMA
from actions.memory.get_schema import get_schema
from actions.memory.get_schema_schema import GET_SCHEMA_SCHEMA
from actions.memory.search import search
from actions.memory.search_schema import SEARCH_MEMORIES_SCHEMA
from actions.memory.search_schemas import search_schemas
from actions.memory.search_schemas_schema import SEARCH_SCHEMAS_SCHEMA
from bao.memory.api.types import (
    CreateMemoryRequest,
    SearchRequest,
    SearchSchemasRequest,
    ObjectMemoryOut,
    SchemaOut,
)

API_URL = "https://databao.services"
AUTH_JWT = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJ2MU1OVFAtMG5CUEVMTEdSclpBMEZrYmphblZGOUs3dHhvWTJvdnJ6U2JVIn0.eyJleHAiOjE3ODIxMzEzMzUsImlhdCI6MTc4MjEzMTAzNSwiYXV0aF90aW1lIjoxNzgyMTIyNjkzLCJqdGkiOiJvbnJ0cnQ6ZTA2NDg1MjMtMzc0Ny0xMGMwLTczZDEtMmY2OTA0N2FhMGZhIiwiaXNzIjoiaHR0cHM6Ly9hdXRoLmNvbnNvbGUuZGF0YWJhby5hcHAvcmVhbG1zL2RhdGFiYW8tcGxhdGZvcm0iLCJhdWQiOiJhY2NvdW50Iiwic3ViIjoiMGMxNDE5Y2QtMjM4My00MmRhLThmNGQtNDEwOTUyODFjMWRiIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiZGF0YWJhby1jbGkiLCJzaWQiOiJKWHkxUmRBOU4xSXpZaXM3Q09KeTNiVDUiLCJhY3IiOiIwIiwiYWxsb3dlZC1vcmlnaW5zIjpbIi8qIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLWRhdGFiYW8tcGxhdGZvcm0iLCJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBlbWFpbCIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJuYW1lIjoiSGVsZW5hIFBhbmtvdiIsInByZWZlcnJlZF91c2VybmFtZSI6ImhlbGVuYS5wYW5rb3ZAamV0YnJhaW5zLmNvbSIsImdpdmVuX25hbWUiOiJIZWxlbmEiLCJmYW1pbHlfbmFtZSI6IlBhbmtvdiIsImVtYWlsIjoiaGVsZW5hLnBhbmtvdkBqZXRicmFpbnMuY29tIn0.gRNUZHJpWoIlxoM2HxMvtxOaUl088OpFxhds9o7KeQgns51H92Dn2rUc9WzHC5k6FgN4iKu7fMBGm2OTJ571iJWIjODTO7k7cw0HNy9iioVHh8chgEsFmzC8Px9nlJqWlEeSAh0oV82BZa2bQ2IO34rE-bp7XXgbp7-RvwyK-6ZyOB1BLkHlvlloGueXiC0pEEk7EIqzKejM9Mjd9TOizy4WJ2tq0mr271BgKgYDh61Gmt2B1US1lgOMyKW9o3N6T3PQS-R1jT0skg_VV83AjqeuOYBviU73S3htK4PJIwuZm2H3B4dFvAuIZddYl2skreYfnUt-MWRAiEqim5AhCQ"


async def main():
    print(json.dumps(DBSummary.model_json_schema(), indent=2))

    # ---------------

    # action = (
    #     actionengine.Action.from_schema(GET_SCHEMA_SCHEMA)
    #     .bind_handler(get_schema)
    #     .set_header(LLMHeaders.PROVIDER, "openai")
    #     .set_header("x-ae-otel-trace-id", "")
    #     .run()
    # )
    # await action["name"].put_and_finalize("SqlRightAndWrongUsageTipDemo")
    # await action["api_url"].put_and_finalize(API_URL)
    # await action["authorization"].put_and_finalize(AUTH_JWT)
    #
    # try:
    #     schema = await action["schema"].consume(timeout=10.0)
    #     print(schema)
    #     print()
    # finally:
    #     await action.wait_until_complete()

    # -----------------

    # request = SearchRequest(
    #     # search="",
    #     return_awaiting_feedback=True,
    # )
    # action = (
    #     actionengine.Action.from_schema(SEARCH_SCHEMAS_SCHEMA)
    #     .bind_handler(search_schemas)
    #     .set_header(LLMHeaders.PROVIDER, "openai")
    #     .set_header("x-ae-otel-trace-id", "")
    #     .run()
    # )
    # await action["request"].put_and_finalize(request)
    # await action["query_params"].finalize()
    # await action["api_url"].put_and_finalize(API_URL)
    # await action["authorization"].put_and_finalize(AUTH_JWT)
    #
    # try:
    #     schema: SchemaOut
    #     async for schema in action["schemas"]:
    #         print(schema)
    # finally:
    #     await action.wait_until_complete()

    # ----------------

    # request = SearchRequest(
    #     # search="",
    #     return_awaiting_feedback=True,
    # )
    # search_memories = (
    #     actionengine.Action.from_schema(SEARCH_MEMORIES_SCHEMA)
    #     .bind_handler(search)
    #     .set_header(LLMHeaders.PROVIDER, "openai")
    #     .set_header("x-ae-otel-trace-id", "")
    #     .run()
    # )
    # await search_memories["request"].put_and_finalize(request)
    # await search_memories["query_params"].finalize()
    # await search_memories["api_url"].put_and_finalize(API_URL)
    # await search_memories["authorization"].put_and_finalize(AUTH_JWT)
    #
    # try:
    #     memory: ObjectMemoryOut
    #     async for memory in search_memories["memories"]:
    #         print(memory)
    # finally:
    #     await search_memories.wait_until_complete()

    # -------------------

    # memory = CreateMemoryRequest(
    #     title="My other memory",
    #     object={"type": "text", "text": "This is my second memory."},
    #     description="This is a description of my second memory.",
    #     brief_description="This is a brief description of my second memory.",
    # )
    #
    # action = (
    #     actionengine.Action.from_schema(CREATE_MEMORY_SCHEMA)
    #     .bind_handler(create_memory)
    #     .set_header(LLMHeaders.PROVIDER, "openai")
    #     .set_header("x-ae-otel-trace-id", "")
    #     .run()
    # )
    #
    # await action["memory"].put_and_finalize(memory)
    # await action["api_url"].put_and_finalize(API_URL)
    # await action["authorization"].put_and_finalize(AUTH_JWT)
    # await action.wait_until_complete()
    #
    # uid = await action["uid"].consume()
    # print(f"Memory created. UID: {uid}")


def sync_main():
    logging.set_verbosity(logging.DEBUG)
    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
