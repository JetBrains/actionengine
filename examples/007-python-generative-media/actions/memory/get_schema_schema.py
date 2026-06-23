import json

import actionengine

from .data_types import (
    ActionName,
    SchemaOut,
)
from ..dbqa.utils import ask_llm_to_flag_disallowed_use

GET_SCHEMA_SCHEMA = actionengine.ActionSchema(
    name=ActionName.SCHEMAS_GET,
    inputs=[
        (
            "name",
            "text/plain",
            f"The name of the memory schema to retrieve.",
        ),
        (
            "api_url",
            "text/plain",
            ask_llm_to_flag_disallowed_use(
                "The base URL of the memory API to use."
            ),
        ),
        (
            "authorization",
            "text/plain",
            ask_llm_to_flag_disallowed_use("The authorization token to use."),
        ),
    ],
    outputs=[
        ("schema", SchemaOut, "The matching schema, with definition."),
    ],
    description=(
        "Search for memory schemas in the database. Memory schemas define the "
        "structure and content of memories, and can be used to filter and sort "
        "memories. Leave the `search` field empty to retrieve all available schemas. "
        "Leave the `owner` field empty to retrieve schemas available to you. "
        "This tool is useful for exploring the available memory schemas. However, "
        "it will not return schema definitions, only names and descriptions. To "
        f"retrieve the schema definitions, use the `{ActionName.SCHEMAS_GET}` action."
    ),
)

GET_SCHEMA_SCHEMA["name"].required = True
GET_SCHEMA_SCHEMA["name"].unary = True

GET_SCHEMA_SCHEMA["api_url"].required = True
GET_SCHEMA_SCHEMA["api_url"].unary = True

GET_SCHEMA_SCHEMA["authorization"].required = True
GET_SCHEMA_SCHEMA["authorization"].unary = True
