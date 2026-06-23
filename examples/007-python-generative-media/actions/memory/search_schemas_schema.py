import json

import actionengine

from .data_types import (
    ActionName,
    SearchSchemasRequest,
    SchemaOut,
    QueryParameters,
)
from ..dbqa.utils import ask_llm_to_flag_disallowed_use

SEARCH_SCHEMAS_SCHEMA = actionengine.ActionSchema(
    name=ActionName.SCHEMAS_SEARCH,
    inputs=[
        (
            "request",
            SearchSchemasRequest,
            f"An object containing the query to search memory schemas. Schema:\n"
            f"{json.dumps(SearchSchemasRequest.model_json_schema(), indent=2)}",
        ),
        (
            "query_params",
            QueryParameters,
            "Query parameters for the search. Most notably, `limit` and `offset` may be used for pagination.",
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
        ("schemas", SchemaOut, "A stream of matching memory schemas."),
    ],
    description=(
        "Search for memory schemas in the database. Memory schemas define the "
        "structure and content of memories, and can be used to filter and sort "
        "memories. Leave the `search` field empty to retrieve all available schemas. "
        "Leave the `owner` field empty to retrieve schemas available to you. "
        "This tool is useful for exploring the available memory schemas. However, "
        "it will not return schema definitions, only names and descriptions. Definitions are "
        f"available through the `{ActionName.SCHEMAS_GET}` action."
    ),
)

SEARCH_SCHEMAS_SCHEMA["request"].required = True
SEARCH_SCHEMAS_SCHEMA["request"].unary = True

SEARCH_SCHEMAS_SCHEMA["api_url"].required = True
SEARCH_SCHEMAS_SCHEMA["api_url"].unary = True

SEARCH_SCHEMAS_SCHEMA["authorization"].required = True
SEARCH_SCHEMAS_SCHEMA["authorization"].unary = True
