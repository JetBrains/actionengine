import json

import actionengine

from .data_types import (
    ActionName,
    SearchRequest,
    ObjectMemoryOut,
    QueryParameters,
)
from ..dbqa.utils import ask_llm_to_flag_disallowed_use

SEARCH_MEMORIES_SCHEMA = actionengine.ActionSchema(
    name=ActionName.MEMORIES_SEARCH,
    inputs=[
        (
            "request",
            SearchRequest,
            f"An object containing the query to search memories. Schema:\n"
            f"{json.dumps(SearchRequest.model_json_schema(), indent=2)}",
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
        ("memories", ObjectMemoryOut, "A stream of matching memories."),
    ],
    description=(
        "Search for memories in the database. Memories are structured, and can be "
        "filtered and sorted by various criteria, for example, by type, through the `schema_name` field. "
    ),
)

SEARCH_MEMORIES_SCHEMA["request"].required = True
SEARCH_MEMORIES_SCHEMA["request"].unary = True

SEARCH_MEMORIES_SCHEMA["query_params"].required = False
SEARCH_MEMORIES_SCHEMA["query_params"].unary = True

SEARCH_MEMORIES_SCHEMA["api_url"].required = True
SEARCH_MEMORIES_SCHEMA["api_url"].unary = True

SEARCH_MEMORIES_SCHEMA["authorization"].required = True
SEARCH_MEMORIES_SCHEMA["authorization"].unary = True
