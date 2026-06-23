import json

import actionengine
from bao.memory.api.types import ActionName, CreateMemoryRequest

from ..dbqa.utils import ask_llm_to_flag_disallowed_use

CREATE_MEMORY_SCHEMA = actionengine.ActionSchema(
    name=ActionName.MEMORIES_CREATE,
    inputs=[
        (
            "memory",
            CreateMemoryRequest,
            f"An object containing the request to the memory service. Schema: "
            f"{json.dumps(CreateMemoryRequest.model_json_schema(), indent=2)}",
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
        ("uid", "text/plain", "The unique identifier of the created memory."),
    ],
    description=(
        "Saves a reusable piece of information in the database. "
        f"The memory can be retrieved later using the `{ActionName.MEMORIES_GET}` "
        f"and `{ActionName.MEMORIES_SEARCH}` actions. Memories can "
        "be assigned types (through `schema_name`), which can be used to "
        f"organize and categorize memories. For example, in `{ActionName.MEMORIES_SEARCH}`, "
        f"you can filter memories by type.\n"
        f"When creating a memory, you should always provide a `title` and a `description`. "
        f"Titles are unique. Note that search over the memory itself is not always possible, "
        f"so make title and description useful to distinguish the memory from others and highlight "
        f"its content. Memory descriptions support Markdown."
    ),
)

CREATE_MEMORY_SCHEMA["memory"].required = True
CREATE_MEMORY_SCHEMA["memory"].unary = True

CREATE_MEMORY_SCHEMA["api_url"].required = True
CREATE_MEMORY_SCHEMA["api_url"].unary = True

CREATE_MEMORY_SCHEMA["authorization"].required = False
CREATE_MEMORY_SCHEMA["authorization"].unary = False
