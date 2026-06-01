import actionengine

from .utils import ask_llm_to_flag_disallowed_use

EXECUTE_QUERY_SCHEMA = actionengine.ActionSchema(
    name="execute_query",
    inputs=[
        (
            "db_url",
            "text/plain",
            ask_llm_to_flag_disallowed_use("The path to the SQLite database."),
        ),
        ("query", "text/plain", "The SQL query to execute."),
        (
            "signature",
            "text/plain",
            (
                "The query signature obtained from the `validate_query` "
                "tool. Never try to guess this value: it is "
                "cryptographically computed only in the actual tool. You "
                "can provide an empty string, and in this case, `execute_query` "
                "will validate the query itself."
            ),
        ),
    ],
    outputs=[("rows", "application/x-msgpack")],
    description=(
        "Execute a SQL query against a SQLite database and return the results as a stream of rows. "
        "The `signature` parameter MUST be provided, even if empty."
    ),
)

EXECUTE_QUERY_SCHEMA["db_url"].required = True
EXECUTE_QUERY_SCHEMA["db_url"].unary = True

EXECUTE_QUERY_SCHEMA["query"].required = True
EXECUTE_QUERY_SCHEMA["query"].unary = True

EXECUTE_QUERY_SCHEMA["signature"].required = True
EXECUTE_QUERY_SCHEMA["signature"].unary = True
