import actionengine

from .utils import ask_llm_to_flag_disallowed_use

VALIDATE_QUERY_SCHEMA = actionengine.ActionSchema(
    name="validate_query",
    inputs=[
        ("query", "text/plain", "The SQL query to validate."),
        (
            "rules",
            "text/plain",
            ask_llm_to_flag_disallowed_use(
                "Configuration for the query validator."
            ),
        ),
    ],
    outputs=[
        ("signature", "text/plain"),
        ("validated_query", "text/plain"),
        ("violations", "text/plain"),
        ("remarks", "text/plain"),
    ],
    description=(
        "Checks if the `query` does not violate any of the specified `rules`. "
        "As an LLM, you will not know the rules, but violations, if any, "
        "will be returned in the `violations` output. If the query is valid, "
        "the `validated_query` output will contain the query and the "
        "`signature` field will contain the inferred signature of the query, "
        "which you MUST pass any tools that require it. It is ESSENTIAL that "
        "the signature is correct, and that the query is passed to the tools "
        "verbatim, as the signature is sensitive to character level changes. "
        "if `validated_query` is not empty, you MUST pass it to the tools "
        "instead of your original `query`. Otherwise, you MUST pass your "
        "original `query` to the tools, verbatim. "
        "In the `remarks` output, you will receive hints on how to improve "
        "the query. If the query is valid, you do not have to use these hints."
    ),
)

VALIDATE_QUERY_SCHEMA["query"].required = True
VALIDATE_QUERY_SCHEMA["query"].unary = True

VALIDATE_QUERY_SCHEMA["rules"].required = False
VALIDATE_QUERY_SCHEMA["rules"].unary = False
