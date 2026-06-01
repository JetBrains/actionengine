import actionengine

DESCRIBE_DB_SCHEMA = actionengine.ActionSchema(
    name="describe_db",
    inputs=[
        ("db_url", "text/plain", "The URL of the database to describe"),
    ],
    outputs=[
        (
            "description",
            "text/plain",
            (
                "A description of the database, including prominent tables, "
                "columns, and their types, as well as indexes, constraints "
                "and quirks."
            ),
        )
    ],
    description="An overview of the database, including tables, columns, and their types.",
)

DESCRIBE_DB_SCHEMA["db_url"].required = True
DESCRIBE_DB_SCHEMA["db_url"].unary = True
