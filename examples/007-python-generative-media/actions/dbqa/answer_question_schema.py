import actionengine

from .utils import ask_llm_to_flag_disallowed_use

ANSWER_QUESTION_SCHEMA = actionengine.ActionSchema(
    name="answer_question",
    inputs=[
        (
            "db_url",
            "text/plain",
            ask_llm_to_flag_disallowed_use("The path to the database."),
        ),
        ("question", "text/plain", "The question to answer."),
        (
            "additional_inquiries",
            "text/plain",
            (
                "Pointers on how specifically to answer the question, any "
                "external resources to consult, etc."
            ),
        ),
    ],
    outputs=[
        ("answer", "text/plain"),
        ("query", "text/plain"),
        ("reasoning", "text/plain"),
    ],
    description=(
        "Answer a question about a database, providing reasoning for the "
        "answer and the specific query that gets the precise data required "
        "to answer the question."
    ),
)

ANSWER_QUESTION_SCHEMA["db_url"].required = True
ANSWER_QUESTION_SCHEMA["db_url"].unary = True

ANSWER_QUESTION_SCHEMA["question"].required = True
ANSWER_QUESTION_SCHEMA["question"].unary = True

ANSWER_QUESTION_SCHEMA["additional_inquiries"].required = False
ANSWER_QUESTION_SCHEMA["additional_inquiries"].unary = False
