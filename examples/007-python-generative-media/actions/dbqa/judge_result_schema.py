import actionengine

from .utils import ask_llm_to_flag_disallowed_use

JUDGE_RESULT_SCHEMA = actionengine.ActionSchema(
    name="judge_result",
    inputs=[
        (
            "db_url",
            "text/plain",
            ask_llm_to_flag_disallowed_use("The URL of the database to use."),
        ),
        ("question", "text/plain", "The user's initial question."),
        ("answer", "text/plain", "The generated answer to the question."),
        (
            "query",
            "text/plain",
            "The SQL query that provides the data for the answer.",
        ),
        (
            "reasoning",
            "text/plain",
            "The reasoning behind the answer and query.",
        ),
    ],
    outputs=[("response", "application/json")],
    description=(
        "Submit the question answering result to the judge and receive the "
        "judge's response. The judge is an expert in SQL and has resources "
        "to assess the fitness of the answer to the question. The judge "
        "will provide a correctness judgement, as well as a list of mistakes "
        "and a list of recommendations for improvement. You will be provided "
        "with the judge's response in JSON format with the following keys: "
        "`correct` (boolean), `mistakes` (list of strings), and "
        "`improvements` (list of strings)."
    ),
)

JUDGE_RESULT_SCHEMA["db_url"].required = True
JUDGE_RESULT_SCHEMA["db_url"].unary = True

JUDGE_RESULT_SCHEMA["question"].required = True
JUDGE_RESULT_SCHEMA["question"].unary = True

JUDGE_RESULT_SCHEMA["answer"].required = True
JUDGE_RESULT_SCHEMA["answer"].unary = True

JUDGE_RESULT_SCHEMA["query"].required = True
JUDGE_RESULT_SCHEMA["query"].unary = True

JUDGE_RESULT_SCHEMA["reasoning"].required = True
JUDGE_RESULT_SCHEMA["reasoning"].unary = True
