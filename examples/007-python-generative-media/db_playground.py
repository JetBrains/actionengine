import argparse
import asyncio
import json
import logging
import os
from pathlib import Path

import actionengine
from actionengine.logging import TextColor
from actionengine.sdk.llm import LLMHeaders
from actionengine.sdk.llm_tool_runner import (
    enable_llm_tool_runner,
    set_allowed_tools,
)
from bao.memory.api.types import ActionName as MemoryActionName

from actions.dbqa.answer_question_schema import ANSWER_QUESTION_SCHEMA
from actions.dbqa.answer_question import answer_question
from actions.dbqa.describe_db_schema import DESCRIBE_DB_SCHEMA
from actions.dbqa.describe_db import describe_db
from actions.dbqa.execute_query_schema import EXECUTE_QUERY_SCHEMA
from actions.dbqa.execute_query import execute_query
from actions.dbqa.judge_result_schema import JUDGE_RESULT_SCHEMA
from actions.dbqa.judge_result import judge_result
from actions.dbqa.validate_query_schema import VALIDATE_QUERY_SCHEMA
from actions.dbqa.validate_query import sign_query, validate_query
from actions.memory.utils import (
    register_actions as register_memory_actions,
    autofill_api_inputs as autofill_memory_api_inputs,
)

SPIDER2_LITE_ROOT = Path("/Users/helena/dev/Spider2/spider2-lite")
DB_URL = (
    SPIDER2_LITE_ROOT
    / "resource/databases/sqlite/California_Traffic_Collision.sqlite"
)


def make_client_actions():
    """
    Create and configure an ActionRegistry for database operations to
    expose to the LLM.
    """

    registry = actionengine.ActionRegistry()
    registry.register("describe_db", DESCRIBE_DB_SCHEMA, describe_db)
    registry.register("execute_query", EXECUTE_QUERY_SCHEMA, execute_query)
    registry.register("judge_result", JUDGE_RESULT_SCHEMA, judge_result)
    registry.register("validate_query", VALIDATE_QUERY_SCHEMA, validate_query)
    registry.get_schema("validate_query")["rules"].autofill_with([])

    register_memory_actions(registry)

    # this adds a special action that will run the tool calls:
    enable_llm_tool_runner(registry)

    return registry


# already expired anyway
MEMORY_AUTH = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3OTIxNTIwNjksImlhdCI6MTc4MjE1MTc2OSwiYXV0aF90aW1lIjoxNzgyMTUxNzQ0LCJqdGkiOiJvbnJ0ZGc6Y2IxM2FlMjgtZmQxNy05NDQzLWE5N2MtYWMyNWE2YzliZDFmIiwiaXNzIjoiaHR0cHM6Ly9hdXRoLmNvbnNvbGUuZGF0YWJhby5hcHAvcmVhbG1zL2RhdGFiYW8tcGxhdGZvcm0iLCJhdWQiOiJhY2NvdW50Iiwic3ViIjoiMGMxNDE5Y2QtMjM4My00MmRhLThmNGQtNDEwOTUyODFjMWRiIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiZGF0YWJhby1jbGkiLCJzaWQiOiJ2RnBSUF9tYmZkWFloNVdrYlVvaU10UmQiLCJhY3IiOiIwIiwiYWxsb3dlZC1vcmlnaW5zIjpbIi8qIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLWRhdGFiYW8tcGxhdGZvcm0iLCJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBlbWFpbCIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJuYW1lIjoiSGVsZW5hIFBhbmtvdiIsInByZWZlcnJlZF91c2VybmFtZSI6ImhlbGVuYS5wYW5rb3ZAamV0YnJhaW5zLmNvbSIsImdpdmVuX25hbWUiOiJIZWxlbmEiLCJmYW1pbHlfbmFtZSI6IlBhbmtvdiIsImVtYWlsIjoiaGVsZW5hLnBhbmtvdkBqZXRicmFpbnMuY29tIn0.Dky_dfnd-CgRzfwR8atD8m4QQUlCoe-QlU9Zz5shxd8"


async def solve_problem(
    problem: dict,
    result_root: str | Path,
):
    instance_id = problem["instance_id"]
    db = problem["db"]
    question = problem["question"]
    external_knowledge_file = problem["external_knowledge"]

    result_root = Path(result_root)

    db_path = (
        SPIDER2_LITE_ROOT / "resource/databases/sqlite" / db
    ).with_suffix(".sqlite")
    external_knowledge_path = None
    if external_knowledge_file:
        external_knowledge_path = (
            SPIDER2_LITE_ROOT / "resource/documents" / external_knowledge_file
        )
    external_knowledge = (
        external_knowledge_path.read_text() if external_knowledge_path else None
    )

    print(f"Solving problem {instance_id}.")
    print(f"  db: {db_path}")
    print(f"  question: {question}")
    print(f"  external knowledge: {external_knowledge_path}")

    client_tools = make_client_actions()
    autofill_memory_api_inputs(
        client_tools, "http://localhost:8001", MEMORY_AUTH
    )
    memory_scope = db_path.stem
    produce_answer = (
        actionengine.Action.from_schema(ANSWER_QUESTION_SCHEMA)
        .bind_handler(answer_question)
        .bind_registry(client_tools)
        .set_header(LLMHeaders.PROVIDER, "openai")
        .set_header("x-ae-otel-trace-id", "")
        .set_header("x-ae-memory-scope", memory_scope)
    )
    set_allowed_tools(
        produce_answer,
        (
            "execute_query",
            "validate_query",
            "describe_db",
            "judge_result",
            MemoryActionName.MEMORIES_CREATE,
            MemoryActionName.MEMORIES_SEARCH,
            MemoryActionName.SCHEMAS_SEARCH,
            MemoryActionName.SCHEMAS_GET,
        ),
    )

    # Notice that we run the action even though we don't have any inputs yet!
    # This is allowed in Action Engine: actions are asynchronous by default.
    produce_answer.run()

    # Now we can add inputs to the action.
    await produce_answer["db_url"].put_and_finalize(str(db_path))
    await produce_answer["question"].put_and_finalize(question)

    if external_knowledge:
        await produce_answer["additional_inquiries"].put(
            f"When performing the analysis, consider the following notes from "
            f"an external expert:\n\n{external_knowledge}"
        )
    await produce_answer["additional_inquiries"].finalize()

    answer, query, reasoning = await asyncio.gather(
        produce_answer["answer"].consume(timeout=3000.0),
        produce_answer["query"].consume(timeout=3000.0),
        produce_answer["reasoning"].consume(timeout=3000.0),
    )

    await produce_answer.wait_until_complete()
    print(answer)
    print()
    print("Saving results.")

    sql_path = result_root / f"{instance_id}.sql"
    sql_path.write_text(str(query))

    answer_path = result_root / f"{instance_id}.md"
    answer_path.write_text(str(answer))

    reasoning_path = result_root / f"{instance_id}.reasoning.md"
    reasoning_path.write_text(str(reasoning))


async def main(args: argparse.Namespace):
    # # set asyncio executor to use a thread pool with 300 threads
    # asyncio.get_running_loop().set_default_executor(
    #     concurrent.futures.ThreadPoolExecutor(300)
    # )

    problem_file = SPIDER2_LITE_ROOT / "spider2-lite.jsonl"
    local_problems = []
    with open(problem_file, "r") as f:
        for line in f:
            problem = await asyncio.to_thread(json.loads, line)
            if not problem.get("instance_id", "").startswith("local"):
                continue
            local_problems.append(problem)

    semaphore = asyncio.Semaphore(64)

    async def _solve_problem(problem: dict, result_root: str | Path):
        async with semaphore:
            print(f"Solving problem {problem['instance_id']}.")

            try:
                await solve_problem(problem, result_root)
            except Exception as exc:
                print(
                    TextColor.red(
                        f"Error solving problem {problem['instance_id']}: {exc}".replace(
                            "\\n", "\n"
                        )
                    )
                )
                return
            finally:
                pass

    async with asyncio.TaskGroup() as tg:
        for problem_idx, problem in enumerate(local_problems):
            tg.create_task(
                _solve_problem(
                    problem,
                    result_root=SPIDER2_LITE_ROOT / "resource" / "results1",
                )
            )


def sync_main():
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            f"{TextColor.gray('%(levelname)8s')} %(name)s | %(message)s"
        )
    )
    logging.root.addHandler(handler)
    logging.root.getChild("actionengine").setLevel(logging.DEBUG)
    logging.root.getChild("dbqa").setLevel(logging.DEBUG)

    if not os.environ.get("VALIDATE_QUERY_SIGNING_KEY"):
        print("Setting VALIDATE_QUERY_SIGNING_KEY to 'secret'")
        os.environ["VALIDATE_QUERY_SIGNING_KEY"] = "secret"

    settings = actionengine.get_global_settings()
    settings.readers_read_in_order = True
    settings.readers_deserialise_automatically = True

    parser = argparse.ArgumentParser()
    parser.add_argument("--llm", type=str, default="claude")
    parser.add_argument("--db-url", type=str, default=DB_URL)
    parser.add_argument("--question", type=str, default="")

    asyncio.run(main(parser.parse_args()))


if __name__ == "__main__":
    sync_main()
