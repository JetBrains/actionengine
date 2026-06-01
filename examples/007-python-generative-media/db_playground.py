import argparse
import asyncio
import concurrent.futures
import json
import logging
import os
import uuid
from pathlib import Path

import actionengine
from actionengine.logging import TextColor
from actionengine.sdk.llm import LLMHeaders
from actionengine.sdk.llm_tool_runner import (
    enable_llm_tool_runner,
    set_allowed_tools,
)

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

    # this adds a special action that will run the tool calls:
    enable_llm_tool_runner(registry)

    return registry


async def example1_produce_answer(
    db_url: str, question: str = "What are the tables in the database?"
):
    client_tools = make_client_actions()

    # Create an action that will produce an answer to the question. Actions
    # in Action Engine have a rich, but modular context. In this case, we
    # want to expose the database operations to the LLM, so we use the
    # registry to enable the tool runner and set headers to configure the
    # LLM provider and allowed tools.
    produce_answer = (
        actionengine.Action.from_schema(ANSWER_QUESTION_SCHEMA)
        .bind_handler(answer_question)
        .bind_registry(client_tools)
        .set_header(LLMHeaders.PROVIDER, "claude")
    )
    # the tool runner will only run the tools that are allowed here
    set_allowed_tools(produce_answer, ("execute_query", "validate_query"))
    # Notice that we run the action even though we don't have any inputs yet!
    # This is allowed in Action Engine: actions are asynchronous by default.
    produce_answer.run()

    # Now we can add inputs to the action.
    await produce_answer["db_url"].put_and_finalize(db_url)
    await produce_answer["question"].put_and_finalize(question)
    await produce_answer["additional_inquiries"].finalize()

    # Wait for the answer to be produced.
    answer = await produce_answer["answer"].consume(timeout=60.0)
    # Good practice: wait for the action to complete before moving on.
    await produce_answer.wait_until_complete()

    print(answer)
    print(f"\x1b[38;5;242m{await produce_answer["query"].consume()}\x1b[0m\n")
    print(await produce_answer["reasoning"].consume())


async def example2_run_query(db_url: str, query: str):
    # Less context here: we're just running a query, so we don't need to
    # configure the tool runner or set headers.
    run_query = (
        actionengine.Action.from_schema(EXECUTE_QUERY_SCHEMA)
        .bind_handler(execute_query)
        .run()
    )

    # `execute_query` will only run if the query is valid, so we need to
    # sign it.
    signature = await asyncio.to_thread(sign_query, query)
    await asyncio.gather(
        run_query["db_url"].put_and_finalize(db_url),
        run_query["query"].put_and_finalize(query),
        run_query["signature"].put_and_finalize(signature),
    )

    # Here, we can iterate over the rows returned by the query, one by one.
    async for row in run_query["rows"]:
        print(row)


async def example3_judge_result(db_url: str):
    client_tools = make_client_actions()

    judge = (
        actionengine.Action.from_schema(JUDGE_RESULT_SCHEMA)
        .bind_handler(judge_result)
        .bind_registry(client_tools)
        .set_header(LLMHeaders.PROVIDER, "claude")
    )
    set_allowed_tools(judge, ("execute_query", "validate_query", "describe_db"))
    judge.run()

    await asyncio.gather(
        judge["db_url"].put_and_finalize(db_url),
        judge["answer"].put_and_finalize(
            """Using a simple linear regression model (slope ≈ 0.00458026, intercept ≈ 5.59962257) fitted on daily toy sales from Jan 1, 2017 to Aug 29, 2018, the 5-day symmetric moving averages of predicted toy sales are:
- Dec 5, 2018: 8.819546
- Dec 6, 2018: 8.824126
- Dec 7, 2018: 8.828707
- Dec 8, 2018: 8.833287

The sum of these four 5-day moving averages is 35.305666."""
        ),
        judge["reasoning"].put_and_finalize(
            """Daily toy sales were fetched from 2017-01-01 to 2018-08-29 by joining orders, order_items, products, and product_category_name_translation tables filtering for the 'toys' category. A simple linear regression was computed in SQL using the OLS formulas (slope and intercept) with x = days elapsed since 2017-01-01 and y = daily sales count. Predictions were generated for Dec 3–10, 2018 (8 days, to provide the ±2 day window for 5-day symmetric MAs centered on Dec 5–8). The 5-day symmetric moving average for each center date is the average of its predicted value and those of the 2 preceding and 2 following days. The sum of the four moving averages was computed directly in SQL."""
        ),
        judge["query"].put_and_finalize("""WITH daily AS (
            SELECT
                CAST(julianday(DATE(o.order_purchase_timestamp)) - julianday('2017-01-01') AS REAL) AS x,
                CAST(COUNT(oi.order_item_id) AS REAL) AS y
            FROM orders o
                     JOIN order_items oi ON o.order_id = oi.order_id
                     JOIN products p ON oi.product_id = p.product_id
                     JOIN product_category_name_translation t ON p.product_category_name = t.product_category_name
            WHERE t.product_category_name_english = 'toys'
              AND DATE(o.order_purchase_timestamp) BETWEEN '2017-01-01' AND '2018-08-29'
                                           GROUP BY DATE(o.order_purchase_timestamp)
                                               ),
                                               stats AS (
                                           SELECT COUNT(*) AS n, SUM(x) AS sum_x, SUM(y) AS sum_y, SUM(x*x) AS sum_xx, SUM(x*y) AS sum_xy FROM daily
                                               ),
                                               regression AS (
                                           SELECT
                                               (sum_xy - sum_x * sum_y / n) / (sum_xx - sum_x * sum_x / n) AS slope,
                                               sum_y / n - ((sum_xy - sum_x * sum_y / n) / (sum_xx - sum_x * sum_x / n)) * (sum_x / n) AS intercept
                                           FROM stats
                                               ),
                                               prediction_days AS (
                                           SELECT day_offset, date('2017-01-01', '+' || CAST(day_offset AS TEXT) || ' days') AS pred_date
                                           FROM (
                                               SELECT CAST(julianday('2018-12-03') - julianday('2017-01-01') AS INTEGER) AS day_offset UNION ALL
                                               SELECT CAST(julianday('2018-12-04') - julianday('2017-01-01') AS INTEGER) UNION ALL
                                               SELECT CAST(julianday('2018-12-05') - julianday('2017-01-01') AS INTEGER) UNION ALL
                                               SELECT CAST(julianday('2018-12-06') - julianday('2017-01-01') AS INTEGER) UNION ALL
                                               SELECT CAST(julianday('2018-12-07') - julianday('2017-01-01') AS INTEGER) UNION ALL
                                               SELECT CAST(julianday('2018-12-08') - julianday('2017-01-01') AS INTEGER) UNION ALL
                                               SELECT CAST(julianday('2018-12-09') - julianday('2017-01-01') AS INTEGER) UNION ALL
                                               SELECT CAST(julianday('2018-12-10') - julianday('2017-01-01') AS INTEGER)
                                               )
                                               ),
                                               predictions AS (
                                           SELECT p.pred_date, p.day_offset AS x, r.intercept + r.slope * CAST(p.day_offset AS REAL) AS predicted_sales
                                           FROM prediction_days p, regression r
                                               ),
                                               ma AS (
                                           SELECT p1.pred_date AS center_date,
                                               (SELECT AVG(p2.predicted_sales) FROM predictions p2 WHERE p2.pred_date BETWEEN date(p1.pred_date, '-2 days') AND date(p1.pred_date, '+2 days')) AS ma5
                                           FROM predictions p1
                                           WHERE p1.pred_date BETWEEN '2018-12-05' AND '2018-12-08'
                                               )
        SELECT ROUND(SUM(ma5), 6) AS sum_of_ma5 FROM ma"""),
        judge["question"].put_and_finalize(
            "Can you calculate the 5-day symmetric moving average of predicted toy sales for December 5 to 8, 2018, using daily sales data from January 1, 2017, to August 29, 2018, with a simple linear regression model? Finally provide the sum of those four 5-day moving averages?"
        ),
    )

    response = await judge["response"].consume(timeout=3000.0)
    print(response)


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
    produce_answer = (
        actionengine.Action.from_schema(ANSWER_QUESTION_SCHEMA)
        .bind_handler(answer_question)
        .bind_registry(client_tools)
        .set_header(LLMHeaders.PROVIDER, "openai")
        .set_header("x-ae-otel-trace-id", "")
    )
    set_allowed_tools(
        produce_answer,
        ("execute_query", "validate_query", "describe_db", "judge_result"),
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

    semaphore = asyncio.Semaphore(4)

    async def _solve_problem(problem: dict, result_root: str | Path):
        async with semaphore:
            print(f"Solving problem {problem['instance_id']}.")

            try:
                await solve_problem(problem, result_root)
            except Exception as exc:
                print(
                    TextColor.red(
                        f"Error solving problem {problem['instance_id']}: {exc}"
                    )
                )
                return
            finally:
                pass

    async with asyncio.TaskGroup() as tg:
        for problem_idx, problem in enumerate(local_problems[24:]):
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
