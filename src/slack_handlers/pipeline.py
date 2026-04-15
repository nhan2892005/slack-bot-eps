import logging
from src.llm.classifier import get_schema_for_question
from src.llm.clarity import check_clarity
from src.llm.sql_generator import generate_sql
from src.llm.answer import generate_answer
from src.bq import run_query
from src.slack_handlers.thread import fetch_thread_history

logger = logging.getLogger(__name__)


def answer_question(question: str, channel: str, thread_ts: str, client, current_ts: str = None):
    """Run the full pipeline: question -> SQL -> BQ -> answer -> Slack reply."""
    # Fetch history BEFORE posting "Analyzing..." to keep history clean
    history = fetch_thread_history(client, channel, thread_ts, exclude_ts=current_ts)
    logger.info(f"=== Thread history for question '{question}' ===\n{history}\n=== end history ===")

    client.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text=":hourglass_flowing_sand: Analyzing your question...",
    )
    try:
        # Classify once so we only load the relevant schema for both clarity + SQL steps
        schema = get_schema_for_question(question, history)

        # Step 0: clarity check
        clarity = check_clarity(question, history=history, schema=schema)
        if clarity.upper() != "CLEAR":
            client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=f":raising_hand: {clarity}",
            )
            return

        sql = generate_sql(question, history=history, schema=schema)
        logger.info(f"Generated SQL: {sql}")

        query_result = run_query(sql)

        if query_result.startswith("Query error:"):
            client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=f":x: {query_result}\n\n*SQL attempted:*\n```{sql}```",
            )
            return

        answer = generate_answer(question, sql, query_result, history=history)
        response_text = f"{answer}\n\n:mag: *SQL Query:*\n```{sql}```"
        client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=response_text)
    except Exception as e:
        logger.error(f"Error: {e}")
        client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f":x: Sorry, something went wrong: {e}",
        )
