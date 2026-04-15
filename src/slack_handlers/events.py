import re
import logging
from src.clients import app
from src.config import TRIGGER_EMOJIS
from src.slack_handlers.pipeline import answer_question

logger = logging.getLogger(__name__)


@app.event("app_mention")
def handle_mention(event, client):
    """Handle @bot mentions in Slack."""
    text = event.get("text", "")
    channel = event["channel"]
    thread_ts = event.get("thread_ts", event.get("ts"))

    question = re.sub(r"<@[A-Z0-9]+>", "", text).strip()

    if not question:
        client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text="Please ask me a question! I can query insurance data for you.",
        )
        return

    answer_question(question, channel, thread_ts, client, current_ts=event.get("ts"))


@app.event("reaction_added")
def handle_reaction(event, client):
    """Trigger bot when a user reacts with a specific emoji on a message."""
    if event.get("reaction") not in TRIGGER_EMOJIS:
        return

    item = event.get("item", {})
    if item.get("type") != "message":
        return

    channel = item["channel"]
    msg_ts = item["ts"]

    # Fetch the original message text
    try:
        result = client.conversations_history(
            channel=channel, latest=msg_ts, limit=1, inclusive=True
        )
        messages = result.get("messages", [])
        if not messages:
            return
        original_text = messages[0].get("text", "").strip()
    except Exception as e:
        logger.error(f"Failed to fetch message: {e}")
        return

    # Strip any bot mentions from the text
    question = re.sub(r"<@[A-Z0-9]+>", "", original_text).strip()
    if not question:
        return

    # Reply in the same thread (or start a thread if none)
    thread_ts = messages[0].get("thread_ts", msg_ts)
    answer_question(question, channel, thread_ts, client, current_ts=msg_ts)
