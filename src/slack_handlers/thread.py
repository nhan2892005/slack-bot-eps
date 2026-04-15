import re
import logging

logger = logging.getLogger(__name__)


def fetch_thread_history(client, channel: str, thread_ts: str, exclude_ts: str = None) -> str:
    """Fetch all messages in a Slack thread and format as plain text."""
    try:
        result = client.conversations_replies(channel=channel, ts=thread_ts, limit=50)
        messages = result.get("messages", [])
    except Exception as e:
        logger.error(f"Failed to fetch thread history: {e}")
        return ""

    lines = []
    for m in messages:
        if exclude_ts and m.get("ts") == exclude_ts:
            continue
        text = m.get("text", "").strip()
        if not text:
            continue
        # Skip the "thinking" placeholder messages
        if text.startswith(":hourglass_flowing_sand:"):
            continue
        text = re.sub(r"<@[A-Z0-9]+>", "", text).strip()
        role = "Bot" if m.get("bot_id") else "User"
        lines.append(f"{role}: {text}")
    return "\n".join(lines)
