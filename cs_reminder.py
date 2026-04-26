"""
Entry point: run the Customer Service daily reminder once and exit.

Usage:
  python3 cs_reminder.py            # query, generate, create canvas, post to Slack
  python3 cs_reminder.py --dry-run  # generate only, print canvas markdown to stdout
"""

import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from src.cs_reminder import run_daily_reminder


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    result = run_daily_reminder(post=not dry_run)
    if not dry_run:
        url = result.get("canvas_url")
        if url:
            print(f"Canvas: {url}")
        print(f"Channel message ts: {result.get('channel_message_ts')}")