"""
Entry point: run the Customer Service daily reminder once and exit.

Usage:
  python3 cs_reminder.py            # query, generate, post to Slack
  python3 cs_reminder.py --dry-run  # generate only, print to stdout, do NOT post
"""

import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from src.cs_reminder import run_daily_reminder


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    msg = run_daily_reminder(post=not dry_run)
    print(msg)