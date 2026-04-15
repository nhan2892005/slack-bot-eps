import os
import logging

logging.basicConfig(level=logging.INFO)

from slack_bolt.adapter.socket_mode import SocketModeHandler
from src.clients import app
# Import handlers module to register @app.event decorators
from src.slack_handlers import events  # noqa: F401


if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    print("⚡ EPS Bot is running!")
    handler.start()
