import os

DATASET = os.environ["BQ_DATASET"]
PROJECT = os.environ["GCP_PROJECT_ID"]
MODEL = "claude-sonnet-4-5"

# Emoji name(s) that trigger the bot when used as a reaction
TRIGGER_EMOJIS = {"capybara"}
