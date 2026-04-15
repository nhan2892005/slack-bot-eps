import os
from slack_bolt import App
import anthropic
from google.cloud import bigquery
import google.auth
from google.auth import impersonated_credentials

# --- Clients ---
app = App(token=os.environ["SLACK_BOT_TOKEN"])
claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# BigQuery client using SA impersonation (so we get Drive scope without SA keys)
source_credentials, _ = google.auth.default()
target_sa = os.environ["BQ_IMPERSONATE_SA"]
credentials = impersonated_credentials.Credentials(
    source_credentials=source_credentials,
    target_principal=target_sa,
    target_scopes=[
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/cloud-platform",
    ],
    lifetime=3600,
)
bq_client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"], credentials=credentials)
