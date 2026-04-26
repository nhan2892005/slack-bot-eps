"""
Customer Service Daily Reminder
================================

A once-a-day operations job for the EPS Customer Service team
(Health-insurance backoffice). The flow is deliberately split into three
deterministic SQL passes plus one LLM synthesis step so the digest stays
auditable:

    1.  BASE_CTE — types raw STRING columns from the Notion-backed external
        table, classifies each task into an SLA tier
        (critical_overdue / overdue / due_today / due_soon / open) and flags
        stalled work (low engagement + no recent edits).
    2.  Three roll-ups feed Claude:
          - per-responsible workload (counts by tier),
          - critical / overdue task details (must-act-today),
          - stalled & unassigned task details (slipping through the cracks).
    3.  Claude turns those tables into a Vietnamese Slack digest, governed by
        an English system prompt that codifies the business semantics.
    4.  We post the digest to a configured Slack channel and (optionally)
        @-mention the manager.

Run:  python3 cs_reminder.py [--dry-run]
"""

import os
import logging
from datetime import datetime

from src.clients import bq_client, claude, app
from src.config import MODEL

logger = logging.getLogger(__name__)

TASK_TABLE = os.environ.get("CS_TASK_TABLE", "eps-470914.eps_data.health_task_raw")
CS_CHANNEL = os.environ.get("CS_REMINDER_CHANNEL")
CS_MANAGER_MENTIONS = os.environ.get("CS_MANAGER_MENTIONS", "").strip()
OVERDUE_LIMIT = int(os.environ.get("CS_OVERDUE_LIMIT", "20"))
STALLED_LIMIT = int(os.environ.get("CS_STALLED_LIMIT", "15"))


# ---------------------------------------------------------------------------
# Business knowledge base — embedded in the LLM system prompt so the model
# reasons with the same definitions the SQL uses.
# ---------------------------------------------------------------------------
TASK_KNOWLEDGE_BASE = """
Domain
  EPS Customer Service team handles back-office support for Health-insurance
  members: enrollment, plan changes, claims follow-up, document collection,
  renewal reminders, escalations from sales agents.

Source of truth
  Tasks live in Notion. An Apps Script mirrors them to BigQuery as the
  external table `eps-470914.eps_data.health_task_raw` (all columns STRING).
  A second table, `eps-470914.eps_data.health_task_pivot`, explodes one row
  per (task × responsible-user) and adds pre-computed flags. This job uses
  raw for task-level analysis.

Field semantics (raw)
  - record_id        Notion page ID. Unique key.
  - tasks            Task title.
  - task_summary     AI-generated short summary.
  - task_category    Bucket: enrollment / claims / document / follow_up / ...
  - agent            Sales agent the task RELATES to. Informational only —
                     NOT the accountable owner.
  - responsible      CS owner accountable for completion. PRIMARY ownership.
                     Empty/blank => the task was never assigned (process gap).
  - due_date         SLA deadline (YYYY-MM-DD HH:MM[:SS]).
  - rating           Manager-rated priority 0..5. Higher = more urgent.
                     >= 4 means same-day must-handle.
  - completed        'Yes' / 'No'.
  - created_time     When the task was created.
  - last_edited_time Last activity timestamp (proxy for engagement).
  - num_comments     Comment count on the Notion thread (engagement signal).

Tier definitions used by this job
  - critical_overdue : open AND due_date < today AND days_overdue >= 3
                       AND emergency_task >= 3
  - overdue          : open AND due_date < today (and not critical)
  - due_today        : open AND due_date = today
  - due_soon         : open AND due_date in (today+1, today+2)
  - open             : open AND due_date > today+2 OR due_date is null
  - stalled          : open AND num_comments <= 1
                       AND days_since_edit >= 7

Rules of interpretation
  • The accountable person is `responsible`. Never blame `agent` for overdue.
  • emergency_task >= 4 with any overdue is a red flag — high priority but
    not being handled.
  • Stalled tasks are the silent risk — they're not always overdue yet, but
    no one is touching them.
  • An empty `responsible` is a process failure. Surface it to the manager
    as an unassigned-queue problem, not as an individual's fault.
"""


SYSTEM_PROMPT = f"""You are the Operations Reminder Assistant for the EPS
Customer Service team. Each morning you produce ONE Slack digest that the
team manager will use to drive the daily standup.

{TASK_KNOWLEDGE_BASE}

WHY-analysis discipline
  For every task you call out, append exactly ONE short WHY sentence whose
  claim is grounded in the numeric signals provided (days_overdue,
  emergency_task, num_comments, days_since_edit, responsible='(unassigned)',
  category). Never invent a cause. Examples of valid WHY phrasings:
    - "emergency=5 nhưng 4d overdue, num_comments=0 — chưa được follow up."
    - "Không edit trong 12 ngày, num_comments=1 — đang bị bỏ quên."
    - "Chưa assign responsible — process gap, cần gán người ngay."
    - "Overdue 1d, emergency=2 — trễ nhẹ nhưng ưu tiên thấp, dồn cuối list."

Output format (Slack-flavored markdown)
  • Use *single asterisks* for bold. Do NOT use **double asterisks** or
    underscores for italics.
  • Use '-' or '•' for bullets.
  • Field names (responsible, due, emergency, days_overdue, etc.) stay in
    English. Narrative text is in Vietnamese.
  • Keep the entire message under ~3000 characters so it fits one Slack post.
  • Do NOT wrap the response in code fences.

Required sections (in this order, with these exact Vietnamese headers)
  *Tổng quan*
    1–2 lines: total open, total overdue (and how many are critical),
    due_today, stalled, plus the responsible person carrying the heaviest
    overdue load. Use plain numbers from the data.

  *Critical — cần xử lý hôm nay*
    Up to 6 lines. Each line:
      - responsible · due=YYYY-MM-DD (Nd overdue) · emergency=X · category · "<task>"
        → WHY: <one short sentence grounded in signals>.

  *Đang bị stuck (stalled / unassigned)*
    Up to 5 lines, same line format. Cover unassigned tasks first, then
    stalled ones with the highest emergency_task / longest days_since_edit.

  *Đề xuất cho manager*
    2–3 concrete bullets. Reference specific people or task counts when
    possible (e.g. "Re-prio NAM: 2 critical overdue cần đóng trước EOD",
    "Assign owner cho 3 task unassigned trong category enrollment").

If a section has no data, write the section header and a single line
"(không có)". Never omit a section.
"""


# ---------------------------------------------------------------------------
# SQL: one CTE that does typing + tier classification + stalled flag.
# Every downstream query selects from this CTE so business definitions stay
# in exactly one place.
# ---------------------------------------------------------------------------
BASE_CTE = f"""
WITH base AS (
  SELECT
    record_id,
    agent,
    tasks,
    task_summary,
    task_category,
    COALESCE(NULLIF(TRIM(responsible), ''), '(unassigned)') AS responsible,
    DATE(COALESCE(
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', due_date),
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', due_date)
    )) AS due_date,
    COALESCE(CAST(SAFE_CAST(rating AS FLOAT64) AS INT64), 0) AS emergency_task,
    CASE WHEN LOWER(completed) = 'yes' THEN 1 ELSE 0 END AS is_completed,
    DATE(COALESCE(
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', last_edited_time),
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', last_edited_time)
    )) AS last_edited_date,
    CAST(SAFE_CAST(num_comments AS FLOAT64) AS INT64) AS num_comments
  FROM `{TASK_TABLE}`
  WHERE tasks IS NOT NULL
),
tiered AS (
  SELECT
    *,
    DATE_DIFF(CURRENT_DATE(), due_date, DAY) AS days_overdue,
    DATE_DIFF(CURRENT_DATE(), last_edited_date, DAY) AS days_since_edit,
    CASE
      WHEN is_completed = 1 THEN 'done'
      WHEN due_date IS NULL THEN 'no_due_date'
      WHEN due_date < CURRENT_DATE()
           AND DATE_DIFF(CURRENT_DATE(), due_date, DAY) >= 3
           AND emergency_task >= 3 THEN 'critical_overdue'
      WHEN due_date < CURRENT_DATE() THEN 'overdue'
      WHEN due_date = CURRENT_DATE() THEN 'due_today'
      WHEN due_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 2 DAY) THEN 'due_soon'
      ELSE 'open'
    END AS tier,
    (
      is_completed = 0
      AND COALESCE(num_comments, 0) <= 1
      AND DATE_DIFF(CURRENT_DATE(), last_edited_date, DAY) >= 7
    ) AS is_stalled
  FROM base
)
"""


def query_workload_by_responsible():
    """One row per CS owner, with workload counts by tier."""
    sql = BASE_CTE + """
    SELECT
      responsible,
      COUNTIF(is_completed = 0) AS open_tasks,
      COUNTIF(is_completed = 0 AND tier = 'critical_overdue') AS critical_overdue,
      COUNTIF(is_completed = 0 AND tier IN ('critical_overdue', 'overdue')) AS overdue_tasks,
      COUNTIF(is_completed = 0 AND tier = 'due_today') AS due_today,
      COUNTIF(is_completed = 0 AND tier = 'due_soon') AS due_soon,
      COUNTIF(is_stalled) AS stalled_tasks,
      COUNTIF(is_completed = 0 AND emergency_task >= 4) AS high_priority_open
    FROM tiered
    GROUP BY responsible
    HAVING open_tasks > 0
    ORDER BY critical_overdue DESC, overdue_tasks DESC, open_tasks DESC
    """
    return [dict(r) for r in bq_client.query(sql).result()]


def query_critical_and_overdue(limit: int = OVERDUE_LIMIT):
    """Detail rows for the LLM to reason about: most critical overdue first."""
    sql = BASE_CTE + f"""
    SELECT
      record_id,
      responsible,
      agent,
      task_category,
      tasks,
      task_summary,
      due_date,
      days_overdue,
      emergency_task,
      num_comments,
      days_since_edit,
      tier
    FROM tiered
    WHERE is_completed = 0
      AND tier IN ('critical_overdue', 'overdue')
    ORDER BY
      CASE tier WHEN 'critical_overdue' THEN 0 ELSE 1 END,
      emergency_task DESC,
      days_overdue DESC
    LIMIT {limit}
    """
    return [dict(r) for r in bq_client.query(sql).result()]


def query_stalled_and_unassigned(limit: int = STALLED_LIMIT):
    """Stalled tasks (low engagement) and unassigned-responsible queue."""
    sql = BASE_CTE + f"""
    SELECT
      record_id,
      responsible,
      task_category,
      tasks,
      task_summary,
      due_date,
      days_overdue,
      emergency_task,
      num_comments,
      days_since_edit,
      CASE
        WHEN responsible = '(unassigned)' THEN 'unassigned'
        ELSE 'stalled'
      END AS reason
    FROM tiered
    WHERE is_completed = 0
      AND (is_stalled OR responsible = '(unassigned)')
    ORDER BY
      CASE WHEN responsible = '(unassigned)' THEN 0 ELSE 1 END,
      emergency_task DESC,
      days_since_edit DESC
    LIMIT {limit}
    """
    return [dict(r) for r in bq_client.query(sql).result()]


# ---------------------------------------------------------------------------
# Render BQ rows into compact tables for the LLM prompt.
# ---------------------------------------------------------------------------
def _render_table(rows, columns) -> str:
    if not rows:
        return "(empty)"
    header = " | ".join(columns)
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(" | ".join(str(r.get(c, "")) for c in columns))
    return "\n".join(lines)


def render_workload(rows) -> str:
    return _render_table(
        rows,
        [
            "responsible",
            "open_tasks",
            "critical_overdue",
            "overdue_tasks",
            "due_today",
            "due_soon",
            "stalled_tasks",
            "high_priority_open",
        ],
    )


def render_overdue(rows) -> str:
    return _render_table(
        rows,
        [
            "tier",
            "responsible",
            "due_date",
            "days_overdue",
            "emergency_task",
            "num_comments",
            "days_since_edit",
            "task_category",
            "tasks",
        ],
    )


def render_stalled(rows) -> str:
    return _render_table(
        rows,
        [
            "reason",
            "responsible",
            "due_date",
            "days_overdue",
            "emergency_task",
            "num_comments",
            "days_since_edit",
            "task_category",
            "tasks",
        ],
    )


# ---------------------------------------------------------------------------
# LLM synthesis.
# ---------------------------------------------------------------------------
def llm_synthesize(workload_tbl: str, overdue_tbl: str, stalled_tbl: str) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    user_msg = f"""Today is {today}. Produce the Slack digest using the data below.

[Workload by responsible]
{workload_tbl}

[Critical and overdue tasks (top {OVERDUE_LIMIT})]
{overdue_tbl}

[Stalled and unassigned tasks (top {STALLED_LIMIT})]
{stalled_tbl}
"""
    response = claude.messages.create(
        model=MODEL,
        max_tokens=2000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    return response.content[0].text.strip()


def build_message() -> str:
    workload_rows = query_workload_by_responsible()
    overdue_rows = query_critical_and_overdue()
    stalled_rows = query_stalled_and_unassigned()
    logger.info(
        "rows: workload=%d overdue=%d stalled=%d",
        len(workload_rows),
        len(overdue_rows),
        len(stalled_rows),
    )
    body = llm_synthesize(
        render_workload(workload_rows),
        render_overdue(overdue_rows),
        render_stalled(stalled_rows),
    )
    today = datetime.now().strftime("%Y-%m-%d")
    header = f":bell: *Customer Service Daily Reminder* — {today}"
    if CS_MANAGER_MENTIONS:
        header += f" — {CS_MANAGER_MENTIONS}"
    return f"{header}\n{body}"


def run_daily_reminder(post: bool = True) -> str:
    message = build_message()
    if post:
        if not CS_CHANNEL:
            raise RuntimeError(
                "CS_REMINDER_CHANNEL is not set. Export it (channel ID or '#name')."
            )
        app.client.chat_postMessage(channel=CS_CHANNEL, text=message)
        logger.info("posted reminder to %s", CS_CHANNEL)
    return message