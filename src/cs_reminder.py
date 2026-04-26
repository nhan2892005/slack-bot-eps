"""

Customer Service Daily Reminder
================================

A once-a-day operations job for the EPS Customer Service team
(Health-insurance backoffice). The flow is:

    1.  BASE_CTE — types raw STRING columns from the Notion-backed external
        table, classifies each task into an SLA tier
        (critical_overdue / overdue / due_today / due_soon / open) and flags
        stalled work (low engagement + no recent edits).
    2.  Three deterministic roll-ups feed Claude:
          - per-responsible workload (counts by tier),
          - critical / overdue task details (must-act-today),
          - stalled & unassigned task details (slipping through the cracks).
    3.  Claude returns a structured JSON report (exec_summary, critical[],
        stuck[], manager_actions[]). All numeric stats come from BigQuery —
        Claude only narrates and explains WHY each task is stuck.
    4.  We render the JSON into TWO Slack surfaces:
          - a rich Canvas (full report — tables, headings, callouts) for the
            manager to read end-to-end,
          - a Block Kit message in the channel with the headline KPIs, top
            critical preview, and a "View full report" button to the canvas.

Run:  python3 cs_reminder.py [--dry-run]
"""

import json
import os
import re
import logging
from datetime import datetime
from typing import Optional

from src.clients import bq_client, claude, app
from src.config import MODEL

logger = logging.getLogger(__name__)

TASK_TABLE = os.environ.get("CS_TASK_TABLE", "eps-470914.eps_data.health_task_raw")
CS_CHANNEL = os.environ.get("CS_REMINDER_CHANNEL")
CS_MANAGER_MENTIONS = os.environ.get("CS_MANAGER_MENTIONS", "").strip()
OVERDUE_LIMIT = int(os.environ.get("CS_OVERDUE_LIMIT", "20"))
STALLED_LIMIT = int(os.environ.get("CS_STALLED_LIMIT", "15"))
WORKLOAD_TABLE_LIMIT = int(os.environ.get("CS_WORKLOAD_TABLE_LIMIT", "15"))


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

Field semantics (raw)
  - record_id        Notion page ID. Unique key.
  - tasks            Task title.
  - task_summary     AI-generated short summary.
  - task_category    Bucket: enrollment / claims / document / follow_up / ...
  - agent            Sales agent the task RELATES to. Informational only —
                     NOT the accountable owner.
  - responsible      CS owner accountable for completion. PRIMARY ownership.
                     Empty/blank => the task was never assigned (process gap).
  - due_date         SLA deadline.
  - rating           Manager-rated priority 0..5. >=4 means same-day must-handle.
  - completed        'Yes' / 'No'.
  - num_comments     Comment count on the Notion thread (engagement signal).
  - last_edited_time Last activity timestamp (proxy for engagement).

Tier definitions used by this job
  - critical_overdue : open AND days_overdue >= 3 AND emergency_task >= 3
  - overdue          : open AND due_date < today (and not critical)
  - due_today        : open AND due_date = today
  - stalled          : open AND num_comments <= 1 AND days_since_edit >= 7

Rules of interpretation
  • The accountable person is `responsible`. Never blame `agent` for overdue.
  • emergency_task >= 4 with any overdue is a red flag.
  • Stalled tasks are the silent risk — not always overdue yet, but unattended.
  • An empty `responsible` is a process failure. Surface to manager as a
    queue problem, not as an individual's fault.
"""

# Claude returns ONE JSON object matching this schema. We render it into
# Canvas markdown and Block Kit blocks downstream.
JSON_SCHEMA_DOC = """{
  "exec_summary": "1-2 Vietnamese sentences. Reference total open, total overdue (and how many are critical), due_today, stalled, and the responsible person carrying the heaviest overdue load.",
  "critical": [
    {
      "responsible": "string",
      "due": "YYYY-MM-DD",
      "days_overdue": <int>,
      "emergency": <int 0..5>,
      "category": "string",
      "task": "short Vietnamese title (truncate to ~80 chars)",
      "why": "ONE Vietnamese sentence grounded in the numeric signals (days_overdue, emergency, num_comments, days_since_edit)."
    }
  ],
  "stuck": [
    {
      "responsible": "string or '(unassigned)'",
      "reason": "unassigned" | "stalled",
      "due": "YYYY-MM-DD or null",
      "days_overdue": <int, may be negative if not overdue yet>,
      "emergency": <int 0..5>,
      "category": "string",
      "task": "short Vietnamese title",
      "why": "ONE Vietnamese sentence grounded in signals."
    }
  ],
  "manager_actions": [
    "Concrete bullet referencing a specific person or task count (Vietnamese)."
  ]
}"""


SYSTEM_PROMPT = f"""You are the Operations Reminder Assistant for the EPS
Customer Service team. Each morning you produce ONE structured JSON report
that downstream code renders into a Slack canvas + channel message for the
team manager.

{TASK_KNOWLEDGE_BASE}

Output contract
  Return EXACTLY ONE JSON object matching this shape, with no prose, no
  preamble, and no markdown code fences:

{JSON_SCHEMA_DOC}

Selection rules
  - critical[]: pick up to 6 tasks from the "Critical and overdue" input,
    prioritising tier=critical_overdue, then highest emergency, then largest
    days_overdue.
  - stuck[]: pick up to 5 tasks from the "Stalled and unassigned" input.
    Place reason='unassigned' items first.
  - manager_actions[]: 2 to 4 concrete bullets. Each must reference either a
    specific person, a specific count, or a specific category.

WHY-analysis discipline
  Each `why` is exactly ONE short Vietnamese sentence whose claim is grounded
  in numeric signals from the data row. Never invent a cause. Examples:
    - "emergency=5 nhưng 4d overdue, num_comments=0 — chưa được follow up."
    - "Không edit trong 12 ngày, num_comments=1 — đang bị bỏ quên."
    - "Chưa assign responsible — process gap, cần gán người ngay."

Language
  - Narrative fields (exec_summary, task, why, manager_actions) are in
    Vietnamese. Field-name-like terms (responsible, due, emergency, category)
    stay in English in prose.
  - Do not include emojis in JSON values — the renderer adds them.
"""


# ---------------------------------------------------------------------------
# SQL: one CTE that does typing + tier classification + stalled flag.
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
    sql = BASE_CTE + f"""
    SELECT
      record_id, responsible, agent, task_category, tasks, task_summary,
      due_date, days_overdue, emergency_task, num_comments, days_since_edit, tier
    FROM tiered
    WHERE is_completed = 0 AND tier IN ('critical_overdue', 'overdue')
    ORDER BY
      CASE tier WHEN 'critical_overdue' THEN 0 ELSE 1 END,
      emergency_task DESC, days_overdue DESC
    LIMIT {limit}
    """
    return [dict(r) for r in bq_client.query(sql).result()]


def query_stalled_and_unassigned(limit: int = STALLED_LIMIT):
    sql = BASE_CTE + f"""
    SELECT
      record_id, responsible, task_category, tasks, task_summary,
      due_date, days_overdue, emergency_task, num_comments, days_since_edit,
      CASE WHEN responsible = '(unassigned)' THEN 'unassigned' ELSE 'stalled' END AS reason
    FROM tiered
    WHERE is_completed = 0 AND (is_stalled OR responsible = '(unassigned)')
    ORDER BY
      CASE WHEN responsible = '(unassigned)' THEN 0 ELSE 1 END,
      emergency_task DESC, days_since_edit DESC
    LIMIT {limit}
    """
    return [dict(r) for r in bq_client.query(sql).result()]


# ---------------------------------------------------------------------------
# Stats are computed deterministically from BigQuery, not by Claude.
# ---------------------------------------------------------------------------
def compute_stats(workload_rows):
    if not workload_rows:
        return {
            "open": 0, "overdue": 0, "critical": 0, "due_today": 0,
            "stalled": 0, "high_priority_open": 0, "top_overdue_owner": None,
        }
    total_open = sum(r["open_tasks"] for r in workload_rows)
    total_overdue = sum(r["overdue_tasks"] for r in workload_rows)
    total_critical = sum(r["critical_overdue"] for r in workload_rows)
    total_due_today = sum(r["due_today"] for r in workload_rows)
    total_stalled = sum(r["stalled_tasks"] for r in workload_rows)
    total_high_prio = sum(r["high_priority_open"] for r in workload_rows)
    # workload_rows already sorted by critical_overdue DESC, overdue_tasks DESC
    top_owner = workload_rows[0]["responsible"] if workload_rows[0]["overdue_tasks"] > 0 else None
    return {
        "open": total_open,
        "overdue": total_overdue,
        "critical": total_critical,
        "due_today": total_due_today,
        "stalled": total_stalled,
        "high_priority_open": total_high_prio,
        "top_overdue_owner": top_owner,
    }


# ---------------------------------------------------------------------------
# LLM I/O: render BQ rows into compact tables, ask Claude for JSON, parse.
# ---------------------------------------------------------------------------
def _render_table(rows, columns) -> str:
    if not rows:
        return "(empty)"
    header = " | ".join(columns)
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(" | ".join(str(r.get(c, "")) for c in columns))
    return "\n".join(lines)


def _strip_code_fence(text: str) -> str:
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def llm_synthesize_json(workload_rows, overdue_rows, stalled_rows) -> dict:
    today = datetime.now().strftime("%Y-%m-%d")
    user_msg = f"""Today is {today}. Produce the daily JSON report from the data below.

[Workload by responsible]
{_render_table(workload_rows, [
    "responsible", "open_tasks", "critical_overdue", "overdue_tasks",
    "due_today", "due_soon", "stalled_tasks", "high_priority_open",
])}

[Critical and overdue tasks (top {OVERDUE_LIMIT})]
{_render_table(overdue_rows, [
    "tier", "responsible", "due_date", "days_overdue", "emergency_task",
    "num_comments", "days_since_edit", "task_category", "tasks",
])}

[Stalled and unassigned tasks (top {STALLED_LIMIT})]
{_render_table(stalled_rows, [
    "reason", "responsible", "due_date", "days_overdue", "emergency_task",
    "num_comments", "days_since_edit", "task_category", "tasks",
])}

Return ONLY the JSON object.
"""
    response = claude.messages.create(
        model=MODEL,
        max_tokens=2500,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = response.content[0].text
    cleaned = _strip_code_fence(raw)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error("JSON parse failed: %s\nRaw output:\n%s", e, raw[:2000])
        raise


# ---------------------------------------------------------------------------
# Renderer 1 — Slack Canvas (GitHub-flavored markdown, the long-form report).
# ---------------------------------------------------------------------------
def render_canvas_markdown(report: dict, stats: dict, workload_rows, today: str) -> str:
    lines = []
    lines.append(f"# 🔔 Customer Service Daily Reminder")
    lines.append(f"**{today}**")
    lines.append("")
    lines.append(f"> {report.get('exec_summary', '').strip()}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # KPI row
    lines.append("## 📊 KPIs")
    lines.append("")
    lines.append("| Metric | Count |")
    lines.append("|---|---:|")
    lines.append(f"| Total open | **{stats['open']}** |")
    lines.append(f"| Overdue | **{stats['overdue']}** |")
    lines.append(f"| 🔥 Critical (≥3d overdue · emergency≥3) | **{stats['critical']}** |")
    lines.append(f"| Due today | {stats['due_today']} |")
    lines.append(f"| Stalled (no engagement 7d+) | {stats['stalled']} |")
    lines.append(f"| High priority open (emergency≥4) | {stats['high_priority_open']} |")
    lines.append("")

    # Workload by responsible
    lines.append("## 👥 Workload by responsible")
    lines.append("")
    lines.append("| Responsible | Open | 🔥 Critical | Overdue | Today | Stalled | Hi-prio |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for r in workload_rows[:WORKLOAD_TABLE_LIMIT]:
        name = r["responsible"]
        if r["critical_overdue"] > 0 or r["overdue_tasks"] >= 3:
            name = f"**{name}**"
        lines.append(
            f"| {name} | {r['open_tasks']} | {r['critical_overdue']} | "
            f"{r['overdue_tasks']} | {r['due_today']} | "
            f"{r['stalled_tasks']} | {r['high_priority_open']} |"
        )
    lines.append("")

    # Critical
    lines.append("## 🔥 Critical — phải xử lý hôm nay")
    lines.append("")
    critical = report.get("critical") or []
    if not critical:
        lines.append("_Không có task critical._")
        lines.append("")
    else:
        for i, t in enumerate(critical, 1):
            lines.append(f"### {i}. {t['responsible']} — {t.get('category', '')}")
            lines.append(f"**Task:** {t['task']}  ")
            lines.append(
                f"**Due:** {t['due']} · **{t['days_overdue']}d overdue** · "
                f"**emergency={t['emergency']}**"
            )
            lines.append("")
            lines.append(f"> 🧠 **WHY:** {t['why']}")
            lines.append("")

    # Stuck
    lines.append("## 🚧 Đang bị stuck")
    lines.append("")
    stuck = report.get("stuck") or []
    if not stuck:
        lines.append("_Không có task stuck._")
        lines.append("")
    else:
        for t in stuck:
            tag = "🆕 **UNASSIGNED**" if t.get("reason") == "unassigned" else "⏸ **STALLED**"
            owner = t["responsible"] if t["responsible"] != "(unassigned)" else "—"
            lines.append(f"### {tag} · {owner} · {t.get('category', '')}")
            lines.append(f"**Task:** {t['task']}  ")
            due_part = t.get("due") or "no due date"
            days = t.get("days_overdue")
            if isinstance(days, int) and days > 0:
                due_part += f" · {days}d overdue"
            lines.append(f"**Due:** {due_part} · emergency={t['emergency']}")
            lines.append("")
            lines.append(f"> 🧠 **WHY:** {t['why']}")
            lines.append("")

    # Manager actions
    lines.append("## ✅ Đề xuất cho manager")
    lines.append("")
    actions = report.get("manager_actions") or []
    if not actions:
        lines.append("_Không có đề xuất._")
    else:
        for i, a in enumerate(actions, 1):
            lines.append(f"{i}. {a}")
    lines.append("")

    lines.append("---")
    lines.append(
        f"_Generated at {datetime.now().strftime('%Y-%m-%d %H:%M')} · "
        f"source: `{TASK_TABLE}` · model: `{MODEL}`._"
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Renderer 2 — Block Kit channel notification (the eye-catching summary).
# ---------------------------------------------------------------------------
def _truncate(s: str, n: int) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


def render_channel_blocks(
    report: dict,
    stats: dict,
    today: str,
    canvas_url: Optional[str],
    mentions: str,
) -> list:
    blocks = []

    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": f"🔔 CS Daily Reminder — {today}", "emoji": True},
    })

    summary_text = report.get("exec_summary", "").strip() or "(no summary)"
    if mentions:
        summary_text = f"{mentions}\n{summary_text}"
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": summary_text},
    })

    blocks.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*Open*\n{stats['open']}"},
            {"type": "mrkdwn", "text": f"*Overdue*\n{stats['overdue']}  _({stats['critical']} 🔥 critical)_"},
            {"type": "mrkdwn", "text": f"*Due today*\n{stats['due_today']}"},
            {"type": "mrkdwn", "text": f"*Stalled*\n{stats['stalled']}"},
        ],
    })

    blocks.append({"type": "divider"})

    critical = report.get("critical") or []
    if critical:
        items = []
        for t in critical[:3]:
            items.append(
                f"• `{t['responsible']}` — {_truncate(t['task'], 80)} "
                f"_(em={t['emergency']} · {t['days_overdue']}d overdue)_"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*🔥 Top critical*\n" + "\n".join(items)},
        })

    stuck = report.get("stuck") or []
    if stuck:
        items = []
        for t in stuck[:3]:
            tag = "🆕" if t.get("reason") == "unassigned" else "⏸"
            owner = t["responsible"] if t["responsible"] != "(unassigned)" else "unassigned"
            items.append(
                f"• {tag} `{owner}` — {_truncate(t['task'], 80)} _(em={t['emergency']})_"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*🚧 Stuck*\n" + "\n".join(items)},
        })

    actions = report.get("manager_actions") or []
    if actions:
        text = "*✅ Đề xuất cho manager*\n" + "\n".join(f"• {a}" for a in actions)
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": text},
        })

    if canvas_url:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "📋 Xem báo cáo đầy đủ", "emoji": True},
                "url": canvas_url,
                "style": "primary",
            }],
        })

    return blocks


# ---------------------------------------------------------------------------
# Slack canvas creation. Requires `canvases:write` scope on the bot token.
# Returns the canvas permalink, or None if creation/sharing fails.
# ---------------------------------------------------------------------------
def create_and_share_canvas(title: str, markdown: str, channel_id: str) -> Optional[str]:
    try:
        resp = app.client.canvases_create(
            title=title,
            document_content={"type": "markdown", "markdown": markdown},
        )
    except Exception as e:
        logger.warning("canvases.create failed (missing scope `canvases:write`?): %s", e)
        return None

    canvas_id = resp.get("canvas_id")
    if not canvas_id:
        logger.warning("canvases.create returned no canvas_id: %s", resp)
        return None

    try:
        app.client.canvases_access_set(
            canvas_id=canvas_id,
            access_level="read",
            channel_ids=[channel_id],
        )
    except Exception as e:
        logger.warning("canvases.access.set failed: %s", e)

    try:
        info = app.client.files_info(file=canvas_id)
        return info.get("file", {}).get("permalink")
    except Exception as e:
        logger.warning("files.info failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Orchestration.
# ---------------------------------------------------------------------------
def run_daily_reminder(post: bool = True) -> dict:
    workload_rows = query_workload_by_responsible()
    overdue_rows = query_critical_and_overdue()
    stalled_rows = query_stalled_and_unassigned()
    logger.info(
        "rows: workload=%d overdue=%d stalled=%d",
        len(workload_rows), len(overdue_rows), len(stalled_rows),
    )

    stats = compute_stats(workload_rows)
    report = llm_synthesize_json(workload_rows, overdue_rows, stalled_rows)
    today = datetime.now().strftime("%Y-%m-%d")
    canvas_md = render_canvas_markdown(report, stats, workload_rows, today)

    if not post:
        # Dry-run: print the canvas markdown so it can be previewed locally.
        print(canvas_md)
        return {"stats": stats, "report": report, "canvas_markdown": canvas_md}

    if not CS_CHANNEL:
        raise RuntimeError("CS_REMINDER_CHANNEL is not set.")

    canvas_url = create_and_share_canvas(
        title=f"CS Daily Reminder — {today}",
        markdown=canvas_md,
        channel_id=CS_CHANNEL,
    )

    blocks = render_channel_blocks(report, stats, today, canvas_url, CS_MANAGER_MENTIONS)
    fallback_text = f"CS Daily Reminder — {today}: {report.get('exec_summary', '')}"

    msg_resp = app.client.chat_postMessage(
        channel=CS_CHANNEL,
        blocks=blocks,
        text=fallback_text,
    )
    logger.info("posted notification to %s", CS_CHANNEL)

    # If canvas wasn't created (missing scope etc.), post the full markdown
    # as a thread reply so the report is still accessible.
    if not canvas_url:
        try:
            app.client.chat_postMessage(
                channel=CS_CHANNEL,
                thread_ts=msg_resp["ts"],
                text=canvas_md[:39000],
            )
            logger.info("posted full markdown as thread fallback")
        except Exception as e:
            logger.warning("thread fallback failed: %s", e)

    return {
        "stats": stats,
        "report": report,
        "canvas_url": canvas_url,
        "channel_message_ts": msg_resp.get("ts"),
    }