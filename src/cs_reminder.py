"""
Customer Service Daily Reminder — Enhanced v2
==============================================

New in v2:
  - Per-AGENT breakdown section: each sales agent gets a dedicated insight paragraph
    explaining their backlog status, overdue reasons, and risk level for the manager.
  - Per-CS-STAFF analysis: who is overloaded, why, and coordination gaps from
    shared-ownership tasks.
  - Pattern detection injected into LLM context: category hotspots, recurring
    blockers, tasks with no activity and no owner.
  - PDF: new "Agent Breakdown" section after team analysis.
  - Slack: agent-level KPI fields in the channel message.

Usage:
  python3 cs_reminder.py            # run full flow and post to Slack
  python3 cs_reminder.py --dry-run  # generate PDF locally only
"""

import io
import json
import logging
import os
import re
import tempfile
from collections import defaultdict
from datetime import datetime
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from src.clients import app, bq_client, claude
from src.config import MODEL

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AGENT_FILTER = os.environ.get("CS_AGENT_FILTER", "")          # empty = all agents
TASK_TABLE = os.environ.get("CS_TASK_TABLE", "eps-470914.eps_data.health_task_raw")
CS_CHANNEL = os.environ.get("CS_REMINDER_CHANNEL")
CS_MANAGER_MENTIONS = os.environ.get("CS_MANAGER_MENTIONS", "").strip()
OVERDUE_LIMIT = int(os.environ.get("CS_OVERDUE_LIMIT", "25"))
STALLED_LIMIT = int(os.environ.get("CS_STALLED_LIMIT", "15"))

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
_agent_filter_context = (
    f"The report covers only tasks where agent = '{AGENT_FILTER}'."
    if AGENT_FILTER else
    "The report covers ALL sales agents."
)

SYSTEM_PROMPT = f"""You are a senior operations analyst for EPS, a Vietnamese-American
health insurance brokerage. Your task is to produce a daily briefing for the CS team
manager about the Customer Service backlog.

{_agent_filter_context}

DOMAIN KNOWLEDGE
  - CS tasks come from Notion (mirrored to BigQuery). Each task has a Slack thread
    (comments_json) that records the actual work conversation between CS staff.
    Staff often write in Vietnamese — read and interpret those threads accurately.
  - The 'responsible' field is the CS staff accountable for completion.
  - 'agent' is the sales agent the task RELATES to — informational only.
  - Task categories: Scheduling Appointment, Verify Insurance/Network, Resolve Billing
    Issue, Submit/Follow-up Referral, Call Doctor Office, Call Insurance Company,
    Document Processing, Update Client Info, etc.
  - Emergency rating 0–5: 4+ means must handle today. 0 = low urgency.
  - "Critical overdue": open AND days_overdue >= 3 AND emergency >= 3.
  - "Stalled": open AND num_comments <= 1 AND days_since_edit >= 7.
  - Multi-person responsible fields (e.g. "Kay Huynh, Dung Ha") indicate shared
    ownership — a coordination risk that often leads to tasks falling through.

WHAT MAKES A GOOD ANALYSIS
  - Read comment threads carefully. They reveal the real status: "waiting for callback",
    "referral not received", "client hasn't confirmed", etc.
  - Comment threads are in Vietnamese — translate key facts into English in your output.
  - Identify the SPECIFIC blocker, not just that a task is overdue.
  - Surface patterns: one clinic causing repeated referral delays, one CS staff
    consistently having stalled tasks, shared-ownership tasks going cold.
  - Be concrete: name specific people, categories, counts. The manager reads this
    to make DECISIONS.
  - Even when there are ZERO overdue tasks: analyse the full backlog. Are tasks
    progressing (high avg_comments, recent edits) or are they dormant (low comments,
    high max_days_idle)? Is the queue growing? Are shared-ownership tasks at risk?
    Is throughput (completed_last_7d) keeping pace with open count?
  - For narrative fields (team_analysis, agent_breakdown items): write explanatory
    paragraphs. DO NOT use bullet lists inside JSON strings. Write in flowing prose.

JSON SAFETY RULES (CRITICAL — violations cause parse errors):
  - Never use unescaped double-quotes inside a JSON string value.
  - Never put literal newlines inside a JSON string value (use a space instead).
  - Never use smart quotes (" " ' ') — ASCII only.
  - Every string value must be on a single line within the JSON.
  - Close every array and object before ending the response.

OUTPUT FORMAT — Return exactly ONE JSON object. No prose. No markdown fences.

{{
  "executive_summary": "2–3 sentences. State total open, total overdue (with critical
    count), due today, stalled. Name the single biggest risk and why.",

  "agent_breakdown": [
    {{
      "agent": "Sales agent name",
      "open": <int>,
      "overdue": <int>,
      "critical": <int>,
      "due_today": <int>,
      "stalled": <int>,
      "top_categories": ["category1", "category2"],
      "status_insight": "2–3 sentence paragraph. What is the real state of this agent's
        backlog? Why are tasks overdue — is it a systemic blocker (e.g. a slow clinic),
        a staffing gap, or communication breakdown? Name the responsible CS staff
        carrying this load. Flag coordination risk if multiple people share tasks.",
      "manager_action": "One concrete sentence: what should the manager do today
        specifically for this agent's tasks?"
    }}
  ],

  "cs_staff_analysis": [
    {{
      "name": "CS staff name",
      "open_tasks": <int>,
      "overdue_tasks": <int>,
      "critical_overdue": <int>,
      "stalled_tasks": <int>,
      "dominant_category": "category with most of their open tasks",
      "load_assessment": "overloaded | manageable | light",
      "insight": "1–2 sentences. What does this person's queue actually look like?
        Are they progressing tasks or are things sitting? Note any shared-ownership
        tasks in their queue that are likely to stall."
    }}
  ],

  "critical_tasks": [
    {{
      "task": "Client name or short task title (max 80 chars)",
      "agent": "Sales agent name",
      "responsible": "CS staff name",
      "category": "task category",
      "due": "YYYY-MM-DD",
      "days_overdue": <int>,
      "emergency": <int 0-5>,
      "current_status": "What has been done, drawn from comment thread.",
      "blocker": "The specific blocker right now.",
      "recommended_action": "Concrete action: who does what."
    }}
  ],

  "stuck_tasks": [
    {{
      "task": "...",
      "agent": "...",
      "responsible": "... or '(unassigned)'",
      "reason": "unassigned | stalled | shared_owner_inactive",
      "days_since_activity": <int>,
      "emergency": <int 0-5>,
      "category": "...",
      "analysis": "WHY this is stuck, grounded in data.",
      "recommended_action": "Specific next step with owner named."
    }}
  ],

  "pattern_alerts": [
    {{
      "pattern": "Short label, e.g. 'Referral delays at one clinic'",
      "count": <int — number of tasks affected>,
      "impact": "One sentence explaining the operational impact.",
      "recommendation": "One sentence with a systemic fix."
    }}
  ],

  "team_analysis": "One paragraph (4–6 sentences). Who is overloaded? Who has capacity?
    Where is shared-ownership hurting throughput? One concrete staffing recommendation.
    Reference specific staff names.",

  "risk_summary": "One sentence naming the single highest systemic risk today.",

  "priority_actions": [
    "Specific, actionable item referencing a person, count, or category. Max 4 items."
  ]
}}

Rules:
  - agent_breakdown: one entry per agent present in the data.
  - cs_staff_analysis: top 6 CS staff by open task count.
  - critical_tasks: up to 6 items. Prioritise critical_overdue tier, then highest
    emergency, then largest days_overdue.
  - stuck_tasks: up to 5 items. Unassigned or shared_owner_inactive first.
  - pattern_alerts: up to 4 patterns. Only include if count >= 2.
  - Every factual claim grounded in data — never invent.
  - All output fields in English.
"""

# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------
def _agent_filter_clause() -> str:
    if AGENT_FILTER:
        return f"AND TRIM(agent) = '{AGENT_FILTER}'"
    return ""


def _base_cte() -> str:
    return f"""
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
    CAST(SAFE_CAST(num_comments AS FLOAT64) AS INT64) AS num_comments,
    comments_json
  FROM `{TASK_TABLE}`
  WHERE tasks IS NOT NULL
    {_agent_filter_clause()}
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
    ) AS is_stalled,
    ARRAY_LENGTH(SPLIT(responsible, ',')) > 1 AS is_shared_owner
  FROM base
)
"""


def query_workload_by_responsible() -> list:
    sql = _base_cte() + """
    SELECT
      responsible,
      COUNTIF(is_completed = 0) AS open_tasks,
      COUNTIF(is_completed = 0 AND tier = 'critical_overdue') AS critical_overdue,
      COUNTIF(is_completed = 0 AND tier IN ('critical_overdue','overdue')) AS overdue_tasks,
      COUNTIF(is_completed = 0 AND tier = 'due_today') AS due_today,
      COUNTIF(is_completed = 0 AND tier = 'due_soon') AS due_soon,
      COUNTIF(is_stalled) AS stalled_tasks,
      COUNTIF(is_completed = 0 AND emergency_task >= 4) AS high_priority_open,
      COUNTIF(is_completed = 0 AND is_shared_owner) AS shared_owner_tasks
    FROM tiered
    GROUP BY responsible
    HAVING open_tasks > 0
    ORDER BY critical_overdue DESC, overdue_tasks DESC, open_tasks DESC
    """
    return [dict(r) for r in bq_client.query(sql).result()]


def query_workload_by_agent() -> list:
    """Per-agent summary including top categories."""
    sql = _base_cte() + """
    SELECT
      agent,
      COUNTIF(is_completed = 0) AS open_tasks,
      COUNTIF(is_completed = 0 AND tier = 'critical_overdue') AS critical_overdue,
      COUNTIF(is_completed = 0 AND tier IN ('critical_overdue','overdue')) AS overdue_tasks,
      COUNTIF(is_completed = 0 AND tier = 'due_today') AS due_today,
      COUNTIF(is_stalled) AS stalled_tasks,
      COUNTIF(is_completed = 0 AND is_shared_owner) AS shared_owner_tasks,
      -- top category by count (approximate: most frequent category)
      APPROX_TOP_COUNT(IF(is_completed=0, task_category, NULL), 3) AS top_categories_struct
    FROM tiered
    WHERE agent IS NOT NULL AND TRIM(agent) != ''
    GROUP BY agent
    ORDER BY overdue_tasks DESC, open_tasks DESC
    """
    rows = []
    for r in bq_client.query(sql).result():
        d = dict(r)
        # Flatten APPROX_TOP_COUNT result
        top_cats = []
        if d.get('top_categories_struct'):
            for item in d['top_categories_struct']:
                if item.get('value'):
                    top_cats.append(item['value'])
        d['top_categories'] = top_cats
        d.pop('top_categories_struct', None)
        rows.append(d)
    return rows


def query_patterns() -> dict:
    """Detect category hotspots, shared-owner stalls, and unassigned clusters."""
    sql = _base_cte() + """
    SELECT
      task_category,
      responsible,
      is_shared_owner,
      COUNT(*) AS task_count,
      COUNTIF(is_completed=0 AND tier IN ('critical_overdue','overdue')) AS overdue_count,
      COUNTIF(is_stalled) AS stalled_count,
      COUNTIF(is_completed=0 AND responsible='(unassigned)') AS unassigned_count
    FROM tiered
    WHERE is_completed = 0
    GROUP BY task_category, responsible, is_shared_owner
    ORDER BY overdue_count DESC, task_count DESC
    LIMIT 50
    """
    rows = [dict(r) for r in bq_client.query(sql).result()]

    # Roll up category-level overdue counts
    cat_overdue = defaultdict(int)
    cat_stalled = defaultdict(int)
    shared_owner_overdue = 0
    unassigned_total = 0

    for r in rows:
        cat = r.get('task_category', 'unknown') or 'unknown'
        cat_overdue[cat] += r.get('overdue_count', 0)
        cat_stalled[cat] += r.get('stalled_count', 0)
        if r.get('is_shared_owner'):
            shared_owner_overdue += r.get('overdue_count', 0)
        unassigned_total += r.get('unassigned_count', 0)

    return {
        'category_overdue': dict(sorted(cat_overdue.items(), key=lambda x: -x[1])),
        'category_stalled': dict(sorted(cat_stalled.items(), key=lambda x: -x[1])),
        'shared_owner_overdue': shared_owner_overdue,
        'unassigned_total': unassigned_total,
    }


def query_critical_and_overdue(limit: int = OVERDUE_LIMIT) -> list:
    sql = _base_cte() + f"""
    SELECT
      record_id, agent, responsible, task_category, tasks, task_summary,
      due_date, days_overdue, emergency_task, num_comments, days_since_edit,
      tier, is_shared_owner, comments_json
    FROM tiered
    WHERE is_completed = 0 AND tier IN ('critical_overdue','overdue')
    ORDER BY
      CASE tier WHEN 'critical_overdue' THEN 0 ELSE 1 END,
      emergency_task DESC, days_overdue DESC
    LIMIT {limit}
    """
    return [dict(r) for r in bq_client.query(sql).result()]


def query_open_tasks_full_summary() -> dict:
    """Full breakdown of ALL open tasks — feeds the LLM full-picture context."""
    # Category x responsible breakdown (all open, not just problem tasks)
    sql_cat = _base_cte() + """
    SELECT
      task_category,
      responsible,
      agent,
      COUNT(*) AS cnt,
      COUNTIF(is_stalled) AS stalled,
      COUNTIF(is_shared_owner) AS shared,
      COUNTIF(tier IN ('critical_overdue','overdue')) AS overdue,
      COUNTIF(tier = 'due_today') AS due_today,
      COUNTIF(tier = 'due_soon') AS due_soon,
      COUNTIF(tier = 'no_due_date') AS no_due_date,
      AVG(COALESCE(num_comments, 0)) AS avg_comments,
      MAX(days_since_edit) AS max_days_idle
    FROM tiered
    WHERE is_completed = 0
    GROUP BY task_category, responsible, agent
    ORDER BY overdue DESC, cnt DESC
    LIMIT 120
    """
    cat_rows = [dict(r) for r in bq_client.query(sql_cat).result()]

    # Velocity: tasks completed in last 7 days vs created in last 7 days (throughput signal)
    sql_velocity = _base_cte() + """
    SELECT
      responsible,
      COUNTIF(is_completed = 1 AND last_edited_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AS completed_last_7d,
      COUNTIF(is_completed = 0) AS still_open
    FROM tiered
    GROUP BY responsible
    HAVING completed_last_7d > 0 OR still_open > 0
    ORDER BY completed_last_7d DESC
    LIMIT 30
    """
    velocity_rows = [dict(r) for r in bq_client.query(sql_velocity).result()]

    return {'category_breakdown': cat_rows, 'velocity': velocity_rows}


def query_stalled_and_unassigned(limit: int = STALLED_LIMIT) -> list:
    sql = _base_cte() + f"""
    SELECT
      record_id, agent, responsible, task_category, tasks, task_summary,
      due_date, days_overdue, emergency_task, num_comments, days_since_edit,
      is_shared_owner,
      CASE
        WHEN responsible = '(unassigned)' THEN 'unassigned'
        WHEN is_shared_owner AND num_comments <= 1 THEN 'shared_owner_inactive'
        ELSE 'stalled'
      END AS reason,
      comments_json
    FROM tiered
    WHERE is_completed = 0 AND (is_stalled OR responsible = '(unassigned)')
    ORDER BY
      CASE WHEN responsible = '(unassigned)' THEN 0
           WHEN is_shared_owner THEN 1
           ELSE 2 END,
      emergency_task DESC, days_since_edit DESC
    LIMIT {limit}
    """
    return [dict(r) for r in bq_client.query(sql).result()]


# ---------------------------------------------------------------------------
# Data enrichment
# ---------------------------------------------------------------------------
_SLACK_MENTION = re.compile(r'<@[A-Z0-9]+>')
_SLACK_PHONE = re.compile(r'<tel:[^|]+\|([^>]+)>')
_SLACK_URL = re.compile(r'<https?://[^|>]+\|([^>]+)>')


def _clean_slack_text(text: str) -> str:
    text = _SLACK_MENTION.sub('', text)
    text = _SLACK_PHONE.sub(r'\1', text)
    text = _SLACK_URL.sub(r'\1', text)
    return text.strip()


def _parse_comments(raw) -> list:
    if not raw:
        return []
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _comment_context(comments: list, max_comments: int = 6) -> str:
    if not comments:
        return "(no comments)"
    recent = comments[-max_comments:]
    lines = []
    for c in recent:
        user = c.get('user', '?')
        text = _clean_slack_text(c.get('text', ''))[:250]
        ts = c.get('timestamp', '')
        lines.append(f"  [{user} | {ts}]: {text}")
    return "\n".join(lines)


def enrich_rows(rows: list) -> list:
    enriched = []
    for r in rows:
        t = dict(r)
        comments = _parse_comments(t.pop('comments_json', None))
        t['parsed_comments'] = comments
        t['comment_context'] = _comment_context(comments)
        t['last_commenter'] = comments[-1].get('user', '') if comments else ''
        t['num_comments'] = int(t.get('num_comments') or 0)
        t['emergency_task'] = int(t.get('emergency_task') or 0)
        t['days_overdue'] = int(t.get('days_overdue') or 0)
        t['days_since_edit'] = int(t.get('days_since_edit') or 0)
        t['responsible'] = (str(t.get('responsible') or '')).strip() or '(unassigned)'
        t['task_category'] = (t.get('task_category') or 'unknown').strip()
        t['tasks'] = (t.get('tasks') or '').strip()
        t['task_summary'] = (t.get('task_summary') or '').strip()
        t['agent'] = (t.get('agent') or '').strip()
        enriched.append(t)
    return enriched


def compute_stats(workload_rows: list) -> dict:
    if not workload_rows:
        return {k: 0 for k in
                ('open', 'overdue', 'critical', 'due_today', 'stalled', 'high_priority_open')}
    return {
        'open': sum(r['open_tasks'] for r in workload_rows),
        'overdue': sum(r['overdue_tasks'] for r in workload_rows),
        'critical': sum(r['critical_overdue'] for r in workload_rows),
        'due_today': sum(r.get('due_today', 0) for r in workload_rows),
        'stalled': sum(r['stalled_tasks'] for r in workload_rows),
        'high_priority_open': sum(r.get('high_priority_open', 0) for r in workload_rows),
    }


# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------
def _format_task_block(task: dict) -> str:
    due_str = str(task.get('due_date', 'N/A'))
    shared_note = " [SHARED OWNER]" if task.get('is_shared_owner') else ""
    return (
        f"Task: {task['tasks']}\n"
        f"Agent: {task.get('agent', 'N/A')}\n"
        f"Summary: {task.get('task_summary', '')}\n"
        f"Category: {task['task_category']}\n"
        f"Responsible: {task['responsible']}{shared_note}\n"
        f"Due: {due_str} | Days overdue: {task['days_overdue']}\n"
        f"Emergency: {task['emergency_task']}/5\n"
        f"Comments: {task['num_comments']} | Days since last edit: {task['days_since_edit']}\n"
        f"Last person to comment: {task['last_commenter']}\n"
        f"Recent conversation:\n{task['comment_context']}"
    )


def _strip_fence(text: str) -> str:
    text = text.strip()
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    return text.strip()


def _repair_truncated_json(raw: str) -> dict:
    """
    Best-effort repair of a truncated JSON response.
    Strategy: keep re-truncating at the last valid structural boundary
    until json.loads succeeds, then return whatever was parseable.
    """
    text = _strip_fence(raw)

    # Try as-is first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Walk back from the end looking for a closing brace that gives valid JSON
    # Try progressively shorter cuts
    for cut in range(len(text) - 1, max(len(text) - 2000, 0), -1):
        chunk = text[:cut].rstrip()
        if not chunk.endswith(('}', ']', '"')):
            continue
        # Close all unclosed structures
        candidate = _close_json(chunk)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue

    raise ValueError("Could not repair truncated JSON")


def _close_json(text: str) -> str:
    """Close any unclosed JSON arrays/objects."""
    stack = []
    in_string = False
    escape_next = False
    for ch in text:
        if escape_next:
            escape_next = False
            continue
        if ch == '\\' and in_string:
            escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch in ('{', '['):
            stack.append(ch)
        elif ch == '}':
            if stack and stack[-1] == '{':
                stack.pop()
        elif ch == ']':
            if stack and stack[-1] == '[':
                stack.pop()

    # Close remaining open structures in reverse order
    closers = {'{': '}', '[': ']'}
    closing = ''.join(closers[c] for c in reversed(stack))
    return text + closing


def llm_generate_report(
    workload_rows: list,
    agent_rows: list,
    overdue_rows: list,
    stalled_rows: list,
    patterns: dict,
    full_summary: dict,
) -> dict:
    today = datetime.now().strftime("%Y-%m-%d")

    # --- Workload by responsible ---
    workload_lines = [
        "Staff | Open | Critical | Overdue | Today | Stalled | SharedOwner | CompletedLast7d | Note"
    ]
    velocity_map = {r['responsible']: r.get('completed_last_7d', 0)
                    for r in full_summary.get('velocity', [])}
    for r in workload_rows:
        name = r['responsible']
        notes = []
        if ',' in name:
            notes.append("shared ownership")
        if r['critical_overdue'] > 0:
            notes.append(f"{r['critical_overdue']} critical overdue")
        if r['overdue_tasks'] >= 5:
            notes.append("heavy overdue load")
        if r['stalled_tasks'] > 0:
            notes.append(f"{r['stalled_tasks']} stalled")
        if r.get('shared_owner_tasks', 0) > 0:
            notes.append(f"{r['shared_owner_tasks']} shared-ownership open")
        completed = velocity_map.get(name, 0)
        workload_lines.append(
            f"{name} | {r['open_tasks']} | {r['critical_overdue']} | "
            f"{r['overdue_tasks']} | {r.get('due_today',0)} | {r['stalled_tasks']} | "
            f"{r.get('shared_owner_tasks',0)} | {completed} | {'; '.join(notes)}"
        )

    # --- Workload by agent ---
    agent_lines = ["Agent | Open | Critical | Overdue | Today | Stalled | SharedOwner | TopCategories"]
    for r in agent_rows:
        cats = ", ".join(r.get('top_categories', [])[:3])
        agent_lines.append(
            f"{r['agent']} | {r['open_tasks']} | {r['critical_overdue']} | "
            f"{r['overdue_tasks']} | {r.get('due_today',0)} | {r['stalled_tasks']} | "
            f"{r.get('shared_owner_tasks',0)} | {cats}"
        )

    # --- Full category breakdown (ALL open tasks) ---
    cat_summary_lines = [
        "Category | Responsible | Agent | Total | Overdue | Stalled | DueToday | NoDueDate | Shared | AvgComments | MaxDaysIdle"
    ]
    for r in full_summary.get('category_breakdown', [])[:80]:
        cat_summary_lines.append(
            f"{r.get('task_category','?')} | {r.get('responsible','?')} | {r.get('agent','?')} | "
            f"{r.get('cnt',0)} | {r.get('overdue',0)} | {r.get('stalled',0)} | "
            f"{r.get('due_today',0)} | {r.get('no_due_date',0)} | {r.get('shared',0)} | "
            f"{round(r.get('avg_comments') or 0, 1)} | {r.get('max_days_idle',0)}"
        )

    # --- Pattern summary ---
    cat_lines = []
    for cat, cnt in list(patterns['category_overdue'].items())[:6]:
        stalled = patterns['category_stalled'].get(cat, 0)
        cat_lines.append(f"  {cat}: {cnt} overdue, {stalled} stalled")
    pattern_block = (
        "Category overdue hotspots:\n" + "\n".join(cat_lines) + "\n"
        f"Shared-ownership tasks that are overdue: {patterns['shared_owner_overdue']}\n"
        f"Unassigned open tasks: {patterns['unassigned_total']}"
    )

    # --- Task detail blocks ---
    overdue_blocks = "\n\n---\n\n".join(
        f"[CRITICAL/OVERDUE #{i+1}]\n{_format_task_block(t)}"
        for i, t in enumerate(overdue_rows)
    )
    stalled_blocks = "\n\n---\n\n".join(
        f"[STUCK #{i+1} | reason={t.get('reason','stalled')}]\n{_format_task_block(t)}"
        for i, t in enumerate(stalled_rows)
    )

    user_msg = f"""Today: {today}.

IMPORTANT: Analyse the FULL open backlog below — not just overdue tasks. Draw insights
from workload distribution, velocity (tasks completed last 7 days), category mix, and
engagement signals (avg_comments, max_days_idle). When there are zero overdue tasks, the
insights should explain what the backlog looks like, where the risk is, and what the
manager should watch.

=== WORKLOAD BY CS STAFF ===
{chr(10).join(workload_lines)}

=== WORKLOAD BY SALES AGENT ===
{chr(10).join(agent_lines)}

=== FULL OPEN TASK BREAKDOWN (all open tasks by category x staff) ===
{chr(10).join(cat_summary_lines)}

=== PATTERN SIGNALS ===
{pattern_block}

=== CRITICAL AND OVERDUE TASK DETAILS (with comment threads) ===
{overdue_blocks if overdue_blocks else '(none — no overdue tasks today)'}

=== STUCK / UNASSIGNED TASK DETAILS ===
{stalled_blocks if stalled_blocks else '(none)'}

Rules for this response:
- agent_breakdown: one entry per agent in the workload-by-agent table.
- cs_staff_analysis: top 6 CS staff by open_tasks.
- pattern_alerts: only if count >= 2; max 4.
- critical_tasks: up to 6; empty array [] if none.
- stuck_tasks: up to 5; empty array [] if none.
- Keep ALL string values free of unescaped double-quotes or newlines inside JSON strings.
- Output ONLY the JSON object. No commentary before or after."""

    response = claude.messages.create(
        model=MODEL,
        max_tokens=12000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = response.content[0].text
    cleaned = _strip_fence(raw)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.warning("JSON parse failed (%s) — attempting repair", e)
        try:
            repaired = _repair_truncated_json(raw)
            logger.info("JSON repair succeeded")
            return repaired
        except Exception as e2:
            logger.error("JSON repair also failed: %s\nRaw (first 3000 chars):\n%s", e2, raw[:3000])
            raise


# ---------------------------------------------------------------------------
# PDF colours & styles
# ---------------------------------------------------------------------------
_DARK = colors.HexColor('#1a1a2e')
_ACCENT = colors.HexColor('#0f3460')
_RULE = colors.HexColor('#cccccc')
_LIGHT_BG = colors.HexColor('#f5f7fa')
_RED = colors.HexColor('#c0392b')
_ORANGE = colors.HexColor('#d35400')
_GREEN = colors.HexColor('#27ae60')
_YELLOW = colors.HexColor('#f39c12')
_MUTED = colors.HexColor('#666666')
_AGENT_BG = colors.HexColor('#eaf0fb')

PAGE_W, PAGE_H = A4
MARGIN = 2 * cm


def _styles():
    return {
        'doc_title': ParagraphStyle('doc_title', fontName='Helvetica-Bold', fontSize=18,
                                    textColor=_DARK, leading=22),
        'doc_sub': ParagraphStyle('doc_sub', fontName='Helvetica', fontSize=10,
                                  textColor=_MUTED, leading=14),
        'section_h': ParagraphStyle('section_h', fontName='Helvetica-Bold', fontSize=12,
                                    textColor=_ACCENT, leading=16, spaceBefore=14, spaceAfter=4),
        'agent_h': ParagraphStyle('agent_h', fontName='Helvetica-Bold', fontSize=11,
                                  textColor=_ACCENT, leading=14, spaceBefore=10, spaceAfter=2),
        'task_h': ParagraphStyle('task_h', fontName='Helvetica-Bold', fontSize=10,
                                 textColor=_DARK, leading=13, spaceBefore=8, spaceAfter=2),
        'body': ParagraphStyle('body', fontName='Helvetica', fontSize=9,
                               textColor=_DARK, leading=13, spaceAfter=3),
        'body_vn': ParagraphStyle('body_vn', fontName='Helvetica', fontSize=9.5,
                                  textColor=_DARK, leading=14, spaceAfter=4),
        'meta': ParagraphStyle('meta', fontName='Helvetica-Oblique', fontSize=8,
                               textColor=_MUTED, leading=11),
        'kpi_val': ParagraphStyle('kpi_val', fontName='Helvetica-Bold', fontSize=22,
                                  textColor=_ACCENT, leading=26, alignment=TA_CENTER),
        'kpi_lbl': ParagraphStyle('kpi_lbl', fontName='Helvetica', fontSize=7.5,
                                  textColor=_MUTED, leading=10, alignment=TA_CENTER),
        'action': ParagraphStyle('action', fontName='Helvetica', fontSize=9.5,
                                 textColor=_DARK, leading=14, leftIndent=10, spaceAfter=3),
        'risk': ParagraphStyle('risk', fontName='Helvetica-BoldOblique', fontSize=9.5,
                               textColor=_RED, leading=13),
        'pattern_h': ParagraphStyle('pattern_h', fontName='Helvetica-Bold', fontSize=9,
                                    textColor=_ORANGE, leading=12, spaceBefore=5),
        'staff_h': ParagraphStyle('staff_h', fontName='Helvetica-Bold', fontSize=9.5,
                                  textColor=_DARK, leading=12, spaceBefore=6),
    }


def _kpi_row(stats: dict, st: dict) -> Table:
    kpis = [
        ('Open', stats['open']),
        ('Overdue', stats['overdue']),
        ('Critical', stats['critical']),
        ('Due Today', stats['due_today']),
        ('Stalled', stats['stalled']),
        ('High Priority', stats['high_priority_open']),
    ]
    top = [Paragraph(str(v), st['kpi_val']) for _, v in kpis]
    bot = [Paragraph(k, st['kpi_lbl']) for k, _ in kpis]
    col_w = (PAGE_W - 2 * MARGIN) / len(kpis)
    tbl = Table([top, bot], colWidths=[col_w] * len(kpis))
    tbl.setStyle(TableStyle([
        ('BOX', (0, 0), (-1, -1), 0.5, _RULE),
        ('INNERGRID', (0, 0), (-1, -1), 0.5, _RULE),
        ('BACKGROUND', (0, 0), (-1, -1), _LIGHT_BG),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ('TEXTCOLOR', (2, 0), (2, 0), _RED if stats['critical'] > 0 else _ACCENT),
    ]))
    return tbl


def _agent_breakdown_card(ab: dict, st: dict) -> list:
    """Render one agent breakdown as a visually distinct card."""
    flows = []

    # Agent header with inline KPIs
    agent_name = ab.get('agent', '')
    overdue = ab.get('overdue', 0)
    critical = ab.get('critical', 0)
    header_color = _RED if critical > 0 else (_ORANGE if overdue > 3 else _ACCENT)

    # Mini KPI table for this agent
    kpi_data = [
        [
            Paragraph(str(ab.get('open', 0)), ParagraphStyle('av', fontName='Helvetica-Bold',
                      fontSize=14, textColor=_ACCENT, alignment=TA_CENTER, leading=16)),
            Paragraph(str(overdue), ParagraphStyle('av', fontName='Helvetica-Bold',
                      fontSize=14, textColor=_ORANGE if overdue > 0 else _DARK,
                      alignment=TA_CENTER, leading=16)),
            Paragraph(str(critical), ParagraphStyle('av', fontName='Helvetica-Bold',
                      fontSize=14, textColor=_RED if critical > 0 else _DARK,
                      alignment=TA_CENTER, leading=16)),
            Paragraph(str(ab.get('due_today', 0)), ParagraphStyle('av', fontName='Helvetica-Bold',
                      fontSize=14, textColor=_DARK, alignment=TA_CENTER, leading=16)),
            Paragraph(str(ab.get('stalled', 0)), ParagraphStyle('av', fontName='Helvetica-Bold',
                      fontSize=14, textColor=_YELLOW if ab.get('stalled', 0) > 0 else _DARK,
                      alignment=TA_CENTER, leading=16)),
        ],
        [
            Paragraph('Open', ParagraphStyle('al', fontName='Helvetica', fontSize=7,
                      textColor=_MUTED, alignment=TA_CENTER, leading=9)),
            Paragraph('Overdue', ParagraphStyle('al', fontName='Helvetica', fontSize=7,
                      textColor=_MUTED, alignment=TA_CENTER, leading=9)),
            Paragraph('Critical', ParagraphStyle('al', fontName='Helvetica', fontSize=7,
                      textColor=_MUTED, alignment=TA_CENTER, leading=9)),
            Paragraph('Due Today', ParagraphStyle('al', fontName='Helvetica', fontSize=7,
                      textColor=_MUTED, alignment=TA_CENTER, leading=9)),
            Paragraph('Stalled', ParagraphStyle('al', fontName='Helvetica', fontSize=7,
                      textColor=_MUTED, alignment=TA_CENTER, leading=9)),
        ],
    ]
    col_w_agent = (PAGE_W - 2 * MARGIN) / 5
    kpi_tbl = Table(kpi_data, colWidths=[col_w_agent] * 5)
    kpi_tbl.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), _AGENT_BG),
        ('BOX', (0, 0), (-1, -1), 0.5, _RULE),
        ('INNERGRID', (0, 0), (-1, -1), 0.3, _RULE),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))

    # Top categories label
    cats = ab.get('top_categories', [])
    cat_str = "  |  ".join(cats[:3]) if cats else "—"

    flows.append(Paragraph(
        f"Agent: {agent_name}",
        ParagraphStyle('ah', fontName='Helvetica-Bold', fontSize=11,
                       textColor=header_color, leading=14, spaceBefore=12)
    ))
    flows.append(Paragraph(f"Top task categories: {cat_str}", st['meta']))
    flows.append(Spacer(1, 0.15 * cm))
    flows.append(kpi_tbl)
    flows.append(Spacer(1, 0.15 * cm))

    # Insight paragraph
    insight = ab.get('status_insight', '')
    if insight:
        flows.append(Paragraph(f"<b>Insight:</b> {insight}", st['body_vn']))

    # Manager action
    action = ab.get('manager_action', '')
    if action:
        flows.append(Paragraph(
            f"<b>Manager Action:</b> {action}",
            ParagraphStyle('ma', fontName='Helvetica-Bold', fontSize=9,
                           textColor=_ACCENT, leading=13, leftIndent=8)
        ))

    flows.append(HRFlowable(width='100%', thickness=0.5, color=_RULE, spaceAfter=6))
    return flows


def _cs_staff_table(cs_staff: list, st: dict) -> Table:
    load_color = {'overloaded': _RED, 'manageable': _ORANGE, 'light': _GREEN}
    headers = ['CS Staff', 'Open', 'Critical', 'Overdue', 'Stalled',
               'Dom. Category', 'Load']
    col_w = [(PAGE_W - 2 * MARGIN) * f for f in
             [0.22, 0.07, 0.08, 0.08, 0.08, 0.30, 0.12]]

    def cell(txt, bold=False, color=_DARK, align=TA_CENTER):
        fn = 'Helvetica-Bold' if bold else 'Helvetica'
        return Paragraph(str(txt), ParagraphStyle('c', fontName=fn, fontSize=8,
                         textColor=color, alignment=align, leading=11))

    rows = [[cell(h, bold=True, color=colors.white) for h in headers]]
    for r in cs_staff:
        load = r.get('load_assessment', 'manageable')
        lc = load_color.get(load, _DARK)
        is_alert = r.get('critical_overdue', 0) > 0 or r.get('overdue_tasks', 0) >= 5
        rows.append([
            cell(r.get('name', ''), bold=is_alert,
                 color=_RED if is_alert else _DARK, align=TA_LEFT),
            cell(r.get('open_tasks', 0)),
            cell(r.get('critical_overdue', 0),
                 color=_RED if r.get('critical_overdue', 0) > 0 else _DARK),
            cell(r.get('overdue_tasks', 0),
                 color=_ORANGE if r.get('overdue_tasks', 0) > 0 else _DARK),
            cell(r.get('stalled_tasks', 0),
                 color=_YELLOW if r.get('stalled_tasks', 0) > 0 else _DARK),
            cell(r.get('dominant_category', '')[:30], align=TA_LEFT),
            cell(load.upper(), bold=True, color=lc),
        ])

    tbl = Table(rows, colWidths=col_w, repeatRows=1)
    tbl.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), _ACCENT),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, _LIGHT_BG]),
        ('GRID', (0, 0), (-1, -1), 0.3, _RULE),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('LEFTPADDING', (0, 0), (-1, -1), 5),
    ]))
    return tbl


def _task_card(task: dict, index: int, st: dict, is_critical: bool = True) -> list:
    flows = []
    em = task.get('emergency', task.get('emergency_task', 0))
    days = task.get('days_overdue', task.get('days_since_activity', 0))
    due = task.get('due', str(task.get('due_date', 'N/A')))
    resp = task.get('responsible', '')
    cat = task.get('category', task.get('task_category', ''))
    agent_name = task.get('agent', '')
    shared_note = " [SHARED OWNER]" if task.get('is_shared_owner') else ""

    header_color = _RED if (is_critical and em >= 3) else _ORANGE if em >= 2 else _DARK
    flows.append(Paragraph(
        f"{index}. {task.get('task', task.get('tasks', ''))}",
        ParagraphStyle('th', fontName='Helvetica-Bold', fontSize=9.5,
                       textColor=header_color, leading=13, spaceBefore=6)
    ))

    meta_parts = [f"Responsible: {resp}{shared_note}", f"Agent: {agent_name}",
                  f"Category: {cat}", f"Due: {due}"]
    if is_critical:
        meta_parts += [f"Days overdue: {days}", f"Emergency: {em}/5"]
    else:
        meta_parts += [f"Days without activity: {days}", f"Emergency: {em}/5"]
    flows.append(Paragraph("  |  ".join(meta_parts), st['meta']))

    if is_critical:
        for lbl, key in [("Current Status", "current_status"),
                         ("Blocker", "blocker"),
                         ("Recommended Action", "recommended_action")]:
            val = task.get(key, '')
            if val:
                flows.append(Paragraph(f"<b>{lbl}:</b> {val}", st['body_vn']))
    else:
        for lbl, key in [("Analysis", "analysis"),
                         ("Recommended Action", "recommended_action")]:
            val = task.get(key, '')
            if val:
                flows.append(Paragraph(f"<b>{lbl}:</b> {val}", st['body_vn']))

    flows.append(HRFlowable(width='100%', thickness=0.3, color=_RULE, spaceAfter=4))
    return flows


def render_pdf(report: dict, stats: dict, workload_rows: list, today: str) -> bytes:
    buf = io.BytesIO()
    title_agent = AGENT_FILTER if AGENT_FILTER else "All Agents"
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=MARGIN, bottomMargin=MARGIN,
        title=f"CS Daily Reminder — {title_agent} — {today}",
        author="EPS Operations",
    )
    st = _styles()
    story = []

    # === Header ===
    story.append(Paragraph("Customer Service Daily Reminder", st['doc_title']))
    story.append(Paragraph(
        f"Coverage: {title_agent}  |  Report date: {today}  |  "
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        st['doc_sub']
    ))
    story.append(Spacer(1, 0.3 * cm))
    story.append(HRFlowable(width='100%', thickness=1.5, color=_ACCENT, spaceAfter=10))

    # === Executive Summary ===
    story.append(Paragraph("Executive Summary", st['section_h']))
    story.append(Paragraph(report.get('executive_summary', ''), st['body_vn']))
    story.append(Spacer(1, 0.15 * cm))

    # === KPI Row ===
    story.append(Paragraph("Key Performance Indicators", st['section_h']))
    story.append(_kpi_row(stats, st))
    story.append(Spacer(1, 0.3 * cm))

    # === Risk Summary ===
    risk = report.get('risk_summary', '')
    if risk:
        story.append(Paragraph("Risk Assessment", st['section_h']))
        story.append(Paragraph(risk, st['risk']))
        story.append(Spacer(1, 0.15 * cm))

    # === Agent Breakdown (NEW) ===
    agent_breakdown = report.get('agent_breakdown') or []
    if agent_breakdown:
        story.append(PageBreak())
        story.append(Paragraph("Agent Backlog Breakdown", st['section_h']))
        story.append(Paragraph(
            "Per-agent analysis with insight into current status, blockers, "
            "and recommended manager actions.",
            st['body']
        ))
        story.append(Spacer(1, 0.15 * cm))
        for ab in agent_breakdown:
            story.extend(_agent_breakdown_card(ab, st))

    # === CS Staff Analysis (NEW: table + narrative per person) ===
    cs_staff = report.get('cs_staff_analysis') or []
    if cs_staff:
        story.append(Spacer(1, 0.3 * cm))
        story.append(Paragraph("CS Staff Workload Analysis", st['section_h']))
        story.append(_cs_staff_table(cs_staff, st))
        story.append(Spacer(1, 0.2 * cm))
        # Per-staff insight text
        for s in cs_staff:
            insight = s.get('insight', '')
            if insight:
                story.append(Paragraph(
                    f"<b>{s.get('name','')}</b> — {insight}",
                    st['body_vn']
                ))
        story.append(Spacer(1, 0.1 * cm))

    # === Team Analysis ===
    story.append(Paragraph("Team Analysis", st['section_h']))
    team_analysis = report.get('team_analysis', '')
    if team_analysis:
        story.append(Paragraph(team_analysis, st['body_vn']))

    # === Pattern Alerts (NEW) ===
    patterns = report.get('pattern_alerts') or []
    if patterns:
        story.append(Spacer(1, 0.15 * cm))
        story.append(Paragraph("Pattern Alerts", st['section_h']))
        story.append(Paragraph(
            "Systemic patterns detected across the task backlog:",
            st['body']
        ))
        story.append(Spacer(1, 0.1 * cm))
        for p in patterns:
            story.append(Paragraph(
                f"{p.get('pattern', '')} ({p.get('count', 0)} tasks)",
                st['pattern_h']
            ))
            story.append(Paragraph(
                f"<b>Impact:</b> {p.get('impact', '')}  "
                f"<b>Fix:</b> {p.get('recommendation', '')}",
                st['body_vn']
            ))

    # === Critical Tasks ===
    critical = report.get('critical_tasks') or []
    if critical:
        story.append(PageBreak())
        story.append(Paragraph("Critical and Overdue Tasks", st['section_h']))
        story.append(Paragraph(
            f"{len(critical)} tasks require immediate attention.",
            st['body']
        ))
        story.append(Spacer(1, 0.15 * cm))
        for i, task in enumerate(critical, 1):
            story.extend(_task_card(task, i, st, is_critical=True))

    # === Stuck Tasks ===
    stuck = report.get('stuck_tasks') or []
    if stuck:
        story.append(Spacer(1, 0.2 * cm))
        story.append(Paragraph("Stalled and Unassigned Tasks", st['section_h']))
        story.append(Paragraph(
            f"{len(stuck)} tasks are stalled or have no assigned owner.",
            st['body']
        ))
        story.append(Spacer(1, 0.15 * cm))
        for i, task in enumerate(stuck, 1):
            label_map = {
                'unassigned': 'UNASSIGNED',
                'shared_owner_inactive': 'SHARED/INACTIVE',
                'stalled': 'STALLED',
            }
            label = label_map.get(task.get('reason', 'stalled'), 'STALLED')
            t2 = dict(task)
            t2['task'] = f"[{label}] {t2.get('task', '')}"
            story.extend(_task_card(t2, i, st, is_critical=False))

    # === Priority Actions ===
    actions = report.get('priority_actions') or []
    if actions:
        story.append(PageBreak())
        story.append(Paragraph("Priority Actions for Manager", st['section_h']))
        story.append(HRFlowable(width='100%', thickness=0.5, color=_ACCENT, spaceAfter=8))
        for i, action in enumerate(actions, 1):
            story.append(Paragraph(f"{i}. {action}", st['action']))
        story.append(Spacer(1, 0.4 * cm))

    # === Footer ===
    story.append(HRFlowable(width='100%', thickness=0.5, color=_RULE, spaceBefore=16))
    story.append(Paragraph(
        f"Generated by EPS Operations System | Source: {TASK_TABLE} | Model: {MODEL}",
        st['meta']
    ))

    doc.build(story)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------
def upload_pdf_to_slack(pdf_bytes: bytes, filename: str, channel_id: str) -> Optional[str]:
    try:
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as f:
            f.write(pdf_bytes)
            tmp_path = f.name
        resp = app.client.files_upload_v2(
            channel=channel_id, file=tmp_path, filename=filename,
            title=filename.replace('_', ' ').replace('.pdf', ''),
        )
        return resp.get('file', {}).get('permalink')
    except Exception as e:
        logger.warning("files_upload_v2 failed: %s", e)
        try:
            resp = app.client.files_upload(
                channels=channel_id, file=pdf_bytes, filename=filename, filetype='pdf',
            )
            return resp.get('file', {}).get('permalink')
        except Exception as e2:
            logger.error("files_upload fallback failed: %s", e2)
            return None
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def render_channel_blocks(
    report: dict,
    stats: dict,
    today: str,
    pdf_permalink: Optional[str],
    mentions: str,
) -> list:
    blocks = []
    title_agent = AGENT_FILTER if AGENT_FILTER else "All Agents"

    # Header
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"CS Daily Reminder — {title_agent} — {today}",
            "emoji": False,
        },
    })

    # Summary + mentions
    summary = report.get('executive_summary', '').strip()
    if mentions:
        summary = f"{mentions}\n{summary}"
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": summary}})

    # Overall KPIs
    blocks.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*Open*\n{stats['open']}"},
            {"type": "mrkdwn",
             "text": f"*Overdue*\n{stats['overdue']} ({stats['critical']} critical)"},
            {"type": "mrkdwn", "text": f"*Due Today*\n{stats['due_today']}"},
            {"type": "mrkdwn", "text": f"*Stalled*\n{stats['stalled']}"},
        ],
    })
    blocks.append({"type": "divider"})

    # Per-agent breakdown (compact)
    agent_breakdown = report.get('agent_breakdown') or []
    if agent_breakdown:
        agent_lines = []
        for ab in agent_breakdown:
            flags = []
            if ab.get('critical', 0) > 0:
                flags.append(f"{ab['critical']} critical")
            if ab.get('overdue', 0) > 0:
                flags.append(f"{ab['overdue']} overdue")
            if ab.get('stalled', 0) > 0:
                flags.append(f"{ab['stalled']} stalled")
            flag_str = " | ".join(flags) if flags else "on track"
            agent_lines.append(
                f"*{ab.get('agent','')}*: {ab.get('open',0)} open — {flag_str}"
            )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Agent Backlog:*\n" + "\n".join(agent_lines),
            },
        })
        blocks.append({"type": "divider"})

    # Top critical tasks (3 items)
    critical = report.get('critical_tasks') or []
    if critical:
        lines = []
        for t in critical[:3]:
            em = t.get('emergency', 0)
            days = t.get('days_overdue', 0)
            lines.append(
                f"• `{t.get('responsible','')}` [{t.get('agent','')}] — "
                f"{t.get('task','')[:65]}  _(em={em}, {days}d overdue)_"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": "*Critical tasks (top 3):*\n" + "\n".join(lines)},
        })

    # Pattern alerts (top 2)
    patterns = report.get('pattern_alerts') or []
    if patterns:
        pat_lines = [
            f"• *{p.get('pattern','')}* ({p.get('count',0)} tasks): {p.get('impact','')}"
            for p in patterns[:2]
        ]
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": "*Pattern Alerts:*\n" + "\n".join(pat_lines)},
        })

    # Risk
    risk = report.get('risk_summary', '')
    if risk:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Risk:* {risk}"},
        })

    # PDF link
    if pdf_permalink:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"Full report PDF: {pdf_permalink}"},
        })

    return blocks


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_daily_reminder(post: bool = True) -> dict:
    logger.info("Querying BigQuery (agent filter: '%s')", AGENT_FILTER or "ALL")

    workload_rows = query_workload_by_responsible()
    agent_rows = query_workload_by_agent()
    overdue_rows_raw = query_critical_and_overdue()
    stalled_rows_raw = query_stalled_and_unassigned()
    patterns = query_patterns()
    full_summary = query_open_tasks_full_summary()

    overdue_rows = enrich_rows(overdue_rows_raw)
    stalled_rows = enrich_rows(stalled_rows_raw)

    logger.info(
        "rows: workload=%d agents=%d overdue=%d stalled=%d cat_breakdown=%d",
        len(workload_rows), len(agent_rows), len(overdue_rows), len(stalled_rows),
        len(full_summary.get('category_breakdown', [])),
    )

    stats = compute_stats(workload_rows)
    report = llm_generate_report(
        workload_rows, agent_rows, overdue_rows, stalled_rows, patterns, full_summary
    )

    today = datetime.now().strftime("%Y-%m-%d")
    pdf_bytes = render_pdf(report, stats, workload_rows, today)

    if not post:
        out_path = f"/tmp/cs_reminder_{today}.pdf"
        with open(out_path, 'wb') as f:
            f.write(pdf_bytes)
        logger.info("Dry-run: PDF saved to %s", out_path)
        return {"stats": stats, "report": report, "pdf_path": out_path}

    if not CS_CHANNEL:
        raise RuntimeError("CS_REMINDER_CHANNEL is not set.")

    title_agent = AGENT_FILTER.replace(' ', '_') if AGENT_FILTER else "AllAgents"
    filename = f"CS_Reminder_{title_agent}_{today}.pdf"
    pdf_permalink = upload_pdf_to_slack(pdf_bytes, filename, CS_CHANNEL)

    blocks = render_channel_blocks(report, stats, today, pdf_permalink, CS_MANAGER_MENTIONS)
    fallback = f"CS Daily Reminder — {AGENT_FILTER or 'All Agents'} — {today}"

    msg_resp = app.client.chat_postMessage(
        channel=CS_CHANNEL, blocks=blocks, text=fallback,
    )
    logger.info("Posted to %s (ts=%s)", CS_CHANNEL, msg_resp.get('ts'))

    return {
        "stats": stats,
        "report": report,
        "pdf_permalink": pdf_permalink,
        "channel_message_ts": msg_resp.get('ts'),
    }