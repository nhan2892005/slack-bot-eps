"""
Customer Service Daily Reminder — v4 (Signal-to-Noise)
=======================================================

Philosophy:
  - Classify every task into one of 3 buckets:
      🔴 NEEDS ATTENTION — blocked, escalation needed, conflicting instructions, stale
      🟡 MONITORING      — waiting on external party with clear deadline/risk
      🟢 ON TRACK        — progressing normally, no manager action needed
  - NEEDS ATTENTION tasks get full story-telling (timeline, who did what, why stuck)
  - MONITORING tasks get 2-3 sentences (status + follow-up date + risk)
  - ON TRACK tasks get 1 line (task name + next step + date)

  The result: a 2-3 page PDF instead of 7, with actionable signal front-and-center.

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
from reportlab.lib.styles import ParagraphStyle
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
AGENT_FILTER = os.environ.get("CS_AGENT_FILTER", "")
TASK_TABLE = os.environ.get("CS_TASK_TABLE", "eps-470914.eps_data.health_task_raw")
CS_CHANNEL = os.environ.get("CS_REMINDER_CHANNEL")
CS_MANAGER_MENTIONS = os.environ.get("CS_MANAGER_MENTIONS", "").strip()
OVERDUE_LIMIT = int(os.environ.get("CS_OVERDUE_LIMIT", "30"))
# Condensed report toggle: set CS_SHORTEN_REPORT=1|true to enable
SHORTEN_REPORT = str(os.environ.get("CS_SHORTEN_REPORT", "")).lower() in ("1", "true", "yes")

# ---------------------------------------------------------------------------
# System prompt — signal-to-noise, 3-bucket classification
# ---------------------------------------------------------------------------
_agent_filter_context = (
    f"The report covers only tasks where agent = '{AGENT_FILTER}'."
    if AGENT_FILTER else
    "The report covers ALL sales agents."
)

SYSTEM_PROMPT = f"""You are a senior operations analyst for EPS, a Vietnamese-American
health insurance brokerage. Your job: produce a daily CS briefing that separates
SIGNAL from NOISE — surface what needs manager action, compress what does not.

{_agent_filter_context}

DOMAIN KNOWLEDGE
  - CS tasks come from Notion (mirrored to BigQuery). Each task has a Slack thread
    (comments_json) that records the actual work conversation. Staff write in Vietnamese.
  - 'responsible' = the CS staff accountable. 'agent' = the sales agent the task relates to.
  - Emergency 0-5: >= 4 means handle today.
  - "Stalled": open AND num_comments <= 1 AND days_since_edit >= 7.
  - Multi-person responsible = shared ownership = coordination risk.

CLASSIFICATION RULES (CRITICAL)
  For each open task, read the comment thread carefully and classify into exactly ONE bucket:

  🔴 NEEDS ATTENTION — Manager must act. Criteria (any one is enough):
    - Blocker persists > 3 days with no workaround
    - Provider not returning calls after 2+ attempts
    - Conflicting or unclear instructions from manager/Kay
    - Escalation needed (past promised timeline, needs supervisor)
    - Shared ownership with no clear primary driver
    - Client documents missing with deadline pressure
    - Complex case with multiple failures (payment misapplied, etc.)

  🟡 MONITORING — No manager action now, but has deadline or risk. Criteria:
    - Waiting on processing window (insurance claim, bill transfer, etc.)
    - Waiting on single callback with clear follow-up date
    - Refund check in transit, payment pending reflection
    - Task on hold per manager instruction with clear resume date

  🟢 ON TRACK — Normal progress, no issues. Criteria:
    - Task completed or ready to close
    - Simple follow-up scheduled, no blockers
    - Routine verification done, awaiting routine next step

STORY-TELLING DEPTH BY BUCKET
  🔴 NEEDS ATTENTION: Full story. Read every comment. Build a timeline:
    when created, key actions taken (with dates), what specifically went wrong
    or is blocking, who is involved, and ONE concrete recommended action.
    Translate Vietnamese accurately. Name people, providers, reference numbers.
    3-6 sentences.

  🟡 MONITORING: Brief status. What is being waited on, when to follow up,
    what happens if it slips. 2-3 sentences.

  🟢 ON TRACK: One line. Task name + current status + next step date.

JSON SAFETY RULES (CRITICAL):
  - Never use unescaped double-quotes inside a JSON string value.
  - Never put literal newlines inside a JSON string value. Use spaces instead.
  - Never use smart quotes — ASCII only.
  - Close every array and object.

OUTPUT FORMAT — Return exactly ONE JSON object. No prose. No markdown fences.

{{
  "executive_summary": "2-3 sentences. Total open tasks, how many need attention, single biggest risk today.",

  "overall_stats": {{
    "total_open": <int>,
    "needs_attention": <int>,
    "monitoring": <int>,
    "on_track": <int>,
    "total_overdue": <int>,
    "total_stalled": <int>
  }},

  "needs_attention": [
    {{
      "task_title": "Client name - short description",
      "responsible": "CS staff name",
      "agent": "Sales agent name",
      "category": "task category",
      "created_date": "YYYY-MM-DD",
      "due_date": "YYYY-MM-DD or N/A",
      "emergency": <int 0-5>,
      "story": "Full timeline story. When created, what has been done (with dates), what went wrong, who is involved, specific blocker. 3-6 sentences. Concrete names, providers, reference numbers from the thread.",
      "blocker": "One sentence: the specific thing blocking progress right now.",
      "action": "One concrete next step. Who does what, by when."
    }}
  ],

  "monitoring": [
    {{
      "task_title": "Client name - short description",
      "responsible": "CS staff name",
      "agent": "Sales agent name",
      "status": "2-3 sentences: what is being waited on, when to follow up, risk if delayed.",
      "follow_up_date": "YYYY-MM-DD or N/A"
    }}
  ],

  "on_track": [
    {{
      "task_title": "Client name - short description",
      "responsible": "CS staff name",
      "agent": "Sales agent name",
      "one_liner": "One sentence: current status + next step."
    }}
  ],

  "pattern_alerts": [
    {{
      "pattern": "Short label",
      "count": <int>,
      "impact": "One sentence on operational impact.",
      "fix": "One sentence systemic fix."
    }}
  ],

  "priority_actions": [
    "Specific actionable item for manager. Max 4 items. Each names who/what/when."
  ],

  "staff_workload": [
    {{
      "name": "Staff name",
      "total_open": <int>,
      "attention_count": <int>,
      "assessment": "overloaded | manageable | light",
      "note": "One sentence: key observation or recommendation for this person."
    }}
  ]
}}

Rules:
  - needs_attention: ordered by urgency. Include ALL tasks classified as needs_attention.
  - monitoring: ordered by follow_up_date (soonest first).
  - on_track: ordered by responsible name.
  - pattern_alerts: max 4, only if count >= 2.
  - priority_actions: max 4, derived from needs_attention tasks.
  - staff_workload: one entry per staff with open tasks.
  - All output in English (translate Vietnamese from comments).
  - Every claim grounded in data — never invent.
"""

# ---------------------------------------------------------------------------
# SQL helpers (unchanged from v3)
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
    DATE(COALESCE(
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', created_time),
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', created_time)
    )) AS created_date,
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


def query_all_open_tasks(limit: int = OVERDUE_LIMIT) -> list:
    sql = _base_cte() + f"""
    SELECT
      record_id, agent, responsible, task_category, tasks, task_summary,
      created_date, due_date, days_overdue, emergency_task, num_comments,
      days_since_edit, tier, is_stalled, is_shared_owner, comments_json
    FROM tiered
    WHERE is_completed = 0
    ORDER BY
      responsible,
      CASE tier
        WHEN 'critical_overdue' THEN 0
        WHEN 'overdue' THEN 1
        WHEN 'due_today' THEN 2
        WHEN 'due_soon' THEN 3
        ELSE 4
      END,
      emergency_task DESC,
      days_overdue DESC
    LIMIT {limit}
    """
    return [dict(r) for r in bq_client.query(sql).result()]


def query_workload_summary() -> list:
    sql = _base_cte() + """
    SELECT
      responsible,
      COUNTIF(is_completed = 0) AS open_tasks,
      COUNTIF(is_completed = 0 AND tier = 'critical_overdue') AS critical_overdue,
      COUNTIF(is_completed = 0 AND tier IN ('critical_overdue','overdue')) AS overdue_tasks,
      COUNTIF(is_completed = 0 AND tier = 'due_today') AS due_today,
      COUNTIF(is_stalled) AS stalled_tasks,
      COUNTIF(is_completed = 1 AND last_edited_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AS completed_last_7d
    FROM tiered
    GROUP BY responsible
    HAVING open_tasks > 0
    ORDER BY critical_overdue DESC, overdue_tasks DESC, open_tasks DESC
    """
    return [dict(r) for r in bq_client.query(sql).result()]


def query_patterns() -> dict:
    sql = _base_cte() + """
    SELECT
      task_category,
      is_shared_owner,
      COUNT(*) AS task_count,
      COUNTIF(is_completed=0 AND tier IN ('critical_overdue','overdue')) AS overdue_count,
      COUNTIF(is_stalled) AS stalled_count,
      COUNTIF(is_completed=0 AND responsible='(unassigned)') AS unassigned_count
    FROM tiered
    WHERE is_completed = 0
    GROUP BY task_category, is_shared_owner
    ORDER BY overdue_count DESC, task_count DESC
    LIMIT 30
    """
    rows = [dict(r) for r in bq_client.query(sql).result()]
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


# ---------------------------------------------------------------------------
# Data enrichment (unchanged)
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


def _comment_context(comments: list, max_comments: int = 8) -> str:
    if not comments:
        return "(no comments)"
    recent = comments[-max_comments:]
    lines = []
    for c in recent:
        user = c.get('user', '?')
        text = _clean_slack_text(c.get('text', ''))[:300]
        ts = c.get('timestamp', '')
        lines.append(f"  [{user} | {ts}]: {text}")
    return "\n".join(lines)


def _truncate(text: str, length: int) -> str:
    if not text:
        return ''
    t = text.strip()
    if len(t) <= length:
        return t
    return t[: max(0, length - 3)].rstrip() + '...'


def enrich_rows(rows: list) -> list:
    enriched = []
    for r in rows:
        t = dict(r)
        comments = _parse_comments(t.pop('comments_json', None))
        t['parsed_comments'] = comments
        t['comment_context'] = _comment_context(comments)
        t['num_comments'] = int(t.get('num_comments') or 0)
        t['emergency_task'] = int(t.get('emergency_task') or 0)
        t['days_overdue'] = int(t.get('days_overdue') or 0)
        t['days_since_edit'] = int(t.get('days_since_edit') or 0)
        t['responsible'] = (str(t.get('responsible') or '')).strip() or '(unassigned)'
        t['task_category'] = (t.get('task_category') or 'unknown').strip()
        t['tasks'] = (t.get('tasks') or '').strip()
        t['task_summary'] = (t.get('task_summary') or '').strip()
        t['agent'] = (t.get('agent') or '').strip()
        t['created_date'] = str(t.get('created_date', 'N/A'))
        enriched.append(t)
    return enriched


def compute_stats(workload_rows: list) -> dict:
    if not workload_rows:
        return {k: 0 for k in ('open', 'overdue', 'critical', 'due_today', 'stalled')}
    return {
        'open': sum(r['open_tasks'] for r in workload_rows),
        'overdue': sum(r['overdue_tasks'] for r in workload_rows),
        'critical': sum(r['critical_overdue'] for r in workload_rows),
        'due_today': sum(r.get('due_today', 0) for r in workload_rows),
        'stalled': sum(r['stalled_tasks'] for r in workload_rows),
    }


# ---------------------------------------------------------------------------
# LLM — signal-to-noise classification
# ---------------------------------------------------------------------------
def _format_task_for_llm(task: dict) -> str:
    shared_note = " [SHARED OWNER — coordination risk]" if task.get('is_shared_owner') else ""
    stalled_note = " [STALLED — no activity 7+ days]" if task.get('is_stalled') else ""
    return (
        f"  Task: {task['tasks']}\n"
        f"  Agent: {task.get('agent', 'N/A')}\n"
        f"  Category: {task['task_category']}\n"
        f"  Created: {task['created_date']} | Due: {task.get('due_date', 'N/A')} | "
        f"Days overdue: {task['days_overdue']} | Emergency: {task['emergency_task']}/5{shared_note}{stalled_note}\n"
        f"  Comments ({task['num_comments']} total), recent thread:\n{task['comment_context']}"
    )


def _strip_fence(text: str) -> str:
    text = text.strip()
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    return text.strip()


def _repair_truncated_json(raw: str) -> dict:
    text = _strip_fence(raw)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    for cut in range(len(text) - 1, max(len(text) - 2000, 0), -1):
        chunk = text[:cut].rstrip()
        if not chunk.endswith(('}', ']', '"')):
            continue
        candidate = _close_json(chunk)
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    raise ValueError("Could not repair truncated JSON")


def _close_json(text: str) -> str:
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
    closers = {'{': '}', '[': ']'}
    return text + ''.join(closers[c] for c in reversed(stack))


def llm_generate_report(
    all_tasks: list,
    workload_rows: list,
    patterns: dict,
) -> dict:
    today = datetime.now().strftime("%Y-%m-%d")

    tasks_by_staff = defaultdict(list)
    for t in all_tasks:
        tasks_by_staff[t['responsible']].append(t)

    staff_sections = []
    for staff_name, tasks in sorted(tasks_by_staff.items(),
                                    key=lambda x: (-sum(1 for t in x[1] if t['tier'] in ('critical_overdue', 'overdue')), -len(x[1]))):
        section = f"\n=== {staff_name} | {len(tasks)} open tasks ===\n"
        for i, task in enumerate(tasks, 1):
            section += f"\n[Task {i}]\n{_format_task_for_llm(task)}\n"
        staff_sections.append(section)

    cat_lines = [f"  {cat}: {cnt} overdue" for cat, cnt in list(patterns['category_overdue'].items())[:5]]
    pattern_block = (
        "Category overdue:\n" + "\n".join(cat_lines) + "\n"
        f"Shared-ownership overdue: {patterns['shared_owner_overdue']}\n"
        f"Unassigned open: {patterns['unassigned_total']}"
    )

    user_msg = f"""Today: {today}.

Your job: classify each task into NEEDS ATTENTION, MONITORING, or ON TRACK.
Read every comment thread. Translate Vietnamese. Be specific with names, dates, ref numbers.

NEEDS ATTENTION = manager must act (blocked >3d, escalation needed, conflicting instructions, shared ownership unclear)
MONITORING = waiting on external party with clear follow-up date
ON TRACK = progressing normally

For NEEDS ATTENTION: tell the FULL STORY (3-6 sentences with timeline).
For MONITORING: 2-3 sentences (what waiting on, when follow up, risk).
For ON TRACK: 1 sentence only.

{chr(10).join(staff_sections)}

=== PATTERN SIGNALS ===
{pattern_block}

OUTPUT: Only the JSON object. No markdown fences."""

    response = claude.messages.create(
        model=MODEL,
        max_tokens=16000,
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
            logger.error("JSON repair failed: %s\nRaw (first 3000):\n%s", e2, raw[:3000])
            raise


# ---------------------------------------------------------------------------
# PDF design — v4 (condensed, 2-3 pages)
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
_BLUE = colors.HexColor('#2980b9')
_PURPLE = colors.HexColor('#8e44ad')
_RED_BG = colors.HexColor('#fdf2f2')
_YELLOW_BG = colors.HexColor('#fefce8')
_GREEN_BG = colors.HexColor('#f0fdf4')

PAGE_W, PAGE_H = A4
MARGIN = 1.8 * cm


def _styles():
    return {
        'doc_title': ParagraphStyle('doc_title', fontName='Helvetica-Bold', fontSize=18,
                                    textColor=_DARK, leading=22),
        'doc_sub': ParagraphStyle('doc_sub', fontName='Helvetica', fontSize=10,
                                  textColor=_MUTED, leading=14),
        'section_h': ParagraphStyle('section_h', fontName='Helvetica-Bold', fontSize=12,
                                    textColor=_ACCENT, leading=16, spaceBefore=14, spaceAfter=4),
        'bucket_h': ParagraphStyle('bucket_h', fontName='Helvetica-Bold', fontSize=11,
                                   textColor=_DARK, leading=14, spaceBefore=10, spaceAfter=4),
        'body': ParagraphStyle('body', fontName='Helvetica', fontSize=9,
                               textColor=_DARK, leading=13, spaceAfter=2),
        'body_indent': ParagraphStyle('body_indent', fontName='Helvetica', fontSize=9,
                                      textColor=_DARK, leading=13, spaceAfter=2, leftIndent=10),
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
        'on_track_line': ParagraphStyle('on_track_line', fontName='Helvetica', fontSize=8.5,
                                        textColor=_DARK, leading=12, leftIndent=10, spaceAfter=1),
        'staff_line': ParagraphStyle('staff_line', fontName='Helvetica', fontSize=9,
                                     textColor=_DARK, leading=13, spaceAfter=1),
    }


def _kpi_row(report: dict, st: dict) -> Table:
    stats = report.get('overall_stats', {})
    kpis = [
        ('Open', stats.get('total_open', 0), _ACCENT),
        ('Needs Attention', stats.get('needs_attention', 0),
         _RED if stats.get('needs_attention', 0) > 0 else _ACCENT),
        ('Monitoring', stats.get('monitoring', 0),
         _YELLOW if stats.get('monitoring', 0) > 0 else _ACCENT),
        ('On Track', stats.get('on_track', 0), _GREEN),
        ('Stalled', stats.get('total_stalled', 0),
         _PURPLE if stats.get('total_stalled', 0) > 0 else _ACCENT),
    ]
    top = [Paragraph(str(v), ParagraphStyle('kv', fontName='Helvetica-Bold', fontSize=22,
                     textColor=c, leading=26, alignment=TA_CENTER)) for _, v, c in kpis]
    bot = [Paragraph(k, st['kpi_lbl']) for k, _, _ in kpis]
    col_w = (PAGE_W - 2 * MARGIN) / len(kpis)
    tbl = Table([top, bot], colWidths=[col_w] * len(kpis))
    tbl.setStyle(TableStyle([
        ('BOX', (0, 0), (-1, -1), 0.5, _RULE),
        ('INNERGRID', (0, 0), (-1, -1), 0.5, _RULE),
        ('BACKGROUND', (0, 0), (-1, -1), _LIGHT_BG),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
    ]))
    return tbl


def _render_needs_attention(tasks: list, st: dict, condensed: bool = False) -> list:
    """Render full story-telling for tasks needing manager action.
    If `condensed` is True, limit number of items and truncate long text.
    """
    flows = []
    if not tasks:
        return flows
    flows.append(Paragraph(
        '<font color="#c0392b">&#x25cf;</font> Needs Attention',
        st['section_h']
    ))
    # Limit items in condensed mode
    if condensed and len(tasks) > 5:
        tasks = tasks[:5]

    for i, t in enumerate(tasks, 1):
        em = t.get('emergency', 0)
        created = t.get('created_date', 'N/A')
        due = t.get('due_date', 'N/A')

        # Title
        flows.append(Paragraph(
            f'<b>{i}. {t.get("task_title", "")}</b>  '
            f'<font color="#666666" size="8">{t.get("responsible", "")} | {t.get("category", "")}</font>',
            ParagraphStyle('att_title', fontName='Helvetica-Bold', fontSize=10,
                           textColor=_DARK, leading=14, spaceBefore=8, spaceAfter=1)
        ))

        # Meta
        meta_parts = [f"Agent: {t.get('agent', 'N/A')}", f"Created: {created}", f"Due: {due}"]
        if em > 0:
            meta_parts.append(f"Emergency: {em}/5")
        flows.append(Paragraph("  |  ".join(meta_parts), st['meta']))

        # Story
        story = t.get('story', '')
        if story:
            if condensed:
                story = _truncate(story, 300)
            flows.append(Paragraph(f'<b>Story:</b> {story}', st['body_indent']))

        # Blocker
        blocker = t.get('blocker', '')
        if blocker:
            if condensed:
                blocker = _truncate(blocker, 120)
            flows.append(Paragraph(
                f'<font color="#c0392b"><b>Blocker:</b> {blocker}</font>',
                ParagraphStyle('bl', fontName='Helvetica-Bold', fontSize=9,
                               textColor=_RED, leading=12, leftIndent=10, spaceAfter=1)
            ))

        # Action
        action = t.get('action', '')
        if action:
            if condensed:
                action = _truncate(action, 120)
            flows.append(Paragraph(
                f'<font color="#0f3460"><b>&#x2192; Action:</b> {action}</font>',
                ParagraphStyle('ac', fontName='Helvetica-Bold', fontSize=9,
                               textColor=_ACCENT, leading=12, leftIndent=10, spaceAfter=3)
            ))
    return flows


def _render_monitoring(tasks: list, st: dict, condensed: bool = False) -> list:
    """Render brief status for monitoring tasks."""
    flows = []
    if not tasks:
        return flows
    flows.append(Paragraph(
        '<font color="#d35400">&#x25cf;</font> Monitoring',
        st['section_h']
    ))
    # Limit in condensed mode
    if condensed and len(tasks) > 8:
        tasks = tasks[:8]

    for i, t in enumerate(tasks, 1):
        fu = t.get('follow_up_date', '')
        fu_str = f" — follow up {fu}" if fu and fu != 'N/A' else ""
        flows.append(Paragraph(
            f'<b>{i}. {t.get("task_title", "")}</b>  '
            f'<font color="#666666" size="8">({t.get("responsible", "")}){fu_str}</font>',
            ParagraphStyle('mon_title', fontName='Helvetica-Bold', fontSize=9.5,
                           textColor=_DARK, leading=13, spaceBefore=5, spaceAfter=1)
        ))
        status = t.get('status', '')
        if status:
            if condensed:
                status = _truncate(status, 220)
            flows.append(Paragraph(status, st['body_indent']))
    return flows


def _render_on_track(tasks: list, st: dict, condensed: bool = False) -> list:
    """Render one-liner list for on-track tasks."""
    flows = []
    if not tasks:
        return flows
    flows.append(Paragraph(
        '<font color="#27ae60">&#x25cf;</font> On Track',
        st['section_h']
    ))
    # Group by responsible for compactness
    by_staff = defaultdict(list)
    for t in tasks:
        by_staff[t.get('responsible', '(unknown)')].append(t)

    for staff, staff_tasks in sorted(by_staff.items()):
        flows.append(Paragraph(
            f'<b>{staff}</b> ({len(staff_tasks)} tasks)',
            ParagraphStyle('ot_staff', fontName='Helvetica-Bold', fontSize=9,
                           textColor=_ACCENT, leading=12, spaceBefore=4, spaceAfter=1)
        ))
        # In condensed mode, show up to 3 tasks per staff, otherwise show all
        display_tasks = staff_tasks if not condensed else staff_tasks[:3]
        for t in display_tasks:
            one_liner = t.get('one_liner', t.get('task_title', ''))
            if condensed:
                one_liner = _truncate(one_liner, 140)
            flows.append(Paragraph(
                f'&#x2022; <b>{t.get("task_title", "")}</b> — {one_liner}',
                st['on_track_line']
            ))
    return flows


def _render_patterns(patterns: list, st: dict) -> list:
    flows = []
    if not patterns:
        return flows
    flows.append(Paragraph("Pattern Alerts", st['section_h']))
    for p in patterns:
        flows.append(Paragraph(
            f'<b>{p.get("pattern", "")} ({p.get("count", 0)} tasks)</b> — {p.get("impact", "")}',
            ParagraphStyle('pa', fontName='Helvetica', fontSize=9, textColor=_ORANGE,
                           leading=13, spaceBefore=3)
        ))
        fix = p.get('fix', '') or p.get('recommendation', '')
        if fix:
            flows.append(Paragraph(f'Fix: {fix}', st['body_indent']))
    return flows


def _render_staff_workload(staff_list: list, st: dict) -> list:
    flows = []
    if not staff_list:
        return flows
    flows.append(Paragraph("Staff Workload", st['section_h']))
    for s in staff_list:
        att = s.get('attention_count', 0)
        load = s.get('assessment', 'manageable')
        load_colors = {'overloaded': _RED, 'manageable': _ORANGE, 'light': _GREEN}
        lc = load_colors.get(load, _DARK)
        att_str = f'  <font color="#c0392b">({att} needs attention)</font>' if att > 0 else ''
        flows.append(Paragraph(
            f'<b>{s.get("name", "")}</b> — {s.get("total_open", 0)} open '
            f'<font color="#{lc.hexval()[2:]}"><b>[{load.upper()}]</b></font>{att_str}  '
            f'{s.get("note", "")}',
            st['staff_line']
        ))
    return flows


def render_pdf(report: dict, stats: dict, today: str) -> bytes:
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

    # ── PAGE 1: Action Dashboard ──────────────────────────────────
    story.append(Paragraph("Customer Service Daily Reminder", st['doc_title']))
    story.append(Paragraph(
        f"Coverage: {title_agent}  |  Date: {today}  |  "
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        st['doc_sub']
    ))
    story.append(Spacer(1, 0.3 * cm))
    story.append(HRFlowable(width='100%', thickness=1.5, color=_ACCENT, spaceAfter=10))

    # Executive Summary
    story.append(Paragraph("Executive Summary", st['section_h']))
    story.append(Paragraph(report.get('executive_summary', ''), st['body']))
    story.append(Spacer(1, 0.15 * cm))

    # KPI Row
    story.append(_kpi_row(report, st))
    story.append(Spacer(1, 0.2 * cm))

    # Priority Actions
    actions = report.get('priority_actions') or []
    if actions:
        story.append(Paragraph("Priority Actions for Manager", st['section_h']))
        for i, a in enumerate(actions, 1):
            story.append(Paragraph(f"{i}. {a}", st['action']))
        story.append(Spacer(1, 0.15 * cm))

    # 🔴 Needs Attention (full story-telling)
    story.extend(_render_needs_attention(report.get('needs_attention') or [], st, condensed=SHORTEN_REPORT))

    # ── PAGE 2: Full Status ───────────────────────────────────────
    story.append(PageBreak())

    # 🟡 Monitoring
    story.extend(_render_monitoring(report.get('monitoring') or [], st, condensed=SHORTEN_REPORT))

    # 🟢 On Track
    story.extend(_render_on_track(report.get('on_track') or [], st, condensed=SHORTEN_REPORT))

    # Pattern Alerts
    story.extend(_render_patterns(report.get('pattern_alerts') or [], st))

    # Staff Workload
    story.extend(_render_staff_workload(report.get('staff_workload') or [], st))

    # Footer
    story.append(Spacer(1, 0.3 * cm))
    story.append(HRFlowable(width='100%', thickness=0.5, color=_RULE, spaceBefore=16))
    story.append(Paragraph(
        f"Generated by EPS Operations System | Source: {TASK_TABLE} | Model: {MODEL}",
        st['meta']
    ))

    doc.build(story)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Slack — brief outside, detail in PDF
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
    o_stats = report.get('overall_stats', {})

    # Header
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text",
                 "text": f"CS Daily Reminder — {title_agent} — {today}",
                 "emoji": True},
    })

    # Mentions + summary
    summary = report.get('executive_summary', '').strip()
    if mentions:
        summary = f"{mentions}\n\n{summary}"
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": summary}})

    # KPI fields
    blocks.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*Open*\n{o_stats.get('total_open', stats.get('open', 0))}"},
            {"type": "mrkdwn", "text": f"*:red_circle: Needs Attention*\n{o_stats.get('needs_attention', 0)}"},
            {"type": "mrkdwn", "text": f"*:large_orange_circle: Monitoring*\n{o_stats.get('monitoring', 0)}"},
            {"type": "mrkdwn", "text": f"*:white_check_mark: On Track*\n{o_stats.get('on_track', 0)}"},
        ],
    })
    blocks.append({"type": "divider"})

    # Needs attention tasks — brief in Slack
    att_tasks = report.get('needs_attention') or []
    if att_tasks:
        lines = []
        att_limit = 3 if SHORTEN_REPORT else 5
        for t in att_tasks[:att_limit]:
            blocker = t.get('blocker', '')
            max_len = 60 if SHORTEN_REPORT else 80
            blocker_short = (blocker[:max_len] + '...') if len(blocker) > max_len else blocker
            lines.append(
                f":red_circle: *{t.get('task_title', '')[:50]}* ({t.get('responsible', '')})\n"
                f"      _{blocker_short}_"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": "*Needs Attention:*\n" + "\n".join(lines)},
        })

    # Priority actions
    p_actions = report.get('priority_actions') or []
    if p_actions:
        blocks.append({"type": "divider"})
        action_limit = 2 if SHORTEN_REPORT else 4
        action_lines = [f"{i}. {a}" for i, a in enumerate(p_actions[:action_limit], 1)]
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": "*Priority Actions:*\n" + "\n".join(action_lines)},
        })

    # PDF link
    if pdf_permalink:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f":page_facing_up: *Full report:* {pdf_permalink}"},
        })
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": "_Full report: see attached PDF_"},
        })

    return blocks


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_daily_reminder(post: bool = True) -> dict:
    logger.info("Querying BigQuery (agent filter: '%s')", AGENT_FILTER or "ALL")

    all_tasks_raw = query_all_open_tasks()
    workload_rows = query_workload_summary()
    patterns = query_patterns()

    all_tasks = enrich_rows(all_tasks_raw)

    logger.info(
        "rows: all_open=%d staff=%d",
        len(all_tasks), len(workload_rows),
    )

    stats = compute_stats(workload_rows)
    report = llm_generate_report(all_tasks, workload_rows, patterns)

    today = datetime.now().strftime("%Y-%m-%d")
    pdf_bytes = render_pdf(report, stats, today)

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