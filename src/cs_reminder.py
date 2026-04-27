"""
Customer Service Daily Reminder — v3 (Insight-Driven)
======================================================

Philosophy:
  - BRIEF Slack message: overall KPIs + who has critical/overdue tasks → manager knows where to look
  - DETAILED PDF: per-staff breakdown → each staff's tasks listed one-by-one with
    created date, due date, category, LLM-read comment thread → what's stuck and why
  - Story-telling over numbers: "Quan Nguyen còn 6 task" is useless.
    "Quan Nguyen - 6 tasks: task A tạo 3 ngày trước đang chờ callback từ provider,
    task B stuck vì khách chưa confirm..." là đúng.

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
AGENT_FILTER = os.environ.get("CS_AGENT_FILTER", "")
TASK_TABLE = os.environ.get("CS_TASK_TABLE", "eps-470914.eps_data.health_task_raw")
CS_CHANNEL = os.environ.get("CS_REMINDER_CHANNEL")
CS_MANAGER_MENTIONS = os.environ.get("CS_MANAGER_MENTIONS", "").strip()
OVERDUE_LIMIT = int(os.environ.get("CS_OVERDUE_LIMIT", "30"))

# ---------------------------------------------------------------------------
# System prompt — story-telling, per-staff, per-task
# ---------------------------------------------------------------------------
_agent_filter_context = (
    f"The report covers only tasks where agent = '{AGENT_FILTER}'."
    if AGENT_FILTER else
    "The report covers ALL sales agents."
)

SYSTEM_PROMPT = f"""You are a senior operations analyst for EPS, a Vietnamese-American
health insurance brokerage. Your job: produce a daily CS briefing that TELLS A STORY
about each staff member's workload — not just numbers.

{_agent_filter_context}

DOMAIN KNOWLEDGE
  - CS tasks come from Notion (mirrored to BigQuery). Each task has a Slack thread
    (comments_json) that records the actual work conversation. Staff write in Vietnamese.
  - 'responsible' = the CS staff accountable. 'agent' = the sales agent the task relates to.
  - Emergency 0–5: >= 4 means handle today. "Critical overdue": open AND days_overdue >= 3 AND emergency >= 3.
  - "Stalled": open AND num_comments <= 1 AND days_since_edit >= 7.
  - Multi-person responsible = shared ownership = coordination risk.

STORY-TELLING RULES (CRITICAL)
  - For each CS staff member: list EVERY open task individually. Don't summarize away the details.
  - For each task: read the comment thread. What has been done? What is blocking progress?
    Translate Vietnamese comments accurately. Surface the SPECIFIC blocker.
  - "6 tasks" alone is useless. Tell the manager: task 1 đang chờ callback từ provider,
    task 2 khách chưa confirm lịch hẹn, task 3 referral gửi 2 ngày rồi chưa nhận được...
  - Be concrete: name specific people, providers, dates from comments. Don't invent.
  - Priority tiers for tasks within each staff section:
      🔴 critical_overdue (days_overdue >= 3 AND emergency >= 3) — list first
      🟠 overdue (due_date < today)
      🟡 due_today
      🟢 open / in progress

JSON SAFETY RULES (CRITICAL):
  - Never use unescaped double-quotes inside a JSON string value.
  - Never put literal newlines inside a JSON string value.
  - Never use smart quotes — ASCII only.
  - Close every array and object.

OUTPUT FORMAT — Return exactly ONE JSON object. No prose. No markdown fences.

{{
  "executive_summary": "2–3 sentences. Total open/overdue/critical. Single biggest risk. Who is most overloaded.",

  "overall_stats": {{
    "total_open": <int>,
    "total_overdue": <int>,
    "total_critical": <int>,
    "total_due_today": <int>,
    "total_stalled": <int>
  }},

  "staff_detail": [
    {{
      "name": "CS staff name",
      "open": <int>,
      "overdue": <int>,
      "critical": <int>,
      "due_today": <int>,
      "stalled": <int>,
      "load_assessment": "overloaded | manageable | light",
      "staff_summary": "1–2 sentences. Overall picture of this person's queue. Are they making progress or stuck? Any patterns across their tasks?",
      "tasks": [
        {{
          "task_title": "Short task name / client name",
          "agent": "Sales agent name",
          "category": "task category",
          "created_date": "YYYY-MM-DD",
          "due_date": "YYYY-MM-DD or N/A",
          "days_overdue": <int — 0 if not overdue>,
          "emergency": <int 0-5>,
          "tier": "critical_overdue | overdue | due_today | due_soon | open | stalled",
          "progress": "What has been done so far, drawn from comment thread. 1–2 sentences.",
          "blocker": "The specific thing blocking completion right now, or 'No blocker — actively progressing' if on track.",
          "action": "One concrete next step. Who does what."
        }}
      ],
      "manager_note": "One sentence: what should the manager do for this staff today, if anything?"
    }}
  ],

  "critical_alerts": [
    {{
      "task": "Task title / client name",
      "responsible": "CS staff name",
      "agent": "Sales agent",
      "category": "category",
      "days_overdue": <int>,
      "emergency": <int>,
      "blocker": "Specific blocker from thread",
      "action": "Immediate action needed"
    }}
  ],

  "pattern_alerts": [
    {{
      "pattern": "Short label",
      "count": <int>,
      "impact": "One sentence on operational impact.",
      "recommendation": "One sentence systemic fix."
    }}
  ],

  "team_analysis": "3–4 sentences. Who is overloaded? Who has capacity? Coordination risks? One staffing recommendation.",

  "risk_summary": "One sentence: the single highest systemic risk today.",

  "priority_actions": [
    "Specific actionable item with person/count/category named. Max 4."
  ]
}}

Rules:
  - staff_detail: one entry per CS staff with open tasks, ordered by overdue DESC.
  - tasks array: ALL open tasks for that staff, ordered: critical_overdue → overdue → due_today → open.
  - critical_alerts: top 5 most urgent tasks across all staff (critical_overdue tier first).
  - pattern_alerts: max 4, only if count >= 2.
  - All output in English (translate Vietnamese from comments).
  - Every claim grounded in data — never invent.
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
    """Fetch ALL open tasks with full detail including comments, ordered for story-telling."""
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
    """High-level per-responsible summary for stats."""
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
    """Category and coordination pattern signals."""
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
# LLM — story-telling per staff
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

    # Group tasks by responsible for the prompt
    tasks_by_staff = defaultdict(list)
    for t in all_tasks:
        tasks_by_staff[t['responsible']].append(t)

    # Build staff sections
    staff_sections = []
    for staff_name, tasks in sorted(tasks_by_staff.items(),
                                    key=lambda x: (-sum(1 for t in x[1] if t['tier'] in ('critical_overdue', 'overdue')), -len(x[1]))):
        overdue_count = sum(1 for t in tasks if t['tier'] in ('critical_overdue', 'overdue'))
        critical_count = sum(1 for t in tasks if t['tier'] == 'critical_overdue')
        section = f"\n=== {staff_name} | {len(tasks)} open | {overdue_count} overdue | {critical_count} critical ===\n"
        for i, task in enumerate(tasks, 1):
            tier_label = {
                'critical_overdue': '🔴 CRITICAL OVERDUE',
                'overdue': '🟠 OVERDUE',
                'due_today': '🟡 DUE TODAY',
                'due_soon': '🔵 DUE SOON',
                'open': '🟢 OPEN',
                'no_due_date': '⚪ NO DUE DATE',
            }.get(task.get('tier', 'open'), '🟢 OPEN')
            section += f"\n[Task {i} — {tier_label}]\n{_format_task_for_llm(task)}\n"
        staff_sections.append(section)

    # Pattern summary
    cat_lines = [f"  {cat}: {cnt} overdue" for cat, cnt in list(patterns['category_overdue'].items())[:5]]
    pattern_block = (
        "Category overdue:\n" + "\n".join(cat_lines) + "\n"
        f"Shared-ownership overdue: {patterns['shared_owner_overdue']}\n"
        f"Unassigned open: {patterns['unassigned_total']}"
    )

    user_msg = f"""Today: {today}.

Your job: produce an insight-driven, story-telling report about each CS staff member.
For EVERY open task: read the comment thread, translate Vietnamese, identify what's been done
and what is blocking progress. Be specific — name providers, dates, reference numbers from threads.

{chr(10).join(staff_sections)}

=== PATTERN SIGNALS ===
{pattern_block}

OUTPUT RULES:
- staff_detail: one entry per staff, tasks array = ALL their open tasks (not just overdue).
- critical_alerts: top 5 most urgent across all staff.
- pattern_alerts: only count >= 2, max 4.
- JSON strings: no unescaped double-quotes, no literal newlines, ASCII only.
- Output ONLY the JSON object."""

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
# PDF design
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

PAGE_W, PAGE_H = A4
MARGIN = 1.8 * cm

TIER_COLORS = {
    'critical_overdue': _RED,
    'overdue': _ORANGE,
    'due_today': _YELLOW,
    'due_soon': _BLUE,
    'open': _GREEN,
    'no_due_date': _MUTED,
    'stalled': colors.HexColor('#8e44ad'),
}

TIER_LABELS = {
    'critical_overdue': 'CRITICAL',
    'overdue': 'OVERDUE',
    'due_today': 'DUE TODAY',
    'due_soon': 'DUE SOON',
    'open': 'OPEN',
    'no_due_date': 'NO DATE',
    'stalled': 'STALLED',
}


def _styles():
    return {
        'doc_title': ParagraphStyle('doc_title', fontName='Helvetica-Bold', fontSize=18,
                                    textColor=_DARK, leading=22),
        'doc_sub': ParagraphStyle('doc_sub', fontName='Helvetica', fontSize=10,
                                  textColor=_MUTED, leading=14),
        'section_h': ParagraphStyle('section_h', fontName='Helvetica-Bold', fontSize=12,
                                    textColor=_ACCENT, leading=16, spaceBefore=14, spaceAfter=4),
        'staff_h': ParagraphStyle('staff_h', fontName='Helvetica-Bold', fontSize=11,
                                  textColor=_DARK, leading=14, spaceBefore=12, spaceAfter=3),
        'task_label': ParagraphStyle('task_label', fontName='Helvetica-Bold', fontSize=9.5,
                                     textColor=_DARK, leading=13, spaceBefore=6, spaceAfter=1),
        'body': ParagraphStyle('body', fontName='Helvetica', fontSize=9,
                               textColor=_DARK, leading=13, spaceAfter=2),
        'body_detail': ParagraphStyle('body_detail', fontName='Helvetica', fontSize=9,
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
        'blocker': ParagraphStyle('blocker', fontName='Helvetica-Bold', fontSize=9,
                                  textColor=_RED, leading=12),
        'action_item': ParagraphStyle('action_item', fontName='Helvetica-Bold', fontSize=9,
                                      textColor=_ACCENT, leading=12),
    }


def _kpi_row(stats: dict, st: dict) -> Table:
    kpis = [
        ('Open', stats['open'], _ACCENT),
        ('Overdue', stats['overdue'], _ORANGE if stats['overdue'] > 0 else _ACCENT),
        ('Critical', stats['critical'], _RED if stats['critical'] > 0 else _ACCENT),
        ('Due Today', stats['due_today'], _YELLOW if stats['due_today'] > 0 else _ACCENT),
        ('Stalled', stats['stalled'], colors.HexColor('#8e44ad') if stats['stalled'] > 0 else _ACCENT),
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


def _tier_badge(tier: str) -> Paragraph:
    color = TIER_COLORS.get(tier, _MUTED)
    label = TIER_LABELS.get(tier, tier.upper())
    return Paragraph(
        f'<font color="#{color.hexval()[2:]}"><b>[{label}]</b></font>',
        ParagraphStyle('badge', fontName='Helvetica-Bold', fontSize=8.5, leading=11)
    )


def _staff_section(staff: dict, st: dict) -> list:
    """Render one staff member's full task breakdown."""
    flows = []
    name = staff.get('name', '')
    open_c = staff.get('open', 0)
    overdue_c = staff.get('overdue', 0)
    critical_c = staff.get('critical', 0)
    load = staff.get('load_assessment', 'manageable')

    load_colors = {'overloaded': _RED, 'manageable': _ORANGE, 'light': _GREEN}
    load_color = load_colors.get(load, _DARK)

    # Staff header
    flows.append(HRFlowable(width='100%', thickness=1.5, color=_ACCENT, spaceBefore=8, spaceAfter=4))
    flows.append(Paragraph(
        f'{name} — {open_c} open tasks | {overdue_c} overdue | {critical_c} critical | '
        f'<font color="#{load_color.hexval()[2:]}"><b>{load.upper()}</b></font>',
        st['staff_h']
    ))

    # Staff summary
    summary = staff.get('staff_summary', '')
    if summary:
        flows.append(Paragraph(summary, st['body']))

    # Per-task breakdown
    tasks = staff.get('tasks', [])
    for i, task in enumerate(tasks, 1):
        tier = task.get('tier', 'open')
        tier_color = TIER_COLORS.get(tier, _MUTED)
        tier_label = TIER_LABELS.get(tier, tier.upper())

        # Task title row
        em = task.get('emergency', 0)
        days_od = task.get('days_overdue', 0)
        due = task.get('due_date', 'N/A')
        created = task.get('created_date', 'N/A')

        title_text = f'{i}. {task.get("task_title", "")}'
        flows.append(Paragraph(
            f'<font color="#{tier_color.hexval()[2:]}"><b>[{tier_label}]</b></font>  '
            f'<b>{title_text}</b>',
            ParagraphStyle('tth', fontName='Helvetica-Bold', fontSize=9.5,
                           textColor=_DARK, leading=13, spaceBefore=7, spaceAfter=1)
        ))

        # Meta line
        meta_parts = [
            f"Agent: {task.get('agent', 'N/A')}",
            f"Category: {task.get('category', 'N/A')}",
            f"Created: {created}",
            f"Due: {due}",
        ]
        if days_od > 0:
            meta_parts.append(f"Overdue: {days_od}d")
        if em > 0:
            meta_parts.append(f"Emergency: {em}/5")
        flows.append(Paragraph("  |  ".join(meta_parts), st['meta']))

        # Progress
        progress = task.get('progress', '')
        if progress:
            flows.append(Paragraph(f'<b>Progress:</b> {progress}', st['body_detail']))

        # Blocker
        blocker = task.get('blocker', '')
        if blocker and 'no blocker' not in blocker.lower():
            flows.append(Paragraph(
                f'<b>⚠ Blocker:</b> {blocker}',
                ParagraphStyle('bl', fontName='Helvetica-Bold', fontSize=9,
                               textColor=_RED if tier in ('critical_overdue', 'overdue') else _ORANGE,
                               leading=12, leftIndent=10, spaceAfter=1)
            ))

        # Action
        action = task.get('action', '')
        if action:
            flows.append(Paragraph(
                f'<b>→ Action:</b> {action}',
                ParagraphStyle('ac', fontName='Helvetica-Bold', fontSize=9,
                               textColor=_ACCENT, leading=12, leftIndent=10, spaceAfter=2)
            ))

    # Manager note
    manager_note = staff.get('manager_note', '')
    if manager_note:
        flows.append(Paragraph(
            f'<b>Manager Note:</b> {manager_note}',
            ParagraphStyle('mn', fontName='Helvetica-Bold', fontSize=9,
                           textColor=_DARK, leading=12, spaceBefore=4,
                           borderPad=4)
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

    # Header
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
    story.append(Paragraph("Backlog Snapshot", st['section_h']))
    story.append(_kpi_row(stats, st))
    story.append(Spacer(1, 0.2 * cm))

    # Risk
    risk = report.get('risk_summary', '')
    if risk:
        story.append(Paragraph(f"⚠ Highest Risk: {risk}", st['risk']))
        story.append(Spacer(1, 0.2 * cm))

    # Critical Alerts
    alerts = report.get('critical_alerts') or []
    if alerts:
        story.append(Paragraph("Critical Alerts", st['section_h']))
        for i, alert in enumerate(alerts, 1):
            em = alert.get('emergency', 0)
            days = alert.get('days_overdue', 0)
            story.append(Paragraph(
                f"{i}. <b>{alert.get('task', '')}</b> — {alert.get('responsible', '')} "
                f"[Agent: {alert.get('agent', '')}]",
                ParagraphStyle('ca', fontName='Helvetica-Bold', fontSize=9.5,
                               textColor=_RED, leading=13, spaceBefore=5)
            ))
            story.append(Paragraph(
                f"Category: {alert.get('category', '')} | {days}d overdue | Emergency: {em}/5",
                st['meta']
            ))
            if alert.get('blocker'):
                story.append(Paragraph(f"Blocker: {alert['blocker']}", st['body_detail']))
            if alert.get('action'):
                story.append(Paragraph(f"→ {alert['action']}", st['action_item']))
        story.append(Spacer(1, 0.2 * cm))

    # Pattern Alerts
    patterns = report.get('pattern_alerts') or []
    if patterns:
        story.append(Paragraph("Pattern Alerts", st['section_h']))
        for p in patterns:
            story.append(Paragraph(
                f"<b>{p.get('pattern', '')} ({p.get('count', 0)} tasks)</b> — {p.get('impact', '')}",
                ParagraphStyle('pa', fontName='Helvetica', fontSize=9, textColor=_ORANGE,
                               leading=13, spaceBefore=3)
            ))
            if p.get('recommendation'):
                story.append(Paragraph(f"Fix: {p['recommendation']}", st['body_detail']))

    # Priority Actions
    actions = report.get('priority_actions') or []
    if actions:
        story.append(Paragraph("Priority Actions for Manager", st['section_h']))
        for i, a in enumerate(actions, 1):
            story.append(Paragraph(f"{i}. {a}", st['action']))

    # Per-Staff Task Breakdown (main detailed section)
    staff_detail = report.get('staff_detail') or []
    if staff_detail:
        story.append(PageBreak())
        story.append(Paragraph("Staff Task Breakdown (Full Detail)", st['section_h']))
        story.append(Paragraph(
            "Each staff member's open tasks, with progress and blockers drawn from Slack thread analysis.",
            st['body']
        ))
        for staff in staff_detail:
            story.extend(_staff_section(staff, st))

    # Team Analysis
    team_analysis = report.get('team_analysis', '')
    if team_analysis:
        story.append(Spacer(1, 0.3 * cm))
        story.append(Paragraph("Team Analysis", st['section_h']))
        story.append(Paragraph(team_analysis, st['body']))

    # Footer
    story.append(HRFlowable(width='100%', thickness=0.5, color=_RULE, spaceBefore=16))
    story.append(Paragraph(
        f"Generated by EPS Operations System | Source: {TASK_TABLE} | Model: {MODEL}",
        st['meta']
    ))

    doc.build(story)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Slack — BRIEF outside, detail inside PDF
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
    """
    BRIEF Slack message:
    - Overall KPIs
    - Who has critical/overdue tasks + brief reason
    - Top critical alerts (3 max)
    - Link to PDF for full detail
    """
    blocks = []
    title_agent = AGENT_FILTER if AGENT_FILTER else "All Agents"

    # Header
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text",
                 "text": f"📋 CS Daily Reminder — {title_agent} — {today}",
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
            {"type": "mrkdwn", "text": f"*📂 Open*\n{stats['open']}"},
            {"type": "mrkdwn", "text": f"*🟠 Overdue*\n{stats['overdue']}"},
            {"type": "mrkdwn", "text": f"*🔴 Critical*\n{stats['critical']}"},
            {"type": "mrkdwn", "text": f"*🟡 Due Today*\n{stats['due_today']}"},
        ],
    })
    blocks.append({"type": "divider"})

    # Per-staff brief: who has issues
    staff_detail = report.get('staff_detail') or []
    problem_staff = [s for s in staff_detail if s.get('overdue', 0) > 0 or s.get('critical', 0) > 0]
    if problem_staff:
        lines = []
        for s in problem_staff[:6]:
            flags = []
            if s.get('critical', 0) > 0:
                flags.append(f"🔴 {s['critical']} critical")
            if s.get('overdue', 0) > 0:
                flags.append(f"🟠 {s['overdue']} overdue")
            if s.get('stalled', 0) > 0:
                flags.append(f"⚫ {s['stalled']} stalled")
            flag_str = "  ".join(flags)
            lines.append(f"*{s.get('name', '')}*  ({s.get('open', 0)} open) — {flag_str}")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": "*⚠️ Staff with urgent tasks:*\n" + "\n".join(lines)},
        })
        blocks.append({"type": "divider"})

    # Top 3 critical alerts
    alerts = report.get('critical_alerts') or []
    if alerts:
        lines = []
        for a in alerts[:3]:
            days = a.get('days_overdue', 0)
            blocker = a.get('blocker', '')
            blocker_short = blocker[:80] + '...' if len(blocker) > 80 else blocker
            lines.append(
                f"• `{a.get('responsible', '')}` — *{a.get('task', '')[:60]}*\n"
                f"  _{days}d overdue_ | {blocker_short}"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": "*🚨 Critical tasks (top 3):*\n" + "\n".join(lines)},
        })

    # Risk
    risk = report.get('risk_summary', '')
    if risk:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*⚠️ Key Risk:* {risk}"},
        })

    # PDF link
    if pdf_permalink:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"📄 *Full detail (per-staff, per-task breakdown):* {pdf_permalink}"},
        })
    elif not pdf_permalink:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": "_Full task detail: see attached PDF_"},
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