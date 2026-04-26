"""
Customer Service Daily Reminder — Enhanced
==========================================

Improvements over v1:
  - Filters to a single agent (AGENT_FILTER = "Khang Nguyen")
  - Parses and cleans comments_json before feeding to AI
  - AI receives full task context (conversation thread, not just metadata)
  - Generates a professional PDF report (no emojis, no router buttons)
  - Uploads PDF to Slack; channel message shows only the headline KPIs
  - Deep per-task narrative analysis instead of data tables

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
AGENT_FILTER = os.environ.get("CS_AGENT_FILTER", "Khang Nguyen")
TASK_TABLE = os.environ.get("CS_TASK_TABLE", "eps-470914.eps_data.health_task_raw")
CS_CHANNEL = os.environ.get("CS_REMINDER_CHANNEL")
CS_MANAGER_MENTIONS = os.environ.get("CS_MANAGER_MENTIONS", "").strip()
OVERDUE_LIMIT = int(os.environ.get("CS_OVERDUE_LIMIT", "20"))
STALLED_LIMIT = int(os.environ.get("CS_STALLED_LIMIT", "15"))

# ---------------------------------------------------------------------------
# System prompt — deep domain context for the AI analyst
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = f"""You are a senior operations analyst for EPS, a Vietnamese-American
health insurance brokerage. Your task is to produce a daily briefing for the team manager
about the Customer Service backlog for sales agent {AGENT_FILTER}'s clients.

DOMAIN KNOWLEDGE
  - CS tasks come from Notion (mirrored to BigQuery). Each task has a Slack thread
    (comments_json) that records the actual work conversation between CS staff.
    Staff often write in Vietnamese — read and interpret those threads accurately.
  - The 'responsible' field is the CS staff accountable for completion.
  - 'agent' is the sales agent (always {AGENT_FILTER}) — informational only.
  - Task categories: Scheduling Appointment, Verify Insurance/Network, Resolve Billing
    Issue, Submit/Follow-up Referral, Call Doctor Office, Call Insurance Company, etc.
  - Emergency rating 0-5: 4+ means must handle today. 0 = low urgency.
  - "Critical overdue": open AND days_overdue >= 3 AND emergency >= 3.
  - "Stalled": open AND num_comments <= 1 AND days_since_edit >= 7.
  - Multi-person responsible fields (e.g. "Kay Huynh, Dung Ha") indicate shared
    ownership — a coordination risk that often leads to tasks falling through the cracks.

WHAT MAKES A GOOD ANALYSIS
  - Read the comment threads carefully. They reveal the real status (e.g. "waiting for
    callback", "referral not received", "client hasn't confirmed yet").
  - The comment threads are in Vietnamese — translate the key facts into your English analysis.
  - Identify the specific blocker, not just that a task is overdue.
  - Name specific people when recommending actions.
  - Surface patterns across tasks (e.g. referral delays with one clinic, one staff member
    overloaded, shared-ownership tasks consistently going stale).
  - The manager reads this to make decisions — be concrete, not generic.
  - For team_analysis: do NOT describe numbers in table form. Write a paragraph that
    explains what the distribution actually means — who is carrying the most weight,
    who has capacity, where coordination overhead is hurting throughput, and what the
    manager should watch. Mention specific staff names with context.

OUTPUT FORMAT
  Return exactly ONE JSON object. No prose. No markdown fences. All text in English.

{{
  "executive_summary": "2-3 sentences. State total open, total overdue (with critical
    count), due today, stalled, and name the biggest risk.",

  "critical_tasks": [
    {{
      "task": "Client name or short task title (max 80 chars)",
      "responsible": "CS staff name",
      "category": "task category",
      "due": "YYYY-MM-DD",
      "days_overdue": <int>,
      "emergency": <int 0-5>,
      "current_status": "One sentence: what has been done so far, drawn from the
        comment thread.",
      "blocker": "One sentence: the specific blocker right now.",
      "recommended_action": "One sentence: concrete action naming who should do what."
    }}
  ],

  "stuck_tasks": [
    {{
      "task": "...",
      "responsible": "... or '(unassigned)'",
      "reason": "unassigned | stalled",
      "days_since_activity": <int>,
      "emergency": <int 0-5>,
      "category": "...",
      "analysis": "One sentence explaining WHY this is stuck, grounded in the data
        signals (no comments, no edit, unassigned, shared ownership with no follow-up).",
      "recommended_action": "One sentence with a specific next step and owner."
    }}
  ],

  "team_analysis": "One paragraph (4-6 sentences) analyzing the workload across the
    team. Explain what the distribution means in practice — who is overloaded, who has
    capacity, whether shared-ownership tasks are a risk, and one concrete staffing
    recommendation for the manager. Reference specific staff names.",

  "risk_summary": "One sentence naming the single highest systemic risk today.",

  "priority_actions": [
    "Specific, actionable item. Reference a person, a count, or a category. Max 4 items."
  ]
}}

Rules:
  - critical_tasks: up to 6 items. Prioritise tier=critical_overdue, then highest
    emergency, then largest days_overdue.
  - stuck_tasks: up to 5 items. Unassigned first.
  - Every factual claim must be grounded in the data — never invent.
  - All output fields in English.
"""

# ---------------------------------------------------------------------------
# SQL — all queries filter to AGENT_FILTER
# ---------------------------------------------------------------------------
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
    AND TRIM(agent) = '{AGENT_FILTER}'
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
      COUNTIF(is_completed = 0 AND emergency_task >= 4) AS high_priority_open
    FROM tiered
    GROUP BY responsible
    HAVING open_tasks > 0
    ORDER BY critical_overdue DESC, overdue_tasks DESC, open_tasks DESC
    """
    return [dict(r) for r in bq_client.query(sql).result()]


def query_critical_and_overdue(limit: int = OVERDUE_LIMIT) -> list:
    sql = _base_cte() + f"""
    SELECT
      record_id, responsible, task_category, tasks, task_summary,
      due_date, days_overdue, emergency_task, num_comments, days_since_edit,
      tier, comments_json
    FROM tiered
    WHERE is_completed = 0 AND tier IN ('critical_overdue','overdue')
    ORDER BY
      CASE tier WHEN 'critical_overdue' THEN 0 ELSE 1 END,
      emergency_task DESC, days_overdue DESC
    LIMIT {limit}
    """
    return [dict(r) for r in bq_client.query(sql).result()]


def query_stalled_and_unassigned(limit: int = STALLED_LIMIT) -> list:
    sql = _base_cte() + f"""
    SELECT
      record_id, responsible, task_category, tasks, task_summary,
      due_date, days_overdue, emergency_task, num_comments, days_since_edit,
      CASE WHEN responsible = '(unassigned)' THEN 'unassigned' ELSE 'stalled' END AS reason,
      comments_json
    FROM tiered
    WHERE is_completed = 0 AND (is_stalled OR responsible = '(unassigned)')
    ORDER BY
      CASE WHEN responsible = '(chua phan cong)' THEN 0 ELSE 1 END,
      emergency_task DESC, days_since_edit DESC
    LIMIT {limit}
    """
    return [dict(r) for r in bq_client.query(sql).result()]


# ---------------------------------------------------------------------------
# Data cleaning and enrichment
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
    """Return the last N comments as a readable block for the LLM."""
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
    """Parse comments, clean fields, add derived signals."""
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
        'due_today': sum(r['due_today'] for r in workload_rows),
        'stalled': sum(r['stalled_tasks'] for r in workload_rows),
        'high_priority_open': sum(r['high_priority_open'] for r in workload_rows),
    }


# ---------------------------------------------------------------------------
# LLM — build enriched task blocks and call Claude
# ---------------------------------------------------------------------------
def _format_task_block(task: dict) -> str:
    due_str = str(task.get('due_date', 'N/A'))
    return (
        f"Task: {task['tasks']}\n"
        f"Summary: {task.get('task_summary', '')}\n"
        f"Category: {task['task_category']}\n"
        f"Responsible: {task['responsible']}\n"
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


def llm_generate_report(workload_rows: list, overdue_rows: list, stalled_rows: list) -> dict:
    today = datetime.now().strftime("%Y-%m-%d")

    # Build workload as a structured prose hint — give Claude the numbers so it
    # can reason, but the JSON schema forces it to output a narrative paragraph.
    workload_lines = [
        "Staff member | Open tasks | Critical | Overdue | Due today | Stalled | "
        "High-priority open | Note"
    ]
    for r in workload_rows:
        name = r['responsible']
        notes = []
        if ',' in name:
            notes.append("shared ownership")
        if r['critical_overdue'] > 0:
            notes.append(f"{r['critical_overdue']} critical overdue")
        if r['overdue_tasks'] >= 3:
            notes.append("heavy overdue load")
        if r['stalled_tasks'] > 0:
            notes.append(f"{r['stalled_tasks']} stalled")
        note_str = "; ".join(notes) if notes else ""
        workload_lines.append(
            f"{name} | {r['open_tasks']} | {r['critical_overdue']} | "
            f"{r['overdue_tasks']} | {r['due_today']} | {r['stalled_tasks']} | "
            f"{r['high_priority_open']} | {note_str}"
        )

    overdue_blocks = "\n\n---\n\n".join(
        f"[CRITICAL/OVERDUE #{i+1}]\n{_format_task_block(t)}"
        for i, t in enumerate(overdue_rows)
    )

    stalled_blocks = "\n\n---\n\n".join(
        f"[STUCK #{i+1} | reason={t.get('reason','stalled')}]\n{_format_task_block(t)}"
        for i, t in enumerate(stalled_rows)
    )

    user_msg = f"""Today: {today}. Agent: {AGENT_FILTER}.

=== WORKLOAD BY RESPONSIBLE ===
{chr(10).join(workload_lines)}

=== CRITICAL AND OVERDUE TASKS (read comment threads carefully) ===
{overdue_blocks if overdue_blocks else '(none)'}

=== STUCK AND UNASSIGNED TASKS ===
{stalled_blocks if stalled_blocks else '(none)'}

Produce the JSON report now."""

    response = claude.messages.create(
        model=MODEL,
        max_tokens=3000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = response.content[0].text
    cleaned = _strip_fence(raw)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error("JSON parse failed: %s\nRaw:\n%s", e, raw[:3000])
        raise


# ---------------------------------------------------------------------------
# PDF report renderer — professional, no emojis
# ---------------------------------------------------------------------------
_DARK = colors.HexColor('#1a1a2e')
_MID = colors.HexColor('#16213e')
_ACCENT = colors.HexColor('#0f3460')
_RULE = colors.HexColor('#cccccc')
_LIGHT_BG = colors.HexColor('#f5f7fa')
_RED = colors.HexColor('#c0392b')
_ORANGE = colors.HexColor('#d35400')
_GREEN = colors.HexColor('#27ae60')
_MUTED = colors.HexColor('#666666')

PAGE_W, PAGE_H = A4
MARGIN = 2 * cm


def _styles():
    base = getSampleStyleSheet()
    S = lambda name, **kw: ParagraphStyle(name, **kw)
    return {
        'doc_title': S('doc_title', fontName='Helvetica-Bold', fontSize=18,
                       textColor=_DARK, leading=22, alignment=TA_LEFT),
        'doc_sub': S('doc_sub', fontName='Helvetica', fontSize=10,
                     textColor=_MUTED, leading=14, alignment=TA_LEFT),
        'section_h': S('section_h', fontName='Helvetica-Bold', fontSize=12,
                       textColor=_ACCENT, leading=16, spaceBefore=14, spaceAfter=4),
        'task_h': S('task_h', fontName='Helvetica-Bold', fontSize=10,
                    textColor=_DARK, leading=13, spaceBefore=8, spaceAfter=2),
        'label': S('label', fontName='Helvetica-Bold', fontSize=8,
                   textColor=_MUTED, leading=11),
        'body': S('body', fontName='Helvetica', fontSize=9,
                  textColor=_DARK, leading=13, spaceAfter=3),
        'body_vn': S('body_vn', fontName='Helvetica', fontSize=9.5,
                     textColor=_DARK, leading=14, spaceAfter=4),
        'meta': S('meta', fontName='Helvetica-Oblique', fontSize=8,
                  textColor=_MUTED, leading=11),
        'kpi_val': S('kpi_val', fontName='Helvetica-Bold', fontSize=22,
                     textColor=_ACCENT, leading=26, alignment=TA_CENTER),
        'kpi_lbl': S('kpi_lbl', fontName='Helvetica', fontSize=7.5,
                     textColor=_MUTED, leading=10, alignment=TA_CENTER),
        'action': S('action', fontName='Helvetica', fontSize=9.5,
                    textColor=_DARK, leading=14, leftIndent=10, spaceAfter=3),
        'risk': S('risk', fontName='Helvetica-BoldOblique', fontSize=9.5,
                  textColor=_RED, leading=13, borderPad=4),
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
    tbl = Table([top, bot], colWidths=[(PAGE_W - 2 * MARGIN) / len(kpis)] * len(kpis))
    tbl.setStyle(TableStyle([
        ('BOX', (0, 0), (-1, -1), 0.5, _RULE),
        ('INNERGRID', (0, 0), (-1, -1), 0.5, _RULE),
        ('BACKGROUND', (0, 0), (-1, -1), _LIGHT_BG),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ('TEXTCOLOR', (2, 0), (2, 0), _RED if stats['critical'] > 0 else _ACCENT),
    ]))
    return tbl


def _workload_table(workload_rows: list, st: dict) -> Table:
    headers = ['Responsible', 'Open', 'Critical', 'Overdue', 'Today', 'Stalled', 'Hi-prio']
    col_w = [(PAGE_W - 2 * MARGIN) * f for f in [0.32, 0.1, 0.1, 0.11, 0.1, 0.1, 0.1, 0.07]]
    # drop last col (uneven split OK)
    col_w = [(PAGE_W - 2 * MARGIN) / len(headers)] * len(headers)
    col_w[0] = (PAGE_W - 2 * MARGIN) * 0.30
    rest = ((PAGE_W - 2 * MARGIN) * 0.70) / (len(headers) - 1)
    col_w = [col_w[0]] + [rest] * (len(headers) - 1)

    def cell(txt, bold=False, color=_DARK, align=TA_CENTER):
        fn = 'Helvetica-Bold' if bold else 'Helvetica'
        return Paragraph(str(txt), ParagraphStyle('c', fontName=fn, fontSize=8,
                                                   textColor=color, alignment=align, leading=11))

    rows = [[cell(h, bold=True, color=_MUTED) for h in headers]]
    for r in workload_rows:
        is_alert = r['critical_overdue'] > 0 or r['overdue_tasks'] >= 3
        name_color = _RED if is_alert else _DARK
        rows.append([
            cell(r['responsible'], bold=is_alert, color=name_color, align=TA_LEFT),
            cell(r['open_tasks']),
            cell(r['critical_overdue'],
                 color=_RED if r['critical_overdue'] > 0 else _DARK),
            cell(r['overdue_tasks'],
                 color=_ORANGE if r['overdue_tasks'] > 0 else _DARK),
            cell(r['due_today']),
            cell(r['stalled_tasks']),
            cell(r['high_priority_open']),
        ])

    tbl = Table(rows, colWidths=col_w, repeatRows=1)
    style = [
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BACKGROUND', (0, 0), (-1, 0), _ACCENT),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, _LIGHT_BG]),
        ('GRID', (0, 0), (-1, -1), 0.3, _RULE),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
    ]
    tbl.setStyle(TableStyle(style))
    return tbl


def _task_card(task: dict, index: int, st: dict, is_critical: bool = True) -> list:
    """Render one task as a sequence of Platypus flowables."""
    flows = []
    # Task header
    em = task.get('emergency', task.get('emergency_task', 0))
    days = task.get('days_overdue', task.get('days_since_activity', 0))
    due = task.get('due', str(task.get('due_date', 'N/A')))
    resp = task.get('responsible', '')
    cat = task.get('category', task.get('task_category', ''))

    header_color = _RED if (is_critical and em >= 3) else _ORANGE if em >= 2 else _DARK
    flows.append(Paragraph(
        f"{index}. {task.get('task', task.get('tasks', ''))}",
        ParagraphStyle('th', fontName='Helvetica-Bold', fontSize=9.5,
                       textColor=header_color, leading=13, spaceBefore=6)
    ))

    # Meta row
    meta_parts = [f"Responsible: {resp}", f"Category: {cat}", f"Due: {due}"]
    if is_critical:
        meta_parts += [f"Days overdue: {days}", f"Emergency: {em}/5"]
    else:
        meta_parts += [f"Days without activity: {days}", f"Emergency: {em}/5"]
    flows.append(Paragraph("  |  ".join(meta_parts), st['meta']))

    # Analysis fields
    if is_critical:
        for lbl, key in [
            ("Current Status", "current_status"),
            ("Blocker", "blocker"),
            ("Recommended Action", "recommended_action"),
        ]:
            val = task.get(key, '')
            if val:
                flows.append(Paragraph(f"<b>{lbl}:</b> {val}", st['body_vn']))
    else:
        for lbl, key in [
            ("Analysis", "analysis"),
            ("Recommended Action", "recommended_action"),
        ]:
            val = task.get(key, '')
            if val:
                flows.append(Paragraph(f"<b>{lbl}:</b> {val}", st['body_vn']))

    flows.append(HRFlowable(width='100%', thickness=0.3, color=_RULE, spaceAfter=4))
    return flows


def render_pdf(report: dict, stats: dict, workload_rows: list, today: str) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=MARGIN, bottomMargin=MARGIN,
        title=f"CS Daily Reminder — {AGENT_FILTER} — {today}",
        author="EPS Operations",
    )
    st = _styles()
    story = []

    # === Cover / Header ===
    story.append(Paragraph(f"Customer Service Daily Reminder", st['doc_title']))
    story.append(Paragraph(
        f"Agent: {AGENT_FILTER}  |  Report date: {today}  |  "
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        st['doc_sub']
    ))
    story.append(Spacer(1, 0.3 * cm))
    story.append(HRFlowable(width='100%', thickness=1.5, color=_ACCENT, spaceAfter=10))

    # === Executive Summary ===
    story.append(Paragraph("Executive Summary", st['section_h']))
    story.append(Paragraph(report.get('executive_summary', ''), st['body_vn']))
    story.append(Spacer(1, 0.2 * cm))

    # === KPI Row ===
    story.append(Paragraph("Key Performance Indicators", st['section_h']))
    story.append(_kpi_row(stats, st))
    story.append(Spacer(1, 0.4 * cm))

    # === Risk Summary ===
    risk = report.get('risk_summary', '')
    if risk:
        story.append(Paragraph("Risk Assessment", st['section_h']))
        story.append(Paragraph(risk, st['risk']))
        story.append(Spacer(1, 0.2 * cm))

    # === Team Workload Analysis (narrative, no table) ===
    story.append(Paragraph("Team Workload Analysis", st['section_h']))
    team_analysis = report.get('team_analysis', '')
    if team_analysis:
        story.append(Paragraph(team_analysis, st['body_vn']))
    story.append(Spacer(1, 0.2 * cm))

    # === Critical Tasks ===
    critical = report.get('critical_tasks') or []
    if critical:
        story.append(PageBreak())
        story.append(Paragraph("Critical and Overdue Tasks", st['section_h']))
        story.append(Paragraph(
            f"The following {len(critical)} tasks require immediate attention.",
            st['body']
        ))
        story.append(Spacer(1, 0.2 * cm))
        for i, task in enumerate(critical, 1):
            story.extend(_task_card(task, i, st, is_critical=True))

    # === Stuck / Unassigned Tasks ===
    stuck = report.get('stuck_tasks') or []
    if stuck:
        story.append(Spacer(1, 0.3 * cm))
        story.append(Paragraph("Stalled and Unassigned Tasks", st['section_h']))
        story.append(Paragraph(
            f"{len(stuck)} tasks are stalled or have no assigned owner.",
            st['body']
        ))
        story.append(Spacer(1, 0.2 * cm))
        for i, task in enumerate(stuck, 1):
            label = "UNASSIGNED" if task.get('reason') == 'unassigned' else "STALLED"
            task = dict(task)
            task['task'] = f"[{label}] {task.get('task', '')}"
            story.extend(_task_card(task, i, st, is_critical=False))

    # === Priority Actions ===
    actions = report.get('priority_actions') or []
    if actions:
        story.append(PageBreak())
        story.append(Paragraph("Priority Actions for Manager", st['section_h']))
        story.append(HRFlowable(width='100%', thickness=0.5, color=_ACCENT, spaceAfter=8))
        for i, action in enumerate(actions, 1):
            story.append(Paragraph(f"{i}. {action}", st['action']))
        story.append(Spacer(1, 0.5 * cm))

    # === Footer ===
    story.append(HRFlowable(width='100%', thickness=0.5, color=_RULE, spaceBefore=16))
    story.append(Paragraph(
        f"Generated by EPS Operations System | Source: {TASK_TABLE} | Model: {MODEL}",
        st['meta']
    ))

    doc.build(story)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Slack integration
# ---------------------------------------------------------------------------
def upload_pdf_to_slack(pdf_bytes: bytes, filename: str, channel_id: str) -> Optional[str]:
    """Upload PDF to Slack and return the file permalink."""
    try:
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as f:
            f.write(pdf_bytes)
            tmp_path = f.name

        resp = app.client.files_upload_v2(
            channel=channel_id,
            file=tmp_path,
            filename=filename,
            title=filename.replace('_', ' ').replace('.pdf', ''),
        )
        file_info = resp.get('file', {})
        return file_info.get('permalink')
    except Exception as e:
        logger.warning("files_upload_v2 failed: %s", e)
        # Fallback to v1
        try:
            resp = app.client.files_upload(
                channels=channel_id,
                file=pdf_bytes,
                filename=filename,
                filetype='pdf',
            )
            return resp.get('file', {}).get('permalink')
        except Exception as e2:
            logger.error("files_upload fallback also failed: %s", e2)
            return None
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def render_channel_blocks(report: dict, stats: dict, today: str,
                          pdf_permalink: Optional[str], mentions: str) -> list:
    """Lightweight Block Kit message — overview KPIs + top 3 items only."""
    blocks = []

    # Header
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"CS Daily Reminder — {AGENT_FILTER} — {today}",
            "emoji": False,
        },
    })

    # Mentions + summary
    summary = report.get('executive_summary', '').strip()
    if mentions:
        summary = f"{mentions}\n{summary}"
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": summary},
    })

    # KPI fields
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

    # Top 3 critical preview
    critical = report.get('critical_tasks') or []
    if critical:
        lines = []
        for t in critical[:3]:
            em = t.get('emergency', 0)
            days = t.get('days_overdue', 0)
            lines.append(
                f"- `{t.get('responsible','')}` — {t.get('task','')[:70]}"
                f"  _(em={em}, {days}d overdue)_"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": "*Critical tasks (top 3):*\n" + "\n".join(lines)},
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
            "text": {
                "type": "mrkdwn",
                "text": f"Full report PDF: {pdf_permalink}",
            },
        })

    return blocks


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_daily_reminder(post: bool = True) -> dict:
    logger.info("Querying BigQuery for agent: %s", AGENT_FILTER)
    workload_rows = query_workload_by_responsible()
    overdue_rows_raw = query_critical_and_overdue()
    stalled_rows_raw = query_stalled_and_unassigned()

    # Enrich detail rows with parsed comments
    overdue_rows = enrich_rows(overdue_rows_raw)
    stalled_rows = enrich_rows(stalled_rows_raw)

    logger.info(
        "rows: workload=%d overdue=%d stalled=%d",
        len(workload_rows), len(overdue_rows), len(stalled_rows),
    )

    stats = compute_stats(workload_rows)
    report = llm_generate_report(workload_rows, overdue_rows, stalled_rows)

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

    filename = f"CS_Reminder_{AGENT_FILTER.replace(' ', '_')}_{today}.pdf"
    pdf_permalink = upload_pdf_to_slack(pdf_bytes, filename, CS_CHANNEL)

    blocks = render_channel_blocks(report, stats, today, pdf_permalink, CS_MANAGER_MENTIONS)
    fallback = f"CS Daily Reminder — {AGENT_FILTER} — {today}"

    msg_resp = app.client.chat_postMessage(
        channel=CS_CHANNEL,
        blocks=blocks,
        text=fallback,
    )
    logger.info("Posted to %s (ts=%s)", CS_CHANNEL, msg_resp.get('ts'))

    return {
        "stats": stats,
        "report": report,
        "pdf_permalink": pdf_permalink,
        "channel_message_ts": msg_resp.get('ts'),
    }