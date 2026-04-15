from src.clients import claude
from src.config import MODEL
from src.llm.classifier import get_schema_for_question


def check_clarity(question: str, history: str = "", schema: str = "") -> str:
    """Ask Claude to determine if the question is clear enough to query.
    Returns 'CLEAR' if ready, otherwise returns a clarification question to ask the user.
    """
    history_block = f"\nPrevious conversation in this thread:\n{history}\n" if history else ""
    if not schema:
        schema = get_schema_for_question(question, history)
    response = claude.messages.create(
        model=MODEL,
        max_tokens=512,
        messages=[
            {
                "role": "user",
                "content": f"""You are helping a user query insurance data (P&C and Health) on BigQuery.
Your job: decide if the user's question is CLEAR enough to write a precise SQL query.

Schema available:
{schema}

{history_block}
Current question: {question}

Rules:
- If the question is CLEAR (you know which business line, agent, time range, metric), respond with EXACTLY: CLEAR
- If the question is a follow-up that builds on previous context and now becomes clear, respond: CLEAR
- MUST ask for clarification if the user does NOT specify which business line / product line (Health vs P&C). This is the most important thing to clarify. Example clarification: "Bạn muốn hỏi về mảng Health hay P&C?"
- Also ask if something else is genuinely ambiguous (e.g. agent name not specified, unclear metric).
- For date: if no date column is specified, default to `report_month` for health and `effective_date` for pc_mart — do NOT ask about this.
- Do NOT ask for clarification on minor things you can reasonably assume.
- Respond in the same language as the user (Vietnamese if user wrote in Vietnamese).
- Do NOT add explanation. Either return "CLEAR" or the clarification question only.

Response:"""
            }
        ],
    )
    return response.content[0].text.strip()
