import re
from src.clients import claude
from src.config import MODEL
from src.llm.classifier import get_schema_for_question


def generate_sql(question: str, history: str = "", schema: str = "") -> str:
    """Use Claude to generate a BigQuery SQL query from a natural language question."""
    history_block = f"\nPrevious conversation in this thread (oldest -> newest):\n{history}\n" if history else ""
    if not schema:
        schema = get_schema_for_question(question, history)
    response = claude.messages.create(
        model=MODEL,
        max_tokens=1024,
        messages=[
            {
                "role": "user",
                "content": f"""You are a BigQuery SQL expert. Generate ONLY a valid BigQuery SQL query. Return ONLY the SQL, no explanation.

{schema}

Important rules:
- Use fully qualified table names (e.g. `eps-470914.transformed_data.pc_mart`)
- Use standard BigQuery SQL syntax
- Limit results to 50 rows max unless the user asks for more
- For aggregations, always include meaningful GROUP BY

CRITICAL - Handling follow-up questions:
- If the current question is short or seems incomplete (e.g. just "12/2025", "tháng trước", "lọc theo TX"),
  it is a FOLLOW-UP that MODIFIES the previous question in the conversation.
- You MUST combine the previous question's intent with the new constraint from the current question.
- Example: previous "doanh thu của Khang tháng này" + current "12/2025" => "doanh thu của Khang trong tháng 12/2025"
- Do NOT just re-run the previous SQL. Apply the new filter/constraint from the current question.
{history_block}
Current question: {question}

SQL:"""
            }
        ],
    )
    sql = response.content[0].text.strip()
    # Remove markdown code fences if present
    sql = re.sub(r"^```(?:sql)?\s*", "", sql)
    sql = re.sub(r"\s*```$", "", sql)
    return sql
