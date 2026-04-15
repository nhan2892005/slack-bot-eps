from src.clients import claude
from src.config import MODEL


def generate_answer(question: str, sql: str, query_result: str, history: str = "") -> str:
    """Use Claude to generate a natural language answer from query results."""
    history_block = f"\nPrevious conversation in this thread:\n{history}\n" if history else ""
    response = claude.messages.create(
        model=MODEL,
        max_tokens=2048,
        messages=[
            {
                "role": "user",
                "content": f"""Based on the following data, answer the user's question in a clear and concise way.
Answer in the same language as the question.

FORMATTING RULES (STRICT):
- All money values are in USD. Always format as $1,234.56 (with dollar sign and US-style commas). Never use VNĐ, đ, ₫, or any other currency.
- Do NOT use markdown bold (no ** or __). Use plain text only.
- Do NOT use markdown italic (no * or _).
- For lists, use simple Slack-style bullets (•) or dashes (-).
- Reply in English
- Keep the answer compact and direct.
{history_block}
Current question: {question}
SQL used: {sql}
Data:
{query_result}

Answer:"""
            }
        ],
    )
    return response.content[0].text.strip()
