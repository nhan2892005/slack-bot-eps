import logging
from src.clients import claude
from src.config import MODEL
from src.knowledge_base import pc_mart_summary, health_mart_summary

logger = logging.getLogger(__name__)


# --- Knowledge base router ---
def classify_business_line(question: str, history: str = "") -> str:
    """Ask Claude to classify whether the question is about Health, P&C, or BOTH.
    Returns one of: 'HEALTH', 'PC', 'BOTH'.
    """
    history_block = f"\nPrevious conversation:\n{history}\n" if history else ""
    response = claude.messages.create(
        model=MODEL,
        max_tokens=10,
        messages=[
            {
                "role": "user",
                "content": f"""Classify the user's question into one of these insurance business lines:
- HEALTH: about health insurance, deals, carriers like AMBETTER/BCBS/UHC, plan_name, members
- PC: about Property & Casualty insurance, policies, carriers like GEICO/PROGRESSIVE/SAFECO, premium, AUTO/HOME/COMMERCIAL
- BOTH: question explicitly needs both (e.g. total revenue across all lines)

Respond with EXACTLY one word: HEALTH, PC, or BOTH.
{history_block}
Current question: {question}

Classification:"""
            }
        ],
    )
    label = response.content[0].text.strip().upper()
    if label not in ("HEALTH", "PC", "BOTH"):
        return "BOTH"
    return label


def get_schema_for_question(question: str, history: str = "") -> str:
    """Return only the relevant table schema(s) based on the question's business line."""
    line = classify_business_line(question, history)
    logger.info(f"Business line classified as: {line}")
    if line == "HEALTH":
        return health_mart_summary()
    if line == "PC":
        return pc_mart_summary()
    return pc_mart_summary() + "\n" + health_mart_summary()
