from src.clients import bq_client


def run_query(sql: str) -> str:
    """Execute a BigQuery SQL query and return formatted results."""
    try:
        query_job = bq_client.query(sql)
        results = query_job.result()
        rows = [dict(row) for row in results]

        if not rows:
            return "No results found."

        # Format as a readable table
        headers = list(rows[0].keys())
        lines = [" | ".join(str(h) for h in headers)]
        lines.append("-" * len(lines[0]))
        for row in rows[:50]:
            lines.append(" | ".join(str(row.get(h, "")) for h in headers))

        return "\n".join(lines)
    except Exception as e:
        return f"Query error: {e}"
