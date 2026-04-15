"""
Knowledge base for BigQuery tables used by the Slack bot.

Each function returns a structured markdown string summarizing a table:
its purpose, pipeline, schema, business logic, and notes. These blocks
are injected into Claude prompts as context so the model can generate
accurate SQL and interpret the data correctly.
"""


def pc_mart_summary() -> str:
    """P&C (Property & Casualty) policy mart."""
    return """
Table: `eps-470914.transformed_data.pc_mart`
Grain: one row per policy (deduplicated by policy_number).
Purpose: primary analytics table for Property & Casualty policies, covering
  carrier, agent/agency, premium, commission waterfall, and policy status.

Pipeline:
  Policy Tracker (Google Sheet, raw entry)
    -> Apps Script (drops PII cols, extracts zipcode, writes to `Summary` sheet)
    -> `pc_data` (BQ external table, live-linked to Summary, all STRING types)
    -> `pc_mart` (BQ view: typed, normalized, deduped, joined with `uszip`,
                  commission waterfall computed)

Columns:
  - agent_id (STRING): EPS1001=FIONA, EPS1002=LINH, EPS1003=NAM, EPS1004=VUONG
  - agent_name (STRING): Canonical producer name (FIONA / LINH / NAM / VUONG)
  - agency_id (STRING): EPSA001=DP, EPSA002=TWFG
  - agency_name (STRING): DP or TWFG
  - insured_name (STRING): Policyholder name
  - zipcode (INTEGER): 5-digit US zip extracted from address
  - type (STRING): Normalized insurance line (AUTO, HOME, COMMERCIAL, DP,
                   UMBRELLA, FLOOD, ...). Typos are corrected upstream.
  - company (STRING): Canonical carrier name after regex normalization
                      (GEICO, PROGRESSIVE, SAGESURE, ALLSTATE, HARTFORD, etc.)
  - policy_number (STRING): Unique policy key
  - premium (FLOAT): Gross Written Premium. Negative value = cancellation / chargeback.
  - true_premium (FLOAT): Verified/adjusted premium. THIS is used for commission math.
  - effective_date (DATE): Policy start date
  - expired_date (DATE): Policy end date
  - carrier_commission (FLOAT): Decimal rate carrier pays the agency (0.14 = 14%)
  - paid_producer (STRING): Statement payment date (MM/DD/YYYY)
  - statement_number (STRING): Statement batch identifier
  - effective_month_year (STRING): YYYY-MM of effective_date — PRIMARY grouping key
  - expired_month_year (STRING): YYYY-MM of expired_date
  - status (STRING): NEW / RENEWAL / CANCEL
      * CANCEL if premium < 0
      * NEW if earliest effective_date for that policy_number
      * RENEWAL otherwise
  - city (STRING): From uszip lookup
  - state (STRING): From uszip lookup
  - agent_commission_rate (FLOAT): 0.60 for FIONA, 0.75 for all other agents
  - total_commission (FLOAT): Gross commission EPS collects from carrier.
      * DP:   carrier_commission * true_premium * 0.75
      * TWFG: carrier_commission * true_premium * 0.80
  - agent_commission_amount (FLOAT): Net payout to agent
      = agent_commission_rate * total_commission
  - eps_commission_amount (FLOAT): EPS net revenue
      = total_commission - agent_commission_amount

Commission structure:
  DP agency (any agent):         Agency keeps 75% of carrier rate, agent gets 75% of agency share, EPS net 25%
  TWFG agency (any agent):       Agency keeps 80% of carrier rate, agent gets 75% of agency share, EPS net 25%
  TWFG agency (FIONA only):      Agency keeps 80% of carrier rate, agent gets 60% of agency share, EPS net 40%

Important notes for querying:
  - Default date column for P&C questions: `effective_date` (or `effective_month_year`)
  - Use `true_premium`, NOT `premium`, for commission-based analysis
  - Negative `premium` means cancellation; filter/handle accordingly
  - Agent names are ALL CAPS (FIONA, LINH, NAM, VUONG) — use UPPER() when matching
  - Status = 'NEW' for first-time policy, 'RENEWAL' for subsequent terms
"""


def health_mart_summary() -> str:
    """Health insurance mart."""
    return """
Table: `eps-470914.transformed_data.health_mart`
Grain: one row per deal / commission transaction.
Purpose: analytics table for Health insurance deals, plans, and agent commission splits.

Columns:
  - deal_name (STRING): Deal / client name (often includes OB year, e.g. "Quoc Bao Tran - OB25")
  - deal_stage (STRING): Status of the deal (ACTIVE, ENROLLED, etc.)
  - state (STRING): US state code (TX, CA, ...)
  - carrier (STRING): Insurance carrier (AMBETTER, BCBS, UHC, CHC, ...)
  - plan_name (STRING): Health plan name (e.g. "FOCUSED VALUE SILVER", "STANDARD SILVER - EPO")
  - primary_member_id (STRING): Primary member identifier
  - agent (STRING): Agent name (e.g. "NAM NGUYEN", "TRI NGUYEN", "MINHVAN", "PHI NGUYEN")
  - broker_effective_date (DATE): Broker effective date
  - paid_to_date (DATE): Paid-to date for the commission cycle
  - report_month (DATE): First day of the reporting month
  - carriers_messer_paid (FLOAT): Amount carrier / messer paid (USD)
  - agent_received (FLOAT): Amount paid out to the agent (USD)
  - eps_override (FLOAT): EPS override commission (USD)
  - eps_override_received (FLOAT): EPS override actually received (USD)
  - eps_split (FLOAT): EPS split amount (USD)
  - pay_rate_level (STRING): Pay rate level code
  - transaction_id (STRING): Transaction identifier
  - messer_statement (STRING): Messer statement number
  - num_client (INTEGER): Number of clients on the deal
  - report_month_label (STRING): Report month label, "YYYY-MM" (e.g. "2025-01")

Important notes for querying:
  - Default date column for Health questions: `report_month` (or `report_month_label`)
  - Agent names are ALL CAPS with first+last (e.g. "NAM NGUYEN"). Use UPPER()/LIKE when matching partial names.
  - For "how much did agent X receive", sum `agent_received`.
  - For EPS revenue, use `eps_override_received` + `eps_split` (or `eps_override` depending on context — ask if unclear).
  - A single deal_name may appear across multiple rows (multiple report months / transactions).
"""


def full_schema() -> str:
    """Return the combined knowledge base for both tables, ready to inject into a prompt."""
    return pc_mart_summary() + "\n" + health_mart_summary()


if __name__ == "__main__":
    print(full_schema())
