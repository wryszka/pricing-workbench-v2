"""Quote Review — recent quote stream + per-quote drill-down. The UI uses this to
answer 'why was this customer priced this way?'."""
from __future__ import annotations
from fastapi import APIRouter, HTTPException

from ..db import run_sql
from ..config import FQN

router = APIRouter()

@router.get("")
def list_quotes(limit: int = 50, converted: str | None = None):
    where = []
    if converted == "yes":
        where.append("converted_flag = 1")
    elif converted == "no":
        where.append("converted_flag = 0")
    clause = "WHERE " + " AND ".join(where) if where else ""
    try:
        rows = run_sql(f"""
            SELECT quote_id, quote_date, quote_year,
                   region, construction_type, flood_zone, channel,
                   buildings_si, gross_premium_quoted, market_median_rate,
                   vs_market_rate, converted_flag, internal_risk_tier,
                   converted_to_policy_id, is_outlier
            FROM {FQN}.feature_quote_training
            {clause}
            ORDER BY quote_date DESC
            LIMIT {int(limit)}
        """)
    except Exception as e:
        return {"quotes": [], "error": str(e)}
    return {"quotes": rows}

@router.get("/{quote_id}")
def quote_detail(quote_id: str):
    qid = quote_id.replace("'", "''")
    try:
        rows = run_sql(f"""
            SELECT * FROM {FQN}.feature_quote_training WHERE quote_id = '{qid}' LIMIT 1
        """)
    except Exception as e:
        raise HTTPException(500, str(e))
    if not rows:
        raise HTTPException(404, f"quote not found: {quote_id}")
    quote = rows[0]

    # If the quote converted, link to the policy
    policy = None
    if quote.get("converted_to_policy_id"):
        try:
            pr = run_sql(f"""
                SELECT dp.policy_id, dp.status, dp.inception_date, dc.company_name, dc.region
                FROM {FQN}.dim_policies dp
                LEFT JOIN {FQN}.dim_companies dc ON dc.company_id = dp.company_id
                WHERE dp.policy_id = '{quote['converted_to_policy_id']}'
            """)
            policy = pr[0] if pr else None
        except Exception:
            pass

    return {"quote": quote, "policy": policy}
