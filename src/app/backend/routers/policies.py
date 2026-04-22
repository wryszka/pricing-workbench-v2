"""Policies tab — dim_policies + dim_policy_versions + claims drill-down."""
from __future__ import annotations
from fastapi import APIRouter, HTTPException

from ..db import run_sql
from ..config import FQN

router = APIRouter()

@router.get("")
def list_policies(limit: int = 50, status: str | None = None):
    where = ""
    if status:
        where = f"WHERE dp.status = '{status.replace(chr(39), chr(39)+chr(39))}'"
    try:
        rows = run_sql(f"""
            SELECT dp.policy_id, dp.status, dp.inception_date, dp.cancellation_date,
                   dp.company_id, dc.company_name, dc.region, dc.primary_sic_code,
                   COUNT(dpv.policy_version) AS version_count
            FROM {FQN}.dim_policies dp
            LEFT JOIN {FQN}.dim_companies dc  ON dc.company_id = dp.company_id
            LEFT JOIN {FQN}.dim_policy_versions dpv ON dpv.policy_id = dp.policy_id
            {where}
            GROUP BY dp.policy_id, dp.status, dp.inception_date, dp.cancellation_date,
                     dp.company_id, dc.company_name, dc.region, dc.primary_sic_code
            ORDER BY dp.inception_date DESC
            LIMIT {int(limit)}
        """)
    except Exception as e:
        return {"policies": [], "error": str(e)}
    return {"policies": rows}

@router.get("/{policy_id}")
def policy_detail(policy_id: str):
    pid = policy_id.replace("'", "''")
    try:
        policies = run_sql(f"""
            SELECT dp.*, dc.company_name, dc.region, dc.primary_sic_code, dc.annual_turnover
            FROM {FQN}.dim_policies dp
            LEFT JOIN {FQN}.dim_companies dc ON dc.company_id = dp.company_id
            WHERE dp.policy_id = '{pid}'
        """)
        if not policies:
            raise HTTPException(404, f"policy not found: {policy_id}")
        policy = policies[0]

        versions = run_sql(f"""
            SELECT policy_version, effective_from, effective_to, sum_insured, gross_premium,
                   construction_type, channel, model_version_used
            FROM {FQN}.dim_policy_versions
            WHERE policy_id = '{pid}'
            ORDER BY policy_version
        """)

        claims = run_sql(f"""
            SELECT claim_id, policy_version, loss_date, reported_date,
                   peril, status, incurred_amount, paid_amount
            FROM {FQN}.fact_claims
            WHERE policy_id = '{pid}'
            ORDER BY loss_date DESC
        """)

        return {"policy": policy, "versions": versions, "claims": claims}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))
