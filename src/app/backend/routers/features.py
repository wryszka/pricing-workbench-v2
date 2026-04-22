"""Feature table view — feature_policy_year_training and feature_quote_training,
with lineage + versioning + sample."""
from __future__ import annotations
from fastapi import APIRouter, Query

from ..db import run_sql
from ..config import FQN

router = APIRouter()

@router.get("")
def feature_tables():
    """High-level: two main feature tables + the feature_catalog."""
    tables = [
        {
            "id": "policy_year_training",
            "fqn": f"{FQN}.feature_policy_year_training",
            "grain": "(policy_id, policy_version, exposure_year)",
            "description": "The canonical training grain — one row per policy-year of exposure, "
                           "with point-in-time joins to all reference data. Labels: "
                           "claim_count_observed, total_incurred_observed, per-peril breakdowns.",
            "used_by": ["freq_glm", "sev_glm", "sev_gbm", "pure_premium_xgb"],
        },
        {
            "id": "quote_training",
            "fqn": f"{FQN}.feature_quote_training",
            "grain": "(quote_id)",
            "description": "One row per quote — features joined as of quote_date. Label: "
                           "converted_flag (whether the quote bound).",
            "used_by": ["demand_gbm"],
        },
        {
            "id": "policy_current",
            "fqn": f"{FQN}.feature_policy_current",
            "grain": "(policy_id, policy_version) for active policies only",
            "description": "The serving-time lookup — current in-force policies with the same "
                           "features the training grain uses.",
            "used_by": ["(online feature store)"],
        },
    ]
    # Batch count all 3 feature tables in one round-trip
    sel = ", ".join(f"(SELECT COUNT(*) FROM {t['fqn']}) AS t{i}" for i, t in enumerate(tables))
    try:
        r = run_sql(f"SELECT {sel}")
        counts = {k: int(v or 0) for k, v in (r[0].items() if r else [])}
    except Exception:
        counts = {}
    out = []
    for i, t in enumerate(tables):
        delta_v = None
        try:
            hist = run_sql(f"DESCRIBE HISTORY {t['fqn']} LIMIT 1", timeout_s=5)
            delta_v = hist[0]["version"] if hist else None
        except Exception:
            pass
        out.append({**t, "row_count": counts.get(f"t{i}", 0), "delta_version": delta_v})
    return {"feature_tables": out}

@router.get("/{table_id}")
def feature_table_detail(table_id: str):
    ref = {
        "policy_year_training": f"{FQN}.feature_policy_year_training",
        "quote_training":       f"{FQN}.feature_quote_training",
        "policy_current":       f"{FQN}.feature_policy_current",
    }
    fqn = ref.get(table_id)
    if not fqn:
        return {"error": f"unknown table {table_id}"}

    schema = []
    try:
        schema = run_sql(f"DESCRIBE TABLE {fqn}", timeout_s=15)
    except Exception as e:
        return {"error": str(e)}

    try:
        sample = run_sql(f"SELECT * FROM {fqn} LIMIT 20", timeout_s=15)
    except Exception:
        sample = []

    try:
        history = run_sql(f"DESCRIBE HISTORY {fqn} LIMIT 10", timeout_s=15)
    except Exception:
        history = []

    return {
        "fqn": fqn,
        "schema": schema,
        "sample": sample,
        "history": history,
    }

@router.get("/catalog")
def feature_catalog(group: str | None = Query(None)):
    """Return the feature_catalog — used by every report for lineage tracing."""
    sql = f"""
        SELECT feature_name, feature_group, data_type, description,
               source_tables, source_columns, transformation,
               owner, regulatory_sensitive, pii
        FROM {FQN}.feature_catalog
    """
    if group:
        sql += f" WHERE feature_group = '{group}'"
    sql += " ORDER BY feature_group, feature_name"
    try:
        return {"features": run_sql(sql)}
    except Exception as e:
        return {"features": [], "error": str(e)}
