"""Governance tab — audit log timeline + fairness report + lineage summary."""
from __future__ import annotations
from fastapi import APIRouter

from ..db import run_sql
from ..config import FQN

router = APIRouter()

@router.get("")
def summary():
    """Overall governance health — event counts by type, DQ pass rates, latest fairness result."""
    out = {}
    try:
        out["event_counts"] = run_sql(f"""
            SELECT event_type, COUNT(*) AS n,
                   MAX(timestamp) AS latest
            FROM {FQN}.app_audit_log
            GROUP BY event_type
            ORDER BY n DESC
        """)
    except Exception as e:
        out["event_counts"] = []
        out["event_error"] = str(e)

    try:
        out["dataset_approvals"] = run_sql(f"""
            SELECT decision, COUNT(*) AS n
            FROM {FQN}.app_dataset_approvals
            GROUP BY decision
        """)
    except Exception:
        out["dataset_approvals"] = []

    try:
        out["fairness"] = run_sql(f"SELECT * FROM {FQN}.app_fairness_report ORDER BY check")
    except Exception as e:
        out["fairness"] = []
        out["fairness_error"] = str(e)

    return out

@router.get("/audit")
def audit_log(limit: int = 100):
    """Most recent audit entries — dataset approvals, model decisions, agent actions."""
    try:
        rows = run_sql(f"""
            SELECT event_id, event_type, entity_type, entity_id, entity_version,
                   user_id, timestamp, details, source
            FROM {FQN}.app_audit_log
            ORDER BY timestamp DESC
            LIMIT {int(limit)}
        """)
        return {"events": rows}
    except Exception as e:
        return {"events": [], "error": str(e)}

@router.get("/fairness")
def fairness():
    try:
        return {"checks": run_sql(f"SELECT * FROM {FQN}.app_fairness_report")}
    except Exception as e:
        return {"checks": [], "error": str(e)}

@router.get("/lineage")
def lineage_summary():
    """High-level lineage: for each feature_table, list upstream tables + row counts.
    Delta versions are pulled lazily — only DESCRIBE HISTORY works for Delta tables,
    so we skip that for DLT MVs (they show 'DLT-managed')."""
    TABLES = [
        ("feature_policy_year_training", "feature_tables"),
        ("feature_quote_training",       "feature_tables"),
        ("feature_policy_current",       "feature_tables"),
        ("silver_company_bureau",        "silver"),
        ("silver_sic_directory",         "silver"),
        ("silver_geo_hazard",            "silver"),
        ("silver_market_benchmark",      "silver"),
        ("silver_postcode_enrichment",   "silver"),
        ("raw_company_bureau",           "raw"),
        ("raw_sic_directory",            "raw"),
        ("raw_geo_hazard",               "raw"),
        ("raw_market_benchmark",         "raw"),
    ]
    # Batch all 12 counts into one query
    sel = ", ".join(f"(SELECT COUNT(*) FROM {FQN}.{t}) AS {t}" for t, _ in TABLES)
    try:
        r = run_sql(f"SELECT {sel}")
        counts = {k: int(v or 0) for k, v in (r[0].items() if r else [])}
    except Exception:
        counts = {}
    out = {"feature_tables": [], "silver": [], "raw": []}
    for tbl, bucket in TABLES:
        # Try Delta version — cheap on Delta, fails on DLT MVs (which is fine)
        v = None
        try:
            hist = run_sql(f"DESCRIBE HISTORY {FQN}.{tbl} LIMIT 1", timeout_s=5)
            v = hist[0]["version"] if hist else None
        except Exception:
            v = "DLT-managed" if bucket == "silver" else None
        out[bucket].append({"table": tbl, "delta_version": v, "row_count": counts.get(tbl, 0)})
    return out
