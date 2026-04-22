"""Home page — high-level workbench stats, grouped per pillar."""
from __future__ import annotations
from fastapi import APIRouter

from ..db import run_sql, table_exists
from ..config import FQN, ENTITY_NAME

router = APIRouter()

def _scalar(sql: str, default=None):
    try:
        r = run_sql(sql)
        if r and isinstance(r[0], dict) and len(r[0]) == 1:
            return list(r[0].values())[0]
        return r[0] if r else default
    except Exception:
        return default

@router.get("")
def home_summary():
    counts = {}
    for t in ["dim_companies", "dim_policies", "dim_policy_versions",
              "fact_quotes", "fact_claims",
              "feature_policy_year_training", "feature_quote_training",
              "app_audit_log", "model_comparison", "model_governance"]:
        counts[t] = _scalar(f"SELECT COUNT(*) AS n FROM {FQN}.{t}", default=0)

    # Champion aliases — what the pricing system would use right now
    aliases = []
    try:
        for (model, alias) in [
            ("freq_glm", "pricing_champion_frequency"),
            ("sev_gbm", "pricing_champion_severity"),
            ("sev_glm", "pricing_champion_severity"),  # may not exist if sev_gbm won
            ("pure_premium_xgb", "pricing_champion_single"),
            ("demand_gbm", "demand_champion"),
        ]:
            aliases.append({"model": model, "alias": alias})
    except Exception:
        pass

    return {
        "entity": ENTITY_NAME,
        "catalog": FQN,
        "counts": counts,
        "aliases": aliases,
        "pillars": [
            {"id": "ingestion",  "name": "Ingestion",         "path": "/external-data"},
            {"id": "features",   "name": "Feature Table",     "path": "/feature-table"},
            {"id": "modelling",  "name": "Model Factory",     "path": "/model-factory"},
            {"id": "reports",    "name": "Regulatory Reports","path": "/reports"},
            {"id": "serving",    "name": "Serving",           "path": "/serving"},
            {"id": "governance", "name": "Governance",        "path": "/governance"},
        ],
    }
