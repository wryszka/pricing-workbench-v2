"""Home page — high-level workbench stats, grouped per pillar."""
from __future__ import annotations
import time
from fastapi import APIRouter

from ..db import run_sql
from ..config import FQN, ENTITY_NAME

router = APIRouter()

# Tiny 30-second memo cache so navigating between tabs doesn't re-query every time.
# The demo data changes on factory runs, not per-click, so 30s staleness is fine.
_CACHE: dict[str, tuple[float, object]] = {}
def _memoize(key: str, ttl: int, fn):
    now = time.time()
    hit = _CACHE.get(key)
    if hit and (now - hit[0]) < ttl:
        return hit[1]
    val = fn()
    _CACHE[key] = (now, val)
    return val

@router.get("")
def home_summary():
    def _counts():
        tables = [
            "dim_companies", "dim_policies", "dim_policy_versions",
            "fact_quotes", "fact_claims",
            "feature_policy_year_training", "feature_quote_training",
            "app_audit_log", "model_comparison", "model_governance",
        ]
        # One query beats 10 — each COUNT is its own subquery in the SELECT list.
        sel = ", ".join(f"(SELECT COUNT(*) FROM {FQN}.{t}) AS {t}" for t in tables)
        try:
            r = run_sql(f"SELECT {sel}")
            return {k: int(v or 0) for k, v in (r[0].items() if r else [])}
        except Exception:
            return {t: 0 for t in tables}
    counts = _memoize("home.counts", 30, _counts)

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
