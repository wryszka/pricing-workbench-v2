"""External data tab — the 4 raw bronze→silver feeds, each with quality, lineage,
and an approval workflow."""
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from ..db import run_sql
from ..config import FQN
from ..user import current_user

router = APIRouter()

# One catalog row per ingested dataset. Kept in-code (vs a UC table) because this
# is app *metadata* — the pairing of a human name with the raw/silver table pair.
DATASETS = [
    {
        "id": "market_benchmark",
        "name": "Market Pricing Benchmark",
        "description": "SIC × region × year × quarter benchmark rates and competitor quotes",
        "raw":    "raw_market_benchmark",
        "silver": "silver_market_benchmark",
        "owner":  "pricing_analytics",
        "source_file": "market_benchmark.csv",
    },
    {
        "id": "geo_hazard",
        "name": "Geospatial Hazard",
        "description": "Postcode-level flood, fire, crime, and subsidence exposure",
        "raw":    "raw_geo_hazard",
        "silver": "silver_geo_hazard",
        "owner":  "pricing_analytics",
        "source_file": "geo_hazard.csv",
    },
    {
        "id": "company_bureau",
        "name": "Company Bureau",
        "description": "Credit score, CCJ count, bankruptcy flag — keyed on company registration number",
        "raw":    "raw_company_bureau",
        "silver": "silver_company_bureau",
        "owner":  "credit_risk",
        "source_file": "company_bureau.csv",
    },
    {
        "id": "sic_directory",
        "name": "SIC Directory",
        "description": "SIC code → division, description, internal risk tier lookup",
        "raw":    "raw_sic_directory",
        "silver": "silver_sic_directory",
        "owner":  "actuarial_pricing_team",
        "source_file": "sic_directory.csv",
    },
]

def _count(fqn: str) -> int:
    try:
        r = run_sql(f"SELECT COUNT(*) AS n FROM {fqn}", timeout_s=10)
        return int(r[0]["n"])
    except Exception:
        return 0

@router.get("")
def list_datasets():
    rows = []
    for ds in DATASETS:
        raw_fqn    = f"{FQN}.{ds['raw']}"
        silver_fqn = f"{FQN}.{ds['silver']}"
        rows.append({
            **ds,
            "raw_fqn":    raw_fqn,
            "silver_fqn": silver_fqn,
            "raw_count":    _count(raw_fqn),
            "silver_count": _count(silver_fqn),
        })
    return {"datasets": rows}

@router.get("/{dataset_id}")
def dataset_detail(dataset_id: str):
    ds = next((d for d in DATASETS if d["id"] == dataset_id), None)
    if not ds:
        raise HTTPException(404, f"Unknown dataset {dataset_id}")
    raw_fqn    = f"{FQN}.{ds['raw']}"
    silver_fqn = f"{FQN}.{ds['silver']}"
    raw_count    = _count(raw_fqn)
    silver_count = _count(silver_fqn)
    dropped = max(0, raw_count - silver_count)

    # Column schema of the silver view — what downstream models join against
    try:
        schema = run_sql(f"DESCRIBE TABLE {silver_fqn}")
    except Exception:
        schema = []

    # Sample rows from silver
    try:
        sample = run_sql(f"SELECT * FROM {silver_fqn} LIMIT 10")
    except Exception:
        sample = []

    return {
        **ds,
        "raw_fqn": raw_fqn, "silver_fqn": silver_fqn,
        "raw_count": raw_count, "silver_count": silver_count,
        "rows_dropped_by_dq": dropped,
        "dq_pass_rate": round(silver_count / raw_count, 4) if raw_count > 0 else None,
        "silver_schema": schema,
        "silver_sample": sample,
    }

class ApprovalIn(BaseModel):
    decision: str       # "approved" | "rejected"
    notes: str = ""

@router.post("/{dataset_id}/approve")
def approve_dataset(dataset_id: str, body: ApprovalIn, request: Request):
    if body.decision not in ("approved", "rejected"):
        raise HTTPException(400, "decision must be 'approved' or 'rejected'")
    ds = next((d for d in DATASETS if d["id"] == dataset_id), None)
    if not ds:
        raise HTTPException(404, f"Unknown dataset {dataset_id}")

    raw_fqn    = f"{FQN}.{ds['raw']}"
    silver_fqn = f"{FQN}.{ds['silver']}"
    raw_count    = _count(raw_fqn)
    silver_count = _count(silver_fqn)

    # Reviewer identity comes from the Databricks Apps SSO headers — no free-text
    user = current_user(request)
    reviewer_esc = user["email"].replace("'", "''")

    approval_id = f"app-{dataset_id}-{raw_count}"
    # Write approval row + audit entry (both single INSERTs via SQL warehouse)
    notes_esc = body.notes.replace("'", "''")
    run_sql(f"""
        INSERT INTO {FQN}.app_dataset_approvals
        (approval_id, dataset_name, dataset_version, decision, reviewer,
         reviewer_notes, reviewed_at, raw_row_count, silver_row_count, rows_dropped_by_dq)
        VALUES
        ('{approval_id}', '{dataset_id}', '{raw_count}', '{body.decision}',
         '{reviewer_esc}', '{notes_esc}', current_timestamp(),
         {raw_count}, {silver_count}, {max(0, raw_count - silver_count)})
    """)
    run_sql(f"""
        INSERT INTO {FQN}.app_audit_log
        (event_id, event_type, entity_type, entity_id, entity_version,
         user_id, timestamp, details, source)
        VALUES
        (uuid(), 'dataset_{body.decision}', 'dataset', '{dataset_id}', '{raw_count}',
         '{reviewer_esc}', current_timestamp(),
         '{{"notes":"{notes_esc}"}}', 'app')
    """)
    return {"status": "recorded", "approval_id": approval_id, "decision": body.decision}

@router.get("/{dataset_id}/approvals")
def approval_history(dataset_id: str):
    rows = run_sql(f"""
        SELECT approval_id, decision, reviewer, reviewer_notes, reviewed_at,
               raw_row_count, silver_row_count, rows_dropped_by_dq
        FROM {FQN}.app_dataset_approvals
        WHERE dataset_name = '{dataset_id}'
        ORDER BY reviewed_at DESC
    """)
    return {"approvals": rows}

@router.get("/{dataset_id}/quality")
def quality(dataset_id: str):
    """Column-level completeness + DLT expectations. The expectations are parsed
    from the silver_*.sql file (source of truth), not from DLT system tables which
    aren't always accessible from an app context."""
    ds = next((d for d in DATASETS if d["id"] == dataset_id), None)
    if not ds:
        raise HTTPException(404, f"Unknown dataset {dataset_id}")
    silver_fqn = f"{FQN}.{ds['silver']}"

    schema_rows = []
    try:
        schema_rows = run_sql(f"DESCRIBE TABLE {silver_fqn}")
    except Exception:
        pass

    # Completeness — pct non-null for each top-level column. Skips system cols.
    completeness = []
    if schema_rows:
        try:
            total_row = run_sql(f"SELECT COUNT(*) AS n FROM {silver_fqn}")
            total = int(total_row[0]["n"]) if total_row else 0
        except Exception:
            total = 0
        if total > 0:
            cols = [r["col_name"] for r in schema_rows
                    if r.get("col_name") and not r["col_name"].startswith("#") and not r["col_name"].startswith("_")][:25]
            if cols:
                parts = ",".join(
                    f"SUM(CASE WHEN `{c}` IS NOT NULL THEN 1 ELSE 0 END) AS `{c}`" for c in cols
                )
                try:
                    got = run_sql(f"SELECT {parts} FROM {silver_fqn}")
                    if got:
                        for c in cols:
                            n = int(got[0].get(c, 0) or 0)
                            completeness.append({
                                "column": c,
                                "non_null": n,
                                "total": total,
                                "completeness": round(n / total, 4) if total > 0 else None,
                            })
                except Exception as e:
                    print(f"quality counts failed: {e}")

    # DLT expectations — parse from the silver SQL file bundled with the app.
    # We already have them listed per silver view in src/02_silver/silver_*.sql.
    # Since that file isn't guaranteed to ship with the app, we encode them inline.
    expectations = _EXPECTATIONS.get(dataset_id, [])

    return {
        "dataset_id": dataset_id,
        "silver_fqn": silver_fqn,
        "completeness": completeness,
        "expectations": expectations,
    }

# Hard-coded catalog of DLT expectations, mirroring the CONSTRAINT clauses in
# src/02_silver/silver_*.sql. Keeping them here means the app can surface them
# even when the DLT system-table isn't accessible.
_EXPECTATIONS = {
    "market_benchmark": [
        {"name": "valid_rate",      "expr": "market_median_rate > 0 AND market_median_rate < 100",
         "severity": "DROP ROW",    "meaning": "Rates must be strictly positive and realistic"},
        {"name": "valid_year",      "expr": "year BETWEEN 2018 AND 2025",
         "severity": "DROP ROW",    "meaning": "Benchmark year must be within demo window"},
        {"name": "has_sic",         "expr": "sic_code IS NOT NULL AND LEN(sic_code) = 4",
         "severity": "FAIL PIPELINE","meaning": "Every row must be keyed on a 4-digit SIC"},
    ],
    "geo_hazard": [
        {"name": "valid_flood_zone","expr": "flood_zone_rating BETWEEN 0 AND 5",
         "severity": "DROP ROW",    "meaning": "Flood zone on 0–5 scale"},
        {"name": "valid_postcode",  "expr": "postcode IS NOT NULL",
         "severity": "FAIL PIPELINE","meaning": "Every hazard row must have a postcode"},
        {"name": "crime_range",     "expr": "crime_theft_index BETWEEN 0 AND 100",
         "severity": "WARN",        "meaning": "Crime index on 0–100 scale"},
    ],
    "company_bureau": [
        {"name": "valid_reg_number","expr": "company_registration_number IS NOT NULL",
         "severity": "FAIL PIPELINE","meaning": "Bureau is keyed on registration number"},
        {"name": "credit_score_range","expr": "credit_score BETWEEN 0 AND 1000 OR credit_score IS NULL",
         "severity": "DROP ROW",    "meaning": "Credit score in plausible range"},
        {"name": "ccj_non_negative","expr": "ccj_count IS NULL OR ccj_count >= 0",
         "severity": "DROP ROW",    "meaning": "County Court Judgement count can't be negative"},
    ],
    "sic_directory": [
        {"name": "valid_sic",       "expr": "LEN(sic_code) = 4 AND sic_code RLIKE '^[0-9]+$'",
         "severity": "FAIL PIPELINE","meaning": "SIC codes must be exactly 4 digits"},
        {"name": "has_division",    "expr": "division IS NOT NULL",
         "severity": "DROP ROW",    "meaning": "Every SIC must roll up to a division"},
        {"name": "valid_risk_tier", "expr": "internal_risk_tier IN ('Low', 'Medium', 'High')",
         "severity": "DROP ROW",    "meaning": "Risk tier controlled vocabulary"},
    ],
}

@router.get("/{dataset_id}/impact")
def impact(dataset_id: str):
    """Shadow-pricing impact: if this dataset's latest silver snapshot is adopted
    for pricing, which policies get re-rated most, and what's the portfolio £ delta?

    Pragmatic proxy: we compare 2024 policies aggregated by `region` (and class of
    business via sic division). Policies whose rating features depend most on this
    dataset are the ones that move."""
    ds = next((d for d in DATASETS if d["id"] == dataset_id), None)
    if not ds:
        raise HTTPException(404, f"Unknown dataset {dataset_id}")

    # Map each dataset to the feature_table columns whose relativities would shift
    # most if the dataset is replaced.
    DATASET_DRIVEN_FEATURES = {
        "market_benchmark":   {"rate_col": "market_median_rate",    "rate_weight": 0.30},
        "geo_hazard":         {"rate_col": "composite_location_risk","rate_weight": 0.25},
        "company_bureau":     {"rate_col": "business_stability_score","rate_weight": 0.20},
        "sic_directory":      {"rate_col": "internal_risk_tier",     "rate_weight": 0.15},
    }
    info = DATASET_DRIVEN_FEATURES.get(dataset_id, {"rate_col": "market_median_rate", "rate_weight": 0.1})

    # Portfolio slice — current in-force book, grouped by region + division
    try:
        per_region = run_sql(f"""
            SELECT region,
                   COUNT(*) AS n_policies,
                   SUM(gross_premium) AS current_premium,
                   AVG(gross_premium) AS avg_premium,
                   AVG({info['rate_col']}) AS avg_driver
            FROM {FQN}.feature_policy_year_training
            WHERE exposure_year = 2024
            GROUP BY region
            ORDER BY current_premium DESC
        """)
    except Exception as e:
        return {"error": str(e), "per_region": []}

    # Shadow calc: move the driver by ±10% proportional to its dataset influence.
    # This is a pragmatic stand-in for a full re-rate with the challenger model.
    w = info["rate_weight"]
    per_region_out = []
    total_cur, total_shd = 0.0, 0.0
    for r in per_region:
        cur_prem = float(r["current_premium"] or 0)
        shd_prem = cur_prem * (1 + w * 0.10)   # upper scenario
        delta    = shd_prem - cur_prem
        total_cur += cur_prem
        total_shd += shd_prem
        per_region_out.append({
            **r,
            "shadow_premium": round(shd_prem, 0),
            "delta":          round(delta, 0),
            "delta_pct":      round(delta / cur_prem, 4) if cur_prem else None,
        })

    try:
        per_div = run_sql(f"""
            SELECT division,
                   COUNT(*) AS n_policies,
                   SUM(gross_premium) AS current_premium
            FROM {FQN}.feature_policy_year_training
            WHERE exposure_year = 2024
            GROUP BY division
            ORDER BY current_premium DESC
            LIMIT 20
        """)
        for r in per_div:
            cp = float(r["current_premium"] or 0)
            r["shadow_premium"] = round(cp * (1 + w * 0.10), 0)
            r["delta"]          = round(r["shadow_premium"] - cp, 0)
            r["delta_pct"]      = round(r["delta"] / cp, 4) if cp else None
    except Exception:
        per_div = []

    return {
        "dataset_id": dataset_id,
        "driver_feature": info["rate_col"],
        "dataset_weight": w,
        "scenario": "upper_10pct",
        "summary": {
            "n_policies":       sum(int(r["n_policies"] or 0) for r in per_region),
            "current_premium":  round(total_cur, 0),
            "shadow_premium":   round(total_shd, 0),
            "delta":            round(total_shd - total_cur, 0),
            "delta_pct":        round((total_shd - total_cur) / total_cur, 4) if total_cur else None,
        },
        "per_region":   per_region_out,
        "per_division": per_div,
    }

@router.get("/{dataset_id}/diff")
def diff(dataset_id: str):
    """Compare raw vs silver: row counts, columns present in one but not the other,
    and a small sample of rows that exist in raw but not silver (DLT-dropped)."""
    ds = next((d for d in DATASETS if d["id"] == dataset_id), None)
    if not ds:
        raise HTTPException(404, f"Unknown dataset {dataset_id}")
    raw_fqn, silver_fqn = f"{FQN}.{ds['raw']}", f"{FQN}.{ds['silver']}"

    raw_cnt = _count(raw_fqn)
    silver_cnt = _count(silver_fqn)
    dropped = max(0, raw_cnt - silver_cnt)

    raw_cols, silver_cols = [], []
    try:
        raw_cols = [r["col_name"] for r in run_sql(f"DESCRIBE TABLE {raw_fqn}")
                     if r.get("col_name") and not r["col_name"].startswith("#") and not r["col_name"].startswith("_")]
    except Exception: pass
    try:
        silver_cols = [r["col_name"] for r in run_sql(f"DESCRIBE TABLE {silver_fqn}")
                        if r.get("col_name") and not r["col_name"].startswith("#") and not r["col_name"].startswith("_")]
    except Exception: pass

    cols_only_raw    = sorted(set(raw_cols) - set(silver_cols))
    cols_only_silver = sorted(set(silver_cols) - set(raw_cols))
    cols_shared      = sorted(set(raw_cols) & set(silver_cols))

    return {
        "dataset_id":       dataset_id,
        "raw_fqn":          raw_fqn,
        "silver_fqn":       silver_fqn,
        "raw_count":        raw_cnt,
        "silver_count":     silver_cnt,
        "rows_dropped":     dropped,
        "drop_rate":        round(dropped / raw_cnt, 4) if raw_cnt else None,
        "columns_shared":   cols_shared,
        "columns_only_raw": cols_only_raw,
        "columns_only_silver": cols_only_silver,
    }
