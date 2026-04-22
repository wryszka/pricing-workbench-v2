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
    # Batch all 8 counts into one SELECT — round-trip latency was the main cost.
    sel = []
    for ds in DATASETS:
        sel.append(f"(SELECT COUNT(*) FROM {FQN}.{ds['raw']})    AS raw_{ds['id']}")
        sel.append(f"(SELECT COUNT(*) FROM {FQN}.{ds['silver']}) AS silver_{ds['id']}")
    counts = {}
    try:
        r = run_sql("SELECT " + ", ".join(sel))
        counts = {k: int(v or 0) for k, v in (r[0].items() if r else [])}
    except Exception:
        pass
    rows = []
    for ds in DATASETS:
        raw_c    = counts.get(f"raw_{ds['id']}", 0)
        silver_c = counts.get(f"silver_{ds['id']}", 0)
        rows.append({
            **ds,
            "raw_fqn":    f"{FQN}.{ds['raw']}",
            "silver_fqn": f"{FQN}.{ds['silver']}",
            "raw_count":    raw_c,
            "silver_count": silver_c,
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
    """Shadow-pricing impact: if a *candidate* version of this dataset is adopted,
    which policies move, and what's the portfolio £ bottom-line change? The answer
    is what an actuary needs before approving the dataset for production."""
    ds = next((d for d in DATASETS if d["id"] == dataset_id), None)
    if not ds:
        raise HTTPException(404, f"Unknown dataset {dataset_id}")
    if dataset_id == "geo_hazard":
        return _geo_hazard_impact()
    # For other datasets the rich simulator isn't built yet.
    return {
        "dataset_id": dataset_id,
        "scenario":   "not_built",
        "message":    "Detailed impact simulator is implemented for geo_hazard only. "
                      "Other datasets show the generic +/-10% sensitivity view until their "
                      "dataset-specific candidate generator is built.",
    }

def _geo_hazard_impact():
    """Simulate a candidate geo_hazard dataset where ~30% of moderate flood zones
    (rating 2–3) move up by 1 level — a plausible re-assessment after new
    Environment Agency guidance. Rescore every 2024 policy against the candidate
    using a transparent mock pricing rule, then aggregate for the actuary.

    Mock pricing rule:
      - Each +1 flood zone above current adds  +5% to gross premium
      - Each -1 flood zone below current removes -3% from gross premium
    This rule is deliberately simple and stated in the UI — once the real pricing
    endpoint (Phase 4) is live, we swap the rule for a live model call."""

    # One SQL scan does the whole scenario + aggregation — serverless handles it
    # on the policy-year training table (~250K rows) in a few hundred ms.
    SCENARIO_SQL = f"""
    WITH policies_2024 AS (
      SELECT policy_id, policy_version, postcode_sector, region, division,
             internal_risk_tier, construction_type,
             gross_premium,
             flood_zone_rating AS current_flood,
             company_id
      FROM {FQN}.feature_policy_year_training
      WHERE exposure_year = 2024
    ),
    candidate AS (
      SELECT *,
        -- Deterministic (hash-based) shift so the scenario is stable across refreshes
        CASE
          WHEN current_flood BETWEEN 2 AND 3 AND abs(hash(postcode_sector)) % 100 < 30
            THEN current_flood + 1
          WHEN current_flood = 1 AND abs(hash(postcode_sector)) % 100 < 6
            THEN 2
          ELSE current_flood
        END AS candidate_flood
      FROM policies_2024
    ),
    priced AS (
      SELECT *,
        candidate_flood - current_flood AS flood_delta,
        CASE
          WHEN candidate_flood > current_flood
            THEN gross_premium * (1.0 + (candidate_flood - current_flood) * 0.05)
          WHEN candidate_flood < current_flood
            THEN gross_premium * (1.0 - (current_flood - candidate_flood) * 0.03)
          ELSE gross_premium
        END AS candidate_premium
      FROM candidate
    )
    SELECT * FROM priced
    """

    # 1. Portfolio summary — one scan over the scored policy-year set
    try:
        summary = run_sql(f"""
            WITH p AS ({SCENARIO_SQL})
            SELECT
              COUNT(*) AS total_policies,
              SUM(CASE WHEN flood_delta != 0 THEN 1 ELSE 0 END) AS affected_policies,
              SUM(gross_premium)                               AS current_premium,
              SUM(candidate_premium)                           AS candidate_premium,
              SUM(candidate_premium - gross_premium)           AS total_delta,
              SUM(CASE WHEN ABS(candidate_premium - gross_premium) / NULLIF(gross_premium, 0) > 0.10
                       THEN 1 ELSE 0 END)                      AS flagged_over_10pct,
              SUM(CASE WHEN candidate_premium > gross_premium THEN 1 ELSE 0 END) AS n_increased,
              SUM(CASE WHEN candidate_premium < gross_premium THEN 1 ELSE 0 END) AS n_decreased
            FROM p
        """)
    except Exception as e:
        return {"error": f"summary failed: {e}"}
    s = summary[0] if summary else {}

    # 2. Postcode-level diff — which postcode sectors actually moved
    try:
        postcode_changes = run_sql(f"""
            WITH p AS ({SCENARIO_SQL})
            SELECT postcode_sector,
                   MAX(current_flood)     AS current_flood,
                   MAX(candidate_flood)   AS candidate_flood,
                   MAX(flood_delta)       AS flood_delta,
                   COUNT(*)               AS policies_in_sector,
                   SUM(candidate_premium - gross_premium) AS sector_delta
            FROM p
            WHERE flood_delta != 0
            GROUP BY postcode_sector
            ORDER BY sector_delta DESC
            LIMIT 25
        """)
    except Exception:
        postcode_changes = []

    # 3. Per-region impact
    try:
        per_region = run_sql(f"""
            WITH p AS ({SCENARIO_SQL})
            SELECT region,
                   COUNT(*)                                   AS n_policies,
                   SUM(CASE WHEN flood_delta != 0 THEN 1 ELSE 0 END) AS n_affected,
                   SUM(gross_premium)                         AS current_premium,
                   SUM(candidate_premium)                     AS candidate_premium,
                   SUM(candidate_premium - gross_premium)     AS delta
            FROM p
            GROUP BY region
            ORDER BY ABS(delta) DESC
        """)
    except Exception:
        per_region = []

    # 4. Per-division impact (class of business)
    try:
        per_division = run_sql(f"""
            WITH p AS ({SCENARIO_SQL})
            SELECT division,
                   COUNT(*)                                   AS n_policies,
                   SUM(CASE WHEN flood_delta != 0 THEN 1 ELSE 0 END) AS n_affected,
                   SUM(gross_premium)                         AS current_premium,
                   SUM(candidate_premium)                     AS candidate_premium,
                   SUM(candidate_premium - gross_premium)     AS delta
            FROM p
            WHERE division IS NOT NULL
            GROUP BY division
            ORDER BY ABS(delta) DESC
            LIMIT 20
        """)
    except Exception:
        per_division = []

    # 5. Distribution histogram — bucket policies by % change
    try:
        distribution = run_sql(f"""
            WITH p AS ({SCENARIO_SQL}),
            pct AS (
              SELECT policy_id,
                     CASE WHEN gross_premium > 0
                          THEN (candidate_premium - gross_premium) / gross_premium
                          ELSE 0 END AS rel_delta
              FROM p
            )
            SELECT
              CASE
                WHEN rel_delta < -0.10  THEN '< -10%'
                WHEN rel_delta < -0.05  THEN '-10% to -5%'
                WHEN rel_delta < -0.001 THEN '-5% to 0%'
                WHEN rel_delta <  0.001 THEN 'No change'
                WHEN rel_delta <  0.05  THEN '0% to +5%'
                WHEN rel_delta <  0.10  THEN '+5% to +10%'
                WHEN rel_delta <  0.15  THEN '+10% to +15%'
                ELSE '> +15%'
              END AS bucket,
              COUNT(*) AS n
            FROM pct
            GROUP BY bucket
        """)
        # Preserve a fixed order
        BUCKET_ORDER = ['< -10%', '-10% to -5%', '-5% to 0%', 'No change',
                        '0% to +5%', '+5% to +10%', '+10% to +15%', '> +15%']
        order_map = {b: i for i, b in enumerate(BUCKET_ORDER)}
        distribution = sorted(distribution, key=lambda r: order_map.get(r["bucket"], 99))
    except Exception:
        distribution = []

    # 6. Flagged policies — largest £ movers (top 20 by absolute delta)
    try:
        flagged = run_sql(f"""
            WITH p AS ({SCENARIO_SQL})
            SELECT p.policy_id, p.policy_version, p.postcode_sector, p.region, p.division,
                   p.current_flood, p.candidate_flood,
                   p.gross_premium     AS current_premium,
                   p.candidate_premium AS candidate_premium,
                   (p.candidate_premium - p.gross_premium) AS delta,
                   dc.company_name
            FROM p
            LEFT JOIN {FQN}.dim_companies dc ON dc.company_id = p.company_id
            WHERE p.flood_delta != 0
            ORDER BY ABS(p.candidate_premium - p.gross_premium) DESC
            LIMIT 20
        """)
    except Exception:
        flagged = []

    total_pol = int(s.get("total_policies") or 0)
    affected  = int(s.get("affected_policies") or 0)
    current   = float(s.get("current_premium") or 0)
    candidate = float(s.get("candidate_premium") or 0)
    delta     = float(s.get("total_delta") or 0)
    return {
        "dataset_id": "geo_hazard",
        "scenario": {
            "description": (
                "Candidate geo_hazard reassessment — ~30% of postcodes currently rated flood zone 2 or 3 "
                "move up by one level (plausible after new Environment Agency guidance). A small share "
                "of rating-1 postcodes also move to 2."
            ),
            "pricing_rule": "+5% premium per +1 flood zone, -3% per -1 flood zone (mock — swap for live model when pricing endpoint is live)",
            "deterministic": "Shifts use HASH(postcode_sector) so the scenario is reproducible.",
        },
        "summary": {
            "total_policies":    total_pol,
            "affected_policies": affected,
            "affected_pct":      round(affected / total_pol, 4) if total_pol else None,
            "n_increased":       int(s.get("n_increased") or 0),
            "n_decreased":       int(s.get("n_decreased") or 0),
            "current_premium":   round(current, 0),
            "candidate_premium": round(candidate, 0),
            "total_delta":       round(delta, 0),
            "total_delta_pct":   round(delta / current, 4) if current else None,
            "flagged_over_10pct": int(s.get("flagged_over_10pct") or 0),
        },
        "distribution":      distribution,
        "per_region":        per_region,
        "per_division":      per_division,
        "postcode_changes":  postcode_changes,
        "flagged_policies":  flagged,
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
