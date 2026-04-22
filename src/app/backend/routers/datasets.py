"""External data tab — the 4 raw bronze→silver feeds, each with quality, lineage,
and an approval workflow."""
from __future__ import annotations
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..db import run_sql
from ..config import FQN

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
    reviewer: str
    notes: str = ""

@router.post("/{dataset_id}/approve")
def approve_dataset(dataset_id: str, body: ApprovalIn):
    if body.decision not in ("approved", "rejected"):
        raise HTTPException(400, "decision must be 'approved' or 'rejected'")
    ds = next((d for d in DATASETS if d["id"] == dataset_id), None)
    if not ds:
        raise HTTPException(404, f"Unknown dataset {dataset_id}")

    raw_fqn    = f"{FQN}.{ds['raw']}"
    silver_fqn = f"{FQN}.{ds['silver']}"
    raw_count    = _count(raw_fqn)
    silver_count = _count(silver_fqn)

    approval_id = f"app-{dataset_id}-{raw_count}"
    # Write approval row + audit entry (both single INSERTs via SQL warehouse)
    notes_esc = body.notes.replace("'", "''")
    reviewer_esc = body.reviewer.replace("'", "''")
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
