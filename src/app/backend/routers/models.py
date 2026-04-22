"""Model Factory tab — registered models, UC aliases, champion/challenger table, MLflow runs."""
from __future__ import annotations
from fastapi import APIRouter, HTTPException

from ..db import run_sql, get_client
from ..config import CATALOG, SCHEMA, FQN

router = APIRouter()

@router.get("")
def list_registered_models():
    """List every registered model in the workbench schema + their aliases + version counts."""
    wc = get_client()
    out = []
    try:
        models = wc.registered_models.list(catalog_name=CATALOG, schema_name=SCHEMA)
    except Exception as e:
        return {"models": [], "error": str(e)}

    for m in models:
        d = m.as_dict()
        full_name = d.get("full_name")
        aliases = d.get("aliases", []) or []
        # Latest version lookup
        try:
            versions = wc.model_versions.list(full_name=full_name)
            vcount = 0
            latest_v = 0
            for v in versions:
                vcount += 1
                try:
                    vn = int(v.as_dict().get("version", 0))
                    if vn > latest_v: latest_v = vn
                except Exception:
                    pass
        except Exception:
            vcount, latest_v = 0, 0

        out.append({
            "name": d.get("name"),
            "full_name": full_name,
            "aliases": [{"alias": a.get("alias_name"), "version": a.get("version_num")} for a in aliases],
            "version_count": vcount,
            "latest_version": latest_v,
            "comment": d.get("comment"),
            "updated_at": d.get("updated_at"),
        })

    return {"models": out}

@router.get("/comparison")
def model_comparison():
    """The model_factory comparison table — apples-to-apples test-set metrics."""
    try:
        rows = run_sql(f"""
            SELECT model, rmse, mae, pred_total, actual_total, total_ratio,
                   gini_on_incurred, _evaluated_at
            FROM {FQN}.model_comparison
            ORDER BY gini_on_incurred DESC
        """)
    except Exception as e:
        return {"rows": [], "error": str(e)}
    return {"rows": rows}

@router.get("/governance")
def model_governance():
    """One row per generated regulatory report."""
    try:
        rows = run_sql(f"""
            SELECT model_name, version, alias, gini, total_ratio, rmse, mae,
                   shap_method, test_rows, report_path, _generated_at
            FROM {FQN}.model_governance
            ORDER BY _generated_at DESC
        """)
        return {"rows": rows}
    except Exception as e:
        return {"rows": [], "error": str(e)}

@router.get("/{model_name}")
def model_detail(model_name: str):
    """Single model — versions, MLflow run links, schema."""
    wc = get_client()
    full = f"{CATALOG}.{SCHEMA}.{model_name}"
    try:
        m = wc.registered_models.get(full_name=full).as_dict()
    except Exception as e:
        raise HTTPException(404, f"model not found: {e}")

    versions_out = []
    try:
        for v in wc.model_versions.list(full_name=full):
            vd = v.as_dict()
            versions_out.append({
                "version": vd.get("version"),
                "status": vd.get("status"),
                "created_at": vd.get("created_at"),
                "run_id": vd.get("run_id"),
                "source": vd.get("source"),
                "comment": vd.get("comment"),
            })
    except Exception:
        pass

    return {
        "name": m.get("name"),
        "full_name": full,
        "aliases": m.get("aliases", []),
        "versions": versions_out,
    }
