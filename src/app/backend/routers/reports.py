"""Regulatory reports — list + serve markdown + PNG/CSV assets from /Volumes/.../reports/."""
from __future__ import annotations
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse

from ..config import REPORTS_ROOT
from ..db import run_sql
from ..config import FQN

router = APIRouter()

@router.get("")
def list_reports():
    """Every report directory under /Volumes/.../reports/ — model_factory results + fairness."""
    root = Path(REPORTS_ROOT)
    if not root.exists():
        return {"reports": [], "root": str(root), "error": "reports volume not found"}

    out = []
    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        md_file = d / "report.md"
        meta = {
            "id": d.name,
            "path": str(d),
            "has_report": md_file.exists(),
            "has_plots":  (d / "plots").exists(),
            "has_tables": (d / "tables").exists(),
        }
        # Naming convention: {model_name}_v{version}
        if "_v" in d.name:
            mname, mver = d.name.rsplit("_v", 1)
            meta["model_name"] = mname
            meta["version"] = mver
        out.append(meta)
    return {"root": str(root), "reports": out}

@router.get("/governance")
def governance_index():
    """Row-for-row governance table — same as /api/models/governance but scoped to reports."""
    try:
        return {"rows": run_sql(f"SELECT * FROM {FQN}.model_governance ORDER BY _generated_at DESC")}
    except Exception as e:
        return {"rows": [], "error": str(e)}

def _safe_report_path(report_id: str, rel: str | None = None) -> Path:
    """Resolve a path inside the reports volume, rejecting anything that tries to escape."""
    base = Path(REPORTS_ROOT) / report_id
    target = base if rel is None else (base / rel)
    try:
        target = target.resolve()
    except Exception as e:
        raise HTTPException(400, f"bad path: {e}")
    if not str(target).startswith(str(Path(REPORTS_ROOT).resolve())):
        raise HTTPException(400, "path outside reports volume")
    return target

@router.get("/{report_id}")
def report_markdown(report_id: str):
    target = _safe_report_path(report_id, "report.md")
    if not target.exists():
        raise HTTPException(404, f"report not found: {report_id}")
    with open(target, "r") as f:
        return {"report_id": report_id, "markdown": f.read()}

@router.get("/{report_id}/files")
def report_files(report_id: str):
    base = _safe_report_path(report_id)
    if not base.exists():
        raise HTTPException(404, f"report not found: {report_id}")
    files = {"plots": [], "tables": []}
    for sub in ["plots", "tables"]:
        d = base / sub
        if d.exists():
            files[sub] = sorted(p.name for p in d.iterdir() if p.is_file())
    return {"report_id": report_id, "files": files}

@router.get("/{report_id}/plot/{filename}")
def report_plot(report_id: str, filename: str):
    target = _safe_report_path(report_id, f"plots/{filename}")
    if not target.exists():
        raise HTTPException(404, f"plot not found: {filename}")
    return FileResponse(target, media_type="image/png")

@router.get("/{report_id}/table/{filename}")
def report_table(report_id: str, filename: str):
    target = _safe_report_path(report_id, f"tables/{filename}")
    if not target.exists():
        raise HTTPException(404, f"table not found: {filename}")
    return PlainTextResponse(content=target.read_text(), media_type="text/csv")
