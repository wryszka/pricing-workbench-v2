"""FastAPI entry point — serves both the /api/* JSON endpoints and the built
frontend (index.html + assets)."""
from __future__ import annotations
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

from .routers import home, datasets, features, models, reports, governance, policies, serving
from . import config

app = FastAPI(title="Pricing Workbench v2", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tightened in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def unhandled(_: Request, exc: Exception):
    # Keep the wire response small + predictable so the frontend can always render an error state
    return JSONResponse(status_code=500, content={"error": str(exc)})

# ---------- /api routers ----------

app.include_router(home.router,       prefix="/api/home",       tags=["home"])
app.include_router(datasets.router,   prefix="/api/datasets",   tags=["datasets"])
app.include_router(features.router,   prefix="/api/features",   tags=["features"])
app.include_router(models.router,     prefix="/api/models",     tags=["models"])
app.include_router(reports.router,    prefix="/api/reports",    tags=["reports"])
app.include_router(governance.router, prefix="/api/governance", tags=["governance"])
app.include_router(policies.router,   prefix="/api/policies",   tags=["policies"])
app.include_router(serving.router,    prefix="/api/serving",    tags=["serving"])

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "catalog": config.CATALOG,
        "schema":  config.SCHEMA,
        "entity":  config.ENTITY_NAME,
    }

# ---------- static frontend ----------
# During `vite build`, the frontend emits to src/app/frontend/dist. In production
# (Databricks Apps) we mount that directory at /. During local dev, frontend runs
# via `npm run dev` on port 5173 and calls the backend at /api/*.
_frontend_dist = Path(__file__).resolve().parent.parent / "frontend" / "dist"

if _frontend_dist.exists():
    app.mount("/assets", StaticFiles(directory=_frontend_dist / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def spa(full_path: str):
        # Return index.html for any non-api route so react-router handles the URL
        if full_path.startswith("api/"):
            return JSONResponse(status_code=404, content={"error": "not found"})
        file = _frontend_dist / full_path
        if file.is_file():
            return FileResponse(file)
        return FileResponse(_frontend_dist / "index.html")
else:
    @app.get("/")
    def root():
        return {
            "status": "ok",
            "message": "Frontend not built. Run `npm run build` in src/app/frontend/.",
            "api_docs": "/docs",
        }
