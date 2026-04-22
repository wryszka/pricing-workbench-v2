# Pricing Workbench v2 — App

Databricks App (FastAPI + React + Tailwind) that surfaces the v2 pricing workbench
end-to-end: external data lineage, feature tables, model factory, regulatory reports,
and governance.

## Layout

```
app/
├── app.yaml            # Databricks App config (port 8000)
├── requirements.txt    # backend deps
├── backend/
│   ├── main.py         # FastAPI entry point
│   ├── config.py       # env-var driven config
│   ├── db.py           # Databricks SQL wrapper
│   └── routers/        # one module per domain
└── frontend/
    ├── package.json
    ├── vite.config.ts
    └── src/
        ├── pages/      # one file per route
        ├── components/ # Sidebar, Card, Table, Badge, StatNumber
        └── lib/        # api() fetch wrapper
```

## Local dev loop

1. **Backend** (in `src/app/`):
   ```bash
   uv pip install -r requirements.txt
   export DATABRICKS_HOST=... DATABRICKS_TOKEN=... WAREHOUSE_ID=...
   uvicorn backend.main:app --reload
   ```
2. **Frontend** (in `src/app/frontend/`):
   ```bash
   npm install
   npm run dev          # → http://localhost:5173, proxies /api to :8000
   ```

## Build + deploy

1. Build the frontend once (emits to `frontend/dist/`):
   ```bash
   cd src/app/frontend && npm run build
   ```
   The backend's `main.py` serves `frontend/dist/` at `/` when it exists.

2. Deploy via Databricks CLI:
   ```bash
   databricks apps deploy pricing-workbench-v2 --source-code-path src/app
   ```

## Routes

| Path | Page |
| --- | --- |
| `/` | Home dashboard |
| `/external-data`, `/external-data/:id` | 4 vendor feeds + approval workflow |
| `/feature-table`, `/feature-table/:id` | feature_policy_year_training, feature_quote_training, feature_catalog |
| `/policies`, `/policies/:id` | dim_policies + dim_policy_versions + fact_claims drill-down |
| `/model-factory`, `/model-factory/:name` | Registered models + champion aliases + comparison table |
| `/reports`, `/reports/:id` | Regulatory reports rendered inline (markdown + PNGs from volume) |
| `/serving` | Phase 4 placeholder |
| `/monitoring` | Latest model-factory metrics + planned drift detection |
| `/governance` | Audit log, dataset approvals, fairness audit, lineage |
| `/quote-review` | Bolt-on placeholder |
