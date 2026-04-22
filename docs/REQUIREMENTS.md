# Pricing Workbench v2 — Requirements

This document is the **source of truth** for what the v2 app needs to do. It was
distilled from the v1 pricing-workbench app, its notebooks, and its demo docs.
Every language choice is actuary-facing: no `raw`, `silver`, `bronze`, `DLT`,
`Delta`, `schema` terminology. The platform is an implementation detail.

## User personas
- **Head of Pricing** — reviews data approvals, monitors pricing performance, signs off model deployments
- **Actuaries / Underwriters** — compare data versions, analyse shadow-pricing impact, validate models
- **Pricing Operations** — ingest external data, track data quality, manage approval workflows
- **Data Scientists** — develop and train pricing models, analyse factor importance, run impact analyses

## Core workflows

### 1. Ingest & Approve External Data
- **Trigger:** A new vendor data version arrives (updated flood-risk file, market benchmark, credit bureau, etc.).
- **Steps:** System ingests it, applies data-quality checks, actuary reviews quality + deltas + portfolio impact, decides.
- **Outcome:** Approved data becomes the pricing source; rejected data is blocked; every decision is logged.

### 2. Shadow-Pricing Impact Analysis
- **Trigger:** Actuary needs to know the portfolio £-impact of a new dataset before approving it.
- **Steps:** Every active policy is re-rated against the candidate version; premium deltas, affected count, and bottom-line £ impact are summarised.
- **Outcome:** Actuary sees "X policies affected, Y flagged >10%, £Z portfolio delta". One-click approve/reject.

### 3. Train & Compare Pricing Models
- **Trigger:** Actuary decides which candidates to train (frequency GLM, severity GBM, demand, fraud, etc.).
- **Steps:** Feature table is read; models trained; leaderboard ranked by lift + stability + regulatory suitability.
- **Outcome:** Leaderboard + regulatory report per model. Actuary approves a champion.

### 4. Deploy Models to Serving
- **Trigger:** Approved model moved to the live pricing endpoint.
- **Steps:** Champion registered; online feature store configured; latency probed.
- **Outcome:** Live scoring endpoint accepting a quote or policy and returning a premium.

### 5. Investigate an Outlier Quote
- **Trigger:** "Why was this customer quoted £X?".
- **Steps:** Operator pulls the quote, inspects the payloads, replays it through the current model, compares premiums.
- **Outcome:** Root cause identified — data issue, model change, or rating-logic error.

### 6. Audit & Governance — Full Traceability
- **Trigger:** Regulator asks "prove the 2024 pricing model was only trained on 2024 data".
- **Steps:** System retrieves the dataset version as of training, the approval chain, the factor lineage, the model run metadata.
- **Outcome:** PDF + JSON export with model identity, training data version, approval dates, full lineage.

## Pages

### Home
- Hero + horizontal data-flow diagram (Sources → Features → Quote → Pricing → Policy → Outcomes → Retrain)
- Pillar cards with context and feature badges
- Synthetic-data disclaimer, link to GitHub

### Ingestion (list)
- 4 vendor datasets: Market Benchmark, Geospatial Hazard, Company Bureau, SIC Directory
- Columns: source, **approved-version row count**, **new-version row count**, rows blocked by quality checks, approval status (pending / approved / rejected).

### Ingestion → Dataset detail
**5 tabs**, in this order:
1. **Data changes** — compare the currently-approved version with the new version: column additions/removals, row additions, sector-level changes (e.g. "postcode EC1 flood zone moved 2 → 3").
2. **Impact analysis** — full shadow-pricing report: scenario description, portfolio KPIs, distribution of % change, per-region + per-class breakdowns, flagged policies list.
3. **Data quality** — completeness per column, DQ rules + what each rule rejects, freshness.
4. **Upload / Download** — download the current version as CSV; upload an amended version with audit trail.
5. **Approval** — decision buttons (approve / reject) with notes field. Reviewer identity from Databricks SSO. Decision + audit entry written.

### Pricing Factors (was "Feature Table")
- Row count, version, last updated
- Online lookup status (will show "coming soon" until the serving endpoint is built)
- Factor catalogue: every column with name, group (rating factor / enrichment / derived / audit), description, source, transformation, owner, regulatory-sensitive flag, PII flag
- Lineage map linking each factor back to its ingested source
- Rebuild button (triggers the build pipeline)
- Genie chat to explore the catalogue in natural language

### Policies
- List of active policies with company, region, class of business, status, version count
- Detail: policy versions, claim history, an "Explain price" agent widget

### Model Development
- Notebook cards per candidate trainer with description + topics + GitHub link
- **Challenger comparison** — ablation ladder showing cumulative Gini lift per data source added (baseline → +postcode → +geo → +credit → full)

### Model Factory
- List of factory runs with date, status, candidate count
- Run detail: leaderboard ranked by Gini + stability + regulatory suitability. Per-model drill-in.
- AI-assisted planner (optional): Claude proposes the next factory configs based on current champion state, full transparency trace.
- Approve / reject / defer buttons with conditions field on each model.

### Regulatory Reports
- Per-model report: lineage, SHAP reason codes, partial-dependence plots, calibration, fairness audit, protected-attribute absence. (Already working in v2.)

### Serving
- Registered models + serving endpoints (placeholder until Phase 4)
- Live scoring form — override factors, see premium change

### Quote Review
- Recent quotes list + filters (converted / lost / outlier)
- Per-quote: **three payload panels** (sales form, engine request, engine response — when synthetic data ships them) + rating factors + demand score
- **Replay** button — score the quote against the current champion, compare premiums
- Analytics tab: conversion rate by segment, outlier frequency, premium distribution
- Genie chat over the quote stream

### Monitoring
- Data freshness panel (last updated per source)
- Data quality panel (pass rates per source)
- Model stability panel (champion Gini over time, PSI per feature)
- Operational panel (quote volume, approval turnaround, endpoint uptime) — some may show "coming with serving endpoint"

### Governance & Audit
- 4 KPI cards: total events, approved datasets, fairness checks passing, event types logged
- **Audit timeline** — who did what when (dataset approvals, model decisions, agent actions)
- **Activity by type** — event_type × count × last occurrence × unique users
- **Data lineage** — version history per feature / source table with the user who triggered each change
- **AI agent calls** — every Claude call logged with prompt, response, user, tokens
- **Export** — regulatory PDF + JSON package

## AI / Agent features
- **Data-review agent** — on dataset detail, "AI data-quality scan". Reads the snapshot, flags anomalies with severity.
- **Price-explain agent** — on policy detail + quote detail, "Explain this price". Produces an underwriter-readable paragraph.
- **Factor-review agent** — on Pricing Factors page, "Actuarial review of the catalogue". Identifies strengths, gaps, sensitivity concerns, next-feature candidates.
- **Model-plan agent** — on Model Factory, "Propose next factory run". Recommends 3–5 model configs with rationale.
- **Impact-summary agent** — on Impact Analysis tab, "Summarise for the approval committee". One-paragraph narrative of the impact metrics.
- **DQ-monitor agent** — on Governance, "Run AI data-quality scan across all datasets". Surfaces anomalies beyond rule-based checks.

Every agent call is logged to the audit table with prompt + response + token usage + the signed-in user.

## Genie rooms
- **Pricing room** — embedded on Governance and Pricing Factors pages. Covers policies, claims, features.
- **Quote stream room** — embedded on Quote Review. Covers the quote history for conversion / outlier analysis.

## Non-obvious design choices
- **Quote payloads as training data** — every quote keeps its request + response, so serving-time and training-time factor shapes are identical by construction.
- **Shadow-pricing uses a transparent mock multiplier** until the real serving endpoint ships. Documented in the UI with the rule shown.
- **Reviewer identity from SSO** — no free-text name fields. The signed-in Databricks user becomes the reviewer on every decision.
- **Foundation Model by default** — agents call the in-workspace `databricks-claude-sonnet-4-6` endpoint. No external API keys.
- **Error boundary** — any page-render error shows a dismissable red panel, never a blank screen.
- **30-second memo cache** on `/api/home` counts — navigating between tabs is instant.

## Terminology — actuary-facing only

| Avoid (internal) | Use (user-facing) |
| --- | --- |
| raw / silver / bronze | currently-approved version / new version / pending version |
| DLT / medallion | data-quality checks / data pipeline |
| Delta table | dataset |
| schema (of a table) | dataset structure |
| MLflow run | model training record |
| FeatureLookup | factor linkage |
| Gini on incurred | model lift |
| model_comparison | leaderboard |
| PSI | population stability index (also OK: drift score) |
| feature table | pricing factors / factor table |
| exposure_fraction | proportion of year on risk |
| serving endpoint | live pricing endpoint |

## Out of scope (for now)
- Upload-new-version UI (tab defined but no actual upload handler)
- Serving endpoint (Phase 4 — placeholder)
- Online feature store (Phase 4 — placeholder)
- PDF export of the governance package (Phase 4)

---

_This document is versioned in git at `docs/REQUIREMENTS.md`. Changes to scope should update this file in the same commit as the implementation._
