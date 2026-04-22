# v2 vs Requirements — Gap Analysis

State of the v2 app as of today (2026-04-22) measured against `REQUIREMENTS.md`.

Status key: ✅ done · 🟡 partial · ❌ not started · ⚠️ wrong terminology or UX

## Home
| Capability | Status | Note |
|---|---|---|
| Hero narrative | ✅ | |
| Data-flow diagram | ✅ | |
| Pillar cards with badges | ✅ | |
| Synthetic-data disclaimer | ✅ | |
| Champion-alias summary | ✅ | |

## Ingestion (list)
| Capability | Status | Note |
|---|---|---|
| 4 datasets listed | ✅ | |
| Columns: source · row count current · row count pending · blocked · status | ⚠️ | Columns say "Raw rows / Silver rows / DQ drops" — not actuary-facing |
| Approval status badge | ❌ | Status column is missing from list |

## Ingestion → Dataset detail
| Tab | Status | Note |
|---|---|---|
| **Data changes** | ⚠️ | Currently called "Raw vs Silver" and shows column diffs — needs to be user-value-based (sector-level changes, new/removed records, distribution shift) |
| **Impact analysis** | 🟡 | Real scenario + metrics for geo_hazard ✅, other datasets show "not built" ❌ |
| **Data quality** | ⚠️ | Column completeness + DLT expectations show — needs pass-rate %, freshness, reframed language |
| **Upload / Download** | ❌ | Not built |
| **Approval** | ✅ | SSO-backed, writes audit entries |

## Pricing Factors (currently "Feature Table")
| Capability | Status | Note |
|---|---|---|
| Page name | ⚠️ | Sidebar says "Feature Table" — rename to "Pricing Factors" |
| Row count / version per factor table | ✅ | |
| Catalogue with name/group/source/transform/owner/sensitive/PII | ✅ | |
| Online lookup status card | ❌ | No mention of online store; needs a "coming with Phase 4" panel |
| Lineage map from factor → source | 🟡 | "Upstream sources" panel exists; visual lineage per factor row doesn't |
| Rebuild factor-table button | ❌ | No trigger |
| Embedded Genie | ❌ | Only on Governance tab |
| Factor-review agent | ✅ | AI widget already wired |

## Policies
| Capability | Status | Note |
|---|---|---|
| List with filters | ✅ | |
| Detail with versions + claims | ✅ | |
| Price-explain agent widget | ✅ | |

## Model Development
| Capability | Status | Note |
|---|---|---|
| Notebook cards | ✅ | |
| GitHub deep links | ✅ | |
| Leaderboard Gini chart | ✅ | |
| Challenger ablation ladder (baseline → +postcode → +geo → +credit → full) | ❌ | Not implemented — requires an ablation run script + storage |

## Model Factory
| Capability | Status | Note |
|---|---|---|
| Factory runs list | ⚠️ | We only show the "latest" model_comparison; no per-run history |
| Run detail with leaderboard | 🟡 | Leaderboard exists but is flat, not per-run |
| Per-model drill-in | ✅ | `ModelDetail` exists |
| Decision buttons (approve / reject / defer) | ❌ | No workflow |
| AI factory-planner widget | ✅ | `propose_model_plan` agent |
| Regulatory report link per model | ✅ | |

## Regulatory Reports
| Capability | Status | Note |
|---|---|---|
| Per-model report viewer | ✅ | |
| Lineage, SHAP, PDP, fairness, reason codes | ✅ | |
| Markdown render with inline plots | ✅ | |

## Serving
| Capability | Status | Note |
|---|---|---|
| Placeholder page | ✅ | |
| Live scoring form | ❌ | Phase 4 |
| Online store status | ❌ | Phase 4 |
| Endpoint latency panel | ❌ | Phase 4 |

## Quote Review
| Capability | Status | Note |
|---|---|---|
| Recent quotes list | ✅ | |
| Filters (converted / lost / outlier) | 🟡 | Converted filter ✅; outlier filter ❌ |
| Quote detail with rating factors | ✅ | |
| Three payload panels (sales / engine request / response) | ❌ | Synthetic data doesn't generate them yet |
| Replay button | ❌ | No replay endpoint |
| Analytics tab (conversion rate, outlier frequency, distribution) | ❌ | |
| Genie chat | ❌ | Only on Governance |

## Monitoring
| Capability | Status | Note |
|---|---|---|
| 6-panel dashboard | ✅ | |
| Data freshness + DQ | ✅ | |
| Model stability chart | ✅ | |
| Fairness summary | ✅ | |
| Operational metrics | 🟡 | Placeholder language — acceptable |

## Governance & Audit
| Capability | Status | Note |
|---|---|---|
| 4 KPI cards | ✅ | |
| Audit timeline | ✅ | |
| Activity by type | ✅ | |
| Data lineage | 🟡 | Table-level, no per-factor lineage drilldown |
| AI agent calls log | 🟡 | Entries are in audit_log but no dedicated tab |
| Export to PDF/JSON | ❌ | |

## Terminology
| Where | Offending term | Fix |
|---|---|---|
| Ingestion list header | "Raw rows / Silver rows / DQ drops" | "Currently approved / New version / Rows blocked by quality checks" |
| Dataset detail tabs | "Raw vs Silver" | "Data changes" |
| Dataset detail body | "Silver schema" | "Dataset structure" |
| Dataset detail subtitle | mentions "DLT" / "Lakeflow Declarative Pipelines" | drop, or move to a collapsible "platform" detail |
| Feature Table sidebar + title | "Feature Table" | "Pricing Factors" |
| Monitoring table | "Gini on incurred" | "Model lift" |
| Upstream sources panel | "silver_*" table names | "Bureau feed", "Hazard feed", etc., with small code-font source below |

---

# Plan to reach parity — ordered by actuary value

## Iteration 1 — Language + Ingestion rework (the thing you saw first)
**Goal:** When an actuary opens Ingestion → Geospatial Hazard, nothing reads like a data-engineering tool.

1. Rename page: Feature Table → **Pricing Factors**.
2. Ingestion list columns → "Currently approved", "New version", "Rows blocked by checks", "Status".
3. Add approval-status badge to each list row.
4. Dataset detail tab order + names: **Data changes · Impact analysis · Data quality · Upload / Download · Approval**.
5. Rename "Raw vs Silver" tab → "Data changes"; replace column-diff view with a **record-level diff** (new postcodes, removed postcodes, postcodes where values changed — with sector-level before/after).
6. Strip all `raw_*` / `silver_*` / `DLT` / "Lakeflow Declarative" strings from user-facing copy. Keep them in source-code comments only.
7. Add "Upload new version" tab (accept a CSV; placeholder backend that validates columns + writes to landing volume; commit decision in next iteration).

## Iteration 2 — Impact analysis for the other 3 datasets
1. Company bureau: candidate shifts ~15% of scores; rescore using credit-risk-tier pricing multipliers.
2. Market benchmark: candidate shifts market rates by +3%; rescore using market-rate factor.
3. SIC directory: candidate moves 10 SICs up a risk tier; rescore.
4. Keep the same output structure as geo_hazard so the `GeoHazardImpact` component can be generalised into `DatasetImpact`.

## Iteration 3 — Challenger / ablation ladder on Model Development
1. New factory notebook: trains the same severity GBM with feature-group subsets (baseline only → +postcode → +geo → +credit → full).
2. Results written to `model_ablation_comparison` table.
3. Model Development page: "Cumulative lift by data source" waterfall chart.

## Iteration 4 — Model Factory run history + decision workflow
1. Persist every factory run (job run id, timestamp, configs, outcomes) to `factory_runs` table.
2. Runs list page + run detail page with approve / reject / defer per candidate.
3. Decisions write to `mf_actuary_decisions` + audit_log.

## Iteration 5 — Quote Review drill-down
1. Extend synthetic data generator to emit sales form + engine request + engine response JSON blobs per quote (~20 columns each).
2. Quote detail: three collapsible payload panels + rating factors panel + replay button.
3. Replay endpoint calls the champion model with the engine-request payload, compares premiums.
4. Analytics tab: conversion rate / outlier / distribution charts.

## Iteration 6 — Upload handler + PDF/JSON export
1. `/api/datasets/{id}/upload` — accept CSV, validate columns against current dataset, write to landing volume as a timestamped new version.
2. Governance PDF export: bundle audit_log + feature_catalog + model_governance into a single PDF.

---

**Approach from here:** I do one iteration, show you, you tell me what's wrong. I don't touch the next iteration until the current one is signed off. No more "all features in one sprint" — that's how we got here.
