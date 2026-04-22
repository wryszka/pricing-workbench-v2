# Pricing Workbench v2 — actuary-accurate data model

End-to-end commercial P&C pricing on Databricks, rebuilt from scratch to match how a real
insurer's data actually looks. No cut corners, no policy-keyed bureau feeds, no premium-as-a-feature
target leakage. Point-in-time correct feature engineering on policy-year exposures.

## What this is

v1 (the previous iteration at [`wryszka/pricing-workbench`](https://github.com/wryszka/pricing-workbench)) was great for showing the platform
story on Databricks — HITL ingestion, feature store, Model Factory, Quote Review, governance, agents.
What it wasn't: structurally correct. An actuary would spot target leakage on premium, a bureau
feed artificially keyed on policy_id, and no time dimension on policies.

This v2 fixes the data model without losing any of the platform story. Same medallion pattern, same
HITL approvals, same feature store, same Model Factory, same online store — but on a data model
that reflects real insurance company operations.

## Key differences vs v1

| Area | v1 | v2 |
|---|---|---|
| Feature table grain | one row per policy (current state) | **one row per (policy_id, policy_version, exposure_year)** |
| Bureau key | `policy_id` (faked) | `company_registration_number` (real-world correct) |
| Claims linkage | `claim_count_5y` rolling aggregate | tied to specific policy version and exposure period |
| Policy history | none — one row per policy | `dim_policy_versions` with `effective_from / effective_to / status` |
| Quote → policy link | partial (policy_id on quote) | `originating_quote_id` on policy + `converted_to_policy_id` on quote |
| Features + labels | mixed in `unified_pricing_table_live` | separated — features frozen as-at exposure start, labels observed during exposure |
| Company dimension | policies directly carry company attributes | `dim_companies` keyed on `company_registration_number` — one company can hold many policies over time |

## Naming convention

| Prefix | Role | Examples |
|---|---|---|
| `raw_*` | Bronze — direct from ingestion | `raw_market_benchmark` |
| `silver_*` | Silver — DLT-cleansed, usable as reference | `silver_postcode_enrichment` |
| `dim_*` | Gold dimensions (slowly-changing entities) | `dim_companies`, `dim_policies`, `dim_policy_versions` |
| `fact_*` | Gold facts (transactional events) | `fact_quotes`, `fact_claims` |
| `feature_*` | Gold engineered feature tables | `feature_policy_year_training` |
| `model_*` | Model Factory artefacts | `model_factory_leaderboard` |
| `app_*` | App operational tables | `app_audit_log` |

Everything of one role clusters together alphabetically in Catalog Explorer — no more hunting.

## Workspace

- Catalog: `lr_serverless_aws_us_catalog`
- Schema: **`pricing_workbench`** (fresh — v1's `pricing_upt` stays live and untouched)
- Workspace: `https://fevm-lr-serverless-aws-us.cloud.databricks.com`

## Status

Under construction. Phase 1 (data model + feature engineering) lands first.
