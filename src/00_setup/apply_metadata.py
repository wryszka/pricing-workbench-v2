# Databricks notebook source
# MAGIC %md
# MAGIC # Apply Metadata — v2
# MAGIC
# MAGIC Idempotent descriptions across schema, volumes, tables, and columns. Runs at
# MAGIC the end of the setup job and on-demand as its own job.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

def _esc(s: str) -> str:
    return (s or "").replace("'", "''")

def _table_exists(tbl: str) -> bool:
    try:
        spark.table(f"{fqn}.{tbl}").schema
        return True
    except Exception:
        return False

def _tbl(tbl, comment):
    if not _table_exists(tbl):
        print(f"  [skip] {tbl}")
        return
    try:
        spark.sql(f"COMMENT ON TABLE {fqn}.{tbl} IS '{_esc(comment)}'")
        print(f"  ✓ {tbl}")
    except Exception as e:
        print(f"  [err] {tbl}: {str(e)[:100]}")

def _vol(vol, comment):
    try:
        spark.sql(f"COMMENT ON VOLUME {fqn}.{vol} IS '{_esc(comment)}'")
        print(f"  ✓ volume {vol}")
    except Exception as e:
        print(f"  [skip] volume {vol}: {str(e)[:80]}")

# COMMAND ----------

SCHEMA_COMMENT = (
    "Pricing Workbench v2 — actuary-accurate data model for commercial P&C pricing. "
    "Data model: dimensions (dim_companies, dim_policies, dim_policy_versions) + "
    "facts (fact_quotes, fact_claims) + engineered feature tables "
    "(feature_policy_year_training grained on policy × version × exposure year). "
    "Reference data from ingested vendor feeds (silver_*) + real UK public data "
    "(silver_postcode_enrichment from ONSPD + IMD 2019). Bureau keyed on "
    "company_registration_number, not policy_id. Synthetic data throughout — "
    "Bricksurance SE is fictional."
)
try:
    spark.sql(f"COMMENT ON SCHEMA {fqn} IS '{_esc(SCHEMA_COMMENT)}'")
    print(f"✓ schema {fqn}")
except Exception as e:
    print(f"[err] schema: {e}")

# COMMAND ----------

print("Volumes:")
for v, c in {
    "external_landing": "CSV landing zone for synthetic external vendor feeds (market_benchmark, geo_hazard, company_bureau, sic_directory). Populated by generate_reference_data, ingested to raw_* by the bronze notebooks.",
    "saved_payloads":   "Quote Review operator exports — timestamped JSON payloads saved from the investigation UI.",
    "reports":          "Governance PDFs exported from the app (model cards, factory-run logs, regulatory packs).",
    "raw_data":         "ONSPD + IMD 2019 CSV/ZIP downloads for the postcode enrichment build. Idempotent — 00a skips re-download.",
}.items():
    _vol(v, c)

# COMMAND ----------

print("Tables:")
TABLES = {
    # App operational
    "app_audit_log":             "Immutable audit trail — dataset approvals, model decisions, agent interactions, factory-run submissions.",
    "app_dataset_approvals":     "HITL actuary decisions per ingested dataset version.",

    # Raw (bronze)
    "raw_market_benchmark":      "Bronze — market-rate aggregates per SIC × region × quarter from the vendor CSV.",
    "raw_geo_hazard":            "Bronze — per-postcode location risk scores (flood, fire, crime, subsidence).",
    "raw_company_bureau":        "Bronze — company credit feed KEYED ON company_registration_number (not policy_id).",
    "raw_sic_directory":         "Bronze — UK SIC 2007 directory with internal risk tiers.",

    # Silver (DLT-cleansed, reference)
    "silver_market_benchmark":   "Silver — cleansed market aggregates with quarter_start_date added. DLT expectations drop out-of-range rows.",
    "silver_geo_hazard":         "Silver — cleansed location risk with composite_location_risk + location_risk_tier.",
    "silver_company_bureau":     "Silver — cleansed bureau feed keyed on company_registration_number. Adds credit_risk_tier + business_stability_score.",
    "silver_sic_directory":      "Silver — cleansed SIC directory with validated risk tiers.",
    "silver_postcode_enrichment":"Silver — ~1.5M English postcodes from ONSPD + IMD 2019 + ONS RUC + coastal flags. Real UK public data (Open Government Licence).",

    # Dimensions (gold)
    "dim_companies":             "One row per commercial entity we quote or insure. Natural key: company_registration_number. Surrogate key: company_id.",
    "dim_policies":              "One row per policy entity. Traceable back to the originating quote via originating_quote_id. FK company_id → dim_companies.",
    "dim_policy_versions":       "One row per policy version — initial inception + renewals. Composite PK (policy_id, policy_version). effective_from/to bracket the exposure period.",

    # Facts (gold)
    "fact_quotes":               "Historical quote stream 2020-2024. Rating factors captured at quote_date. converted_to_policy_id links to dim_policies for bound quotes.",
    "fact_claims":               "One row per claim. FK (policy_id, policy_version) → dim_policy_versions. loss_date constrained to fall inside that version's exposure period.",

    # Features (gold — engineered)
    "feature_policy_year_training":
        "PRICING FEATURE TABLE — training grain. One row per (policy_id, policy_version, exposure_year) with features frozen as-at exposure_from via point-in-time joins and labels observed during the exposure period. What the frequency + severity models train on.",
    "feature_quote_training":
        "Pricing Feature Table — demand-model training grain. One row per quote_id with features as-at quote quarter. Label: converted_flag.",
    "feature_policy_current":
        "Pricing Feature Table — serving grain. One row per policy at its latest version. Target of online-store promotion (Lakebase) for renewal FeatureLookup scoring.",
    "feature_catalog":
        "Per-feature metadata for feature_policy_year_training — source tables, source columns, transformation, owner, regulatory/PII flags. Foundation for lineage + audit bolt-ons.",
}

for tbl, comment in TABLES.items():
    _tbl(tbl, comment)

# COMMAND ----------

print(f"\nMetadata applied to {fqn}.")
