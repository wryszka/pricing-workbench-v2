# Databricks notebook source
# MAGIC %md
# MAGIC # Build `feature_catalog` — metadata for every feature in the training table
# MAGIC
# MAGIC One row per feature in `feature_policy_year_training`, with full provenance.
# MAGIC Foundation for feature lineage + audit bolt-ons.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql.types import *

FEATURES = [
    # Keys / time
    ("policy_id",                           "key",              "STRING",    "Policy entity ID",                                                                 ["dim_policies"],                     ["policy_id"],            "identity",                                "pricing_data_platform",  False, True),
    ("policy_version",                      "key",              "INT",       "Version of the policy (1 = initial inception, 2+ = renewals)",                    ["dim_policy_versions"],              ["policy_version"],       "identity",                                "pricing_data_platform",  False, False),
    ("exposure_year",                       "key",              "INT",       "Calendar year of this exposure observation",                                       ["dim_policy_versions"],              ["effective_from"],       "YEAR(exposure_from)",                     "pricing_data_platform",  False, False),
    ("exposure_from",                       "time_dim",         "DATE",      "Start of the observed exposure period for this row",                              ["dim_policy_versions"],              ["effective_from"],       "max(effective_from, year_start)",         "pricing_data_platform",  False, False),
    ("exposure_to",                         "time_dim",         "DATE",      "End of the observed exposure period for this row",                                ["dim_policy_versions"],              ["effective_to"],         "min(effective_to, year_end)",             "pricing_data_platform",  False, False),
    ("exposure_days",                       "time_dim",         "INT",       "Days of exposure in this row",                                                     ["dim_policy_versions"],              [],                       "datediff(exposure_to, exposure_from) + 1","pricing_data_platform",  False, False),
    ("exposure_fraction",                   "time_dim",         "DOUBLE",    "Fraction of a year this row represents (exposure_days / 365)",                     ["dim_policy_versions"],              [],                       "exposure_days / 365.0",                   "pricing_data_platform",  False, False),

    # Policy-version rating factors (frozen at inception of this version)
    ("sum_insured",                         "rating_factor",    "BIGINT",    "Total sum insured on the policy version",                                          ["dim_policy_versions"],              ["sum_insured"],          "identity",                                "actuarial_pricing_team", False, False),
    ("gross_premium",                       "rating_factor",    "DOUBLE",    "Premium charged on this policy version",                                           ["dim_policy_versions"],              ["gross_premium"],        "identity",                                "actuarial_pricing_team", False, False),
    ("construction_type",                   "rating_factor",    "STRING",    "ISO construction class",                                                           ["dim_policy_versions"],              ["construction_type"],    "identity",                                "actuarial_pricing_team", False, False),
    ("year_built",                          "rating_factor",    "INT",       "Year the primary premises was built",                                              ["dim_policy_versions"],              ["year_built"],           "identity",                                "actuarial_pricing_team", False, False),
    ("building_age_years_at_exposure_start","derived",          "INT",       "Building age at the start of the exposure period",                                 ["dim_policy_versions"],              ["year_built"],           "exposure_year - year_built",              "actuarial_pricing_team", False, False),
    ("postcode_sector",                     "rating_factor",    "STRING",    "UK postcode outcode — joins to location risk + postcode enrichment",                ["dim_policy_versions"],              ["postcode_sector"],      "identity",                                "actuarial_pricing_team", False, False),
    ("sic_code",                            "rating_factor",    "STRING",    "Industry SIC code from dim_companies (primary_sic_code)",                          ["dim_policy_versions", "dim_companies"], ["sic_code"],         "identity",                                "actuarial_pricing_team", False, False),
    ("channel",                             "rating_factor",    "STRING",    "Distribution channel at the originating quote",                                    ["dim_policy_versions"],              ["channel"],              "identity",                                "actuarial_pricing_team", False, False),
    ("model_version_used",                  "audit",            "STRING",    "Pricing model version that produced the original quote",                           ["dim_policy_versions"],              ["model_version_used"],   "identity",                                "actuarial_pricing_team", False, False),
    ("status",                              "policy_state",     "STRING",    "Policy-version status at exposure_to (ACTIVE / RENEWED / LAPSED / CANCELLED)",     ["dim_policy_versions"],              ["status"],               "identity",                                "actuarial_pricing_team", False, False),
    ("lapsed_in_period",                    "derived",          "INT",       "Did the policy lapse or cancel before the exposure_to boundary",                   ["dim_policy_versions"],              ["status"],               "status IN (LAPSED,CANCELLED) AND exposure_to <= effective_to","actuarial_pricing_team", False, False),

    # Company attributes (from dim_companies)
    ("company_id",                          "foreign_key",      "STRING",    "FK to dim_companies",                                                              ["dim_companies"],                    ["company_id"],           "identity",                                "pricing_data_platform",  False, True),
    ("company_registration_number",         "foreign_key",      "STRING",    "UK Companies House registration — joins to silver_company_bureau",                 ["dim_companies"],                    ["company_registration_number"], "identity",                        "pricing_data_platform",  False, True),
    ("region",                              "rating_factor",    "STRING",    "Region from dim_companies",                                                        ["dim_companies"],                    ["region"],               "identity",                                "actuarial_pricing_team", False, False),
    ("annual_turnover",                     "rating_factor",    "LONG",      "Declared annual revenue from dim_companies",                                       ["dim_companies"],                    ["annual_turnover"],      "identity",                                "actuarial_pricing_team", False, False),
    ("policy_age_years_at_exposure_start",  "derived",          "DOUBLE",    "Years since company incorporation at exposure_from",                               ["dim_companies"],                    ["incorporation_date"],   "(exposure_from - incorporation_date)/365","actuarial_pricing_team", False, False),

    # Bureau — external credit feed (silver_company_bureau)
    ("credit_score",                        "enrichment",       "INT",       "Company credit score (200-900) from bureau",                                       ["silver_company_bureau"],            ["credit_score"],         "identity",                                "credit_risk",            True,  True),
    ("ccj_count",                           "enrichment",       "INT",       "County Court Judgements on file",                                                  ["silver_company_bureau"],            ["ccj_count"],            "identity",                                "credit_risk",            True,  True),
    ("years_trading",                       "enrichment",       "INT",       "Years the business has traded",                                                    ["silver_company_bureau"],            ["years_trading"],        "identity",                                "credit_risk",            False, False),
    ("credit_risk_tier",                    "derived",          "STRING",    "Credit classification: Prime / Standard / Sub-Standard / High Risk",               ["silver_company_bureau"],            ["credit_score","ccj_count","bankruptcy_flag"], "tier from credit_score + CCJ","credit_risk",            True,  False),
    ("business_stability_score",            "derived",          "INT",       "Composite business stability score (0–100)",                                       ["silver_company_bureau"],            [],                       "weighted composite",                      "credit_risk",            True,  False),
    ("bankruptcy_flag",                     "enrichment",       "BOOLEAN",   "Historical bankruptcy flag from bureau",                                           ["silver_company_bureau"],            ["bankruptcy_flag"],      "identity",                                "credit_risk",            True,  True),

    # SIC directory (silver_sic_directory)
    ("internal_risk_tier",                  "rating_factor",    "STRING",    "Internal risk tier (Low/Medium/High) for the SIC code",                            ["silver_sic_directory"],             ["internal_risk_tier"],   "identity",                                "actuarial_pricing_team", False, False),
    ("division",                            "reference",        "STRING",    "SIC high-level division (Manufacturing/Construction/etc.)",                        ["silver_sic_directory"],             ["division"],             "identity",                                "actuarial_pricing_team", False, False),

    # Geo hazard
    ("flood_zone_rating",                   "enrichment",       "INT",       "Flood risk score (1=low, 10=high) — vendor",                                       ["silver_geo_hazard"],                ["flood_zone_rating"],    "identity",                                "pricing_analytics",      False, False),
    ("proximity_to_fire_station_km",        "enrichment",       "DOUBLE",    "Distance to nearest fire station (km)",                                            ["silver_geo_hazard"],                ["proximity_to_fire_station_km"], "identity",                        "pricing_analytics",      False, False),
    ("crime_theft_index",                   "enrichment",       "DOUBLE",    "Local crime / theft index (vendor, synthetic)",                                    ["silver_geo_hazard"],                ["crime_theft_index"],    "identity",                                "pricing_analytics",      True,  False),
    ("subsidence_risk",                     "enrichment",       "DOUBLE",    "Ground subsidence risk (0-10)",                                                    ["silver_geo_hazard"],                ["subsidence_risk"],      "identity",                                "pricing_analytics",      False, False),
    ("composite_location_risk",             "derived",          "DOUBLE",    "Weighted composite of flood/fire/crime/subsidence",                                ["silver_geo_hazard"],                ["flood_zone_rating","proximity_to_fire_station_km","crime_theft_index","subsidence_risk"], "0.3 flood + 0.2 fire_dist + 0.25 crime + 0.25 subsidence", "actuarial_pricing_team", False, False),
    ("location_risk_tier",                  "derived",          "STRING",    "Location risk tier (High/Medium/Low) from composite score",                        ["silver_geo_hazard"],                [],                       "tier from composite_location_risk",       "actuarial_pricing_team", False, False),

    # Real UK postcode enrichment (ONSPD + IMD)
    ("frac_urban",                          "enrichment",       "DOUBLE",    "Fraction of area postcodes classified urban (ONS RUC 2011)",                       ["silver_postcode_enrichment"],       ["is_urban"],             "area-level mean",                         "actuarial_pricing_team", False, False),
    ("is_coastal",                          "enrichment",       "INT",       "Coastal flag (derived from ONS local authority codes)",                            ["silver_postcode_enrichment"],       ["local_authority_code"], "area-level max",                          "actuarial_pricing_team", False, False),
    ("imd_decile",                          "enrichment",       "DOUBLE",    "IMD 2019 overall decile averaged to postcode area (1=most deprived, 10=least)",    ["silver_postcode_enrichment"],       ["imd_decile"],           "area-level mean",                         "actuarial_pricing_team", True,  False),
    ("crime_decile",                        "enrichment",       "DOUBLE",    "IMD 2019 crime sub-decile averaged to postcode area",                              ["silver_postcode_enrichment"],       ["crime_decile"],         "area-level mean",                         "actuarial_pricing_team", True,  False),
    ("urban_score",                         "derived",          "DOUBLE",    "Weighted composite (0-1) — 0.60 frac_urban + 0.40 inverted living_env_decile",     ["silver_postcode_enrichment"],       ["is_urban","living_env_decile"], "0.6 * frac_urban + 0.4 * (10-living_env)/9","actuarial_pricing_team", False, False),
    ("deprivation_composite",               "derived",          "DOUBLE",    "Equal-weighted IMD crime/income/health/living-env composite (0-1, 1=most deprived)",["silver_postcode_enrichment"],       ["crime_decile","income_decile","health_decile","living_env_decile"], "mean of inverted deciles","actuarial_pricing_team", True,  False),

    # Market benchmark (point-in-time)
    ("market_median_rate",                  "enrichment",       "DOUBLE",    "Market median premium rate per £1k SI — as at exposure quarter",                   ["silver_market_benchmark"],          ["market_median_rate"],   "SIC × region × year × quarter lookup",    "pricing_analytics",      False, False),
    ("competitor_a_min_rate",               "enrichment",       "DOUBLE",    "Lowest observed competitor A rate",                                                ["silver_market_benchmark"],          ["competitor_a_min_rate"],"SIC × region × year × quarter lookup",    "pricing_analytics",      False, False),
    ("price_index",                         "enrichment",       "DOUBLE",    "Market price index at exposure quarter",                                           ["silver_market_benchmark"],          ["price_index"],          "SIC × region × year × quarter lookup",    "pricing_analytics",      False, False),

    # Labels (observed during exposure period — DO NOT use as features)
    ("claim_count_observed",                "label",            "BIGINT",    "Number of claims with loss_date in [exposure_from, exposure_to]",                  ["fact_claims"],                      ["loss_date"],            "COUNT",                                   "actuarial_pricing_team", False, False),
    ("total_incurred_observed",             "label",            "DOUBLE",    "Sum of incurred amounts for claims in the exposure period (GBP)",                  ["fact_claims"],                      ["incurred_amount"],      "SUM",                                     "actuarial_pricing_team", False, False),
    ("total_paid_observed",                 "label",            "DOUBLE",    "Sum of paid amounts for claims in the exposure period (GBP)",                      ["fact_claims"],                      ["paid_amount"],          "SUM",                                     "actuarial_pricing_team", False, False),
    ("loss_ratio_observed",                 "label",            "DOUBLE",    "total_incurred_observed / gross_premium for this policy-year",                     ["fact_claims","dim_policy_versions"],["incurred_amount","gross_premium"], "incurred / premium",           "actuarial_pricing_team", False, False),
    ("fire_incurred",                       "label",            "DOUBLE",    "Incurred amount from Fire claims in this exposure period (GBP)",                   ["fact_claims"],                      ["peril","incurred_amount"], "SUM where peril=Fire",                 "actuarial_pricing_team", False, False),
    ("flood_incurred",                      "label",            "DOUBLE",    "Incurred amount from Flood claims in this exposure period",                       ["fact_claims"],                      ["peril","incurred_amount"], "SUM where peril=Flood",                "actuarial_pricing_team", False, False),
    ("theft_incurred",                      "label",            "DOUBLE",    "Incurred amount from Theft claims in this exposure period",                       ["fact_claims"],                      ["peril","incurred_amount"], "SUM where peril=Theft",                "actuarial_pricing_team", False, False),
    ("liability_incurred",                  "label",            "DOUBLE",    "Incurred amount from Liability claims in this exposure period",                   ["fact_claims"],                      ["peril","incurred_amount"], "SUM where peril=Liability",            "actuarial_pricing_team", False, False),
    ("storm_incurred",                      "label",            "DOUBLE",    "Incurred amount from Storm claims in this exposure period",                       ["fact_claims"],                      ["peril","incurred_amount"], "SUM where peril=Storm",                "actuarial_pricing_team", False, False),
    ("subsidence_incurred",                 "label",            "DOUBLE",    "Incurred amount from Subsidence claims in this exposure period",                  ["fact_claims"],                      ["peril","incurred_amount"], "SUM where peril=Subsidence",           "actuarial_pricing_team", False, False),
    ("water_incurred",                      "label",            "DOUBLE",    "Incurred amount from Escape-of-Water claims in this exposure period",             ["fact_claims"],                      ["peril","incurred_amount"], "SUM where peril=EscapeOfWater",        "actuarial_pricing_team", False, False),
]

now = datetime.now(timezone.utc).replace(tzinfo=None)
rows = [
    (n, g, d, desc, srct, srcc, tr, owner, reg, pii, now, now)
    for (n, g, d, desc, srct, srcc, tr, owner, reg, pii) in FEATURES
]

schema_ = StructType([
    StructField("feature_name",         StringType(), False),
    StructField("feature_group",        StringType()),
    StructField("data_type",            StringType()),
    StructField("description",          StringType()),
    StructField("source_tables",        ArrayType(StringType())),
    StructField("source_columns",       ArrayType(StringType())),
    StructField("transformation",       StringType()),
    StructField("owner",                StringType()),
    StructField("regulatory_sensitive", BooleanType()),
    StructField("pii",                  BooleanType()),
    StructField("added_at",             TimestampType()),
    StructField("last_modified",        TimestampType()),
])
df = spark.createDataFrame(rows, schema=schema_)

table_name = f"{fqn}.feature_catalog"
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ {table_name} — {df.count()} features")

spark.sql(f"""
    ALTER TABLE {table_name} SET TBLPROPERTIES (
        'comment' = 'Per-feature metadata for feature_policy_year_training. Each row: source tables + source columns + transformation + owner + regulatory / PII flags. Foundation for lineage and audit bolt-ons.'
    )
""")

from pyspark.sql.functions import count
display(df.groupBy("feature_group").agg(count("*").alias("n")).orderBy("n", ascending=False))
