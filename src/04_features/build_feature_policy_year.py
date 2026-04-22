# Databricks notebook source
# MAGIC %md
# MAGIC # Build `feature_policy_year_training` — THE training feature table
# MAGIC
# MAGIC One row per **(policy_id, policy_version, exposure_year)**. Each row is a
# MAGIC labelled training example for frequency + severity models.
# MAGIC
# MAGIC **Features** are snapshotted **as at the start of the exposure year** via
# MAGIC point-in-time joins:
# MAGIC - Policy-version attributes (sum_insured, construction, postcode) at inception
# MAGIC - Company attributes (sic, region, turnover) current at exposure_from
# MAGIC - Credit bureau record (as-of is simplified to "current" in this demo — bureau
# MAGIC   history is not modelled; in production you'd maintain a time-series)
# MAGIC - Postcode enrichment (static — real UK public data)
# MAGIC - Geospatial hazard (static)
# MAGIC - Market benchmark **as at the year/quarter of the exposure start** (year × quarter lookup)
# MAGIC
# MAGIC **Labels** are observed **during the exposure period**:
# MAGIC - `claim_count_observed` — count of claims with loss_date in [exposure_from, exposure_to]
# MAGIC - `total_incurred_observed`, `total_paid_observed` — summed over that window
# MAGIC - `peril_counts` — breakdown
# MAGIC - `lapsed_in_period` — did the policy lapse before exposure_to?
# MAGIC
# MAGIC This grain makes the whole demo actuarially correct: a regulator can
# MAGIC point to any row and ask "what did the features look like at time T?" and
# MAGIC "what outcomes were observed for that exposure?" and get a clean answer.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window

# Reference / enrichment tables ------------------------------------------------
policies       = spark.table(f"{fqn}.dim_policies")
versions       = spark.table(f"{fqn}.dim_policy_versions")
companies      = spark.table(f"{fqn}.dim_companies")
claims         = spark.table(f"{fqn}.fact_claims")
sic_dir        = spark.table(f"{fqn}.silver_sic_directory")
bureau         = spark.table(f"{fqn}.silver_company_bureau")
geo            = spark.table(f"{fqn}.silver_geo_hazard")
market         = spark.table(f"{fqn}.silver_market_benchmark")
postcode       = spark.table(f"{fqn}.silver_postcode_enrichment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Expand each policy-version into one row per exposure year
# MAGIC A 3-year-old renewed policy contributes 3 rows (one per exposure year).

# COMMAND ----------

# For a version with effective_from=2022-04-01 and effective_to=2024-08-15, we emit
# exposure-years [2022, 2023, 2024]. Each exposure year runs from:
#   max(effective_from, year_start)  →  min(effective_to, year_end)
# We only keep years with at least 30 days of exposure (drop tiny slivers).

version_years = (versions
    .selectExpr(
        "policy_id", "policy_version",
        "effective_from", "effective_to",
        "sum_insured", "gross_premium", "construction_type", "year_built",
        "postcode_sector", "sic_code", "channel", "model_version_used",
        "status",
        "YEAR(effective_from) AS year_from",
        "YEAR(effective_to)   AS year_to",
    )
)

# Build all exposure-years per version
years_df = (version_years
    .withColumn("year_seq", F.sequence(F.col("year_from"), F.col("year_to")))
    .withColumn("exposure_year", F.explode("year_seq"))
    .drop("year_seq", "year_from", "year_to")
)

# Compute exposure window per year
exposure_df = (years_df
    .withColumn("year_start",      F.to_date(F.concat_ws("-", F.col("exposure_year").cast("string"), F.lit("01"), F.lit("01"))))
    .withColumn("year_end",        F.to_date(F.concat_ws("-", F.col("exposure_year").cast("string"), F.lit("12"), F.lit("31"))))
    .withColumn("exposure_from",   F.greatest(F.col("effective_from"), F.col("year_start")))
    .withColumn("exposure_to",     F.least   (F.col("effective_to"),   F.col("year_end")))
    .withColumn("exposure_days",   F.datediff(F.col("exposure_to"), F.col("exposure_from")) + F.lit(1))
    .filter("exposure_days >= 30")
    .drop("year_start", "year_end")
)

print(f"policy-year rows: {exposure_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Join static features (policy attributes + company + enrichment)

# COMMAND ----------

# Company attributes (current state — company attributes don't change often in this demo;
# in production you'd have dim_companies_history with effective_from/to and do as-of join here)
comp_cols = companies.select(
    "company_id", "company_registration_number", "primary_sic_code",
    "region", "annual_turnover", "incorporation_date",
)

# Bureau — current state (in production, bureau history would be time-sliced)
bureau_cols = bureau.select(
    "company_registration_number", "credit_score", "ccj_count",
    "years_trading", "credit_risk_tier", "business_stability_score",
    "bankruptcy_flag",
)

# SIC-tier lookup
sic_cols = sic_dir.select("sic_code", "internal_risk_tier", "division")

# Geospatial hazard
geo_cols = geo.select("postcode_sector", "flood_zone_rating",
                      "proximity_to_fire_station_km", "crime_theft_index",
                      "subsidence_risk", "composite_location_risk",
                      "location_risk_tier")

# Postcode enrichment — real UK data, aggregated to postcode area level for the join
# Area = letters + first digits (e.g. "EC1")
AREA_RE = r"^([A-Z]{1,2}\d+)"
postcode_area = (postcode
    .filter(F.col("imd_decile").isNotNull())
    .withColumn("area", F.regexp_extract("postcode", AREA_RE, 1))
    .filter(F.col("area") != "")
    .groupBy("area")
    .agg(
        F.avg("is_urban").alias("frac_urban"),
        F.max("is_coastal").cast("int").alias("is_coastal"),
        F.avg("imd_decile").alias("imd_decile"),
        F.avg("crime_decile").alias("crime_decile"),
        F.avg("income_decile").alias("income_decile"),
        F.avg("health_decile").alias("health_decile"),
        F.avg("living_env_decile").alias("living_env_decile"),
    )
)

# Derive urban_score + deprivation_composite
postcode_feats = (postcode_area
    .withColumn("urban_score",
        F.round(F.lit(0.60) * F.col("frac_urban") +
                F.lit(0.40) * ((F.lit(10.0) - F.col("living_env_decile")) / F.lit(9.0)), 4))
    .withColumn("deprivation_composite",
        F.round(
            ((F.lit(10.0) - F.col("crime_decile"))      / F.lit(9.0) +
             (F.lit(10.0) - F.col("income_decile"))     / F.lit(9.0) +
             (F.lit(10.0) - F.col("health_decile"))     / F.lit(9.0) +
             (F.lit(10.0) - F.col("living_env_decile")) / F.lit(9.0)) / F.lit(4.0),
            4))
)

# Join policies → companies → bureau → SIC → geo → postcode enrichment
features_static = (exposure_df
    .join(policies.select("policy_id", "company_id", "originating_quote_id"),
          on="policy_id", how="left")
    .join(comp_cols, on="company_id", how="left")
    .join(bureau_cols, on="company_registration_number", how="left")
    .join(sic_cols, on="sic_code", how="left")
    .join(geo_cols, on="postcode_sector", how="left")
    # Extract area from postcode_sector to match postcode_enrichment aggregation grain
    .withColumn("area", F.regexp_extract("postcode_sector", AREA_RE, 1))
    .join(postcode_feats, on="area", how="left")
    .drop("area")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Point-in-time join to market benchmark
# MAGIC For each row, look up market rates **as at the exposure-year's Q1 (or the
# MAGIC quarter containing `exposure_from`)**. One row per (sic, region, year, quarter).

# COMMAND ----------

market_at_exposure = market.select(
    "match_key_sic_region", "sic_code", "region", "year", "quarter",
    "market_median_rate", "competitor_a_min_rate", "competitor_b_min_rate", "price_index",
)

# Compute the quarter of exposure_from
features_with_market = (features_static
    .withColumn("exposure_quarter", F.quarter(F.col("exposure_from")))
    .withColumn("exposure_year_for_market", F.year(F.col("exposure_from")))
    .join(
        market_at_exposure.select(
            F.col("sic_code").alias("m_sic"),
            F.col("region").alias("m_region"),
            F.col("year").alias("m_year"),
            F.col("quarter").alias("m_quarter"),
            "market_median_rate", "competitor_a_min_rate", "competitor_b_min_rate", "price_index",
        ),
        (F.col("sic_code")                  == F.col("m_sic")) &
        (F.col("region")                    == F.col("m_region")) &
        (F.col("exposure_year_for_market")  == F.col("m_year")) &
        (F.col("exposure_quarter")          == F.col("m_quarter")),
        "left",
    )
    .drop("m_sic", "m_region", "m_year", "m_quarter")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Attach labels — claims observed during the exposure window

# COMMAND ----------

# Aggregate claims per (policy_id, policy_version, exposure_year)
claims_agg = (
    claims
    .withColumn("loss_year", F.year("loss_date"))
    .groupBy("policy_id", "policy_version", "loss_year")
    .agg(
        F.count("claim_id")                                                    .alias("claim_count_observed"),
        F.sum("incurred_amount")                                               .alias("total_incurred_observed"),
        F.sum("paid_amount")                                                   .alias("total_paid_observed"),
        F.sum(F.when(F.col("peril") == "Fire",            F.col("incurred_amount")).otherwise(0)).alias("fire_incurred"),
        F.sum(F.when(F.col("peril") == "Flood",           F.col("incurred_amount")).otherwise(0)).alias("flood_incurred"),
        F.sum(F.when(F.col("peril") == "Theft",           F.col("incurred_amount")).otherwise(0)).alias("theft_incurred"),
        F.sum(F.when(F.col("peril") == "Liability",       F.col("incurred_amount")).otherwise(0)).alias("liability_incurred"),
        F.sum(F.when(F.col("peril") == "Storm",           F.col("incurred_amount")).otherwise(0)).alias("storm_incurred"),
        F.sum(F.when(F.col("peril") == "Subsidence",      F.col("incurred_amount")).otherwise(0)).alias("subsidence_incurred"),
        F.sum(F.when(F.col("peril") == "Escape of Water", F.col("incurred_amount")).otherwise(0)).alias("water_incurred"),
    )
)

# Join labels onto features
final_df = (features_with_market
    .join(
        claims_agg
            .withColumnRenamed("loss_year", "exposure_year"),
        on=["policy_id", "policy_version", "exposure_year"],
        how="left",
    )
    # Fill nulls: policy-years with no claims get zero counts / amounts
    .fillna(0, subset=[
        "claim_count_observed", "total_incurred_observed", "total_paid_observed",
        "fire_incurred", "flood_incurred", "theft_incurred", "liability_incurred",
        "storm_incurred", "subsidence_incurred", "water_incurred",
    ])
    .withColumn("lapsed_in_period",
        F.when((F.col("status").isin("LAPSED", "CANCELLED")) &
               (F.col("exposure_to") <= F.col("effective_to")), 1).otherwise(0))
    .withColumn("loss_ratio_observed",
        F.when(F.col("gross_premium") > 0,
               F.round(F.col("total_incurred_observed") / F.col("gross_premium"), 4))
         .otherwise(None))
    # Convenience exposures / policy age columns
    .withColumn("policy_age_years_at_exposure_start",
        F.round(F.datediff(F.col("exposure_from"), F.col("incorporation_date")) / F.lit(365.0), 2))
    .withColumn("building_age_years_at_exposure_start",
        F.col("exposure_year") - F.col("year_built"))
    .withColumn("exposure_fraction",
        F.round(F.col("exposure_days") / F.lit(365.0), 4))
    # Audit columns
    .withColumn("_built_at", F.current_timestamp())
    .withColumn("_source_version", F.lit("feature_policy_year_training_v1"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write, set PK + comments

# COMMAND ----------

table_name = f"{fqn}.feature_policy_year_training"
(final_df
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name))

row_count = spark.table(table_name).count()
col_count = len(spark.table(table_name).columns)
print(f"✓ {table_name} — {row_count:,} rows × {col_count} columns")

# Primary key = (policy_id, policy_version, exposure_year)
try:
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN policy_id SET NOT NULL")
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN policy_version SET NOT NULL")
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN exposure_year SET NOT NULL")
    spark.sql(f"ALTER TABLE {table_name} ADD CONSTRAINT fpy_pk PRIMARY KEY (policy_id, policy_version, exposure_year)")
    print("✓ PK set on (policy_id, policy_version, exposure_year)")
except Exception as e:
    if "already" not in str(e).lower(): print(f"⚠ {e}")

spark.sql(f"""
    ALTER TABLE {table_name} SET TBLPROPERTIES (
        'comment' = 'Pricing Feature Table — training grain. One row per (policy_id, policy_version, exposure_year). Features frozen as-at exposure_from via point-in-time joins; labels observed during [exposure_from, exposure_to]. This is what Frequency and Severity GLMs learn from. Point-in-time correct: a row for exposure_year 2022 sees the policy attributes at 2022-04-01 (say) + market rates as at 2022Q2 + claims observed in 2022.'
    )
""")
tag_sql = ", ".join([
    "'feature_table_role' = 'training'",
    "'grain' = 'policy_id × policy_version × exposure_year'",
    "'business_line' = 'commercial_property'",
    "'demo_environment' = 'true'",
])
spark.sql(f"ALTER TABLE {table_name} SET TAGS ({tag_sql})")

# COMMAND ----------

display(spark.sql(f"""
    SELECT exposure_year,
           COUNT(*) AS policy_years,
           ROUND(AVG(claim_count_observed), 4) AS avg_freq,
           ROUND(SUM(claim_count_observed) * 1.0 / COUNT(*), 4) AS annual_freq,
           ROUND(AVG(total_incurred_observed), 2) AS avg_incurred
    FROM {table_name}
    GROUP BY exposure_year ORDER BY exposure_year
"""))
