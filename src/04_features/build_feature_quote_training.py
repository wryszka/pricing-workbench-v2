# Databricks notebook source
# MAGIC %md
# MAGIC # Build `feature_quote_training` — demand-model training table
# MAGIC
# MAGIC One row per quote_id. Features are whatever was on the quote at quote time
# MAGIC + any enrichment we would have applied at quote time (postcode, geo, bureau
# MAGIC if company_id is known, market benchmark as at quote quarter).
# MAGIC
# MAGIC Label: `converted` (Y/N). Trains the Demand GBM: "given these rating
# MAGIC factors at quote time, what's P(convert)?"
# MAGIC
# MAGIC This is **not** what serves live new-business quotes — live quotes just
# MAGIC send the feature vector directly. This table is the historical corpus we
# MAGIC learn from.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

import pyspark.sql.functions as F

quotes    = spark.table(f"{fqn}.fact_quotes")
companies = spark.table(f"{fqn}.dim_companies")
bureau    = spark.table(f"{fqn}.silver_company_bureau")
sic_dir   = spark.table(f"{fqn}.silver_sic_directory")
geo       = spark.table(f"{fqn}.silver_geo_hazard")
market    = spark.table(f"{fqn}.silver_market_benchmark")
postcode  = spark.table(f"{fqn}.silver_postcode_enrichment")

# COMMAND ----------

# Point-in-time market lookup — as at quote quarter
market_pit = market.select(
    F.col("sic_code").alias("m_sic"),
    F.col("region").alias("m_region"),
    F.col("year").alias("m_year"),
    F.col("quarter").alias("m_quarter"),
    "market_median_rate", "competitor_a_min_rate", "price_index",
)

# Area-level postcode enrichment (same aggregation as policy-year feature table)
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
    )
)

# Build the training frame
df = (quotes
    .join(companies.select("company_id", "company_registration_number", "annual_turnover"),
          on="company_id", how="left")
    .join(bureau.select("company_registration_number", "credit_score", "credit_risk_tier",
                        "business_stability_score"),
          on="company_registration_number", how="left")
    .join(sic_dir.select("sic_code", "internal_risk_tier", "division"), on="sic_code", how="left")
    .join(geo.select("postcode_sector", "flood_zone_rating", "crime_theft_index",
                    "composite_location_risk"),
          on="postcode_sector", how="left")
    .withColumn("area", F.regexp_extract("postcode_sector", AREA_RE, 1))
    .join(postcode_area, on="area", how="left")
    .drop("area")
    # Market as at quote quarter
    .withColumn("quote_quarter", F.quarter(F.col("quote_date")))
    .withColumn("quote_year",    F.year(F.col("quote_date")))
    .join(
        market_pit,
        (F.col("sic_code")      == F.col("m_sic")) &
        (F.col("region")        == F.col("m_region")) &
        (F.col("quote_year")    == F.col("m_year")) &
        (F.col("quote_quarter") == F.col("m_quarter")),
        "left",
    )
    .drop("m_sic", "m_region", "m_year", "m_quarter")
    # Label — convert converted Y/N to 0/1
    .withColumn("converted_flag",
        F.when(F.col("converted") == "Y", 1).otherwise(0))
    # Derived features
    .withColumn("log_gross_premium", F.log1p(F.col("gross_premium_quoted")))
    .withColumn("log_buildings_si",  F.log1p(F.col("buildings_si")))
    .withColumn("rate_per_1k_si",
        F.round(F.col("gross_premium_quoted") / (F.col("buildings_si") / 1000.0), 3))
    .withColumn("vs_market_rate",
        F.when(F.col("market_median_rate") > 0,
               F.round(F.col("rate_per_1k_si") / F.col("market_median_rate"), 3))
         .otherwise(None))
    .withColumn("_built_at", F.current_timestamp())
    .withColumn("_source_version", F.lit("feature_quote_training_v1"))
)

# COMMAND ----------

table_name = f"{fqn}.feature_quote_training"
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

row_count = spark.table(table_name).count()
print(f"✓ {table_name} — {row_count:,} rows")

try:
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN quote_id SET NOT NULL")
    spark.sql(f"ALTER TABLE {table_name} ADD CONSTRAINT fqt_pk PRIMARY KEY (quote_id)")
except Exception as e:
    if "already" not in str(e).lower(): print(f"⚠ {e}")

spark.sql(f"""
    ALTER TABLE {table_name} SET TBLPROPERTIES (
        'comment' = 'Pricing Feature Table — demand-model training grain. One row per quote_id with rating factors captured at quote time + enrichment looked up as at quote_quarter. Label: converted_flag. Trains the Demand GBM.'
    )
""")

display(spark.sql(f"""
    SELECT YEAR(quote_date) AS year,
           COUNT(*) AS n,
           ROUND(AVG(converted_flag) * 100, 1) AS conv_pct,
           ROUND(AVG(vs_market_rate), 3) AS avg_vs_market
    FROM {table_name}
    WHERE quote_date IS NOT NULL
    GROUP BY YEAR(quote_date)
    ORDER BY year
"""))
