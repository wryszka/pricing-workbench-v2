# Databricks notebook source
# MAGIC %md
# MAGIC # Build `feature_policy_current` — serving view for renewal scoring
# MAGIC
# MAGIC One row per policy, features as at the **latest** policy version. This is
# MAGIC the table we'd promote to the online feature store (Lakebase) for sub-10ms
# MAGIC FeatureLookup-based renewal scoring. For new business (Jane) the serving
# MAGIC endpoint takes a feature vector directly — no row in this table is needed.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window

versions  = spark.table(f"{fqn}.dim_policy_versions")
policies  = spark.table(f"{fqn}.dim_policies")
companies = spark.table(f"{fqn}.dim_companies")
bureau    = spark.table(f"{fqn}.silver_company_bureau")
sic_dir   = spark.table(f"{fqn}.silver_sic_directory")
geo       = spark.table(f"{fqn}.silver_geo_hazard")
postcode  = spark.table(f"{fqn}.silver_postcode_enrichment")

# COMMAND ----------

# Latest version per policy
w = Window.partitionBy("policy_id").orderBy(F.col("policy_version").desc())
latest_ver = (versions
    .withColumn("_rk", F.row_number().over(w))
    .filter(F.col("_rk") == 1)
    .drop("_rk")
)

# Area-level postcode enrichment (same aggregation as other feature tables)
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

# Build the serving frame
df = (latest_ver
    .join(policies.select("policy_id", "company_id", "originating_quote_id"),
          on="policy_id", how="left")
    .join(companies.select("company_id", "company_registration_number", "region",
                           "annual_turnover", "incorporation_date"),
          on="company_id", how="left")
    .join(bureau.select("company_registration_number", "credit_score", "credit_risk_tier",
                        "business_stability_score"),
          on="company_registration_number", how="left")
    .join(sic_dir.select("sic_code", "internal_risk_tier", "division"),
          on="sic_code", how="left")
    .join(geo.select("postcode_sector", "flood_zone_rating",
                     "crime_theft_index", "subsidence_risk",
                     "composite_location_risk", "location_risk_tier"),
          on="postcode_sector", how="left")
    .withColumn("area", F.regexp_extract("postcode_sector", AREA_RE, 1))
    .join(postcode_area, on="area", how="left")
    .drop("area")
    .withColumn("_built_at", F.current_timestamp())
    .withColumn("_source_version", F.lit("feature_policy_current_v1"))
)

# COMMAND ----------

table_name = f"{fqn}.feature_policy_current"
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
row_count = spark.table(table_name).count()
print(f"✓ {table_name} — {row_count:,} policies")

try:
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN policy_id SET NOT NULL")
    spark.sql(f"ALTER TABLE {table_name} ADD CONSTRAINT fpc_pk PRIMARY KEY (policy_id)")
except Exception as e:
    if "already" not in str(e).lower(): print(f"⚠ {e}")

spark.sql(f"""
    ALTER TABLE {table_name} SET TBLPROPERTIES (
        'comment' = 'Pricing Feature Table — serving grain. One row per policy at its latest version. Target of the online feature store promotion for sub-10ms FeatureLookup-based renewal scoring. New-business quotes do not read this table — the endpoint takes a feature vector directly.'
    )
""")
