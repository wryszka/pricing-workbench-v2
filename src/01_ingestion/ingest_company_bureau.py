# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest: company bureau CSV → `raw_company_bureau`
# MAGIC Bronze. **Keyed on `company_registration_number`** — the fix vs v1.
# MAGIC Bureau feeds in the real world are keyed on UK company registration numbers,
# MAGIC not on policy IDs (our internal construct).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")
dbutils.widgets.text("volume_name",  "external_landing")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
volume  = dbutils.widgets.get("volume_name")
fqn     = f"{catalog}.{schema}"
path    = f"/Volumes/{catalog}/{schema}/{volume}/company_bureau/"

# COMMAND ----------

import pyspark.sql.functions as F

df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.raw_company_bureau")
print(f"✓ {fqn}.raw_company_bureau — {df.count()} rows (keyed on company_registration_number)")
