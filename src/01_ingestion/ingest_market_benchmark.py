# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest: market benchmark CSV → `raw_market_benchmark`
# MAGIC Bronze layer. No transformations — just land the vendor CSV as Delta with
# MAGIC `_ingested_at` + `_source_file` audit columns. DQ happens in the silver
# MAGIC DLT pipeline downstream.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")
dbutils.widgets.text("volume_name",  "external_landing")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
volume  = dbutils.widgets.get("volume_name")
fqn     = f"{catalog}.{schema}"
path    = f"/Volumes/{catalog}/{schema}/{volume}/market_benchmark/"

# COMMAND ----------

import pyspark.sql.functions as F

df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.raw_market_benchmark")
print(f"✓ {fqn}.raw_market_benchmark — {df.count()} rows")
