# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest: SIC directory CSV → `raw_sic_directory`
# MAGIC Bronze. UK SIC 2007 directory with internal risk tiers.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")
dbutils.widgets.text("volume_name",  "external_landing")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
volume  = dbutils.widgets.get("volume_name")
fqn     = f"{catalog}.{schema}"
path    = f"/Volumes/{catalog}/{schema}/{volume}/sic_directory/"

# COMMAND ----------

import pyspark.sql.functions as F

df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.raw_sic_directory")
print(f"✓ {fqn}.raw_sic_directory — {df.count()} rows")
