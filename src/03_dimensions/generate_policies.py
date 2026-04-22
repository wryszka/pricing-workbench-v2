# Databricks notebook source
# MAGIC %md
# MAGIC # Generate `dim_policies` + `dim_policy_versions`
# MAGIC
# MAGIC Takes every **converted** quote in `fact_quotes` and turns it into:
# MAGIC - One `dim_policies` row (the policy entity)
# MAGIC - One or more `dim_policy_versions` rows (initial + renewals)
# MAGIC
# MAGIC Policy lifecycle modelled:
# MAGIC - Converted quote → `policy_version = 1`, ACTIVE for 12 months
# MAGIC - At end of 12 months, ~70% renew → new policy_version (same policy_id, version+1)
# MAGIC - ~20% lapse (LAPSED status on the final version)
# MAGIC - ~10% cancel mid-term (CANCELLED with effective_to before 12m)
# MAGIC
# MAGIC Also backfills `fact_quotes.converted_to_policy_id` + sets the originating
# MAGIC quote on `dim_policies`.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

import random
from datetime import date, timedelta
from pyspark.sql.types import *
import pyspark.sql.functions as F

random.seed(42)

TODAY = date(2025, 1, 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load converted quotes

# COMMAND ----------

converted_quotes = (
    spark.table(f"{fqn}.fact_quotes")
    .filter(F.col("converted") == "Y")
    .orderBy("quote_date", "quote_id")
    .toPandas()
)
print(f"converted quotes: {len(converted_quotes):,} → these become initial policy versions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create dim_policies (initial rows) + dim_policy_versions (all versions)

# COMMAND ----------

policies_rows      = []
versions_rows      = []
quote_to_policy    = {}  # backfill lookup for fact_quotes

# Renewal behaviour parameters
P_RENEW  = 0.70   # of ACTIVE policies, p(renew at expiry)
P_CANCEL_MID_TERM = 0.05  # p(cancel mid-term for a given version)

for i, q in converted_quotes.iterrows():
    policy_id     = f"POL-{100000 + i}"
    policy_number = f"PL-{q['quote_date'].year}-{100000 + i:06d}"
    quote_to_policy[q["quote_id"]] = policy_id

    # Product type inferred from coverage mix (for this demo, everything is commercial property + liability combined)
    product_type = "commercial_property_liability"

    # If the quote had no company_id (new prospect), we have no dim_companies FK.
    # In the real world we'd create one on bind; for the demo we leave NULL.
    company_id = q["company_id"] if q["company_id"] else None

    # Policy entity
    policies_rows.append((
        policy_id,
        policy_number,
        company_id,
        q["quote_id"],                  # originating_quote_id
        product_type,
    ))

    # Versions — walk the timeline
    version = 1
    ver_inception = q["quote_date"] + timedelta(days=int(q["days_to_decision"] or 1))
    while True:
        ver_expiry = ver_inception + timedelta(days=365)
        cancel_mid_term = random.random() < P_CANCEL_MID_TERM

        if cancel_mid_term:
            # Cancel somewhere in months 2–10
            cancel_days = random.randint(60, 300)
            effective_to = ver_inception + timedelta(days=cancel_days)
            status = "CANCELLED"
            # No renewal follows a mid-term cancellation
            follow_on = None
        else:
            effective_to = ver_expiry
            # After expiry: renew vs lapse vs expire (today cutoff)
            if ver_expiry > TODAY:
                # Still in force on snapshot date
                status = "ACTIVE"
                follow_on = None
            else:
                if random.random() < P_RENEW:
                    status = "RENEWED"
                    follow_on = ver_expiry  # next version starts at prior expiry
                else:
                    status = "LAPSED"
                    follow_on = None

        # Mild premium drift between versions (renewal increases ~2–5%, cancelled stays flat)
        if version == 1:
            sum_insured   = int(q["buildings_si"] + q["contents_si"])
            gross_premium = float(q["gross_premium_quoted"])
        else:
            sum_insured   = int(versions_rows[-1][6] * random.uniform(0.98, 1.05))
            gross_premium = float(versions_rows[-1][7] * random.uniform(1.00, 1.06))

        versions_rows.append((
            policy_id,
            version,
            ver_inception,
            effective_to,
            status,
            product_type,
            sum_insured,
            round(gross_premium, 2),
            q["construction_type"],
            int(q["year_built"]),
            q["postcode_sector"],
            q["sic_code"],
            q["channel"],
            q["model_version_used"],
        ))

        if follow_on is None:
            break
        version += 1
        ver_inception = follow_on
        if ver_inception > TODAY:
            break

print(f"generated: {len(policies_rows):,} policies, {len(versions_rows):,} policy-versions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write `dim_policies`

# COMMAND ----------

pol_schema = StructType([
    StructField("policy_id",              StringType(), False),
    StructField("policy_number",          StringType()),
    StructField("company_id",             StringType()),
    StructField("originating_quote_id",   StringType()),
    StructField("product_type",           StringType()),
])

df_pol = (spark.createDataFrame(policies_rows, schema=pol_schema)
          .withColumn("_created_at", F.current_timestamp()))

pol_table = f"{fqn}.dim_policies"
df_pol.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(pol_table)
print(f"✓ {pol_table} — {df_pol.count():,} policies")

spark.sql(f"""
    ALTER TABLE {pol_table} SET TBLPROPERTIES (
        'comment' = 'One row per policy entity. Traceable back to the originating quote via originating_quote_id. company_id may be NULL if the policy was bound for a first-time prospect not yet in dim_companies.'
    )
""")
try:
    spark.sql(f"ALTER TABLE {pol_table} ALTER COLUMN policy_id SET NOT NULL")
    spark.sql(f"ALTER TABLE {pol_table} ADD CONSTRAINT dim_policies_pk PRIMARY KEY (policy_id)")
except Exception as e:
    if "already" not in str(e).lower(): print(f"⚠ {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write `dim_policy_versions`

# COMMAND ----------

ver_schema = StructType([
    StructField("policy_id",         StringType(), False),
    StructField("policy_version",    IntegerType(), False),
    StructField("effective_from",    DateType()),
    StructField("effective_to",      DateType()),
    StructField("status",            StringType()),
    StructField("product_type",      StringType()),
    StructField("sum_insured",       LongType()),
    StructField("gross_premium",     DoubleType()),
    StructField("construction_type", StringType()),
    StructField("year_built",        IntegerType()),
    StructField("postcode_sector",   StringType()),
    StructField("sic_code",          StringType()),
    StructField("channel",           StringType()),
    StructField("model_version_used",StringType()),
])

df_ver = (spark.createDataFrame(versions_rows, schema=ver_schema)
          .withColumn("_created_at", F.current_timestamp()))

ver_table = f"{fqn}.dim_policy_versions"
df_ver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ver_table)
print(f"✓ {ver_table} — {df_ver.count():,} policy-versions")

spark.sql(f"""
    ALTER TABLE {ver_table} SET TBLPROPERTIES (
        'comment' = 'One row per policy version — initial inception, mid-term amendments, and renewals. Composite key (policy_id, policy_version). effective_from and effective_to bracket the exposure period for this version. Claims reference (policy_id, policy_version) and have a loss_date constrained to the versions effective period.'
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Backfill `fact_quotes.converted_to_policy_id`

# COMMAND ----------

# Build a Spark DataFrame of (quote_id, policy_id) pairs + MERGE into fact_quotes
mapping_rows = [(q, p) for q, p in quote_to_policy.items()]
map_df = spark.createDataFrame(mapping_rows, StructType([
    StructField("quote_id",  StringType(), False),
    StructField("policy_id", StringType(), False),
]))
map_df.createOrReplaceTempView("_quote_to_policy_map")

spark.sql(f"""
    MERGE INTO {fqn}.fact_quotes AS tgt
    USING _quote_to_policy_map AS src
    ON tgt.quote_id = src.quote_id
    WHEN MATCHED THEN UPDATE SET tgt.converted_to_policy_id = src.policy_id
""")
print(f"✓ fact_quotes.converted_to_policy_id backfilled for {len(mapping_rows):,} bound quotes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT status, COUNT(*) AS n,
           ROUND(AVG(sum_insured), 0)   AS avg_si,
           ROUND(AVG(gross_premium), 0) AS avg_gp
    FROM {fqn}.dim_policy_versions
    GROUP BY status ORDER BY n DESC
"""))
