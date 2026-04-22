# Databricks notebook source
# MAGIC %md
# MAGIC # Generate `fact_claims`
# MAGIC
# MAGIC Claims are tied to a **specific (policy_id, policy_version)** and the `loss_date`
# MAGIC is constrained to fall inside the version's `[effective_from, effective_to]`
# MAGIC exposure period. This is the realistic shape — claims can't happen before
# MAGIC the policy attaches nor after it expires.
# MAGIC
# MAGIC Claim frequency is driven by:
# MAGIC - Policy-version-level risk proxies (SIC risk tier, flood zone via postcode,
# MAGIC   construction type) — so higher-risk policies accrue more claims
# MAGIC - Random draw per policy-year
# MAGIC
# MAGIC Severity follows a lognormal distribution with peril-specific parameters.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

import math
import random
import uuid
from datetime import date, timedelta
from pyspark.sql.types import *
import pyspark.sql.functions as F

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the policy universe

# COMMAND ----------

versions = (
    spark.table(f"{fqn}.dim_policy_versions")
    .select("policy_id", "policy_version", "effective_from", "effective_to", "sic_code", "postcode_sector", "construction_type", "sum_insured")
    .toPandas()
)
sic_tier = {r["sic_code"]: r["internal_risk_tier"]
            for r in spark.table(f"{fqn}.silver_sic_directory")
                          .select("sic_code", "internal_risk_tier").toPandas().to_dict("records")}

geo = spark.table(f"{fqn}.silver_geo_hazard").select("postcode_sector", "flood_zone_rating").toPandas()
postcode_flood = dict(zip(geo["postcode_sector"], geo["flood_zone_rating"]))

print(f"versions to process: {len(versions):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Base annual claim frequency — market-realistic for commercial property
BASE_FREQ = 0.22       # 22% of policy-years have at least one claim
FREQ_TIER = {"Low": 0.7, "Medium": 1.0, "High": 1.5}
FREQ_CONSTRUCTION = {
    "Fire Resistive": 0.85, "Non-Combustible": 0.9, "Joisted Masonry": 1.0,
    "Frame": 1.15, "Heavy Timber": 1.25,
}

# Peril distribution
PERILS = [
    # (peril, share_of_claims, sev_mu, sev_sigma)   — lognormal params for severity
    ("Fire",            0.18, 10.5, 1.4),
    ("Flood",           0.10,  9.8, 1.5),
    ("Theft",           0.22,  8.5, 1.0),
    ("Liability",       0.17, 10.0, 1.3),
    ("Storm",           0.12,  9.5, 1.2),
    ("Subsidence",      0.05, 10.2, 1.3),
    ("Escape of Water", 0.16,  9.2, 1.1),
]
PERIL_NAMES = [p[0] for p in PERILS]
PERIL_WEIGHTS = [p[1] for p in PERILS]
PERIL_PARAMS = {p[0]: (p[2], p[3]) for p in PERILS}

TODAY = date(2025, 1, 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate claims (one pass over policy-versions)

# COMMAND ----------

claim_rows = []
for _, v in versions.iterrows():
    ef_from = v["effective_from"]
    ef_to   = min(v["effective_to"], TODAY)
    if ef_to <= ef_from:
        continue
    days_exposure = (ef_to - ef_from).days

    # Expected annual frequency for this version
    tier = sic_tier.get(v["sic_code"], "Medium")
    flood_rating = postcode_flood.get(v["postcode_sector"], 4)
    flood_mult = 1.0 + max(0, flood_rating - 3) * 0.08

    annual_freq = (
        BASE_FREQ
        * FREQ_TIER[tier]
        * FREQ_CONSTRUCTION.get(v["construction_type"], 1.0)
        * flood_mult
    )
    # Scale by actual exposure length
    expected_claims = annual_freq * (days_exposure / 365.0)
    # Poisson-ish draw — use discrete sampling
    n_claims = 0
    while True:
        if random.random() < expected_claims:
            n_claims += 1
            expected_claims -= 0.7  # diminishing return for repeat claims
            if expected_claims <= 0: break
        else:
            break

    for _ in range(n_claims):
        # Loss date uniform within exposure period
        loss_offset = random.randint(0, max(1, days_exposure - 1))
        loss_date   = ef_from + timedelta(days=loss_offset)
        reported_date = loss_date + timedelta(days=random.randint(0, 60))

        # Peril — weighted by typical distribution but bias flood for high-flood postcodes
        weights = list(PERIL_WEIGHTS)
        if flood_rating >= 7:
            weights[1] *= 2.0  # Flood
        peril = random.choices(PERIL_NAMES, weights=weights, k=1)[0]
        mu, sigma = PERIL_PARAMS[peril]
        incurred = round(random.lognormvariate(mu, sigma), 2)
        # Cap at 75% of sum_insured — hard policy limit proxy
        cap = 0.75 * (v["sum_insured"] or 500_000)
        incurred = round(min(incurred, cap), 2)
        paid     = round(incurred * random.uniform(0.65, 1.0), 2) if random.random() > 0.2 else 0.0
        reserve  = round(max(0.0, incurred - paid), 2)
        status   = "Closed" if paid >= incurred * 0.95 and reserve < 100 else "Open"
        close_date = loss_date + timedelta(days=random.randint(30, 720)) if status == "Closed" else None

        claim_rows.append((
            f"CLM-{uuid.uuid4().hex[:10].upper()}",
            v["policy_id"],
            int(v["policy_version"]),
            loss_date,
            reported_date,
            peril,
            incurred,
            paid,
            reserve,
            status,
            close_date,
        ))

print(f"generated: {len(claim_rows):,} claims across {len(versions):,} policy-versions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write `fact_claims`

# COMMAND ----------

schema_ = StructType([
    StructField("claim_id",        StringType(), False),
    StructField("policy_id",       StringType(), False),
    StructField("policy_version",  IntegerType(), False),
    StructField("loss_date",       DateType()),
    StructField("reported_date",   DateType()),
    StructField("peril",           StringType()),
    StructField("incurred_amount", DoubleType()),
    StructField("paid_amount",     DoubleType()),
    StructField("reserve",         DoubleType()),
    StructField("status",          StringType()),
    StructField("close_date",      DateType()),
])
df = spark.createDataFrame(claim_rows, schema=schema_)

table = f"{fqn}.fact_claims"
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
print(f"✓ {table} — {df.count():,} claims")

spark.sql(f"""
    ALTER TABLE {table} SET TBLPROPERTIES (
        'comment' = 'One row per claim. Keyed by claim_id. FK (policy_id, policy_version) into dim_policy_versions. loss_date is constrained to fall within that policy version effective period — enabling clean point-in-time joins for training.'
    )
""")

# COMMAND ----------

display(spark.sql(f"""
    SELECT peril, COUNT(*) AS n,
           ROUND(AVG(incurred_amount), 0) AS avg_incurred,
           ROUND(SUM(incurred_amount), 0) AS total_incurred
    FROM {table}
    GROUP BY peril ORDER BY n DESC
"""))
