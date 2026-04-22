# Databricks notebook source
# MAGIC %md
# MAGIC # Generate `fact_quotes` — historical quote stream (2020-2024)
# MAGIC
# MAGIC One row per quote *event*. Over the 5-year history:
# MAGIC - New prospects and existing companies both request quotes
# MAGIC - Each quote has rating factors frozen at `quote_date`
# MAGIC - Some convert to bound policies, most don't
# MAGIC - When a quote converts, `converted_to_policy_id` is populated — the link
# MAGIC   back from the policy world
# MAGIC
# MAGIC This is the **training dataset for the demand model** — converted Y/N is the label.
# MAGIC
# MAGIC Bindings (quote → policy) are **created in the next notebook** (`generate_policies.py`).
# MAGIC At the end of this notebook, `converted_to_policy_id` is NULL everywhere; we
# MAGIC populate it in the binding step once policies exist.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")
dbutils.widgets.text("scale_factor", "1")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
SCALE   = int(dbutils.widgets.get("scale_factor"))
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

import json
import random
import uuid
from datetime import date, datetime, timedelta, timezone

from pyspark.sql.types import *
import pyspark.sql.functions as F

random.seed(42)

N_QUOTES = 250_000 * SCALE  # ~50k quotes/year over 5 years
START_DATE = date(2020, 1, 1)
END_DATE   = date(2024, 12, 31)
OUTLIER_IDS = ["QTE-BAKERY-48M-2024Q4", "QTE-OUTLIER-RETAIL-2023"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the universe — companies + reference

# COMMAND ----------

companies_df = spark.table(f"{fqn}.dim_companies").toPandas()
print(f"dim_companies: {len(companies_df):,}")

sic_df = spark.table(f"{fqn}.silver_sic_directory").select("sic_code", "internal_risk_tier").toPandas()
SIC_TIER = dict(zip(sic_df["sic_code"], sic_df["internal_risk_tier"]))

# Mapping of postcode outcode → region (same universe used in reference data)
POSTCODE_AREAS = {
    "London":     (["EC1","EC2","EC3","EC4","WC1","WC2","W1","SW1","SE1","E1","N1","NW1"], 0.35),
    "South East": (["RG1","SL1","GU1","OX1","MK1","CT1","ME1","BN1","PO1"],                 0.18),
    "North West": (["M1","M2","M3","L1","L2","L3","PR1","WN1","OL1"],                       0.12),
    "Midlands":   (["B1","B2","B3","CV1","LE1","NG1","NN1","WV1"],                          0.12),
    "Yorkshire":  (["LS1","LS2","S1","S2","HD1","BD1","YO1","HU1"],                         0.08),
    "South West": (["BS1","BS2","EX1","PL1","TR1","BA1"],                                   0.06),
    "North East": (["NE1","NE2","SR1","TS1","DH1"],                                         0.04),
    "Scotland":   (["EH1","EH2","G1","G2","AB1","DD1","KY1"],                               0.03),
    "Wales":      (["CF1","CF2","SA1","NP1","LL1"],                                         0.02),
}
REGIONS = list(POSTCODE_AREAS.keys())
POSTCODE_REGION = {pc: r for r, (pcs, _) in POSTCODE_AREAS.items() for pc in pcs}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the quote stream

# COMMAND ----------

CHANNELS = ["Direct", "Broker", "Aggregator", "Renewal"]
CHANNEL_WEIGHTS = [0.25, 0.45, 0.20, 0.10]  # Renewal is smaller in the historical stream — most "renewals" are modelled as separate quotes

CONSTRUCTION_TYPES = ["Fire Resistive", "Non-Combustible", "Joisted Masonry", "Frame", "Heavy Timber"]
ROOF_TYPES         = ["Metal Deck", "Concrete", "Tiled", "Flat Membrane", "Slated"]
FLOOD_ZONES        = ["Low", "Medium", "High"]
MODEL_VERSIONS_BY_YEAR = {
    2020: "pricing_v4.0",
    2021: "pricing_v5.0",
    2022: "pricing_v6.0",
    2023: "pricing_v7.0_glm",
    2024: "pricing_v7.1_glm",
}
SALES_USERS = ["sales.agent.01", "sales.agent.02", "sales.agent.03",
               "self_service.portal", "broker.portal.uk"]

# Tune conversion rates to roughly match UK commercial reality:
#   base_conversion ~ 0.28 — market-wide new-business conversion for commercial SME
# Adjusted by channel, tier, price-competitiveness
BASE_CONVERSION = 0.28
CHANNEL_CONV_MULT = {"Direct": 1.05, "Broker": 1.0, "Aggregator": 0.85, "Renewal": 2.2}
TIER_CONV_MULT    = {"Low": 1.15, "Medium": 1.0, "High": 0.82}

def pick_outcode():
    # Weighted by region share
    pcs = []
    weights = []
    for region, (outcodes, w) in POSTCODE_AREAS.items():
        for pc in outcodes:
            pcs.append(pc)
            weights.append(w / len(outcodes))
    return random.choices(pcs, weights=weights, k=1)[0]

def full_postcode(outcode):
    return f"{outcode} {random.randint(1,9)}{random.choice('ABDEFGHJLNPQRSTUWXYZ')}{random.choice('ABDEFGHJLNPQRSTUWXYZ')}"

def days_between(a, b):
    return (b - a).days

total_days = days_between(START_DATE, END_DATE)

# COMMAND ----------

quote_rows = []
# ~70% of quotes come from companies already in dim_companies (existing relationships
# or previously-quoted prospects). ~30% are new prospects with company_id = NULL.
for i in range(N_QUOTES):
    # Outlier handling — seeded quotes at a specific date
    if i < len(OUTLIER_IDS):
        quote_id = OUTLIER_IDS[i]
        q_date = date(2024, 6 + i, 15)
        force_outlier = True
    else:
        quote_id = f"QTE-{uuid.uuid4().hex[:10].upper()}"
        # Quote volume grows ~10% y/y — weight later years slightly higher
        year_weight = [(2020, 1.0), (2021, 1.1), (2022, 1.21), (2023, 1.33), (2024, 1.46)]
        year = random.choices([y for y, _ in year_weight], weights=[w for _, w in year_weight])[0]
        doy = random.randint(0, 364)
        q_date = date(year, 1, 1) + timedelta(days=doy)
        if q_date > END_DATE:
            q_date = END_DATE
        force_outlier = False

    # Existing company vs new prospect
    is_existing = random.random() < 0.70 and len(companies_df) > 0
    if is_existing:
        c = companies_df.sample(n=1, random_state=random.randint(0, 10**9)).iloc[0]
        company_id = c["company_id"]
        sic_code   = c["primary_sic_code"]
        outcode    = c["postcode_sector"]
        postcode   = c["postcode"]
        region     = c["region"]
    else:
        company_id = None
        sic_code   = random.choice(sic_df["sic_code"].tolist())
        outcode    = pick_outcode()
        postcode   = full_postcode(outcode)
        region     = POSTCODE_REGION[outcode]

    tier = SIC_TIER.get(sic_code, "Medium")

    # Rating factors declared on the quote
    construction = random.choice(CONSTRUCTION_TYPES)
    year_built   = random.randint(1905, q_date.year)
    roof_type    = random.choice(ROOF_TYPES)
    floor_area   = random.randint(80, 18_000)
    flood_zone   = random.choices(FLOOD_ZONES, weights=[0.72, 0.20, 0.08])[0]
    sprinklered  = random.random() < 0.30
    alarmed      = random.random() < 0.75

    # Coverage requested (scales loosely with turnover if existing company)
    if is_existing:
        turnover = c["annual_turnover"]
        building_multiplier = random.uniform(0.5, 2.0)
        buildings_si  = int(max(250_000, min(40_000_000, turnover * building_multiplier)))
    else:
        buildings_si = random.choice([500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000, 25_000_000])
    contents_si  = int(buildings_si * random.uniform(0.05, 0.20))
    liability_si = random.choice([1_000_000, 2_000_000, 5_000_000, 10_000_000])
    voluntary_excess = random.choice([1_000, 2_500, 5_000, 10_000])

    # Price the quote — simple formula broadly calibrated to market
    tier_mult = {"High": 1.35, "Medium": 1.0, "Low": 0.82}[tier]
    region_mult = {
        "London": 1.25, "South East": 1.10, "South West": 0.95, "Midlands": 0.95,
        "North West": 0.92, "North East": 0.88, "Yorkshire": 0.90, "Scotland": 0.88, "Wales": 0.85,
    }[region]
    base_rate_per_1k = 6.0 * tier_mult * region_mult
    buildings_pp = buildings_si / 1000 * base_rate_per_1k * 0.80
    contents_pp  = contents_si  / 1000 * base_rate_per_1k * 1.20
    liability_pp = liability_si / 1000 * 0.55
    net_premium = buildings_pp + contents_pp + liability_pp
    # Loadings
    if flood_zone == "High":        net_premium *= 1.25
    elif flood_zone == "Medium":    net_premium *= 1.08
    if construction in ("Frame", "Heavy Timber"): net_premium *= 1.08
    if year_built < 1930:                          net_premium *= 1.06
    if not sprinklered:                            net_premium *= 1.04
    if force_outlier:
        net_premium = 48_212_560.0  # the £48M bakery
    ipt_pct = 0.12
    gross_premium = round(net_premium * (1 + ipt_pct), 2)

    # Channel + user + model version
    channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]
    agent   = random.choice(SALES_USERS)
    model_version = MODEL_VERSIONS_BY_YEAR.get(q_date.year, "pricing_v4.0")

    # Convert?
    p_conv = (
        BASE_CONVERSION
        * CHANNEL_CONV_MULT[channel]
        * TIER_CONV_MULT[tier]
        * (0.4 if (gross_premium or 0) > 200_000 else 1.0)  # huge quotes convert less
        * (0.2 if force_outlier else 1.0)                   # outlier never converts
        * (1.15 if is_existing else 1.0)                    # existing relationships stickier
    )
    converted = "Y" if random.random() < p_conv else "N"

    competitor_quoted = "Y" if random.random() < 0.35 else "N"
    days_to_decision = random.choices([1, 2, 3, 5, 7, 10, 14, 21, 30], weights=[0.2,0.2,0.15,0.1,0.1,0.1,0.08,0.04,0.03])[0]

    rating_factors = {
        "sic_code": sic_code, "postcode_sector": outcode, "region": region,
        "construction_type": construction, "year_built": year_built,
        "roof_type": roof_type, "floor_area_sqm": floor_area,
        "flood_zone": flood_zone, "sprinklered": sprinklered, "alarmed": alarmed,
        "buildings_si": buildings_si, "contents_si": contents_si,
        "liability_si": liability_si, "voluntary_excess": voluntary_excess,
        "channel": channel, "ipt_pct": ipt_pct,
    }

    quote_rows.append((
        quote_id,
        q_date,
        days_to_decision,
        company_id,
        sic_code, outcode, postcode, region,
        construction, year_built, roof_type, floor_area, flood_zone,
        sprinklered, alarmed,
        int(buildings_si), int(contents_si), int(liability_si), int(voluntary_excess),
        round(net_premium, 2),
        gross_premium,
        channel,
        agent,
        model_version,
        converted,
        competitor_quoted,
        force_outlier,
        None,  # converted_to_policy_id — populated later by generate_policies.py
        json.dumps(rating_factors),
    ))

# COMMAND ----------

schema_ = StructType([
    StructField("quote_id",                StringType(), False),
    StructField("quote_date",              DateType(), False),
    StructField("days_to_decision",        IntegerType()),
    StructField("company_id",              StringType()),
    StructField("sic_code",                StringType()),
    StructField("postcode_sector",         StringType()),
    StructField("postcode",                StringType()),
    StructField("region",                  StringType()),
    StructField("construction_type",       StringType()),
    StructField("year_built",              IntegerType()),
    StructField("roof_type",               StringType()),
    StructField("floor_area_sqm",          IntegerType()),
    StructField("flood_zone",              StringType()),
    StructField("sprinklered",             BooleanType()),
    StructField("alarmed",                 BooleanType()),
    StructField("buildings_si",            LongType()),
    StructField("contents_si",             LongType()),
    StructField("liability_si",            LongType()),
    StructField("voluntary_excess",        IntegerType()),
    StructField("net_premium_quoted",      DoubleType()),
    StructField("gross_premium_quoted",    DoubleType()),
    StructField("channel",                 StringType()),
    StructField("agent_user",              StringType()),
    StructField("model_version_used",      StringType()),
    StructField("converted",               StringType()),
    StructField("competitor_quoted",       StringType()),
    StructField("is_outlier",              BooleanType()),
    StructField("converted_to_policy_id",  StringType()),
    StructField("rating_factors_json",     StringType()),
])

df_q = spark.createDataFrame(quote_rows, schema=schema_)
table = f"{fqn}.fact_quotes"
df_q.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
print(f"✓ {table} — {df_q.count():,} quotes")

spark.sql(f"""
    ALTER TABLE {table} SET TBLPROPERTIES (
        'comment' = 'Historical quote stream 2020-2024. One row per quote event. Rating factors captured at quote_date. converted_to_policy_id links to dim_policies once the quote was bound (populated by generate_policies.py).'
    )
""")

# COMMAND ----------

display(spark.sql(f"""
    SELECT YEAR(quote_date) AS year, channel,
           COUNT(*) AS n,
           ROUND(AVG(gross_premium_quoted), 0) AS avg_premium,
           ROUND(SUM(CASE WHEN converted = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS conv_pct
    FROM {table}
    GROUP BY YEAR(quote_date), channel
    ORDER BY year, channel
"""))
