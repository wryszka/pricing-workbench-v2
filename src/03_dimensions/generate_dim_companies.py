# Databricks notebook source
# MAGIC %md
# MAGIC # Generate `dim_companies`
# MAGIC
# MAGIC One row per business we've ever insured (or may insure). The natural key
# MAGIC is `company_registration_number` — this is how the bureau feed identifies
# MAGIC companies. Policies reference this dimension via `company_id`.
# MAGIC
# MAGIC A company may:
# MAGIC - Have many quotes over time (some convert to policies)
# MAGIC - Hold multiple policies simultaneously (different coverages)
# MAGIC - Appear on the bureau feed (`silver_company_bureau`) regardless of our relationship
# MAGIC
# MAGIC Counts:
# MAGIC - Bureau universe: ~25K companies (seeded by `generate_reference_data`)
# MAGIC - `dim_companies` universe: ~20K (the subset we've actually quoted/insured)

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")
dbutils.widgets.text("scale_factor", "1")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
SCALE   = int(dbutils.widgets.get("scale_factor"))
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

import random
from datetime import date, timedelta
from pyspark.sql.types import *
import pyspark.sql.functions as F

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull the bureau universe
# MAGIC
# MAGIC Our `dim_companies` is a subset of the full bureau universe — only the
# MAGIC companies we have commercial relationships with. The join key is
# MAGIC `company_registration_number`.

# COMMAND ----------

bureau = spark.table(f"{fqn}.silver_company_bureau").select(
    "company_registration_number", "years_trading", "employee_count_banded"
).toPandas()
print(f"bureau universe: {len(bureau):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference pools — postcodes, SICs, company-name fragments

# COMMAND ----------

# Pull SICs from the silver directory so the demo scales consistently
sic_rows = spark.table(f"{fqn}.silver_sic_directory").select(
    "sic_code", "division", "description", "internal_risk_tier"
).toPandas()
SIC_CODES = sic_rows["sic_code"].tolist()

# Postcode universe — reuse the same outcodes used in the reference-data generator
POSTCODE_AREAS = {
    "London":     (["EC1", "EC2", "EC3", "EC4", "WC1", "WC2", "W1", "SW1", "SE1", "E1", "N1", "NW1"], 0.35),
    "South East": (["RG1", "SL1", "GU1", "OX1", "MK1", "CT1", "ME1", "BN1", "PO1"],                  0.18),
    "North West": (["M1",  "M2",  "M3",  "L1",  "L2",  "L3",  "PR1", "WN1", "OL1"],                  0.12),
    "Midlands":   (["B1",  "B2",  "B3",  "CV1", "LE1", "NG1", "NN1", "WV1"],                         0.12),
    "Yorkshire":  (["LS1", "LS2", "S1",  "S2",  "HD1", "BD1", "YO1", "HU1"],                         0.08),
    "South West": (["BS1", "BS2", "EX1", "PL1", "TR1", "BA1"],                                       0.06),
    "North East": (["NE1", "NE2", "SR1", "TS1", "DH1"],                                              0.04),
    "Scotland":   (["EH1", "EH2", "G1",  "G2",  "AB1", "DD1", "KY1"],                                0.03),
    "Wales":      (["CF1", "CF2", "SA1", "NP1", "LL1"],                                              0.02),
}
POSTCODE_REGION = {pc: region for region, (pcs, _) in POSTCODE_AREAS.items() for pc in pcs}
POSTCODES = list(POSTCODE_REGION.keys())
# Weight each outcode by its region's share (divided across the outcodes in that region)
POSTCODE_WEIGHTS = []
for pc in POSTCODES:
    region = POSTCODE_REGION[pc]
    _, region_w = POSTCODE_AREAS[region]
    POSTCODE_WEIGHTS.append(region_w / len(POSTCODE_AREAS[region][0]))

COMPANY_PREFIXES = [
    "Ashford", "Bright", "Castle", "Delta", "Eastern", "Foundry", "Grange", "Harbour",
    "Iron", "Junction", "Kingfield", "Lakeside", "Meridian", "Northstar", "Oakwell",
    "Pinewood", "Quarry", "Redcliffe", "Summit", "Thornfield", "Uplands", "Vanguard",
    "Westbridge", "Yardley", "Axis", "Bristol", "Coastal", "Dartmoor", "Evergreen",
    "Forge", "Granary", "Hillside", "Ironbridge",
]
COMPANY_SUFFIXES = [
    "Manufacturing Ltd", "Services Ltd", "Trading Ltd", "Holdings Ltd", "Group plc",
    "Logistics Ltd", "Retail Ltd", "Construction Ltd", "Engineering Ltd", "Hospitality Ltd",
    "Foods Ltd", "Metals Ltd", "Technologies Ltd", "Consultancy Ltd", "Partners LLP",
]
STREET_TYPES = ["Street", "Road", "Lane", "Avenue", "Way"]
STREET_NAMES = ["High", "Church", "Mill", "Park", "Station", "Industrial", "Business",
                "Trade", "Commerce", "Market"]

def random_address():
    return (
        f"{random.randint(1, 250)} "
        f"{random.choice(['Industrial', 'Business', 'Trade', 'Commerce', 'Market'])} "
        f"{random.choice(['Park', 'Estate', 'Way', 'Lane'])}"
    )

def full_postcode(outcode: str) -> str:
    return (
        f"{outcode} "
        f"{random.randint(1, 9)}"
        f"{random.choice('ABDEFGHJLNPQRSTUWXYZ')}"
        f"{random.choice('ABDEFGHJLNPQRSTUWXYZ')}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate `dim_companies`

# COMMAND ----------

# Sample ~80% of the bureau universe as our book
N_COMPANIES = min(20_000 * SCALE, len(bureau))
picks = bureau.sample(n=N_COMPANIES, random_state=42).reset_index(drop=True)

companies_rows = []
for i, b_row in picks.iterrows():
    company_id  = f"COMP-{100000 + i}"
    company_reg = b_row["company_registration_number"]
    company_name = f"{random.choice(COMPANY_PREFIXES)} {random.choice(COMPANY_SUFFIXES)}"
    outcode     = random.choices(POSTCODES, weights=POSTCODE_WEIGHTS, k=1)[0]
    postcode    = full_postcode(outcode)
    region      = POSTCODE_REGION[outcode]
    sic_code    = random.choice(SIC_CODES)
    # Annual turnover roughly correlates with employee count (from bureau).
    # Null-safe: bureau may have null/NaN for employee_count_banded.
    emp_raw = b_row["employee_count_banded"]
    emp = 10 if emp_raw is None or (isinstance(emp_raw, float) and emp_raw != emp_raw) else int(emp_raw)
    turnover_base = emp * random.uniform(40_000, 250_000)
    annual_turnover = int(round(turnover_base * random.uniform(0.5, 2.5)))
    # Incorporation date ~ today - years_trading (if known)
    yrs_raw = b_row["years_trading"]
    if yrs_raw is None or (isinstance(yrs_raw, float) and yrs_raw != yrs_raw) or yrs_raw < 0:
        yrs = random.randint(1, 40)
    else:
        yrs = int(yrs_raw)
    inc_date = date(2025, 1, 1) - timedelta(days=int(yrs * 365 + random.randint(0, 180)))
    companies_rows.append((
        company_id, company_reg, company_name, sic_code, inc_date,
        random_address(), postcode, outcode, region, annual_turnover,
    ))

schema_ = StructType([
    StructField("company_id",                   StringType(), False),
    StructField("company_registration_number",  StringType(), False),
    StructField("company_name",                 StringType()),
    StructField("primary_sic_code",             StringType()),
    StructField("incorporation_date",           DateType()),
    StructField("address_line_1",               StringType()),
    StructField("postcode",                     StringType()),
    StructField("postcode_sector",              StringType()),
    StructField("region",                       StringType()),
    StructField("annual_turnover",              LongType()),
])
df_companies = (
    spark.createDataFrame(companies_rows, schema=schema_)
    .withColumn("_created_at", F.current_timestamp())
)

table_name = f"{fqn}.dim_companies"
df_companies.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✓ {table_name} — {df_companies.count():,} companies")

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {table_name} SET TBLPROPERTIES (
        'comment' = 'One row per commercial entity Bricksurance SE has ever quoted or insured. Natural key: company_registration_number (joins to silver_company_bureau). Surrogate key: company_id. Policies reference dim_companies.company_id.'
    )
""")
try:
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN company_id SET NOT NULL")
    spark.sql(f"ALTER TABLE {table_name} ADD CONSTRAINT dim_companies_pk PRIMARY KEY (company_id)")
    print("✓ PK set")
except Exception as e:
    if "CONSTRAINT_ALREADY_EXISTS" in str(e) or "already" in str(e).lower():
        print("✓ PK already exists")
    else:
        print(f"⚠ {e}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT region, COUNT(*) AS n, ROUND(AVG(annual_turnover), 0) AS avg_turnover
    FROM {table_name}
    GROUP BY region ORDER BY n DESC
"""))
