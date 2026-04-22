# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Reference Data CSVs
# MAGIC
# MAGIC Produces the four external-vendor CSVs that land in the `external_landing`
# MAGIC volume and get picked up by the bronze ingestion notebooks. All four are
# MAGIC synthetic — real vendor feeds in production would replace these.
# MAGIC
# MAGIC 1. **`market_benchmark`** — market median rate per SIC × region × quarter
# MAGIC    (from "PCW aggregator")
# MAGIC 2. **`geo_hazard`** — per-postcode-sector location risk scores
# MAGIC    (from "Ordnance Survey / Environment Agency" combined feed)
# MAGIC 3. **`company_bureau`** — per-company credit + financial health
# MAGIC    **keyed on `company_registration_number`**, not policy_id (the realistic shape)
# MAGIC 4. **`sic_directory`** — full UK SIC 2007 code list with industry descriptions
# MAGIC    and internal risk tier. ~700 codes.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")
dbutils.widgets.text("volume_name",  "external_landing")
dbutils.widgets.text("scale_factor", "1")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
volume  = dbutils.widgets.get("volume_name")
SCALE   = int(dbutils.widgets.get("scale_factor"))

fqn = f"{catalog}.{schema}"
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

# COMMAND ----------

import random
from datetime import date
from pyspark.sql.types import *

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. SIC Directory (reference — 700+ UK SIC 2007 codes)
# MAGIC
# MAGIC Real SIC 2007 is ~700 codes. We use a representative subset of ~80 that
# MAGIC covers the commercial lines we write. Each has an internal risk tier
# MAGIC (Low/Medium/High) assigned by the pricing team.

# COMMAND ----------

SIC_DIRECTORY = [
    # (sic_code, division, description, internal_risk_tier)
    # Manufacturing
    ("1011", "Manufacturing",  "Food processing — meat",                         "Medium"),
    ("1041", "Manufacturing",  "Food processing — oils and fats",                "High"),
    ("1071", "Manufacturing",  "Bakery products",                                "High"),
    ("1102", "Manufacturing",  "Wine production",                                "Medium"),
    ("1107", "Manufacturing",  "Soft drinks and mineral waters",                 "Low"),
    ("1310", "Manufacturing",  "Textile preparation and spinning",               "High"),
    ("1411", "Manufacturing",  "Clothing — leather",                             "Medium"),
    ("1512", "Manufacturing",  "Footwear manufacture",                           "Medium"),
    ("1610", "Manufacturing",  "Wood sawmilling and planing",                    "High"),
    ("1711", "Manufacturing",  "Pulp manufacture",                               "High"),
    ("1812", "Manufacturing",  "Printing (non-newspaper)",                       "Medium"),
    ("2011", "Manufacturing",  "Industrial gases",                               "High"),
    ("2030", "Manufacturing",  "Paints and varnishes",                           "High"),
    ("2120", "Manufacturing",  "Pharmaceutical preparations",                    "Medium"),
    ("2211", "Manufacturing",  "Rubber tyres and tubes",                         "Medium"),
    ("2362", "Manufacturing",  "Plaster products for construction",              "Low"),
    ("2410", "Manufacturing",  "Manufacture of basic iron and steel",            "High"),
    ("2511", "Manufacturing",  "Structural metal products",                      "Medium"),
    ("2562", "Manufacturing",  "Machining",                                      "Medium"),
    ("2611", "Manufacturing",  "Electronic components",                          "Low"),
    ("2711", "Manufacturing",  "Electric motors, generators, transformers",      "Medium"),
    ("2812", "Manufacturing",  "Fluid power equipment",                          "Medium"),
    ("2910", "Manufacturing",  "Motor vehicles",                                 "Medium"),
    ("3092", "Manufacturing",  "Bicycles and invalid carriages",                 "Low"),
    ("3230", "Manufacturing",  "Sports goods",                                   "Low"),
    # Construction
    ("4110", "Construction",   "Development of building projects",               "Medium"),
    ("4120", "Construction",   "Construction of residential buildings",          "Medium"),
    ("4211", "Construction",   "Construction of roads and motorways",            "High"),
    ("4221", "Construction",   "Construction of utility projects for fluids",    "High"),
    ("4311", "Construction",   "Demolition",                                     "High"),
    ("4312", "Construction",   "Site preparation",                               "Medium"),
    ("4321", "Construction",   "Electrical installation",                        "Medium"),
    ("4322", "Construction",   "Plumbing, heat + AC",                            "Medium"),
    ("4333", "Construction",   "Floor and wall covering",                        "Low"),
    # Retail + wholesale
    ("4520", "Retail/Wholesale","Vehicle maintenance",                           "Medium"),
    ("4711", "Retail/Wholesale","Retail — non-specialised",                      "Low"),
    ("4719", "Retail/Wholesale","Retail — other non-specialised",                "Low"),
    ("4721", "Retail/Wholesale","Retail — fruit and vegetables",                 "Low"),
    ("4729", "Retail/Wholesale","Retail — other food",                           "Low"),
    ("4741", "Retail/Wholesale","Retail — computers in specialised stores",      "Low"),
    ("4751", "Retail/Wholesale","Retail — textiles in specialised stores",       "Low"),
    ("4771", "Retail/Wholesale","Retail — clothing",                             "Low"),
    ("4779", "Retail/Wholesale","Retail — second-hand",                          "Low"),
    ("4791", "Retail/Wholesale","Retail — mail order / internet",                "Low"),
    ("4920", "Retail/Wholesale","Freight rail transport",                        "Medium"),
    # Hospitality + food service
    ("5510", "Hospitality",     "Hotels and similar",                            "Medium"),
    ("5520", "Hospitality",     "Holiday and short-stay",                        "Medium"),
    ("5590", "Hospitality",     "Other accommodation",                           "Medium"),
    ("5610", "Hospitality",     "Restaurants and mobile food service",           "Medium"),
    ("5621", "Hospitality",     "Event catering",                                "Medium"),
    ("5629", "Hospitality",     "Other food service",                            "Medium"),
    ("5630", "Hospitality",     "Beverage serving activities",                   "Medium"),
    # Info + communication
    ("5811", "Info/Communication","Book publishing",                             "Low"),
    ("5821", "Info/Communication","Publishing of computer games",                "Low"),
    ("5829", "Info/Communication","Other software publishing",                   "Low"),
    ("6110", "Info/Communication","Wired telecommunications",                    "Low"),
    ("6120", "Info/Communication","Wireless telecommunications",                 "Low"),
    ("6201", "Info/Communication","Computer programming",                        "Low"),
    ("6202", "Info/Communication","Computer consultancy",                        "Low"),
    ("6209", "Info/Communication","Other IT and computer services",              "Low"),
    ("6311", "Info/Communication","Data processing + hosting",                   "Low"),
    # Financial + real estate
    ("6419", "Financial",       "Other monetary intermediation",                 "Low"),
    ("6499", "Financial",       "Other financial service activities",            "Low"),
    ("6820", "Real Estate",     "Renting and operating real estate",             "Low"),
    # Professional services
    ("6910", "Professional",    "Legal activities",                              "Low"),
    ("6920", "Professional",    "Accounting, bookkeeping, auditing",             "Low"),
    ("7010", "Professional",    "Activities of head offices",                    "Low"),
    ("7022", "Professional",    "Management consulting",                         "Low"),
    ("7111", "Professional",    "Architectural activities",                      "Low"),
    ("7112", "Professional",    "Engineering activities",                        "Medium"),
    ("7120", "Professional",    "Technical testing and analysis",                "Medium"),
    ("7490", "Professional",    "Other professional, scientific and technical",  "Low"),
    # Admin + support
    ("8010", "Admin/Support",   "Private security activities",                   "High"),
    ("8020", "Admin/Support",   "Security systems service activities",           "Medium"),
    ("8110", "Admin/Support",   "Combined facilities support",                   "Medium"),
    ("8121", "Admin/Support",   "General cleaning of buildings",                 "Medium"),
    ("8129", "Admin/Support",   "Other building and industrial cleaning",        "Medium"),
    ("8211", "Admin/Support",   "Combined office administrative",                "Low"),
    ("8610", "Health",          "Hospital activities",                           "Medium"),
    ("8622", "Health",          "Specialist medical practice",                   "Medium"),
    ("8810", "Health",          "Social work without accommodation",             "Medium"),
    # Arts, sports, recreation
    ("9311", "Arts/Recreation", "Sports facilities",                             "Medium"),
    ("9312", "Arts/Recreation", "Activities of sport clubs",                     "Medium"),
    ("9321", "Arts/Recreation", "Activities of amusement parks",                 "High"),
    ("9329", "Arts/Recreation", "Other amusement and recreation",                "Medium"),
    ("9604", "Other Services",  "Physical well-being activities",                "Low"),
    ("9609", "Other Services",  "Other personal service activities",             "Low"),
]

sic_schema = StructType([
    StructField("sic_code",            StringType()),
    StructField("division",            StringType()),
    StructField("description",         StringType()),
    StructField("internal_risk_tier",  StringType()),
])
df_sic = spark.createDataFrame(SIC_DIRECTORY, schema=sic_schema)
df_sic.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{volume_path}/sic_directory")
print(f"✓ sic_directory — {len(SIC_DIRECTORY)} SIC codes")

# Share SIC_CODES + tier dict with downstream cells
SIC_CODES = [row[0] for row in SIC_DIRECTORY]
SIC_TIER  = {row[0]: row[3] for row in SIC_DIRECTORY}
SIC_DESC  = {row[0]: row[2] for row in SIC_DIRECTORY}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Postcode reference mapping (postcodes the book uses)
# MAGIC
# MAGIC Shared across all downstream generators. These are UK-style postcode sectors
# MAGIC concentrated in real outcode prefixes (so the later join to
# MAGIC `silver_postcode_enrichment` hits real data).

# COMMAND ----------

# Real UK postcode areas with weights proportional to commercial insurance density.
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
REGIONS = list(POSTCODE_AREAS.keys())
POSTCODE_REGION   = {}
REGION_WEIGHTS    = {}
for region, (pcs, w) in POSTCODE_AREAS.items():
    for pc in pcs:
        POSTCODE_REGION[pc] = region
    REGION_WEIGHTS[region] = w
POSTCODES = list(POSTCODE_REGION.keys())
POSTCODE_WEIGHTS = [REGION_WEIGHTS[POSTCODE_REGION[pc]] / len(POSTCODE_AREAS[POSTCODE_REGION[pc]][0])
                   for pc in POSTCODES]
print(f"Reference: {len(POSTCODES)} postcode outcodes across {len(REGIONS)} regions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Market Benchmark
# MAGIC Per SIC × region × quarter. Represents a synthetic PCW aggregate — what
# MAGIC the rest of the market charges for a given risk profile in a given area.
# MAGIC Historical quarters 2020Q1 to 2024Q4 so training rows can look up market
# MAGIC rates *as at* policy inception year.

# COMMAND ----------

YEARS     = list(range(2020, 2025))
QUARTERS  = [1, 2, 3, 4]

market_rows = []
for year in YEARS:
    for quarter in QUARTERS:
        # Industry-wide trend — a gentle upward pressure year-on-year
        trend_factor = 1.0 + (year - 2020) * 0.025 + (quarter - 1) * 0.004
        for sic in SIC_CODES:
            for region in REGIONS:
                tier_mult = {"High": 1.35, "Medium": 1.0, "Low": 0.82}[SIC_TIER[sic]]
                region_mult = {
                    "London": 1.25, "South East": 1.10, "South West": 0.95,
                    "Midlands": 0.95, "North West": 0.92, "North East": 0.88,
                    "Yorkshire": 0.90, "Scotland": 0.88, "Wales": 0.85,
                }[region]
                base = 6.0 * tier_mult * region_mult * trend_factor
                noise = random.gauss(0, 0.3)
                median_rate = round(max(1.5, base + noise), 3)
                competitor_a_min = round(median_rate * random.uniform(0.85, 0.97), 3)
                competitor_b_min = round(median_rate * random.uniform(0.88, 1.02), 3)
                price_index = round(100 * trend_factor + random.gauss(0, 1.5), 2)
                market_rows.append((
                    f"{sic}_{region}", sic, region, year, quarter,
                    median_rate, competitor_a_min, competitor_b_min, price_index,
                ))

market_schema = StructType([
    StructField("match_key_sic_region",  StringType()),
    StructField("sic_code",              StringType()),
    StructField("region",                StringType()),
    StructField("year",                  IntegerType()),
    StructField("quarter",               IntegerType()),
    StructField("market_median_rate",    DoubleType()),
    StructField("competitor_a_min_rate", DoubleType()),
    StructField("competitor_b_min_rate", DoubleType()),
    StructField("price_index",           DoubleType()),
])
df_market = spark.createDataFrame(market_rows, schema=market_schema)
df_market.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{volume_path}/market_benchmark")
print(f"✓ market_benchmark — {len(market_rows)} rows ({len(SIC_CODES)} SICs × {len(REGIONS)} regions × {len(YEARS)*4} quarters)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Geospatial Hazard
# MAGIC Per postcode outcode. Location-risk-scores provided by a synthetic Ordnance
# MAGIC Survey / Environment Agency combined feed. Static — geo risk moves slowly.

# COMMAND ----------

geo_rows = []
for pc in POSTCODES:
    region = POSTCODE_REGION[pc]
    flood_base = 3 if region in ("London", "South East") else (5 if region in ("Yorkshire", "North East", "Scotland") else 4)
    flood = max(1, min(10, flood_base + random.randint(-2, 3)))
    fire_dist = round(random.uniform(0.5, 25.0), 1)
    crime_base = 65 if region == "London" else (50 if region in ("North West", "Midlands") else 40)
    crime = round(max(10.0, min(95.0, crime_base + random.gauss(0, 15))), 1)
    sub_base = 6 if region in ("London", "South East") else 3
    subsidence = round(max(0.0, min(10.0, sub_base + random.gauss(0, 2))), 1)
    # Dirty data — regulators ask DQ questions
    if random.random() < 0.04: fire_dist = round(random.uniform(-5, -0.1), 1)
    if random.random() < 0.03: flood = random.randint(11, 15)
    if random.random() < 0.03: crime = None
    geo_rows.append((pc, flood, fire_dist, crime, subsidence))

geo_schema = StructType([
    StructField("postcode_sector",               StringType()),
    StructField("flood_zone_rating",             IntegerType()),
    StructField("proximity_to_fire_station_km",  DoubleType()),
    StructField("crime_theft_index",             DoubleType()),
    StructField("subsidence_risk",               DoubleType()),
])
df_geo = spark.createDataFrame(geo_rows, schema=geo_schema)
df_geo.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{volume_path}/geo_hazard")
print(f"✓ geo_hazard — {len(geo_rows)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Company Bureau — keyed on `company_registration_number`
# MAGIC
# MAGIC This is the big fix vs v1. In the real world, a bureau feed is keyed on
# MAGIC the UK company registration number. Our internal systems hold the
# MAGIC `company_id` → `company_registration_number` mapping (in `dim_companies`,
# MAGIC generated later). The bureau never sees policy_ids.

# COMMAND ----------

# We generate a stable universe of ~25K companies that our book may insure.
# Each gets a UK-style company registration number ("GB" + 8 digits).
N_COMPANIES = 25_000 * SCALE
bureau_rows = []
company_numbers = []
for i in range(N_COMPANIES):
    company_reg = f"GB{10_000_000 + i:08d}"
    company_numbers.append(company_reg)
    credit_score    = random.randint(200, 900)
    ccj_count       = random.choice([0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 5])
    years_trading   = random.randint(0, 80)
    director_changes = random.randint(0, 8)
    bankruptcy_flag = random.random() < 0.015
    filing_on_time  = random.random() < 0.93
    employee_count  = random.choice([1, 2, 5, 10, 25, 50, 100, 250, 500, 1000])
    # Dirty data
    if random.random() < 0.02: credit_score = random.randint(950, 1100)
    if random.random() < 0.03: years_trading = None
    if random.random() < 0.02: ccj_count = -1
    bureau_rows.append((
        company_reg, credit_score, ccj_count, years_trading,
        director_changes, bankruptcy_flag, filing_on_time, employee_count,
    ))

bureau_schema = StructType([
    StructField("company_registration_number", StringType()),
    StructField("credit_score",                IntegerType()),
    StructField("ccj_count",                   IntegerType()),
    StructField("years_trading",               IntegerType()),
    StructField("director_changes",            IntegerType()),
    StructField("bankruptcy_flag",             BooleanType()),
    StructField("filing_on_time",              BooleanType()),
    StructField("employee_count_banded",       IntegerType()),
])
df_bureau = spark.createDataFrame(bureau_rows, schema=bureau_schema)
df_bureau.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{volume_path}/company_bureau")
print(f"✓ company_bureau — {len(bureau_rows)} companies keyed on company_registration_number")

# COMMAND ----------

print(f"""
Reference data generation complete.

External CSVs in {volume_path}:
  sic_directory/        ({len(SIC_DIRECTORY)} codes)
  market_benchmark/     ({len(market_rows)} rows — SIC × region × year/quarter)
  geo_hazard/           ({len(geo_rows)} postcodes)
  company_bureau/       ({len(bureau_rows):,} companies keyed on company_registration_number)

Next: Phase 1c — run the 4 bronze ingestion notebooks then the DLT silver pipeline.
""")
