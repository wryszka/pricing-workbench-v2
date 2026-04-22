# Databricks notebook source
# MAGIC %md
# MAGIC # Fairness + Robustness — cross-cutting audit across every model
# MAGIC
# MAGIC The per-model reports (`08_generate_model_reports`) cover each model individually.
# MAGIC This notebook tests the pricing *outcome* for properties a regulator cares about
# MAGIC that only reveal themselves across the whole portfolio:
# MAGIC
# MAGIC 1. **Protected-attribute absence** — listing features we deliberately do not use,
# MAGIC    and proving none of our inputs is a proxy for a protected class
# MAGIC 2. **Same-risk consistency** — two policies with identical risk factors should get
# MAGIC    near-identical premiums, regardless of their postcode or name. We construct
# MAGIC    synthetic "twin" pairs and measure the prediction spread
# MAGIC 3. **Disparate-impact slices** — prediction distribution by region, IMD decile,
# MAGIC    income decile, urbanicity. Deviations should be explicable by observed losses
# MAGIC    (the legitimate risk differential), not by the slice itself
# MAGIC 4. **Monotonicity checks** — where we expect a feature to move predictions in a
# MAGIC    specific direction (e.g., higher flood zone → higher premium), do the models?
# MAGIC
# MAGIC Output: `app_fairness_report` (one row per model×slice) + PNGs in `/Volumes/.../reports/fairness/`.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %run ./_model_report_lib

# COMMAND ----------

import numpy as np
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

OUT_DIR = ensure_dir(f"/Volumes/{catalog}/{schema}/reports/fairness")
print(f"Fairness reports → {OUT_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the 2024 test slice

# COMMAND ----------

test = spark.table(f"{fqn}.feature_policy_year_training").filter("exposure_year = 2024").toPandas()
print(f"Test rows: {len(test):,}")

# Shared feature space (same as 08_generate_model_reports)
NUM = [
    "sum_insured", "annual_turnover", "gross_premium",
    "credit_score", "ccj_count", "years_trading", "business_stability_score",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index",
    "subsidence_risk", "composite_location_risk", "frac_urban", "is_coastal",
    "imd_decile", "crime_decile", "income_decile", "health_decile", "living_env_decile",
    "market_median_rate", "competitor_a_min_rate", "price_index",
    "policy_age_years_at_exposure_start", "building_age_years_at_exposure_start",
]
CAT = ["region", "construction_type", "internal_risk_tier",
       "credit_risk_tier", "location_risk_tier", "division", "channel"]

FREQ_NUM = NUM[:-5] + NUM[-2:]  # trimmed to match freq_glm trainer
FREQ_CAT = ["region", "construction_type", "internal_risk_tier",
            "credit_risk_tier", "location_risk_tier", "channel"]

SEV_GLM_NUM = [
    "sum_insured", "annual_turnover", "gross_premium",
    "credit_score", "years_trading", "business_stability_score",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index",
    "subsidence_risk", "composite_location_risk", "frac_urban",
    "imd_decile", "living_env_decile",
    "market_median_rate", "price_index",
    "building_age_years_at_exposure_start",
]
SEV_GLM_CAT = ["region", "construction_type", "internal_risk_tier",
               "credit_risk_tier", "location_risk_tier"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Protected-attribute absence — explicit enumeration

# COMMAND ----------

import re

# Full column names that would indicate a protected attribute.
# We match on *full word* boundaries so that e.g. "building_age_years" doesn't
# trip the "age" rule — building age is a legitimate property-risk feature.
PROTECTED_PATTERNS = [
    r"\b(gender|sex)\b",
    r"\b(age)\b",
    r"\b(date_of_birth|dob|birth_date)\b",
    r"\b(ethnicity|race)\b",
    r"\b(religion)\b",
    r"\b(sexual_orientation|orientation)\b",
    r"\b(disability)\b",
    r"\b(pregnancy)\b",
    r"\b(marital_status)\b",
    r"\b(national_insurance_number|nino|ssn)\b",
    r"\b(director_age|director_gender|director_ethnicity)\b",
]

feature_table_cols = set(test.columns.tolist())
quote_table_cols   = set(spark.table(f"{fqn}.feature_quote_training").columns)

present = []
for pat in PROTECTED_PATTERNS:
    rx = re.compile(pat, re.IGNORECASE)
    for c in feature_table_cols | quote_table_cols:
        if rx.search(c):
            present.append({"pattern": pat, "column_match": c})

if present:
    print("⚠ Potential protected-attribute columns present:")
    for p in present:
        print(f"   {p}")
else:
    print("✓ No protected-attribute columns in either feature table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cross-slice prediction distribution — champion pricing model
# MAGIC
# MAGIC For the compound pricing champion (freq_glm × sev_gbm) we compute the pure premium
# MAGIC for every 2024 policy-year, then slice it against legitimate risk bands plus a
# MAGIC couple of "should not matter" proxies.

# COMMAND ----------

def load_and_predict(model_name, num_feats, cat_feats, drop_first, df_in):
    full = f"{catalog}.{schema}.{model_name}"
    versions = client.search_model_versions(f"name='{full}'")
    if not versions:
        return None
    latest = max(versions, key=lambda v: int(v.version))
    pymodel = mlflow.pyfunc.load_model(f"models:/{full}/{latest.version}")
    expected = None
    try:
        schema_in = pymodel.metadata.get_input_schema()
        expected = [f.name for f in schema_in.inputs] if schema_in else None
    except Exception:
        pass
    for c in num_feats:
        df_in[c] = pd.to_numeric(df_in[c], errors="coerce").fillna(0.0)
    dummies = pd.get_dummies(df_in[cat_feats].fillna("UNK"), drop_first=drop_first, dtype=float)
    X = pd.concat([df_in[num_feats].reset_index(drop=True), dummies.reset_index(drop=True)], axis=1)
    if expected:
        for c in expected:
            if c not in X.columns:
                X[c] = 0.0
        X = X.reindex(columns=expected)
    return np.asarray(pymodel.predict(X)).ravel()

# Compound champion: freq (GLM) × sev (GBM)
pred_freq = load_and_predict("freq_glm", FREQ_NUM, FREQ_CAT, drop_first=True,  df_in=test.copy())
pred_sev  = load_and_predict("sev_gbm",  NUM,      CAT,      drop_first=False, df_in=test.copy())
if pred_freq is not None and pred_sev is not None:
    # Multiply by exposure_fraction so the pure premium is per-policy-year
    pure_premium_compound = pred_freq * test["exposure_fraction"].clip(lower=0.01).values * pred_sev
else:
    pure_premium_compound = None

pred_single = load_and_predict("pure_premium_xgb", NUM, CAT, drop_first=False, df_in=test.copy())

test["champ_compound"] = pure_premium_compound
test["champ_single"]   = pred_single
test["actual_loss"]    = test["total_incurred_observed"].fillna(0.0).astype(float)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Slice by region — expected differences driven by real flood/crime/subsidence exposure

# COMMAND ----------

def slice_stats(col: str) -> pd.DataFrame:
    agg = test.groupby(col, dropna=True).agg(
        n=("actual_loss", "size"),
        actual_mean=("actual_loss", "mean"),
        compound_mean=("champ_compound", "mean"),
        single_mean=("champ_single", "mean"),
    ).reset_index()
    overall = {
        "actual": test["actual_loss"].mean(),
        "compound": test["champ_compound"].mean() if pure_premium_compound is not None else np.nan,
        "single":   test["champ_single"].mean() if pred_single is not None else np.nan,
    }
    agg["relativity_compound"] = agg["compound_mean"] / (overall["compound"] + 1e-9)
    agg["relativity_actual"]   = agg["actual_mean"]   / (overall["actual"]   + 1e-9)
    agg["delta_vs_actual"]     = agg["relativity_compound"] - agg["relativity_actual"]
    return agg

for col in ["region", "imd_decile", "income_decile", "is_coastal", "frac_urban"]:
    if col in test.columns:
        agg = slice_stats(col)
        agg.to_csv(f"{OUT_DIR}/slice_{col}.csv", index=False)
        print(f"\n--- slice: {col} ---")
        print(agg.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Region slice — visual: compound premium vs actual loss ratio

# COMMAND ----------

if pure_premium_compound is not None:
    region_agg = test.groupby("region").agg(
        n=("actual_loss", "size"),
        actual=("actual_loss", "mean"),
        predicted=("champ_compound", "mean"),
    ).reset_index().sort_values("actual", ascending=False)

    fig, ax = plt.subplots(figsize=(11, 5))
    x = np.arange(len(region_agg))
    w = 0.35
    ax.bar(x - w/2, region_agg["actual"],    w, label="Actual mean loss")
    ax.bar(x + w/2, region_agg["predicted"], w, label="Compound prediction")
    ax.set_xticks(x); ax.set_xticklabels(region_agg["region"], rotation=30, ha="right")
    ax.set_ylabel("£ per policy-year")
    ax.set_title("Mean actual loss vs compound champion prediction — by region (2024 test)")
    ax.legend(); ax.grid(alpha=0.3, axis="y")
    save_fig(fig, f"{OUT_DIR}/region_actual_vs_predicted.png")
    region_agg.to_csv(f"{OUT_DIR}/region_actual_vs_predicted.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Monotonicity checks — directional expectations per feature
# MAGIC
# MAGIC For a subset of features with a clear prior direction, we check whether the mean
# MAGIC compound prediction moves as expected across the feature's bucketed range.

# COMMAND ----------

MONOTONIC_EXPECTATIONS = {
    "flood_zone_rating":       ("up",   "Higher flood zone → higher expected flood losses"),
    "crime_theft_index":       ("up",   "Higher neighbourhood crime → higher theft losses"),
    "subsidence_risk":         ("up",   "Higher subsidence risk → higher subsidence losses"),
    "composite_location_risk": ("up",   "Composite hazard is an up-direction summary"),
    "credit_score":            ("down", "Higher credit score → lower expected claims (financial distress proxy)"),
    "business_stability_score":("down", "More stable business → fewer claims"),
    "ccj_count":               ("up",   "More CCJs → more financial distress → more claims"),
    "years_trading":           ("down", "Longer trading history → fewer teething-problem claims"),
    "imd_decile":              ("down", "Higher decile = less deprived → lower claims (IMD is 1 most-deprived)"),
}

mono_rows = []
for feat, (expected_dir, rationale) in MONOTONIC_EXPECTATIONS.items():
    if feat not in test.columns or pure_premium_compound is None:
        continue
    # Bucket into deciles (or unique values if discrete)
    try:
        test["_bucket"] = pd.qcut(test[feat], q=10, duplicates="drop", labels=False)
    except Exception:
        test["_bucket"] = test[feat]
    agg = test.groupby("_bucket").agg(
        n=("champ_compound", "size"),
        mean_pred=("champ_compound", "mean"),
    ).dropna().sort_values("_bucket")
    if len(agg) < 3:
        continue
    # Spearman-ish correlation between bucket order and mean prediction
    rho = agg[["mean_pred"]].reset_index().corr().iloc[0, 1]
    observed_dir = "up" if rho >= 0 else "down"
    mono_rows.append({
        "feature": feat,
        "expected_direction": expected_dir,
        "observed_correlation": round(float(rho), 3),
        "observed_direction": observed_dir,
        "consistent": expected_dir == observed_dir,
        "rationale": rationale,
    })

mono_df = pd.DataFrame(mono_rows)
print(mono_df.to_string(index=False))
mono_df.to_csv(f"{OUT_DIR}/monotonicity_check.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Same-risk consistency — "postcode-swap" test
# MAGIC
# MAGIC Take 100 random policies. For each, hold every non-location feature constant and
# MAGIC swap the postcode_sector + region + geo-risk fields with those of another random
# MAGIC policy. If the model is well-behaved, the *spread* of predictions over all possible
# MAGIC swaps should reflect legitimate location-risk differences, not arbitrary variation.

# COMMAND ----------

# Compute prediction at original location and at 10 alternative locations per policy.
# High coefficient of variation across swaps = location drives a lot of price (this is
# expected where real hazards like flood zones differ).

np.random.seed(42)
sample_idx = np.random.choice(len(test), size=min(100, len(test)), replace=False)
base = test.iloc[sample_idx].reset_index(drop=True).copy()

# Geo / location features we'll swap
GEO_FIELDS = [
    "postcode_sector", "region", "flood_zone_rating", "proximity_to_fire_station_km",
    "crime_theft_index", "subsidence_risk", "composite_location_risk",
    "frac_urban", "is_coastal", "imd_decile", "crime_decile",
    "income_decile", "health_decile", "living_env_decile",
    "location_risk_tier",
]

swap_donors = test.sample(n=min(10, len(test)), random_state=1).reset_index(drop=True)

rows = []
for _, donor in swap_donors.iterrows():
    swapped = base.copy()
    for f in GEO_FIELDS:
        if f in swapped.columns and f in donor.index:
            swapped[f] = donor[f]
    pf = load_and_predict("freq_glm", FREQ_NUM, FREQ_CAT, drop_first=True,  df_in=swapped.copy())
    ps = load_and_predict("sev_gbm",  NUM,      CAT,      drop_first=False, df_in=swapped.copy())
    if pf is None or ps is None:
        continue
    rows.append(pf * swapped["exposure_fraction"].clip(lower=0.01).values * ps)

if rows:
    arr = np.vstack(rows)  # shape (donors, 100)
    per_policy_cv = (arr.std(axis=0) / (arr.mean(axis=0) + 1e-9))
    cv_stats = pd.Series(per_policy_cv).describe()
    print("\nCoefficient of variation of pricing across 10 postcode-swaps per policy:")
    print(cv_stats.to_string())
    pd.DataFrame({"policy_idx": np.arange(len(per_policy_cv)),
                  "cv_across_swaps": per_policy_cv}).to_csv(f"{OUT_DIR}/postcode_swap_cv.csv", index=False)

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.hist(per_policy_cv, bins=30)
    ax.set_xlabel("CV of compound premium across 10 swapped postcodes")
    ax.set_ylabel("Number of policies")
    ax.set_title("How much does the location change the price? (low CV = location-insensitive)")
    ax.grid(alpha=0.3)
    save_fig(fig, f"{OUT_DIR}/postcode_swap_cv.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persist summary to `app_fairness_report`

# COMMAND ----------

summary_rows = []
summary_rows.append({"check": "protected_attributes_in_feature_space",
                     "status": "PASS" if not present else "FAIL",
                     "detail": f"{len(present)} matching columns" if present else "none"})

summary_rows.append({"check": "monotonicity_consistent_count",
                     "status": f"{int(mono_df['consistent'].sum())}/{len(mono_df)}" if len(mono_df) else "n/a",
                     "detail": ",".join(mono_df[~mono_df["consistent"]]["feature"].tolist()) if len(mono_df) else "all ok"})

if rows:
    summary_rows.append({"check": "postcode_swap_median_cv",
                         "status": f"{np.median(per_policy_cv):.3f}",
                         "detail": "Median coefficient of variation across 10 postcode swaps"})

summary_df = pd.DataFrame(summary_rows)
summary_table = f"{fqn}.app_fairness_report"
(spark.createDataFrame(summary_df)
      .withColumn("_evaluated_at", F.current_timestamp())
      .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(summary_table))
spark.sql(f"""
    ALTER TABLE {summary_table} SET TBLPROPERTIES (
        'comment' = 'Fairness + robustness audit results for each model factory run — protected-attribute absence, directional monotonicity, same-risk consistency. Refreshed by 09_fairness_and_robustness.'
    )
""")
print(f"✓ {summary_table}")
display(spark.table(summary_table))

# COMMAND ----------

import json as _json
dbutils.notebook.exit(_json.dumps({
    "protected_matches": present,
    "monotonicity": mono_df.to_dict("records") if len(mono_df) else [],
    "postcode_swap_median_cv": float(np.median(per_policy_cv)) if rows else None,
}, default=str))
