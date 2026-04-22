# Databricks notebook source
# MAGIC %md
# MAGIC # Model Factory — champion/challenger comparison
# MAGIC
# MAGIC Reads the latest version of every pricing model in Unity Catalog, scores the 2024
# MAGIC test slice of `feature_policy_year_training`, and writes a comparison table.
# MAGIC Two champions get aliased:
# MAGIC - **`pricing_champion_compound`**: the winner amongst (freq_glm × sev_glm, freq_glm × sev_gbm)
# MAGIC - **`pricing_champion_single`**:   pure_premium_xgb
# MAGIC
# MAGIC The app surfaces both so underwriters can choose between transparent compound
# MAGIC pricing and the single-model Tweedie challenger.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

import numpy as np
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient
import pyspark.sql.functions as F

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull the 2024 test slice (same split used by every trainer)

# COMMAND ----------

table = f"{fqn}.feature_policy_year_training"
test = spark.table(table).filter("exposure_year = 2024").toPandas()

print(f"Test rows: {len(test):,} | total losses: £{test['total_incurred_observed'].sum():,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load each registered model's latest version

# COMMAND ----------

def latest_version(name: str):
    try:
        full = f"{catalog}.{schema}.{name}"
        versions = client.search_model_versions(f"name='{full}'")
        if not versions:
            return None
        latest = max(versions, key=lambda v: int(v.version))
        return (full, latest.version)
    except Exception as e:
        print(f"[{name}] not found: {e}")
        return None

models = {
    "freq_glm":         latest_version("freq_glm"),
    "sev_glm":          latest_version("sev_glm"),
    "sev_gbm":          latest_version("sev_gbm"),
    "pure_premium_xgb": latest_version("pure_premium_xgb"),
}
for k, v in models.items():
    print(f"  {k}: {v}")

def _load(name_ver):
    if name_ver is None:
        return None, None
    m = mlflow.pyfunc.load_model(f"models:/{name_ver[0]}/{name_ver[1]}")
    try:
        schema = m.metadata.get_input_schema()
        expected = [f.name for f in schema.inputs] if schema is not None else None
    except Exception:
        expected = None
    return m, expected

freq_glm,  cols_freq_glm  = _load(models["freq_glm"])
sev_glm,   cols_sev_glm   = _load(models["sev_glm"])
sev_gbm,   cols_sev_gbm   = _load(models["sev_gbm"])
pp_xgb,    cols_pp_xgb    = _load(models["pure_premium_xgb"])

for n, c in [("freq_glm", cols_freq_glm), ("sev_glm", cols_sev_glm),
             ("sev_gbm", cols_sev_gbm), ("pure_premium_xgb", cols_pp_xgb)]:
    print(f"  {n} expects {len(c) if c else '?'} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build feature frames in the shape each model expects

# COMMAND ----------

# Candidate numerics + categoricals (superset; each model's signature picks what it wants)
num_common = [
    "sum_insured", "annual_turnover", "gross_premium",
    "credit_score", "ccj_count", "years_trading", "business_stability_score",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index",
    "subsidence_risk", "composite_location_risk", "frac_urban", "is_coastal",
    "imd_decile", "crime_decile", "income_decile", "health_decile", "living_env_decile",
    "market_median_rate", "competitor_a_min_rate", "price_index",
    "policy_age_years_at_exposure_start", "building_age_years_at_exposure_start",
]
cat_common = ["region", "construction_type", "internal_risk_tier",
              "credit_risk_tier", "location_risk_tier", "division", "channel"]

df = test.copy()
for c in num_common:
    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
for c in cat_common:
    df[c] = df[c].fillna("UNK")

# Pre-compute both OH encodings (drop_first True / False) and the native-categorical frame.
oh_true  = pd.get_dummies(df[cat_common], drop_first=True,  dtype=float)
oh_false = pd.get_dummies(df[cat_common], drop_first=False, dtype=float)
X_oh_true  = pd.concat([df[num_common].reset_index(drop=True), oh_true.reset_index(drop=True)],  axis=1)
X_oh_false = pd.concat([df[num_common].reset_index(drop=True), oh_false.reset_index(drop=True)], axis=1)

X_native = df[num_common + cat_common].copy()
for c in cat_common:
    X_native[c] = X_native[c].astype("category")

def align_to_signature(df_candidate, expected_cols):
    """Reindex to match the expected column list, filling absent columns with 0."""
    if expected_cols is None:
        return df_candidate
    missing = [c for c in expected_cols if c not in df_candidate.columns]
    for c in missing:
        df_candidate[c] = 0.0
    return df_candidate.reindex(columns=expected_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score candidates

# COMMAND ----------

import warnings; warnings.filterwarnings("ignore")
import traceback

def safe_predict(model, X, label):
    if model is None:
        print(f"[{label}] model not loaded")
        return None
    try:
        return np.asarray(model.predict(X)).ravel()
    except Exception as e:
        print(f"[{label}] predict failed: {e}")
        traceback.print_exc()
        return None

diag = {}
def _diag(name, X_in, model, expected):
    print(f"\n--- {name} ---")
    print(f"  expected cols ({len(expected) if expected else '?'}):",
          (expected[:5] + ['...'] + expected[-3:]) if expected and len(expected) > 8 else expected)
    print(f"  input cols ({len(X_in.columns)}):",
          list(X_in.columns[:5]) + ['...'] + list(X_in.columns[-3:]) if len(X_in.columns) > 8 else list(X_in.columns))
    print(f"  input dtypes sample: {X_in.dtypes.iloc[:3].to_dict()}")
    diag[name] = {"expected_n": len(expected) if expected else None, "input_n": len(X_in.columns)}
    return safe_predict(model, X_in, name)

pred_freq        = _diag("freq_glm",         align_to_signature(X_oh_true.copy(),  cols_freq_glm), freq_glm, cols_freq_glm)
pred_sev_glm_val = _diag("sev_glm",          align_to_signature(X_oh_true.copy(),  cols_sev_glm),  sev_glm,  cols_sev_glm)
pred_sev_gbm_val = _diag("sev_gbm",          align_to_signature(X_oh_false.copy(), cols_sev_gbm),  sev_gbm,  cols_sev_gbm)
pred_pp_xgb      = _diag("pure_premium_xgb", align_to_signature(X_oh_false.copy(), cols_pp_xgb),   pp_xgb,   cols_pp_xgb)

# Freq GLM was trained with log(exposure_fraction) offset. At serving time the wrapper
# uses offset=0 (full-year equivalent), so multiply by actual exposure to recover counts.
if pred_freq is not None:
    pred_freq = pred_freq * df["exposure_fraction"].clip(lower=0.01).values

actual = df["total_incurred_observed"].values

# Compound: pure premium = E[N] × E[S]
def pure_premium_compound(freq, sev):
    if freq is None or sev is None:
        return None
    return freq * sev

pp_glm_glm = pure_premium_compound(pred_freq, pred_sev_glm_val)
pp_glm_gbm = pure_premium_compound(pred_freq, pred_sev_gbm_val)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Double-lift chart — GBM severity vs GLM severity (champion vs challenger)
# MAGIC
# MAGIC Classic actuarial exhibit: bin policies by the ratio of the challenger's prediction
# MAGIC to the champion's. In the bins where the challenger says "much higher than champion",
# MAGIC does the actual loss confirm the challenger's view? If yes, the challenger is adding
# MAGIC genuine signal and not just noise.

# COMMAND ----------

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def save_double_lift(y_true, pred_champion, pred_challenger, path, champion_label, challenger_label):
    """Double-lift: sort by ratio challenger/champion, bin into deciles, show mean
    actual + mean champion pred + mean challenger pred in each bin."""
    if pred_champion is None or pred_challenger is None:
        return None
    df = pd.DataFrame({
        "y": np.asarray(y_true, dtype=float),
        "ch": np.asarray(pred_champion, dtype=float),
        "cg": np.asarray(pred_challenger, dtype=float),
    })
    df["ratio"] = df["cg"] / (df["ch"] + 1e-9)
    df["decile"] = pd.qcut(df["ratio"].rank(method="first"), q=10, labels=False, duplicates="drop")
    agg = df.groupby("decile").agg(
        n=("y", "size"),
        mean_actual=("y", "mean"),
        mean_champion=("ch", "mean"),
        mean_challenger=("cg", "mean"),
    ).reset_index()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(agg["decile"], agg["mean_actual"],     marker="s", linewidth=2, label="Actual loss")
    ax.plot(agg["decile"], agg["mean_champion"],   marker="o", linewidth=2, label=champion_label)
    ax.plot(agg["decile"], agg["mean_challenger"], marker="^", linewidth=2, label=challenger_label)
    ax.set_xlabel("Decile of (challenger / champion) prediction ratio")
    ax.set_ylabel("Mean £ per policy-year")
    ax.set_title(f"Double-lift — {challenger_label} vs {champion_label} (2024 test)")
    ax.legend(); ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=110, bbox_inches="tight")
    plt.close(fig)
    return agg

# GBM sev vs GLM sev (compound pure premium): both use the same freq_glm leg, so the
# double-lift reflects the severity-leg disagreement alone.
dl_dir = f"/Volumes/{catalog}/{schema}/reports/model_factory"
import os
os.makedirs(dl_dir, exist_ok=True)

dl_agg = save_double_lift(
    actual, pp_glm_glm, pp_glm_gbm,
    path=f"{dl_dir}/double_lift_sev_gbm_vs_sev_glm.png",
    champion_label="compound (GLM × GLM)",
    challenger_label="compound (GLM × GBM)",
)
if dl_agg is not None:
    dl_agg.to_csv(f"{dl_dir}/double_lift_sev_gbm_vs_sev_glm.csv", index=False)
    print(f"✓ {dl_dir}/double_lift_sev_gbm_vs_sev_glm.png")

# Also: pure_premium_xgb vs compound-glm-gbm
dl_agg2 = save_double_lift(
    actual, pp_glm_gbm, pred_pp_xgb,
    path=f"{dl_dir}/double_lift_xgb_vs_compound.png",
    champion_label="compound (GLM × GBM)",
    challenger_label="pure_premium_xgb (Tweedie)",
)
if dl_agg2 is not None:
    dl_agg2.to_csv(f"{dl_dir}/double_lift_xgb_vs_compound.csv", index=False)
    print(f"✓ {dl_dir}/double_lift_xgb_vs_compound.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute headline metrics for each candidate

# COMMAND ----------

def evaluate(name, pred, actual):
    if pred is None:
        return None
    rmse = float(np.sqrt(np.mean((pred - actual) ** 2)))
    mae  = float(np.mean(np.abs(pred - actual)))
    total_pred, total_act = float(pred.sum()), float(actual.sum())
    # Gini on pure premium (using incurred as the target — the standard pricing lift test)
    order = np.argsort(-pred)
    cum_actual = np.cumsum(actual[order]) / (actual.sum() + 1e-9)
    cum_pop    = np.arange(1, len(actual) + 1) / len(actual)
    gini = float(2 * np.trapz(cum_actual, cum_pop) - 1)
    return {
        "model": name,
        "rmse": round(rmse, 2),
        "mae":  round(mae, 2),
        "pred_total": round(total_pred, 0),
        "actual_total": round(total_act, 0),
        "total_ratio": round(total_pred / total_act, 4) if total_act > 0 else None,
        "gini_on_incurred": round(gini, 4),
    }

results = [r for r in [
    evaluate("compound_glm_glm (freq_glm × sev_glm)", pp_glm_glm, actual),
    evaluate("compound_glm_gbm (freq_glm × sev_gbm)", pp_glm_gbm, actual),
    evaluate("pure_premium_xgb",                      pred_pp_xgb, actual),
] if r is not None]

diag["prediction_status"] = {
    "freq_glm":         "ok" if pred_freq is not None else "FAILED",
    "sev_glm":          "ok" if pred_sev_glm_val is not None else "FAILED",
    "sev_gbm":          "ok" if pred_sev_gbm_val is not None else "FAILED",
    "pure_premium_xgb": "ok" if pred_pp_xgb is not None else "FAILED",
}
print("\n=== prediction status ===")
for k, v in diag["prediction_status"].items():
    print(f"  {k}: {v}")

results_df = pd.DataFrame(results).sort_values("gini_on_incurred", ascending=False)
display(spark.createDataFrame(results_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist comparison + alias champions

# COMMAND ----------

comparison_table = f"{fqn}.model_comparison"
sdf = spark.createDataFrame(results_df).withColumn("_evaluated_at", F.current_timestamp())
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(comparison_table)
spark.sql(f"""
    ALTER TABLE {comparison_table} SET TBLPROPERTIES (
        'comment' = 'Side-by-side test-set metrics for every pricing candidate trained in the model factory. Refreshed every time 07_model_factory runs. Gini on incurred is the primary ranking metric; total_ratio checks portfolio-level calibration.'
    )
""")
print(f"✓ {comparison_table}")

def _try_alias(full_name, alias, version, key):
    try:
        client.set_registered_model_alias(full_name, alias, version)
        diag.setdefault("aliases", {})[key] = f"ok → {full_name}/{version}"
        print(f"✓ alias {alias} → {full_name}/{version}")
    except Exception as e:
        diag.setdefault("aliases", {})[key] = f"FAILED: {e!r}"
        print(f"✗ alias {alias} failed: {e!r}")

# Compound champion: better of glm×glm vs glm×gbm
compound_rows = [r for r in results if r["model"].startswith("compound_")]
if compound_rows:
    best_compound = max(compound_rows, key=lambda r: r["gini_on_incurred"])
    sev_name = "sev_glm" if "glm_glm" in best_compound["model"] else "sev_gbm"
    if models[sev_name]:
        _try_alias(models[sev_name][0], "pricing_champion_severity", models[sev_name][1], "severity")
    if models["freq_glm"]:
        _try_alias(models["freq_glm"][0], "pricing_champion_frequency", models["freq_glm"][1], "frequency")

# Single-model champion
if models["pure_premium_xgb"]:
    _try_alias(models["pure_premium_xgb"][0], "pricing_champion_single", models["pure_premium_xgb"][1], "single")

# Demand champion
demand = latest_version("demand_gbm")
if demand:
    _try_alias(demand[0], "demand_champion", demand[1], "demand")

# Surface the diagnostics to get-run-output so we can see them
import json as _json
dbutils.notebook.exit(_json.dumps(diag, default=str))
