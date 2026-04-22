# Databricks notebook source
# MAGIC %md
# MAGIC # Regulatory Model Reports — one per registered pricing model
# MAGIC
# MAGIC For each model in UC, produces a self-contained artefact that an actuary,
# MAGIC model-risk auditor, or PRA/FCA/EIOPA reviewer can walk through end-to-end:
# MAGIC
# MAGIC 1. **Executive summary** — purpose, target, distribution, version, UC aliases
# MAGIC 2. **Lineage** — exact Delta versions from feature table → silver → raw landing CSVs
# MAGIC 3. **Feature catalog** — source table/column, transformation, regulatory sensitivity, PII
# MAGIC 4. **Fit diagnostics**
# MAGIC     - GLM: coefficients with p-values, relativities, confidence intervals
# MAGIC     - GBM/XGB: SHAP global importance, feature importance (gain), PDP plots
# MAGIC 5. **Calibration + lift** — decile calibration table, Gini, double-lift vs champion
# MAGIC 6. **Partial dependence plots** — the "GBMs can be as clear as a GLM" exhibit:
# MAGIC    each top feature plotted as an isolated relativity curve
# MAGIC 7. **SHAP reason codes** — individual prediction decomposition for 3 sample policies,
# MAGIC    answering "why is THIS postcode more expensive than THAT postcode?"
# MAGIC 8. **Protected-attribute audit** — explicit list of features NOT used (no age, no gender,
# MAGIC    no ethnicity, no disability markers; commercial book uses only firmographic +
# MAGIC    geo-risk inputs), plus cross-region outcome fairness
# MAGIC
# MAGIC Output written to `/Volumes/{catalog}/{schema}/reports/{model}_v{version}/`:
# MAGIC `report.md`, plus `plots/*.png` and `tables/*.csv`. Also logged as MLflow artifacts.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %run ./_model_report_lib

# COMMAND ----------

import datetime
import json
import numpy as np
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient
import pyspark.sql.functions as F

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

REPORTS_ROOT = f"/Volumes/{catalog}/{schema}/reports"
FEATURE_CATALOG = f"{fqn}.feature_catalog"
RUN_TS = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"

ensure_dir(REPORTS_ROOT)
print(f"Report root: {REPORTS_ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load test slice for each target feature table (shared across models)

# COMMAND ----------

FEATURE_POLICY_YEAR = f"{fqn}.feature_policy_year_training"
FEATURE_QUOTE       = f"{fqn}.feature_quote_training"

policy_year_dv = delta_version(spark, FEATURE_POLICY_YEAR)
quote_dv       = delta_version(spark, FEATURE_QUOTE)

# Upstream lineage (silver/raw versions) — pulled once, shared across every report
silver_versions = {
    t: delta_version(spark, f"{fqn}.{t}")
    for t in ["silver_company_bureau", "silver_sic_directory",
              "silver_geo_hazard", "silver_market_benchmark",
              "silver_postcode_enrichment"]
}
raw_versions = {
    t: delta_version(spark, f"{fqn}.{t}")
    for t in ["raw_company_bureau", "raw_sic_directory",
              "raw_geo_hazard", "raw_market_benchmark"]
}

# Pull test slices
test_py = spark.table(FEATURE_POLICY_YEAR).filter("exposure_year = 2024").toPandas()
test_q  = spark.table(FEATURE_QUOTE).filter("quote_year = 2024").toPandas()
print(f"Policy-year test: {len(test_py):,} | Quote test: {len(test_q):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Protected-attribute audit (same for every model — we assert the absence)

# COMMAND ----------

PROTECTED_ATTRIBUTES = {
    "gender":            "Not collected — commercial insurance keys on company, not individual",
    "age":               "Not collected — no individual-level data in the pricing pipeline",
    "ethnicity":         "Not collected and cannot be inferred from inputs",
    "disability_status": "Not collected and cannot be inferred from inputs",
    "religion":          "Not collected",
    "sexual_orientation":"Not collected",
    "pregnancy":         "Not collected",
    "marital_status":    "Not collected",
    "director_dob":      "Not used — only years_trading, business_stability_score, ccj_count",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-model report function
# MAGIC
# MAGIC `report_model(model_name, ...)` does the end-to-end workflow for any candidate.
# MAGIC Returns a dict with the path to the written report.

# COMMAND ----------

def _one_hot(df: pd.DataFrame, cat_cols, drop_first: bool) -> pd.DataFrame:
    dummies = pd.get_dummies(df[cat_cols].fillna("UNK"), drop_first=drop_first, dtype=float)
    return dummies

def _pyfunc_predict_fn(pyfunc_model):
    """Wrap an MLflow pyfunc model so it returns a 1-D numpy array."""
    def _f(X):
        out = pyfunc_model.predict(X)
        return np.asarray(out).ravel()
    return _f

def _align(df: pd.DataFrame, expected_cols):
    if expected_cols is None:
        return df
    for c in expected_cols:
        if c not in df.columns:
            df[c] = 0.0
    return df.reindex(columns=expected_cols)

def report_model(
    *,
    model_name: str,
    alias: str | None,          # e.g., "pricing_champion_severity"
    target_col: str,            # the label column in the feature table
    target_description: str,
    distribution: str,
    feature_table_fqn: str,
    feature_table_version: str,
    test_pdf: pd.DataFrame,
    numeric_features: list[str],
    categorical_features: list[str],
    drop_first: bool,           # one-hot encoding style (must match trainer)
    filter_rows: str | None = None,  # e.g., "claim_count_observed > 0" for severity
    weight_col: str | None = None,
) -> dict:
    print(f"\n===== {model_name} =====")
    # ---- resolve model version ----
    full = f"{catalog}.{schema}.{model_name}"
    versions = client.search_model_versions(f"name='{full}'")
    if not versions:
        print(f"  not found in UC — skipping")
        return {"model_name": model_name, "status": "missing"}
    latest = max(versions, key=lambda v: int(v.version))
    version = latest.version
    print(f"  UC version: {version} | alias: {alias}")

    # Output dir
    out_dir = ensure_dir(f"{REPORTS_ROOT}/{model_name}_v{version}")
    plots_dir  = ensure_dir(f"{out_dir}/plots")
    tables_dir = ensure_dir(f"{out_dir}/tables")

    # ---- load model ----
    pymodel = mlflow.pyfunc.load_model(f"models:/{full}/{version}")
    try:
        schema_in = pymodel.metadata.get_input_schema()
        expected = [f.name for f in schema_in.inputs] if schema_in else None
    except Exception:
        expected = None
    print(f"  expected input cols: {len(expected) if expected else '?'}")

    # ---- build test frame ----
    df = test_pdf.copy()
    if filter_rows:
        df = df.query(filter_rows).reset_index(drop=True)
    for c in numeric_features:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    X_oh = pd.concat([
        df[numeric_features].reset_index(drop=True),
        _one_hot(df, categorical_features, drop_first).reset_index(drop=True),
    ], axis=1)
    X_in = _align(X_oh.copy(), expected)
    y_actual = df[target_col].astype(float).fillna(0.0).values

    # Predict
    predict_fn = _pyfunc_predict_fn(pymodel)
    y_pred = predict_fn(X_in)
    print(f"  rows scored: {len(y_pred):,} | pred mean {y_pred.mean():.3f} | actual mean {y_actual.mean():.3f}")

    # ---- headline metrics ----
    gini = gini_on_target(y_actual, y_pred)
    tr   = total_ratio(y_actual, y_pred)
    rmse = float(np.sqrt(np.mean((y_actual - y_pred) ** 2)))
    mae  = float(np.mean(np.abs(y_actual - y_pred)))
    print(f"  Gini {gini:.3f} | total_ratio {tr:.3f} | RMSE {rmse:,.2f}")

    # ---- calibration + lift ----
    calib_df = calibration_by_decile(y_actual, y_pred, n_bins=10)
    calib_fig = plot_calibration(calib_df, title=f"{model_name} — calibration by decile (2024 test)")
    save_fig(calib_fig, f"{plots_dir}/calibration.png")
    calib_df.to_csv(f"{tables_dir}/calibration_table.csv", index=False)

    lift_fig, gini_v = plot_lift(y_actual, y_pred, title=f"{model_name} — lift curve")
    save_fig(lift_fig, f"{plots_dir}/lift_curve.png")

    # ---- SHAP / coefficient decomposition ----
    sample_n = min(2000, len(X_in))
    X_sample = X_in.sample(n=sample_n, random_state=42).reset_index(drop=True)

    shap_method = None
    shap_values = None
    shap_importance_df = None
    model_coef_df = None

    # Strategy: try each flavor-specific loader. Whichever succeeds tells us the
    # underlying model type — tree-based gets TreeSHAP, sklearn-GLM gets analytical
    # xᵢ·βᵢ decomposition.
    model_uri = f"models:/{full}/{version}"

    booster = None
    # LightGBM
    try:
        from mlflow import lightgbm as _lgb_flavor
        booster = _lgb_flavor.load_model(model_uri)
        print(f"  loaded as LightGBM booster")
    except Exception:
        pass
    # XGBoost
    if booster is None:
        try:
            from mlflow import xgboost as _xgb_flavor
            booster = _xgb_flavor.load_model(model_uri)
            print(f"  loaded as XGBoost booster")
        except Exception:
            pass

    if booster is not None:
        try:
            import shap
            explainer = shap.TreeExplainer(booster)
            sv_raw = explainer.shap_values(X_sample)
            if isinstance(sv_raw, list):
                sv_raw = sv_raw[0]
            shap_values = np.asarray(sv_raw)
            shap_method = "TreeSHAP — exact additive decomposition on the model's link scale"
        except Exception as e:
            print(f"  TreeSHAP failed: {e}")
    else:
        # sklearn flavor → our PoissonGLMWrapper / GammaGLMWrapper
        try:
            from mlflow import sklearn as _sklearn_flavor
            sk_model = _sklearn_flavor.load_model(model_uri)
            if hasattr(sk_model, "result") and hasattr(sk_model, "feature_names"):
                fnames = list(sk_model.feature_names)
                coefs = np.asarray(sk_model.result.params)
                # statsmodels fits with a prepended constant — so len(params) == len(fnames) + 1
                if len(coefs) == len(fnames) + 1:
                    feat_coefs = coefs[1:]
                    aligned = X_sample.reindex(columns=fnames).fillna(0.0)
                    shap_values = aligned.values * feat_coefs  # linear-predictor scale
                    shap_method = "Analytical GLM decomposition (xᵢ·βᵢ on linear-predictor scale)"
                    try:
                        pvals = np.asarray(sk_model.result.pvalues)
                        ci = np.asarray(sk_model.result.conf_int())
                        model_coef_df = pd.DataFrame({
                            "feature":      ["intercept"] + fnames,
                            "coefficient":  coefs,
                            "relativity":   np.exp(coefs),
                            "p_value":      pvals,
                            "ci_low":       ci[:, 0] if ci.ndim == 2 else ci,
                            "ci_high":      ci[:, 1] if ci.ndim == 2 else ci,
                        })
                        model_coef_df["significant"] = model_coef_df["p_value"] < 0.05
                    except Exception as e:
                        print(f"  coef table skipped: {e}")
                else:
                    print(f"  coef/name length mismatch: {len(coefs)} vs {len(fnames)}+1")
            else:
                print(f"  sklearn model missing .result — cannot decompose")
        except Exception as e:
            print(f"  sklearn load/decompose failed: {e}")
            import traceback; traceback.print_exc()

    if shap_values is not None:
        # Global importance plot
        shap_importance_df = shap_global_importance(shap_values, X_sample.columns)
        shap_fig = plot_shap_bar(shap_importance_df, top_n=20,
                                 title=f"{model_name} — feature contribution (mean |SHAP|)")
        save_fig(shap_fig, f"{plots_dir}/shap_global.png")
        shap_importance_df.to_csv(f"{tables_dir}/shap_global.csv", index=False)

    # ---- Partial dependence for top features ----
    pdp_features = []
    if shap_importance_df is not None:
        pdp_features = shap_importance_df.head(8)["feature"].tolist()
    else:
        pdp_features = numeric_features[:8]

    pdp_paths = []
    for f in pdp_features:
        try:
            pdp_fig, pdp_df = partial_dependence_1d(predict_fn, X_in, f, n_points=15, sample=1000)
            pth = save_fig(pdp_fig, f"{plots_dir}/pdp_{f.replace('/', '_')[:40]}.png")
            pdp_df.to_csv(f"{tables_dir}/pdp_{f.replace('/', '_')[:40]}.csv", index=False)
            pdp_paths.append((f, pth))
        except Exception as e:
            print(f"  PDP failed for {f}: {e}")

    # ---- Reason codes (3 sample predictions) ----
    reason_rows = []
    if shap_values is not None:
        rng = np.random.RandomState(42)
        picks = rng.choice(len(X_sample), size=min(3, len(X_sample)), replace=False)
        for i, p_idx in enumerate(picks):
            rc = reason_codes(int(p_idx), shap_values, X_sample.columns, X_sample, top_n=8)
            rc.insert(0, "sample_idx", i + 1)
            rc["predicted_value"] = float(y_pred[df.index[p_idx]] if p_idx < len(y_pred) else y_pred[p_idx])
            rc["actual_value"] = float(y_actual[p_idx] if p_idx < len(y_actual) else 0.0)
            rc.to_csv(f"{tables_dir}/reason_codes_sample_{i+1}.csv", index=False)
            reason_rows.append(rc)

    # ---- Feature lineage ----
    lineage_df = feature_lineage(spark, FEATURE_CATALOG, numeric_features + categorical_features)
    lineage_df.to_csv(f"{tables_dir}/feature_lineage.csv", index=False)

    # ---- Build markdown ----
    md = []
    md.append(md_h(1, f"Regulatory Model Report — `{model_name}` v{version}"))
    md.append(md_para(f"_Generated {RUN_TS} — Bricksurance SE pricing workbench v2_"))

    md.append(md_h(2, "1. Executive summary"))
    md.append(md_kv({
        "Model name":        model_name,
        "UC full name":      full,
        "Version":           version,
        "Alias":             alias or "(none)",
        "Target":            f"`{target_col}` — {target_description}",
        "Distribution":      distribution,
        "Training table":    f"`{feature_table_fqn}` (Delta v{feature_table_version})",
        "Test slice":        f"{len(df):,} rows, exposure_year = 2024" + (f", filter: `{filter_rows}`" if filter_rows else ""),
        "Headline metrics":  f"Gini {gini:.3f} · total_ratio {tr:.3f} · RMSE {rmse:,.2f} · MAE {mae:,.2f}",
        "SHAP method":       shap_method or "(not computed)",
    }))

    md.append(md_h(2, "2. Upstream data lineage"))
    md.append(md_para(
        "The training table was derived from silver-layer tables, which in turn were built "
        "by DLT pipelines from bronze-layer raw ingestions of external CSV files. Every "
        "Delta version below is captured at model-training time; re-running the pipeline "
        "at a later date will produce a new version and a new model, both fully traceable."
    ))
    md.append(md_h(3, "Feature table"))
    md.append(md_kv({feature_table_fqn: f"Delta v{feature_table_version}"}))
    md.append(md_h(3, "Silver layer (DLT materialised views)"))
    md.append(md_kv({f"`{fqn}.{k}`": f"Delta v{v}" for k, v in silver_versions.items()}))
    md.append(md_h(3, "Bronze / raw landing tables"))
    md.append(md_kv({f"`{fqn}.{k}`": f"Delta v{v}" for k, v in raw_versions.items()}))
    md.append(md_para(
        "Raw tables were ingested from the `external_landing` Unity Catalog volume. The "
        "ingestion notebooks are `src/01_ingestion/ingest_*.py`; the DLT silver layer is "
        "defined in `src/02_silver/*.sql`."
    ))

    md.append(md_h(2, "3. Feature catalog"))
    md.append(md_para(
        "Each feature below was selected at modelling time. The `source_tables` and "
        "`source_columns` columns trace it back to its origin, the `transformation` "
        "describes any derivation, and `regulatory_sensitive` / `pii` flag inputs that "
        "require additional handling."
    ))
    md.append(md_table(lineage_df, max_rows=200))

    md.append(md_h(2, "4. Fit diagnostics"))
    if model_coef_df is not None:
        md.append(md_para("**GLM coefficient table** (significant at p<0.05 shown first):"))
        md.append(md_table(model_coef_df.sort_values(["significant", "p_value"], ascending=[False, True]), max_rows=80))
    elif shap_importance_df is not None:
        md.append(md_para("**Global feature importance (mean |SHAP|)** — the average absolute "
                          "contribution each feature makes to a single prediction. This is the "
                          "GBM analogue of a GLM's relativity table: it says _how much_ this "
                          "feature moves the premium, per prediction."))
        md.append(md_image("plots/shap_global.png", "SHAP global importance"))
        md.append(md_table(shap_importance_df, max_rows=25))

    md.append(md_h(2, "5. Calibration and discrimination"))
    md.append(md_para(
        "**Calibration** compares the model's mean prediction in each decile to the actual "
        "mean observed. A well-calibrated model's two lines track closely. **Discrimination** "
        "(Gini) measures how well the model ranks policies from lowest to highest risk."
    ))
    md.append(md_image("plots/calibration.png", "Calibration by decile"))
    md.append(md_table(calib_df))
    md.append(md_image("plots/lift_curve.png", "Lift curve"))

    md.append(md_h(2, "6. Partial dependence — per-feature behaviour"))
    md.append(md_para(
        "Each plot below shows the model's average prediction as a single feature is varied "
        "across its observed range, **holding every other feature at its actual value**. "
        "This is the closest direct analogue to a GLM's univariate relativity curve. A monotonic, "
        "smooth PDP is evidence that the model's treatment of that feature is explainable "
        "and stable — the regulator's usual concern with non-linear models."
    ))
    for f, _ in pdp_paths:
        md.append(md_h(3, f))
        md.append(md_image(f"plots/pdp_{f.replace('/', '_')[:40]}.png", f"PDP {f}"))

    md.append(md_h(2, "7. Reason codes — individual prediction decomposition"))
    md.append(md_para(
        "Three randomly-chosen policies from the test set, showing the top features that drove "
        "each prediction away from the portfolio baseline. For a GBM/XGB model these come from "
        "TreeSHAP (exact additive decomposition on the logit/link scale); for a GLM they are the "
        "xᵢ·βᵢ terms. In either case: if an underwriter or regulator asks "
        "**_'why is this quote £X?'_**, this table is the direct answer."
    ))
    for i, rc in enumerate(reason_rows, start=1):
        md.append(md_h(3, f"Sample {i}"))
        md.append(md_table(rc))

    md.append(md_h(2, "8. Protected-attribute audit"))
    md.append(md_para(
        "Bricksurance SE insures commercial entities, not individuals. The inputs to this "
        "model are all firmographic (sum_insured, years_trading, credit_risk_tier) or "
        "geographic (postcode-level flood, crime, deprivation deciles). The attributes below "
        "are **explicitly not used** — they are neither in the ingestion layer nor derivable "
        "from any input:"
    ))
    md.append(md_kv(PROTECTED_ATTRIBUTES))
    md.append(md_para(
        "Cross-region outcome analysis is performed in the companion report "
        "`09_fairness_and_robustness` — that notebook ranks prediction distributions by region "
        "and deprivation decile, and flags deviations beyond the expected risk differential."
    ))

    md.append(md_h(2, "9. Governance + audit trail"))
    md.append(md_kv({
        "Generated at":           RUN_TS,
        "UC model registry":      f"`{full}` v{version}",
        "Training notebook":      f"src/05_modelling/ — (see MLflow run for exact path)",
        "Training run (MLflow)":  latest.run_id,
        "Training run source":    latest.source,
        "Feature-table Delta":    f"v{feature_table_version}",
        "Silver-layer Delta":     ", ".join(f"{k}@{v}" for k, v in silver_versions.items()),
        "Raw-layer Delta":        ", ".join(f"{k}@{v}" for k, v in raw_versions.items()),
        "Report path":            out_dir,
    }))

    # Write report
    report_path = f"{out_dir}/report.md"
    with open(report_path, "w") as f:
        f.write("".join(md))
    print(f"  ✓ {report_path}  ({len(''.join(md)):,} chars)")

    # Also log to MLflow for the training run
    try:
        with mlflow.start_run(run_id=latest.run_id):
            mlflow.log_artifact(report_path, artifact_path="regulatory_report")
            for pth_name in ["calibration.png", "lift_curve.png", "shap_global.png"]:
                p = f"{plots_dir}/{pth_name}"
                if Path(p).exists():
                    mlflow.log_artifact(p, artifact_path="regulatory_report/plots")
    except Exception as e:
        print(f"  (MLflow artifact log skipped: {e})")

    return {
        "model_name": model_name,
        "version": version,
        "alias": alias,
        "gini": round(gini, 4),
        "total_ratio": round(tr, 4),
        "rmse": round(rmse, 2),
        "mae": round(mae, 2),
        "report_path": report_path,
        "shap_method": shap_method or "(none)",
        "test_rows": int(len(df)),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the report for each pricing model

# COMMAND ----------

# Feature lists mirror those in each trainer exactly
FREQ_NUM = [
    "sum_insured", "annual_turnover", "gross_premium",
    "credit_score", "ccj_count", "years_trading", "business_stability_score",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index",
    "subsidence_risk", "composite_location_risk", "frac_urban", "is_coastal",
    "imd_decile", "crime_decile", "income_decile", "health_decile",
    "market_median_rate", "competitor_a_min_rate", "price_index",
    "policy_age_years_at_exposure_start", "building_age_years_at_exposure_start",
]
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

FULL_NUM = [
    "sum_insured", "annual_turnover", "gross_premium",
    "credit_score", "ccj_count", "years_trading", "business_stability_score",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index",
    "subsidence_risk", "composite_location_risk", "frac_urban", "is_coastal",
    "imd_decile", "crime_decile", "income_decile", "health_decile", "living_env_decile",
    "market_median_rate", "competitor_a_min_rate", "price_index",
    "policy_age_years_at_exposure_start", "building_age_years_at_exposure_start",
]
FULL_CAT = ["region", "construction_type", "internal_risk_tier",
            "credit_risk_tier", "location_risk_tier", "division", "channel"]

DEMAND_NUM = [
    "buildings_si", "contents_si", "liability_si", "voluntary_excess",
    "gross_premium_quoted", "log_gross_premium", "log_buildings_si",
    "rate_per_1k_si", "vs_market_rate",
    "market_median_rate", "competitor_a_min_rate", "price_index",
    "annual_turnover", "credit_score", "business_stability_score",
    "flood_zone_rating", "crime_theft_index", "composite_location_risk",
    "frac_urban", "is_coastal", "imd_decile", "crime_decile",
    "year_built", "floor_area_sqm",
]
DEMAND_BOOL = ["sprinklered", "alarmed"]
DEMAND_CAT = ["region", "construction_type", "roof_type", "flood_zone",
              "channel", "internal_risk_tier", "credit_risk_tier", "division"]

# COMMAND ----------

results = []

# 1. freq_glm — Poisson, drop_first=True, no filter
results.append(report_model(
    model_name="freq_glm",
    alias="pricing_champion_frequency",
    target_col="claim_count_observed",
    target_description="claim count per policy-year",
    distribution="Poisson (log link) with log(exposure_fraction) offset",
    feature_table_fqn=FEATURE_POLICY_YEAR,
    feature_table_version=policy_year_dv,
    test_pdf=test_py,
    numeric_features=FREQ_NUM,
    categorical_features=FREQ_CAT,
    drop_first=True,
))

# 2. sev_glm — Gamma, claims > 0, drop_first=True
results.append(report_model(
    model_name="sev_glm",
    alias=None,  # loser in compound race — kept for comparison
    target_col="total_incurred_observed",  # actually mean severity; reported as incurred total
    target_description="mean cost per claim (claim-count weighted)",
    distribution="Gamma (log link), weights = claim_count_observed",
    feature_table_fqn=FEATURE_POLICY_YEAR,
    feature_table_version=policy_year_dv,
    test_pdf=test_py,
    numeric_features=SEV_GLM_NUM,
    categorical_features=SEV_GLM_CAT,
    drop_first=True,
    filter_rows="claim_count_observed > 0",
))

# 3. sev_gbm — LightGBM Gamma, drop_first=False
results.append(report_model(
    model_name="sev_gbm",
    alias="pricing_champion_severity",
    target_col="total_incurred_observed",
    target_description="mean cost per claim (claim-count weighted) — LightGBM challenger",
    distribution="Gamma (LightGBM tree ensemble)",
    feature_table_fqn=FEATURE_POLICY_YEAR,
    feature_table_version=policy_year_dv,
    test_pdf=test_py,
    numeric_features=FULL_NUM,
    categorical_features=FULL_CAT,
    drop_first=False,
    filter_rows="claim_count_observed > 0",
))

# 4. pure_premium_xgb — XGBoost Tweedie, drop_first=False
results.append(report_model(
    model_name="pure_premium_xgb",
    alias="pricing_champion_single",
    target_col="total_incurred_observed",
    target_description="expected loss cost per policy-year (full distribution, no compound)",
    distribution="Tweedie (XGBoost, variance_power=1.5)",
    feature_table_fqn=FEATURE_POLICY_YEAR,
    feature_table_version=policy_year_dv,
    test_pdf=test_py,
    numeric_features=FULL_NUM,
    categorical_features=FULL_CAT,
    drop_first=False,
))

# 5. demand_gbm — binary conversion
# One-hot bool features as plain numerics via the "numeric_features" slot
df_demand = test_q.copy()
for c in DEMAND_BOOL:
    df_demand[c] = df_demand[c].fillna(False).astype(int)

results.append(report_model(
    model_name="demand_gbm",
    alias="demand_champion",
    target_col="converted_flag",
    target_description="probability a quote converts to a bound policy",
    distribution="Binary LightGBM (logistic link)",
    feature_table_fqn=FEATURE_QUOTE,
    feature_table_version=quote_dv,
    test_pdf=df_demand,
    numeric_features=DEMAND_NUM + DEMAND_BOOL,
    categorical_features=DEMAND_CAT,
    drop_first=False,
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist a governance index — one row per (model, version, report)

# COMMAND ----------

gov_df = pd.DataFrame([r for r in results if r.get("status") != "missing"])
gov_df["_generated_at"] = RUN_TS
gov_table = f"{fqn}.model_governance"
spark.createDataFrame(gov_df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(gov_table)
spark.sql(f"""
    ALTER TABLE {gov_table} SET TBLPROPERTIES (
        'comment' = 'One row per generated regulatory report. Points to the report markdown in /Volumes/.../reports/ and records the UC version the report describes. Refreshed every time 08_generate_model_reports runs.'
    )
""")
print(f"✓ {gov_table}")
display(spark.table(gov_table))

# COMMAND ----------

import json as _json
dbutils.notebook.exit(_json.dumps(results, default=str))
