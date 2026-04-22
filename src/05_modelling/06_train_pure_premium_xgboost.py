# Databricks notebook source
# MAGIC %md
# MAGIC # Pure Premium XGBoost — Tweedie (alt framework, single-model approach)
# MAGIC
# MAGIC An alternative to the frequency × severity decomposition: model the expected
# MAGIC **loss cost per unit of exposure** directly. Tweedie compound distribution handles
# MAGIC the point mass at zero (no-claim years) plus the continuous positive tail (claim years).
# MAGIC
# MAGIC - **Target:** `total_incurred_observed` (total losses in the policy-year)
# MAGIC - **Weight:** `exposure_fraction` (proportion of the year actually on-risk)
# MAGIC - **Objective:** XGBoost `reg:tweedie` with variance power 1.5
# MAGIC - **Split:** temporal — train 2020–2023, test 2024

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

import numpy as np
import pandas as pd
import xgboost as xgb
import mlflow, mlflow.data, mlflow.xgboost
from sklearn.metrics import mean_absolute_error, mean_squared_error

mlflow.set_registry_uri("databricks-uc")
try:
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    mlflow.set_experiment(f"/Workspace/Users/{user}/pricing_workbench_pure_premium_xgb")
except Exception:
    pass

# COMMAND ----------

table = f"{fqn}.feature_policy_year_training"
hist = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()
delta_version = hist[0]["version"] if hist else None

numeric_features = [
    "sum_insured", "annual_turnover", "gross_premium",
    "credit_score", "ccj_count", "years_trading", "business_stability_score",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index",
    "subsidence_risk", "composite_location_risk", "frac_urban", "is_coastal",
    "imd_decile", "crime_decile", "income_decile", "health_decile", "living_env_decile",
    "market_median_rate", "competitor_a_min_rate", "price_index",
    "policy_age_years_at_exposure_start", "building_age_years_at_exposure_start",
]
categorical_features = ["region", "construction_type", "internal_risk_tier",
                        "credit_risk_tier", "location_risk_tier", "division", "channel"]

pdf = spark.table(table).select(
    "policy_id", "policy_version", "exposure_year",
    "total_incurred_observed", "exposure_fraction",
    *numeric_features, *categorical_features,
).toPandas()

for c in numeric_features:
    pdf[c] = pd.to_numeric(pdf[c], errors="coerce")
pdf["total_incurred_observed"] = pdf["total_incurred_observed"].fillna(0.0).clip(lower=0.0)
pdf["exposure_fraction"] = pdf["exposure_fraction"].clip(lower=0.01).fillna(1.0)

# One-hot for XGBoost (no native categorical until 2.0; keep portable)
pdf_cat = pd.get_dummies(pdf[categorical_features].fillna("UNK"), drop_first=False, dtype=float)
feature_cols = numeric_features + pdf_cat.columns.tolist()
X_all = pd.concat([pdf[numeric_features].reset_index(drop=True),
                   pdf_cat.reset_index(drop=True)], axis=1)

train_mask = pdf["exposure_year"].isin([2020, 2021, 2022, 2023])
test_mask  = pdf["exposure_year"] == 2024

X_train = X_all[train_mask].values
X_test  = X_all[test_mask].values
y_train = pdf.loc[train_mask, "total_incurred_observed"].values
y_test  = pdf.loc[test_mask,  "total_incurred_observed"].values
w_train = pdf.loc[train_mask, "exposure_fraction"].values
w_test  = pdf.loc[test_mask,  "exposure_fraction"].values

print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
print(f"Train zero-loss rate: {(y_train == 0).mean():.3f}")

# COMMAND ----------

# Monotonicity priors — same regulatory discipline as sev_gbm. XGBoost expects the
# constraints as a bracketed tuple-string in feature-column order.
MONO_PRIORS = {
    "flood_zone_rating":        1,
    "crime_theft_index":        1,
    "subsidence_risk":          1,
    "composite_location_risk":  1,
    "credit_score":            -1,
    "business_stability_score":-1,
    "ccj_count":                1,
    "years_trading":           -1,
    "imd_decile":              -1,
    "building_age_years_at_exposure_start": 1,
    "proximity_to_fire_station_km":         1,
}
mono_tuple = "(" + ",".join(str(MONO_PRIORS.get(c, 0)) for c in feature_cols) + ")"

params = {
    "objective": "reg:tweedie",
    "tweedie_variance_power": 1.5,
    "eval_metric": "tweedie-nloglik@1.5",
    "learning_rate": 0.05,
    "max_depth": 6,
    "min_child_weight": 5,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
    "lambda": 1.0,
    "tree_method": "hist",
    "monotone_constraints": mono_tuple,
}

dtrain = xgb.DMatrix(X_train, label=y_train, weight=w_train, feature_names=feature_cols)
dtest  = xgb.DMatrix(X_test,  label=y_test,  weight=w_test,  feature_names=feature_cols)

with mlflow.start_run(run_name="pure_premium_xgb_tweedie") as run:
    mlflow.log_params({**params,
        "model_type": "XGBoost_Tweedie",
        "tweedie_variance_power": 1.5,
        "train_rows": int(len(X_train)),
        "test_rows":  int(len(X_test)),
        "n_features": len(feature_cols),
        "source_table": table,
        "source_delta_version": delta_version,
        "split": "temporal_2020-2023_vs_2024",
    })
    try:
        ds = mlflow.data.from_spark(spark.table(table), table_name=table, version=str(delta_version))
        mlflow.log_input(ds, context="training")
    except Exception as e:
        print(f"mlflow.data.from_spark skipped: {e}")
    mlflow.set_tag("feature_table", table)

    booster = xgb.train(
        params, dtrain,
        num_boost_round=600,
        evals=[(dtrain, "train"), (dtest, "valid")],
        early_stopping_rounds=40,
        verbose_eval=50,
    )

    y_pred = booster.predict(dtest)
    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred, sample_weight=w_test)))
    mae  = float(mean_absolute_error(y_test, y_pred, sample_weight=w_test))
    # Total-loss comparison — a key pricing metric: does the model track portfolio losses?
    total_actual    = float((y_test * 1.0).sum())
    total_predicted = float((y_pred * 1.0).sum())

    mlflow.log_metrics({
        "rmse": rmse, "mae": mae,
        "best_iteration": float(booster.best_iteration),
        "pred_total": total_predicted,
        "actual_total": total_actual,
        "total_ratio": total_predicted / total_actual if total_actual > 0 else 0.0,
        "pred_mean": float(y_pred.mean()),
        "actual_mean": float(y_test.mean()),
    })

    # Importance as artifact
    imp = pd.DataFrame(
        booster.get_score(importance_type="gain").items(),
        columns=["feature", "gain"],
    ).sort_values("gain", ascending=False)
    imp.to_csv("/tmp/pure_premium_xgb_importance.csv", index=False)
    mlflow.log_artifact("/tmp/pure_premium_xgb_importance.csv")

    sample_in = X_all.iloc[:5].copy()
    signature = mlflow.models.infer_signature(
        sample_in,
        booster.predict(xgb.DMatrix(sample_in.values, feature_names=feature_cols)),
    )
    mlflow.xgboost.log_model(
        xgb_model=booster,
        artifact_path="model",
        signature=signature,
        registered_model_name=f"{catalog}.{schema}.pure_premium_xgb",
        input_example=sample_in,
    )
    print(f"Pure Premium XGB — RMSE £{rmse:,.0f} | MAE £{mae:,.0f} | total ratio {total_predicted/total_actual:.3f}")
    print(f"Run: {run.info.run_id}")

# COMMAND ----------

display(spark.createDataFrame(imp.head(30)))
