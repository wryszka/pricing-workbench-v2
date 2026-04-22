# Databricks notebook source
# MAGIC %md
# MAGIC # Severity GBM — LightGBM Gamma regression (challenger to severity GLM)
# MAGIC
# MAGIC The GBM captures interactions a GLM cannot (construction × flood zone × decile,
# MAGIC non-linear age effects). Uses the same training grain and temporal split as the
# MAGIC severity GLM, so lift vs champion is apples-to-apples.
# MAGIC
# MAGIC - **Target:** mean severity where `claim_count_observed > 0`
# MAGIC - **Objective:** `gamma` (log link; Gamma deviance)
# MAGIC - **Weights:** `claim_count_observed`

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

import numpy as np
import pandas as pd
import lightgbm as lgb
import mlflow, mlflow.data, mlflow.lightgbm
from sklearn.metrics import mean_absolute_error, mean_squared_error

mlflow.set_registry_uri("databricks-uc")
try:
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    mlflow.set_experiment(f"/Workspace/Users/{user}/pricing_workbench_severity_gbm")
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

pdf = spark.table(table).filter("claim_count_observed > 0").select(
    "policy_id", "policy_version", "exposure_year",
    "claim_count_observed", "total_incurred_observed",
    *numeric_features, *categorical_features,
).toPandas()

pdf["mean_severity"] = pdf["total_incurred_observed"] / pdf["claim_count_observed"]
pdf = pdf[pdf["mean_severity"] > 0].copy()

for c in numeric_features:
    pdf[c] = pd.to_numeric(pdf[c], errors="coerce").fillna(0.0)
# One-hot encode to give pyfunc a lossless DataFrame round-trip at serving time.
pdf_cat = pd.get_dummies(pdf[categorical_features].fillna("UNK"), drop_first=False, dtype=float)
feature_cols = numeric_features + pdf_cat.columns.tolist()
X_all = pd.concat([pdf[numeric_features].reset_index(drop=True),
                   pdf_cat.reset_index(drop=True)], axis=1)

train_mask = pdf["exposure_year"].isin([2020, 2021, 2022, 2023])
test_mask  = pdf["exposure_year"] == 2024

X_train, y_train = X_all[train_mask], pdf.loc[train_mask, "mean_severity"].values
X_test,  y_test  = X_all[test_mask],  pdf.loc[test_mask,  "mean_severity"].values
w_train = pdf.loc[train_mask, "claim_count_observed"].values.astype(float)
w_test  = pdf.loc[test_mask,  "claim_count_observed"].values.astype(float)

print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")

# COMMAND ----------

# Monotonicity priors (regulatory defensibility). For features where we have a strong
# actuarial prior on the direction, we constrain the GBM to respect it. Unconstrained
# features are left at 0 for the data to drive. Dummy (one-hot) columns are not
# constrained so that the model can still pick up categorical interactions.
MONO_PRIORS = {
    "flood_zone_rating":        1,   # higher flood zone → higher loss
    "crime_theft_index":        1,   # higher crime → higher theft loss
    "subsidence_risk":          1,   # higher subsidence → higher subsidence loss
    "composite_location_risk":  1,   # composite hazard index
    "credit_score":            -1,   # higher credit → lower claims (financial distress proxy)
    "business_stability_score":-1,   # more stable business → fewer claims
    "ccj_count":                1,   # more CCJs → more financial distress
    "years_trading":           -1,   # longer trading → fewer teething claims
    "imd_decile":              -1,   # higher decile (less deprived) → lower claims
    "building_age_years_at_exposure_start": 1,  # older building → more losses
    "proximity_to_fire_station_km":         1,  # farther from fire station → more fire loss
}
monotone_list = [MONO_PRIORS.get(c, 0) for c in feature_cols]

params = {
    "objective": "gamma",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "max_depth": -1,
    "min_child_samples": 50,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.9,
    "bagging_freq": 5,
    "lambda_l2": 1.0,
    "monotone_constraints": monotone_list,
    "monotone_constraints_method": "advanced",
    "verbose": -1,
}

with mlflow.start_run(run_name="sev_gbm_gamma") as run:
    mlflow.log_params({**params,
        "model_type": "LightGBM_Gamma",
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

    train_ds = lgb.Dataset(X_train, label=y_train, weight=w_train)
    valid_ds = lgb.Dataset(X_test,  label=y_test,  weight=w_test,  reference=train_ds)

    model = lgb.train(
        params,
        train_ds,
        num_boost_round=500,
        valid_sets=[train_ds, valid_ds],
        valid_names=["train", "valid"],
        callbacks=[lgb.early_stopping(30), lgb.log_evaluation(50)],
    )

    y_pred_test = model.predict(X_test)
    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred_test, sample_weight=w_test)))
    mae  = float(mean_absolute_error(y_test, y_pred_test, sample_weight=w_test))
    eps = 1e-9
    d = 2 * np.sum(w_test * (-np.log((y_test + eps) / (y_pred_test + eps)) + (y_test - y_pred_test) / (y_pred_test + eps)))
    dev = float(d / w_test.sum())

    mlflow.log_metrics({
        "rmse": rmse, "mae": mae, "gamma_deviance_mean": dev,
        "best_iteration": float(model.best_iteration or 0),
        "pred_mean": float((y_pred_test * w_test).sum() / w_test.sum()),
        "actual_mean": float((y_test * w_test).sum() / w_test.sum()),
    })

    # Feature importance artifact
    imp = pd.DataFrame({
        "feature": model.feature_name(),
        "gain": model.feature_importance(importance_type="gain"),
        "split": model.feature_importance(importance_type="split"),
    }).sort_values("gain", ascending=False)
    imp.to_csv("/tmp/sev_gbm_importance.csv", index=False)
    mlflow.log_artifact("/tmp/sev_gbm_importance.csv")

    signature = mlflow.models.infer_signature(X_all.iloc[:5], model.predict(X_all.iloc[:5]))
    mlflow.lightgbm.log_model(
        lgb_model=model,
        artifact_path="model",
        signature=signature,
        registered_model_name=f"{catalog}.{schema}.sev_gbm",
        input_example=X_all.iloc[:5],
    )
    print(f"Sev GBM — RMSE £{rmse:,.0f} | MAE £{mae:,.0f} | Gamma Dev {dev:.4f} | best_iter {model.best_iteration}")
    print(f"Run: {run.info.run_id}")

# COMMAND ----------

display(spark.createDataFrame(imp.head(30)))
