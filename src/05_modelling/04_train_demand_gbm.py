# Databricks notebook source
# MAGIC %md
# MAGIC # Demand GBM — Quote conversion probability
# MAGIC
# MAGIC Predicts the probability a quote converts to a bound policy given the quoted
# MAGIC premium, the competitive environment (market_median_rate), and the prospect's
# MAGIC risk/firmographic profile. Drives the *price-optimisation* half of the pricing
# MAGIC engine — without demand, you're only predicting losses, not optimising margin.
# MAGIC
# MAGIC - **Source:** `feature_quote_training` (one row per quote, 4-year quote stream)
# MAGIC - **Target:** `converted_flag` (0/1)
# MAGIC - **Split:** temporal — train through 2023, test on 2024 quotes

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
from sklearn.metrics import roc_auc_score, log_loss, average_precision_score

mlflow.set_registry_uri("databricks-uc")
try:
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    mlflow.set_experiment(f"/Workspace/Users/{user}/pricing_workbench_demand_gbm")
except Exception:
    pass

# COMMAND ----------

table = f"{fqn}.feature_quote_training"
hist = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()
delta_version = hist[0]["version"] if hist else None

numeric_features = [
    "buildings_si", "contents_si", "liability_si", "voluntary_excess",
    "gross_premium_quoted", "log_gross_premium", "log_buildings_si",
    "rate_per_1k_si", "vs_market_rate",
    "market_median_rate", "competitor_a_min_rate", "price_index",
    "annual_turnover", "credit_score", "business_stability_score",
    "flood_zone_rating", "crime_theft_index", "composite_location_risk",
    "frac_urban", "is_coastal", "imd_decile", "crime_decile",
    "year_built", "floor_area_sqm",
]
bool_features = ["sprinklered", "alarmed"]
categorical_features = ["region", "construction_type", "roof_type", "flood_zone",
                        "channel", "internal_risk_tier", "credit_risk_tier",
                        "division"]

pdf = spark.table(table).select(
    "quote_id", "quote_date", "quote_year", "converted_flag",
    *numeric_features, *bool_features, *categorical_features,
).toPandas()

pdf["converted_flag"] = pdf["converted_flag"].fillna(0).astype(int)
for c in numeric_features:
    pdf[c] = pd.to_numeric(pdf[c], errors="coerce").fillna(0.0)
for c in bool_features:
    pdf[c] = pdf[c].fillna(False).astype(int)
pdf_cat = pd.get_dummies(pdf[categorical_features].fillna("UNK"), drop_first=False, dtype=float)
feature_cols = numeric_features + bool_features + pdf_cat.columns.tolist()
X_all = pd.concat([pdf[numeric_features + bool_features].reset_index(drop=True),
                   pdf_cat.reset_index(drop=True)], axis=1)
y_all = pdf["converted_flag"].values

print(f"Rows: {len(pdf):,} | conversion rate: {y_all.mean():.3f}")

# COMMAND ----------

train_mask = pdf["quote_year"] <= 2023
test_mask  = pdf["quote_year"] == 2024

X_train, y_train = X_all[train_mask], y_all[train_mask.values]
X_test,  y_test  = X_all[test_mask],  y_all[test_mask.values]

print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
print(f"Train conv rate: {y_train.mean():.3f} | Test: {y_test.mean():.3f}")

# COMMAND ----------

params = {
    "objective": "binary",
    "metric": ["binary_logloss", "auc"],
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_child_samples": 100,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.9,
    "bagging_freq": 5,
    "lambda_l2": 1.0,
    "verbose": -1,
}

with mlflow.start_run(run_name="demand_gbm") as run:
    mlflow.log_params({**params,
        "model_type": "LightGBM_Binary",
        "train_rows": int(len(X_train)),
        "test_rows":  int(len(X_test)),
        "n_features": len(feature_cols),
        "source_table": table,
        "source_delta_version": delta_version,
        "split": "temporal_<=2023_vs_2024",
    })
    try:
        ds = mlflow.data.from_spark(spark.table(table), table_name=table, version=str(delta_version))
        mlflow.log_input(ds, context="training")
    except Exception as e:
        print(f"mlflow.data.from_spark skipped: {e}")
    mlflow.set_tag("feature_table", table)

    train_ds = lgb.Dataset(X_train, label=y_train)
    valid_ds = lgb.Dataset(X_test,  label=y_test,  reference=train_ds)

    model = lgb.train(
        params,
        train_ds,
        num_boost_round=800,
        valid_sets=[train_ds, valid_ds],
        valid_names=["train", "valid"],
        callbacks=[lgb.early_stopping(40), lgb.log_evaluation(100)],
    )

    y_pred_test = model.predict(X_test)
    auc      = float(roc_auc_score(y_test, y_pred_test))
    ap       = float(average_precision_score(y_test, y_pred_test))
    logloss  = float(log_loss(y_test, np.clip(y_pred_test, 1e-7, 1 - 1e-7)))
    brier    = float(np.mean((y_pred_test - y_test) ** 2))

    mlflow.log_metrics({
        "auc": auc, "avg_precision": ap, "logloss": logloss, "brier": brier,
        "best_iteration": float(model.best_iteration or 0),
        "pred_mean": float(y_pred_test.mean()),
        "actual_conversion": float(y_test.mean()),
    })

    imp = pd.DataFrame({
        "feature": model.feature_name(),
        "gain": model.feature_importance(importance_type="gain"),
        "split": model.feature_importance(importance_type="split"),
    }).sort_values("gain", ascending=False)
    imp.to_csv("/tmp/demand_gbm_importance.csv", index=False)
    mlflow.log_artifact("/tmp/demand_gbm_importance.csv")

    signature = mlflow.models.infer_signature(X_all.iloc[:5], model.predict(X_all.iloc[:5]))
    mlflow.lightgbm.log_model(
        lgb_model=model,
        artifact_path="model",
        signature=signature,
        registered_model_name=f"{catalog}.{schema}.demand_gbm",
        input_example=X_all.iloc[:5],
    )
    print(f"Demand GBM — AUC {auc:.4f} | AP {ap:.4f} | LogLoss {logloss:.4f} | best_iter {model.best_iteration}")
    print(f"Run: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Price elasticity curve — predicted conversion vs rate-to-market ratio

# COMMAND ----------

elast_pdf = pdf[test_mask].copy()
elast_pdf["predicted_conversion"] = y_pred_test
elast_pdf["rate_to_market_bucket"] = pd.cut(
    elast_pdf["vs_market_rate"],
    bins=[-np.inf, 0.7, 0.85, 0.95, 1.05, 1.15, 1.3, np.inf],
    labels=["<0.70", "0.70-0.85", "0.85-0.95", "0.95-1.05", "1.05-1.15", "1.15-1.30", ">1.30"],
)
elast = elast_pdf.groupby("rate_to_market_bucket", observed=True).agg(
    n=("converted_flag", "size"),
    actual=("converted_flag", "mean"),
    predicted=("predicted_conversion", "mean"),
).reset_index()
display(spark.createDataFrame(elast))

# COMMAND ----------

display(spark.createDataFrame(imp.head(30)))
