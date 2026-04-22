# Databricks notebook source
# MAGIC %md
# MAGIC # Frequency GLM — Poisson with exposure offset
# MAGIC
# MAGIC Predicts expected claim **count** per policy-year.
# MAGIC
# MAGIC - **Target:** `claim_count_observed`
# MAGIC - **Offset:** `log(exposure_fraction)` — handles mid-term cancellations and short first years
# MAGIC - **Distribution:** Poisson (log link)
# MAGIC - **Split:** temporal — train 2020–2023, test 2024 (the industry-standard backtest)
# MAGIC
# MAGIC A Poisson GLM gives additive, transparent relativities — exactly what underwriters,
# MAGIC actuaries and regulators expect to see. The exposure offset means the model predicts
# MAGIC a rate; multiplying by exposure recovers the expected count.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")

catalog = dbutils.widgets.get("catalog_name")
schema  = dbutils.widgets.get("schema_name")
fqn     = f"{catalog}.{schema}"

# COMMAND ----------

import numpy as np
import pandas as pd
import statsmodels.api as sm
import mlflow
import mlflow.data
import mlflow.sklearn
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.metrics import mean_absolute_error, mean_squared_error

mlflow.set_registry_uri("databricks-uc")
try:
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    mlflow.set_experiment(f"/Workspace/Users/{user}/pricing_workbench_frequency_glm")
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load training data + capture Delta version (for lineage)

# COMMAND ----------

table = f"{fqn}.feature_policy_year_training"
hist = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()
delta_version = hist[0]["version"] if hist else None
print(f"Training from {table} (Delta version {delta_version})")

numeric_features = [
    "sum_insured", "annual_turnover", "gross_premium",
    "credit_score", "ccj_count", "years_trading", "business_stability_score",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index",
    "subsidence_risk", "composite_location_risk", "frac_urban", "is_coastal",
    "imd_decile", "crime_decile", "income_decile", "health_decile",
    "market_median_rate", "competitor_a_min_rate", "price_index",
    "policy_age_years_at_exposure_start", "building_age_years_at_exposure_start",
]
categorical_features = ["region", "construction_type", "internal_risk_tier",
                        "credit_risk_tier", "location_risk_tier", "channel"]

key_cols = ["policy_id", "policy_version", "exposure_year"]
label_cols = ["claim_count_observed", "exposure_fraction"]

pdf = spark.table(table).select(*key_cols, *label_cols, *numeric_features, *categorical_features).toPandas()

# Coerce numerics, fill nulls
pdf[numeric_features] = pdf[numeric_features].apply(pd.to_numeric, errors="coerce").fillna(0.0)
pdf["claim_count_observed"] = pdf["claim_count_observed"].fillna(0).astype(float)
pdf["exposure_fraction"] = pdf["exposure_fraction"].clip(lower=0.01).fillna(1.0)

# One-hot encode categoricals (drop_first to avoid collinearity)
pdf_cat = pd.get_dummies(pdf[categorical_features].fillna("UNK"), drop_first=True, dtype=float)
feature_cols = numeric_features + pdf_cat.columns.tolist()
X_all = pd.concat([pdf[numeric_features].reset_index(drop=True),
                   pdf_cat.reset_index(drop=True)], axis=1)

print(f"Total rows: {len(pdf):,} | features: {len(feature_cols)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporal split — 2020–2023 train, 2024 test

# COMMAND ----------

train_mask = pdf["exposure_year"].isin([2020, 2021, 2022, 2023])
test_mask  = pdf["exposure_year"] == 2024

X_train = X_all[train_mask].values
X_test  = X_all[test_mask].values
y_train = pdf.loc[train_mask, "claim_count_observed"].values
y_test  = pdf.loc[test_mask,  "claim_count_observed"].values
exp_train = pdf.loc[train_mask, "exposure_fraction"].values
exp_test  = pdf.loc[test_mask,  "exposure_fraction"].values

print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
print(f"Train mean claim count: {y_train.mean():.4f} | Test: {y_test.mean():.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit Poisson GLM with `log(exposure)` offset

# COMMAND ----------

X_train_c = sm.add_constant(X_train, has_constant="add")
X_test_c  = sm.add_constant(X_test,  has_constant="add")
offset_train = np.log(exp_train)
offset_test  = np.log(exp_test)

with mlflow.start_run(run_name="freq_glm_poisson") as run:
    mlflow.log_params({
        "model_type": "GLM_Poisson",
        "link": "log",
        "offset": "log(exposure_fraction)",
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

    glm = sm.GLM(
        y_train, X_train_c,
        family=sm.families.Poisson(link=sm.families.links.Log()),
        offset=offset_train,
    )
    res = glm.fit(maxiter=50)

    y_pred_test = res.predict(X_test_c, offset=offset_test)

    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred_test)))
    mae  = float(mean_absolute_error(y_test, y_pred_test))
    # Poisson deviance on test
    eps = 1e-9
    dev = 2 * np.sum(np.where(y_test > 0, y_test * np.log((y_test + eps) / (y_pred_test + eps)), 0) - (y_test - y_pred_test))
    dev = float(dev / len(y_test))

    mlflow.log_metrics({
        "rmse": rmse, "mae": mae, "poisson_deviance_mean": dev,
        "aic": float(res.aic), "bic": float(res.bic),
        "pred_mean": float(y_pred_test.mean()), "actual_mean": float(y_test.mean()),
    })

    # Model summary as artifact
    with open("/tmp/freq_glm_summary.txt", "w") as f:
        f.write(str(res.summary()))
    mlflow.log_artifact("/tmp/freq_glm_summary.txt")

    class PoissonGLMWrapper(BaseEstimator, RegressorMixin):
        """Minimal sklearn-compatible wrapper; predict() expects features + an `offset`
        column. If absent, assumes full-year exposure."""
        def __init__(self, result, feature_names):
            self.result = result
            self.feature_names = feature_names
        def fit(self, X, y):
            return self
        def predict(self, X):
            if isinstance(X, pd.DataFrame):
                offset = np.log(X["exposure_fraction"].clip(lower=0.01).values) if "exposure_fraction" in X.columns else np.zeros(len(X))
                X_vals = X[self.feature_names].values if all(c in X.columns for c in self.feature_names) else X.values
            else:
                X_vals, offset = X, np.zeros(len(X))
            X_c = sm.add_constant(X_vals, has_constant="add")
            return self.result.predict(X_c, offset=offset)

    wrapper = PoissonGLMWrapper(res, feature_cols)
    signature = mlflow.models.infer_signature(
        X_all.iloc[:5], wrapper.predict(X_all.iloc[:5].values),
    )
    mlflow.sklearn.log_model(
        sk_model=wrapper,
        artifact_path="model",
        signature=signature,
        registered_model_name=f"{catalog}.{schema}.freq_glm",
        input_example=X_all.iloc[:5],
    )

    run_id = run.info.run_id

print(f"Freq GLM — RMSE {rmse:.4f} | MAE {mae:.4f} | Poisson Dev {dev:.4f}")
print(f"Run: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relativities — exponentiated coefficients

# COMMAND ----------

rel_rows = []
for name, coef, pval in zip(["intercept"] + feature_cols, res.params, res.pvalues):
    rel_rows.append({
        "feature": name,
        "coefficient": round(float(coef), 6),
        "relativity": round(float(np.exp(coef)), 4),
        "p_value": round(float(pval), 6),
        "significant": pval < 0.05,
    })
rel_df = spark.createDataFrame(pd.DataFrame(rel_rows))
display(rel_df.orderBy("p_value"))
