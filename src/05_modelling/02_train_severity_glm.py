# Databricks notebook source
# MAGIC %md
# MAGIC # Severity GLM — Gamma regression on mean claim size
# MAGIC
# MAGIC Predicts expected **cost per claim** given that a claim has occurred. Multiplied
# MAGIC by frequency, it gives the pure premium (expected losses per policy-year).
# MAGIC
# MAGIC - **Target:** `total_incurred_observed / claim_count_observed` (mean severity)
# MAGIC - **Filter:** `claim_count_observed > 0` (Gamma is defined on strictly positive support)
# MAGIC - **Distribution:** Gamma (log link)
# MAGIC - **Weights:** `claim_count_observed` — rows with more claims carry more weight
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
import statsmodels.api as sm
import mlflow, mlflow.data, mlflow.sklearn
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.metrics import mean_absolute_error, mean_squared_error

mlflow.set_registry_uri("databricks-uc")
try:
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    mlflow.set_experiment(f"/Workspace/Users/{user}/pricing_workbench_severity_glm")
except Exception:
    pass

# COMMAND ----------

table = f"{fqn}.feature_policy_year_training"
hist = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()
delta_version = hist[0]["version"] if hist else None

numeric_features = [
    "sum_insured", "annual_turnover", "gross_premium",
    "credit_score", "years_trading", "business_stability_score",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index",
    "subsidence_risk", "composite_location_risk", "frac_urban",
    "imd_decile", "living_env_decile",
    "market_median_rate", "price_index",
    "building_age_years_at_exposure_start",
]
categorical_features = ["region", "construction_type", "internal_risk_tier",
                        "credit_risk_tier", "location_risk_tier"]

pdf = spark.table(table).filter("claim_count_observed > 0").select(
    "policy_id", "policy_version", "exposure_year",
    "claim_count_observed", "total_incurred_observed",
    *numeric_features, *categorical_features,
).toPandas()

pdf["mean_severity"] = pdf["total_incurred_observed"] / pdf["claim_count_observed"]
pdf = pdf[pdf["mean_severity"] > 0].copy()  # Gamma needs strictly positive

pdf[numeric_features] = pdf[numeric_features].apply(pd.to_numeric, errors="coerce").fillna(0.0)
pdf_cat = pd.get_dummies(pdf[categorical_features].fillna("UNK"), drop_first=True, dtype=float)
feature_cols = numeric_features + pdf_cat.columns.tolist()
X_all = pd.concat([pdf[numeric_features].reset_index(drop=True),
                   pdf_cat.reset_index(drop=True)], axis=1)

print(f"Rows with ≥1 claim: {len(pdf):,} | features: {len(feature_cols)}")
print(f"Mean severity: £{pdf['mean_severity'].mean():,.0f} | median: £{pdf['mean_severity'].median():,.0f}")

# COMMAND ----------

train_mask = pdf["exposure_year"].isin([2020, 2021, 2022, 2023])
test_mask  = pdf["exposure_year"] == 2024

X_train = X_all[train_mask].values
X_test  = X_all[test_mask].values
y_train = pdf.loc[train_mask, "mean_severity"].values
y_test  = pdf.loc[test_mask,  "mean_severity"].values
w_train = pdf.loc[train_mask, "claim_count_observed"].values.astype(float)
w_test  = pdf.loc[test_mask,  "claim_count_observed"].values.astype(float)

print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")

# COMMAND ----------

X_train_c = sm.add_constant(X_train, has_constant="add")
X_test_c  = sm.add_constant(X_test,  has_constant="add")

with mlflow.start_run(run_name="sev_glm_gamma") as run:
    mlflow.log_params({
        "model_type": "GLM_Gamma",
        "link": "log",
        "weights": "claim_count_observed",
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
        family=sm.families.Gamma(link=sm.families.links.Log()),
        freq_weights=w_train,
    )
    res = glm.fit(maxiter=100)
    y_pred_test = res.predict(X_test_c)

    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred_test, sample_weight=w_test)))
    mae  = float(mean_absolute_error(y_test, y_pred_test, sample_weight=w_test))
    # Gamma deviance
    eps = 1e-9
    d = 2 * np.sum(w_test * (-np.log((y_test + eps) / (y_pred_test + eps)) + (y_test - y_pred_test) / (y_pred_test + eps)))
    dev = float(d / w_test.sum())

    mlflow.log_metrics({
        "rmse": rmse, "mae": mae, "gamma_deviance_mean": dev,
        "aic": float(res.aic), "pred_mean": float((y_pred_test * w_test).sum() / w_test.sum()),
        "actual_mean": float((y_test * w_test).sum() / w_test.sum()),
    })

    with open("/tmp/sev_glm_summary.txt", "w") as f:
        f.write(str(res.summary()))
    mlflow.log_artifact("/tmp/sev_glm_summary.txt")

    class GammaGLMWrapper(BaseEstimator, RegressorMixin):
        def __init__(self, result, feature_names):
            self.result = result
            self.feature_names = feature_names
        def fit(self, X, y):
            return self
        def predict(self, X):
            X_vals = X[self.feature_names].values if isinstance(X, pd.DataFrame) else X
            X_c = sm.add_constant(X_vals, has_constant="add")
            return self.result.predict(X_c)

    wrapper = GammaGLMWrapper(res, feature_cols)
    signature = mlflow.models.infer_signature(X_all.iloc[:5], wrapper.predict(X_all.iloc[:5].values))
    mlflow.sklearn.log_model(
        sk_model=wrapper,
        artifact_path="model",
        signature=signature,
        registered_model_name=f"{catalog}.{schema}.sev_glm",
        input_example=X_all.iloc[:5],
    )
    print(f"Sev GLM — RMSE £{rmse:,.0f} | MAE £{mae:,.0f} | Gamma Dev {dev:.4f}")
    print(f"Run: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relativities

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
display(spark.createDataFrame(pd.DataFrame(rel_rows)).orderBy("p_value"))
