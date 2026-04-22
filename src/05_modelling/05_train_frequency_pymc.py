# Databricks notebook source
# MAGIC %md
# MAGIC # Frequency — PyMC Bayesian Poisson (alt framework)
# MAGIC
# MAGIC Showcase of a second, non-sklearn/non-statsmodels framework running on the same
# MAGIC feature table. Partial pooling on `region` — each region gets its own intercept
# MAGIC drawn from a global prior, so low-exposure regions borrow strength from the book.
# MAGIC
# MAGIC Trains on a **stratified subsample** of the training data (PyMC is slow vs GLM
# MAGIC but produces full posterior distributions, which are valuable for capital / VaR
# MAGIC calculations). Not registered to UC — logged as a research artefact only.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",  "pricing_workbench")
dbutils.widgets.text("sample_size",  "10000")
dbutils.widgets.text("draws",        "500")

catalog      = dbutils.widgets.get("catalog_name")
schema       = dbutils.widgets.get("schema_name")
SAMPLE_SIZE  = int(dbutils.widgets.get("sample_size"))
DRAWS        = int(dbutils.widgets.get("draws"))
fqn          = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %pip install pymc arviz --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# (widgets lost after restartPython — re-read them)
catalog      = dbutils.widgets.get("catalog_name")
schema       = dbutils.widgets.get("schema_name")
SAMPLE_SIZE  = int(dbutils.widgets.get("sample_size"))
DRAWS        = int(dbutils.widgets.get("draws"))
fqn          = f"{catalog}.{schema}"

import numpy as np
import pandas as pd
import pymc as pm
import arviz as az
import mlflow
import mlflow.data

mlflow.set_registry_uri("databricks-uc")
try:
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    mlflow.set_experiment(f"/Workspace/Users/{user}/pricing_workbench_frequency_pymc")
except Exception:
    pass

print(f"PyMC {pm.__version__}")

# COMMAND ----------

table = f"{fqn}.feature_policy_year_training"
hist = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()
delta_version = hist[0]["version"] if hist else None

# Smallish, curated feature set for the Bayesian fit — interpretable priors matter here
numeric_features = [
    "credit_score", "business_stability_score",
    "flood_zone_rating", "crime_theft_index", "composite_location_risk",
    "imd_decile", "market_median_rate",
    "building_age_years_at_exposure_start",
]

base = spark.table(table).filter("exposure_year <= 2023").select(
    "policy_id", "exposure_year", "region",
    "claim_count_observed", "exposure_fraction",
    *numeric_features,
).toPandas()

base[numeric_features] = base[numeric_features].apply(pd.to_numeric, errors="coerce").fillna(0.0)
base["claim_count_observed"] = base["claim_count_observed"].fillna(0).astype(int)
base["exposure_fraction"] = base["exposure_fraction"].clip(lower=0.01).fillna(1.0)
base["region"] = base["region"].fillna("UNK").astype("category")

# Stratified sample by region preserves the regional mix in a smaller dataset
pdf = base.groupby("region", observed=True, group_keys=False).apply(
    lambda g: g.sample(n=min(len(g), max(200, SAMPLE_SIZE // len(base["region"].unique()))), random_state=42)
).reset_index(drop=True)

print(f"Sampled {len(pdf):,} rows from {len(base):,}")

# Standardise numerics for stable sampling
means, stds = pdf[numeric_features].mean(), pdf[numeric_features].std().replace(0, 1)
X = ((pdf[numeric_features] - means) / stds).values
region_idx = pdf["region"].cat.codes.values
regions = list(pdf["region"].cat.categories)
y = pdf["claim_count_observed"].values
offset = np.log(pdf["exposure_fraction"].values)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hierarchical Poisson model
# MAGIC
# MAGIC - Global intercept μ
# MAGIC - Per-region intercept α_r ~ Normal(μ, σ_region) (partial pooling)
# MAGIC - Fixed-effect coefficients β ~ Normal(0, 1) on standardised features
# MAGIC - λ = exp(offset + α_r + Xβ), y ~ Poisson(λ)

# COMMAND ----------

with pm.Model() as model:
    mu         = pm.Normal("mu", mu=-2.5, sigma=1.0)
    sigma_reg  = pm.HalfNormal("sigma_region", sigma=0.5)
    alpha_reg  = pm.Normal("alpha_region", mu=mu, sigma=sigma_reg, shape=len(regions))
    beta       = pm.Normal("beta", mu=0.0, sigma=1.0, shape=X.shape[1])
    linpred    = offset + alpha_reg[region_idx] + pm.math.dot(X, beta)
    lam        = pm.math.exp(linpred)
    _          = pm.Poisson("y_obs", mu=lam, observed=y)

    with mlflow.start_run(run_name="freq_pymc_bayesian") as run:
        mlflow.log_params({
            "model_type": "PyMC_Poisson_Hierarchical",
            "draws": DRAWS,
            "tune": DRAWS,
            "chains": 2,
            "n_regions": len(regions),
            "n_features": len(numeric_features),
            "sample_rows": int(len(pdf)),
            "source_table": table,
            "source_delta_version": delta_version,
        })
        try:
            ds = mlflow.data.from_spark(spark.table(table), table_name=table, version=str(delta_version))
            mlflow.log_input(ds, context="training")
        except Exception as e:
            print(f"mlflow.data.from_spark skipped: {e}")
        mlflow.set_tag("feature_table", table)

        idata = pm.sample(draws=DRAWS, tune=DRAWS, chains=2, cores=1,
                          target_accept=0.9, progressbar=False, random_seed=42)

        summary = az.summary(idata, var_names=["mu", "sigma_region", "alpha_region", "beta"])
        summary.to_csv("/tmp/freq_pymc_summary.csv")
        mlflow.log_artifact("/tmp/freq_pymc_summary.csv")

        # PPC mean on the training sample — should recover the observed mean
        ppc = pm.sample_posterior_predictive(idata, progressbar=False, random_seed=42)
        y_pred = ppc.posterior_predictive["y_obs"].mean(dim=["chain", "draw"]).values
        rmse   = float(np.sqrt(np.mean((y - y_pred) ** 2)))
        mae    = float(np.mean(np.abs(y - y_pred)))

        # Convergence diagnostics
        rhat = summary["r_hat"].max()
        ess_bulk_min = summary["ess_bulk"].min()

        mlflow.log_metrics({
            "rmse_in_sample": rmse, "mae_in_sample": mae,
            "rhat_max": float(rhat), "ess_bulk_min": float(ess_bulk_min),
            "pred_mean": float(y_pred.mean()), "actual_mean": float(y.mean()),
        })

        # Save region effects as a readable artefact
        region_effects = pd.DataFrame({
            "region": regions,
            "posterior_mean": idata.posterior["alpha_region"].mean(dim=["chain", "draw"]).values,
            "posterior_sd":   idata.posterior["alpha_region"].std(dim=["chain", "draw"]).values,
        }).sort_values("posterior_mean", ascending=False)
        region_effects.to_csv("/tmp/freq_pymc_region_effects.csv", index=False)
        mlflow.log_artifact("/tmp/freq_pymc_region_effects.csv")

        beta_effects = pd.DataFrame({
            "feature": numeric_features,
            "posterior_mean": idata.posterior["beta"].mean(dim=["chain", "draw"]).values,
            "posterior_sd":   idata.posterior["beta"].std(dim=["chain", "draw"]).values,
        })
        beta_effects.to_csv("/tmp/freq_pymc_beta.csv", index=False)
        mlflow.log_artifact("/tmp/freq_pymc_beta.csv")

        print(f"PyMC — rhat_max {rhat:.3f} | ess_min {ess_bulk_min:.0f} | in-sample RMSE {rmse:.4f}")
        print(f"Run: {run.info.run_id}")

# COMMAND ----------

display(spark.createDataFrame(region_effects))

# COMMAND ----------

display(spark.createDataFrame(beta_effects))
