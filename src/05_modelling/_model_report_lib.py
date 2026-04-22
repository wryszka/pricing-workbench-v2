# Databricks notebook source
# MAGIC %md
# MAGIC # Explainability + report helpers (shared module)
# MAGIC
# MAGIC Called by `08_generate_model_reports` and `09_fairness_and_robustness`.
# MAGIC Keeps the notebook-level code focused on the report narrative; the maths sits here.
# MAGIC
# MAGIC Design principle: **every plot returns both the figure and the underlying DataFrame**,
# MAGIC so reports can show the chart AND a table regulators can audit.

# COMMAND ----------

from __future__ import annotations
import io
import json
import base64
from pathlib import Path
from typing import Callable, Iterable

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")  # non-interactive — we write PNGs to volume, not to notebook DOM
import matplotlib.pyplot as plt

# COMMAND ----------

# ---------- filesystem + IO ----------

def ensure_dir(path: str) -> str:
    Path(path).mkdir(parents=True, exist_ok=True)
    return path

def save_fig(fig, path: str, dpi: int = 110) -> str:
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    return path

def fig_to_base64(fig, dpi: int = 110) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")

# COMMAND ----------

# ---------- core actuarial metrics ----------

def gini_on_target(y_true, y_pred) -> float:
    """Cumulative-capture Gini — standard pricing-model lift metric."""
    y_true  = np.asarray(y_true, dtype=float)
    y_pred  = np.asarray(y_pred, dtype=float)
    order   = np.argsort(-y_pred)
    cum_y   = np.cumsum(y_true[order]) / (y_true.sum() + 1e-9)
    cum_pop = np.arange(1, len(y_true) + 1) / len(y_true)
    return float(2.0 * np.trapz(cum_y, cum_pop) - 1.0)

def total_ratio(y_true, y_pred) -> float:
    t = float(np.sum(y_true)); p = float(np.sum(y_pred))
    return p / t if t != 0 else float("nan")

def weighted_mean(values, weights) -> float:
    values = np.asarray(values, dtype=float); weights = np.asarray(weights, dtype=float)
    return float((values * weights).sum() / (weights.sum() + 1e-9))

# COMMAND ----------

# ---------- calibration, lift, PDP ----------

def calibration_by_decile(y_true, y_pred, n_bins: int = 10) -> pd.DataFrame:
    df = pd.DataFrame({"y": np.asarray(y_true, dtype=float),
                       "yhat": np.asarray(y_pred, dtype=float)})
    df["decile"] = pd.qcut(df["yhat"].rank(method="first"),
                           q=n_bins, labels=False, duplicates="drop")
    return df.groupby("decile").agg(
        n=("y", "size"),
        avg_predicted=("yhat", "mean"),
        avg_actual=("y", "mean"),
        total_predicted=("yhat", "sum"),
        total_actual=("y", "sum"),
    ).reset_index()

def plot_calibration(calib_df: pd.DataFrame, title: str = "Calibration"):
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(calib_df["decile"], calib_df["avg_predicted"], marker="o", label="Predicted", linewidth=2)
    ax.plot(calib_df["decile"], calib_df["avg_actual"],    marker="s", label="Actual",    linewidth=2)
    ax.set_xlabel("Prediction decile (10 = highest predicted risk)")
    ax.set_ylabel("Mean value")
    ax.set_title(title)
    ax.legend(); ax.grid(alpha=0.3)
    return fig

def plot_lift(y_true, y_pred, title: str = "Lift curve"):
    y_true  = np.asarray(y_true, dtype=float)
    y_pred  = np.asarray(y_pred, dtype=float)
    order   = np.argsort(-y_pred)
    cum_y   = np.cumsum(y_true[order]) / (y_true.sum() + 1e-9)
    cum_pop = np.arange(1, len(y_true) + 1) / len(y_true)
    g = gini_on_target(y_true, y_pred)
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(cum_pop, cum_y, label="Model", linewidth=2)
    ax.plot([0, 1], [0, 1], "k--", alpha=0.5, label="Random")
    ax.set_title(f"{title} — Gini {g:.3f}")
    ax.set_xlabel("Cumulative share of policies (sorted by predicted)")
    ax.set_ylabel("Cumulative share of actual target")
    ax.legend(); ax.grid(alpha=0.3)
    return fig, g

def plot_double_lift(y_true, pred_champion, pred_challenger,
                     champion_label: str, challenger_label: str):
    """Classic actuarial double-lift: sort by ratio challenger/champion, bin into
    deciles, show average actual, champion prediction, challenger prediction in each.
    If the challenger's extra differentiation adds signal, its curve will deviate from
    the champion's in the right direction."""
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
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(agg["decile"], agg["mean_actual"],     marker="s", linewidth=2, label="Actual")
    ax.plot(agg["decile"], agg["mean_champion"],   marker="o", linewidth=2, label=champion_label)
    ax.plot(agg["decile"], agg["mean_challenger"], marker="^", linewidth=2, label=challenger_label)
    ax.set_xlabel("Decile of challenger/champion prediction ratio")
    ax.set_ylabel("Mean target value")
    ax.set_title(f"Double lift: {challenger_label} vs {champion_label}")
    ax.legend(); ax.grid(alpha=0.3)
    return fig, agg

def partial_dependence_1d(predict_fn: Callable[[pd.DataFrame], np.ndarray],
                          X: pd.DataFrame, feature: str,
                          n_points: int = 20, sample: int = 2000):
    X_s = X.sample(n=min(sample, len(X)), random_state=42).reset_index(drop=True)
    vals = X_s[feature]
    if pd.api.types.is_numeric_dtype(vals):
        lo, hi = vals.quantile(0.02), vals.quantile(0.98)
        if lo == hi:
            grid = np.array([float(vals.iloc[0])])
        else:
            grid = np.linspace(lo, hi, n_points)
    else:
        grid = vals.value_counts().head(n_points).index.tolist()
    rows = []
    for v in grid:
        X_mod = X_s.copy()
        X_mod[feature] = v
        yhat = predict_fn(X_mod)
        rows.append({"value": v, "avg_prediction": float(np.mean(yhat))})
    pdp_df = pd.DataFrame(rows)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(pdp_df["value"], pdp_df["avg_prediction"], marker="o", linewidth=2)
    ax.set_title(f"Partial dependence — {feature}")
    ax.set_xlabel(feature); ax.set_ylabel("Avg. prediction (all else held constant)")
    ax.grid(alpha=0.3)
    return fig, pdp_df

# COMMAND ----------

# ---------- SHAP — tree-based models ----------

def shap_for_tree_model(model, X: pd.DataFrame):
    """Returns (shap_values, base_value, feature_names). Works for LightGBM Booster,
    XGBoost Booster, and any sklearn-wrapped tree estimator. Raises if the model
    isn't tree-based — callers should fall back to analytical GLM decomposition."""
    import shap
    # shap's TreeExplainer handles LightGBM + XGBoost natively
    explainer = shap.TreeExplainer(model)
    sv = explainer.shap_values(X)
    # TreeExplainer returns ndarray; base_value sometimes list
    if isinstance(sv, list):
        sv = sv[0]
    base = explainer.expected_value
    if isinstance(base, (list, np.ndarray)):
        base = float(np.asarray(base).ravel()[0])
    return np.asarray(sv), float(base), list(X.columns)

def shap_for_glm(glm_result, X: pd.DataFrame):
    """Analytical 'SHAP' for a GLM = deviation from mean prediction attributed per
    feature × coefficient. Exact for GLMs (linearity on linpred scale); more
    interpretable than SHAP since it's just xᵢ·βᵢ. We return it on the linear-predictor
    scale (pre-link), because that's where the relativities live."""
    # Drop intercept from feature list if present
    params = dict(zip(glm_result.params.index, glm_result.params.values))
    # Assume X has same column order as the design matrix (caller guarantees)
    contribs = X.values * np.asarray([params.get(c, 0.0) for c in X.columns])
    base = float(params.get("const", params.get("intercept", 0.0)))
    return contribs, base, list(X.columns)

def shap_global_importance(shap_values: np.ndarray, feature_names: Iterable[str]) -> pd.DataFrame:
    abs_mean = np.abs(shap_values).mean(axis=0)
    return (pd.DataFrame({"feature": list(feature_names), "mean_abs_shap": abs_mean})
              .sort_values("mean_abs_shap", ascending=False)
              .reset_index(drop=True))

def plot_shap_bar(shap_importance_df: pd.DataFrame, top_n: int = 20, title: str = "Mean |SHAP|"):
    df = shap_importance_df.head(top_n).copy()
    df = df.sort_values("mean_abs_shap", ascending=True)
    fig, ax = plt.subplots(figsize=(8, max(4, 0.35 * len(df))))
    ax.barh(df["feature"], df["mean_abs_shap"])
    ax.set_title(title)
    ax.set_xlabel("Mean |SHAP| — average contribution magnitude per prediction")
    ax.grid(alpha=0.3, axis="x")
    fig.tight_layout()
    return fig

def reason_codes(row_idx: int, shap_values: np.ndarray, feature_names,
                 X: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    row = shap_values[row_idx]
    df = pd.DataFrame({
        "feature":            list(feature_names),
        "value":              [X.iloc[row_idx][f] if f in X.columns else None for f in feature_names],
        "contribution":       row,
    })
    df["abs"] = df["contribution"].abs()
    df = df.sort_values("abs", ascending=False).head(top_n).drop(columns=["abs"]).reset_index(drop=True)
    df["direction"] = np.where(df["contribution"] >= 0, "INCREASES", "DECREASES")
    return df

# COMMAND ----------

# ---------- lineage ----------

def delta_version(spark, table_fqn: str) -> str:
    """Latest Delta version of a UC table — the exact commit used for training.
    For DLT materialised views, DESCRIBE HISTORY isn't available on the UC-visible
    table — fall back to the `DELTA_LAST_WRITE_VERSION` table property."""
    try:
        return str(spark.sql(f"DESCRIBE HISTORY {table_fqn} LIMIT 1").collect()[0]["version"])
    except Exception:
        pass
    try:
        props = spark.sql(f"DESCRIBE TABLE EXTENDED {table_fqn}").collect()
        for r in props:
            name = r[0] if r[0] else ""
            val  = r[1] if len(r) > 1 else ""
            if isinstance(val, str) and "delta.lastCommitTimestamp" in (name or ""):
                return val
        # DLT sometimes exposes a `pipelines.datasetVersion` table property
        tbl_props = spark.sql(f"SHOW TBLPROPERTIES {table_fqn}").collect()
        for r in tbl_props:
            if r[0] == "pipelines.datasetVersion":
                return r[1]
        return "DLT-managed"
    except Exception:
        return "?"

def feature_lineage(spark, feature_catalog_fqn: str, features: Iterable[str]) -> pd.DataFrame:
    """Pull one row per feature from feature_catalog — source table, source column,
    transformation. This is the auditable 'where does this feature come from' view."""
    feat_list = ",".join(f"'{f}'" for f in features)
    try:
        return spark.sql(f"""
            SELECT feature_name, source_tables, source_columns, transformation,
                   owner, regulatory_sensitive, pii
            FROM {feature_catalog_fqn}
            WHERE feature_name IN ({feat_list})
        """).toPandas()
    except Exception:
        return pd.DataFrame({"feature_name": list(features)})

# COMMAND ----------

# ---------- markdown report assembly ----------

def md_h(level: int, text: str) -> str:
    return f"{'#' * level} {text}\n\n"

def md_para(text: str) -> str:
    return text.strip() + "\n\n"

def md_table(df: pd.DataFrame, max_rows: int = 50) -> str:
    if df is None or df.empty:
        return "_(no data)_\n\n"
    df_show = df.head(max_rows).copy()
    # Format floats for readability
    for c in df_show.columns:
        if pd.api.types.is_float_dtype(df_show[c]):
            df_show[c] = df_show[c].round(4)
    header = "| " + " | ".join(df_show.columns) + " |"
    sep    = "| " + " | ".join(["---"] * len(df_show.columns)) + " |"
    rows   = ["| " + " | ".join(str(v) for v in row) + " |" for row in df_show.values.tolist()]
    out = "\n".join([header, sep] + rows) + "\n\n"
    if len(df) > max_rows:
        out += f"_(showing {max_rows} of {len(df)} rows)_\n\n"
    return out

def md_image(rel_path: str, alt: str = "") -> str:
    return f"![{alt}]({rel_path})\n\n"

def md_kv(items: dict) -> str:
    rows = [f"- **{k}**: {v}" for k, v in items.items()]
    return "\n".join(rows) + "\n\n"
