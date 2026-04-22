"""Claude-powered agent endpoints — every UI widget that says 'Explain' / 'DQ scan' /
'Propose plan' lands here. Uses the Databricks Foundation Model endpoint
`databricks-claude-sonnet-4-6` (available in-workspace, no external API key). Every
call is audit-logged so the model's influence on decisions is always traceable."""
from __future__ import annotations
import json
import time
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..db import run_sql, get_client
from ..config import FQN

router = APIRouter()

FM_ENDPOINT = "databricks-claude-sonnet-4-6"  # in-workspace foundation model
FM_MAX_TOKENS = 1500

# ---------- shared foundation-model call ----------

def _call_claude(system: str, user: str, max_tokens: int = FM_MAX_TOKENS) -> dict[str, Any]:
    """One-shot call to the in-workspace foundation-model serving endpoint.
    Returns a dict with {content, model, prompt_tokens, completion_tokens, latency_ms}."""
    wc = get_client()
    t0 = time.time()
    r = wc.serving_endpoints.query(
        name=FM_ENDPOINT,
        messages=[
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        max_tokens=max_tokens,
    ).as_dict()
    latency_ms = int((time.time() - t0) * 1000)
    msg = r.get("choices", [{}])[0].get("message", {})
    return {
        "content":          msg.get("content", ""),
        "model":            r.get("model") or FM_ENDPOINT,
        "prompt_tokens":    r.get("usage", {}).get("prompt_tokens", 0),
        "completion_tokens":r.get("usage", {}).get("completion_tokens", 0),
        "latency_ms":       latency_ms,
    }

def _log_agent_call(kind: str, entity_id: str, user: str, prompt: str, response: dict[str, Any]):
    """Audit every agent call. JSON-encoded details keep everything traceable without
    bloating the main audit-log fields."""
    try:
        details = {
            "endpoint":  FM_ENDPOINT,
            "prompt_ch": len(prompt),
            "response_ch": len(response.get("content", "")),
            "prompt_tokens":    response.get("prompt_tokens", 0),
            "completion_tokens":response.get("completion_tokens", 0),
            "latency_ms":       response.get("latency_ms", 0),
        }
        det_json = json.dumps(details).replace("'", "''")
        user_esc = user.replace("'", "''")
        run_sql(f"""
            INSERT INTO {FQN}.app_audit_log
              (event_id, event_type, entity_type, entity_id, entity_version,
               user_id, timestamp, details, source)
            VALUES
              (uuid(), 'agent_{kind}', 'agent', '{entity_id}', '{FM_ENDPOINT}',
               '{user_esc}', current_timestamp(), '{det_json}', 'app')
        """)
    except Exception as e:
        # Never fail an agent call because auditing failed — log to stdout.
        print(f"[agent] audit log insert failed: {e}")

# ---------- status ----------

@router.get("/status")
def status():
    """Check the foundation-model endpoint is live before the UI enables agent buttons."""
    try:
        wc = get_client()
        r = wc.serving_endpoints.get(FM_ENDPOINT).as_dict()
        ready = r.get("state", {}).get("ready") == "READY"
        return {"ready": ready, "endpoint": FM_ENDPOINT}
    except Exception as e:
        return {"ready": False, "endpoint": FM_ENDPOINT, "error": str(e)}

# ---------- /api/agent/explain_pricing ----------

class ExplainIn(BaseModel):
    policy_id: str | None = None
    quote_id:  str | None = None
    user:      str = "actuary.demo"

@router.post("/explain_pricing")
def explain_pricing(body: ExplainIn):
    """Turn a single prediction + its SHAP reason codes into an underwriter-readable
    English paragraph. Uses the champion model's scoring output as context."""
    if not body.policy_id and not body.quote_id:
        raise HTTPException(400, "policy_id or quote_id required")

    if body.policy_id:
        pid_esc = body.policy_id.replace("'", "''")
        try:
            ctx_rows = run_sql(f"""
                SELECT policy_id, policy_version, exposure_year,
                       gross_premium, sum_insured, region, construction_type,
                       flood_zone_rating, crime_theft_index, credit_risk_tier,
                       internal_risk_tier, total_incurred_observed, claim_count_observed
                FROM {FQN}.feature_policy_year_training
                WHERE policy_id = '{pid_esc}'
                ORDER BY exposure_year DESC
                LIMIT 1
            """)
        except Exception as e:
            raise HTTPException(500, f"lookup failed: {e}")
        if not ctx_rows:
            raise HTTPException(404, f"policy {body.policy_id} not found in training slice")
        ctx = ctx_rows[0]
        entity_id = body.policy_id
        subject = "policy-year pricing"
    else:
        qid_esc = body.quote_id.replace("'", "''")
        ctx_rows = run_sql(f"""
            SELECT quote_id, quote_date, region, construction_type, flood_zone,
                   buildings_si, gross_premium_quoted, market_median_rate, vs_market_rate,
                   converted_flag, internal_risk_tier, credit_risk_tier
            FROM {FQN}.feature_quote_training
            WHERE quote_id = '{qid_esc}'
            LIMIT 1
        """)
        if not ctx_rows:
            raise HTTPException(404, f"quote {body.quote_id} not found")
        ctx = ctx_rows[0]
        entity_id = body.quote_id
        subject = "quote pricing"

    system = (
        "You are an expert commercial-lines pricing actuary explaining a model "
        "prediction in plain language for an underwriter or regulator. Be concise "
        "and technical. Cite specific feature values. Never invent numbers. If a "
        "feature value is missing, say so explicitly. Do NOT claim the model uses "
        "gender, age, ethnicity or any protected attribute — the inputs are "
        "firmographic (business) and geographic only."
    )
    user_prompt = (
        f"Explain this {subject} prediction for an underwriter:\n\n"
        f"```json\n{json.dumps(ctx, default=str, indent=2)}\n```\n\n"
        "In 3–5 sentences: (1) what drove the premium up, (2) what drove it down, "
        "(3) whether this is consistent with the risk profile, (4) any open question."
    )
    result = _call_claude(system, user_prompt, max_tokens=700)
    _log_agent_call("explain_pricing", entity_id, body.user, user_prompt, result)
    return {"context": ctx, "system_prompt": system, "user_prompt": user_prompt, **result}

# ---------- /api/agent/dq_monitor ----------

class DQIn(BaseModel):
    dataset_id: str
    user: str = "actuary.demo"

@router.post("/dq_monitor")
def dq_monitor(body: DQIn):
    """Agent-driven data-quality scan. Pulls raw vs silver row counts + sample rows +
    column stats for the dataset, asks Claude to flag anomalies with severity."""
    raw_fqn    = f"{FQN}.raw_{body.dataset_id}"
    silver_fqn = f"{FQN}.silver_{body.dataset_id}"
    try:
        raw_n    = run_sql(f"SELECT COUNT(*) AS n FROM {raw_fqn}",    timeout_s=10)[0]["n"]
        silver_n = run_sql(f"SELECT COUNT(*) AS n FROM {silver_fqn}", timeout_s=10)[0]["n"]
    except Exception as e:
        raise HTTPException(500, f"row count lookup failed: {e}")

    try:
        schema = run_sql(f"DESCRIBE TABLE {silver_fqn}")
        sample = run_sql(f"SELECT * FROM {silver_fqn} LIMIT 20")
    except Exception:
        schema, sample = [], []

    system = (
        "You are a senior actuary + data engineer. Given a dataset snapshot, "
        "produce a short bullet-point data-quality assessment for an actuary who's "
        "about to approve or reject this dataset for downstream pricing. Flag "
        "anything suspicious: unexpected nulls, suspicious outliers, distribution "
        "shifts, schema changes. Each finding should have a severity label (CRITICAL, "
        "WARNING, INFO) in brackets, then a one-line explanation. Be concrete — "
        "cite column names + approximate values. If everything looks clean, say so."
    )
    user_prompt = (
        f"Dataset: {body.dataset_id}\n"
        f"raw row count: {raw_n:,} | silver row count: {silver_n:,}\n"
        f"DQ drop: {max(0, raw_n - silver_n):,} ({100*max(0,raw_n-silver_n)/max(raw_n,1):.1f}%)\n\n"
        f"Silver schema:\n```\n{json.dumps(schema, default=str)}\n```\n\n"
        f"Sample rows (first 20):\n```json\n{json.dumps(sample, default=str)[:3000]}\n```"
    )
    result = _call_claude(system, user_prompt, max_tokens=1200)
    _log_agent_call("dq_monitor", body.dataset_id, body.user, user_prompt, result)
    return {"raw_count": raw_n, "silver_count": silver_n, "system_prompt": system, "user_prompt": user_prompt, **result}

# ---------- /api/agent/analyze_features ----------

class AnalyzeFeaturesIn(BaseModel):
    user: str = "actuary.demo"

@router.post("/analyze_features")
def analyze_features(body: AnalyzeFeaturesIn):
    """Review the feature_catalog from an actuary's perspective: strengths, gaps,
    sensitivity concerns, next candidate features."""
    try:
        rows = run_sql(f"""
            SELECT feature_name, feature_group, data_type, description,
                   transformation, owner, regulatory_sensitive, pii
            FROM {FQN}.feature_catalog
            ORDER BY feature_group, feature_name
        """)
    except Exception as e:
        raise HTTPException(500, f"feature_catalog unavailable: {e}")

    system = (
        "You are a senior commercial-lines pricing actuary reviewing a feature "
        "catalog for a new pricing model. Your audience is a model-risk committee. "
        "Output ONE short paragraph per section:\n"
        "1. Strengths of the current feature set.\n"
        "2. Gaps the book is likely missing (what would a reviewer ask for?).\n"
        "3. Sensitivity / fairness concerns — any proxies for protected classes?\n"
        "4. Three concrete next-feature candidates, each with a one-line rationale.\n"
        "Be specific and cite feature names."
    )
    user_prompt = (
        f"Feature catalog ({len(rows)} features):\n\n"
        f"```json\n{json.dumps(rows, default=str)[:5000]}\n```"
    )
    result = _call_claude(system, user_prompt, max_tokens=1500)
    _log_agent_call("analyze_features", "feature_catalog", body.user, user_prompt, result)
    return {"feature_count": len(rows), "system_prompt": system, "user_prompt": user_prompt, **result}

# ---------- /api/agent/propose_model_plan ----------

class ProposePlanIn(BaseModel):
    target: str = "frequency"   # frequency | severity | demand | pure_premium
    feature_scope: str = "all"  # all | core | geo_only | firmographic_only
    user: str = "actuary.demo"

@router.post("/propose_model_plan")
def propose_model_plan(body: ProposePlanIn):
    """Claude proposes 3–5 concrete model configurations to train for a given target."""
    try:
        cur = run_sql(f"SELECT * FROM {FQN}.model_comparison ORDER BY gini_on_incurred DESC")
    except Exception:
        cur = []

    system = (
        "You are a commercial-lines pricing actuary proposing the next factory run. "
        "Given a target and feature scope, propose 3–5 model configurations worth "
        "training. For each, give: (name) (family: GLM/GBM/XGB/Bayesian) (target) "
        "(features used) (hyper-params to vary) (what we'd learn vs existing "
        "champion). Keep it concrete and runnable — no hand-waving."
    )
    user_prompt = (
        f"Target: {body.target}\n"
        f"Feature scope: {body.feature_scope}\n\n"
        f"Current champion/challenger leaderboard (2024 test):\n"
        f"```json\n{json.dumps(cur, default=str)}\n```"
    )
    result = _call_claude(system, user_prompt, max_tokens=1500)
    _log_agent_call("propose_model_plan", body.target, body.user, user_prompt, result)
    return {"system_prompt": system, "user_prompt": user_prompt, **result}
