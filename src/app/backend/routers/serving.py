"""Serving tab — placeholder for Phase 4 (model endpoint + online store)."""
from __future__ import annotations
from fastapi import APIRouter

router = APIRouter()

@router.get("")
def status():
    return {
        "status": "placeholder",
        "message": "Serving endpoint + online feature store are scheduled for Phase 4. "
                   "For now, this tab reflects the champion-alias state from the model factory.",
        "phase": 4,
        "plan": [
            "Deploy pricing_champion_severity + pricing_champion_frequency as a single "
            "Databricks Model Serving endpoint (CPU, small, scale-to-zero).",
            "Publish feature_policy_current to the online feature store for sub-50ms lookups.",
            "Add a /score endpoint here that accepts a quote payload and returns the "
            "compound pure premium + SHAP reason codes inline.",
        ],
    }
