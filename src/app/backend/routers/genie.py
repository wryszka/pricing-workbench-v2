"""Genie embed config — returns the workspace URL + space IDs so the frontend can
mount an iframe. We deliberately don't proxy the Genie rooms; they use the user's
SSO session directly."""
from __future__ import annotations
import os
from fastapi import APIRouter

from ..config import CATALOG

router = APIRouter()

@router.get("/spaces")
def spaces():
    host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    return {
        "workspace_host": host,
        "spaces": [
            {
                "id":    os.getenv("GENIE_SPACE_PRICING", ""),
                "name":  "Pricing Workbench — policies, claims, features",
                "desc":  "Ask natural-language questions about dim_policies, dim_companies, fact_claims, feature_policy_year_training.",
            },
            {
                "id":    os.getenv("GENIE_SPACE_QUOTES", ""),
                "name":  "Quote Stream",
                "desc":  "Ask about quote-level data: conversion rates by channel, outlier quotes, demand GBM inputs.",
            },
        ],
    }
