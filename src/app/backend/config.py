"""App-wide config pulled from env vars (populated by Databricks Apps runtime)."""
import os

CATALOG              = os.getenv("CATALOG_NAME", "lr_serverless_aws_us_catalog")
SCHEMA               = os.getenv("SCHEMA_NAME",  "pricing_workbench")
WAREHOUSE_ID         = os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("WAREHOUSE_ID")
ENTITY_NAME          = os.getenv("ENTITY_NAME", "Bricksurance SE")
REPORTS_VOLUME       = os.getenv("REPORTS_VOLUME", "reports")
EXTERNAL_LANDING_VOL = os.getenv("EXTERNAL_LANDING_VOLUME", "external_landing")

FQN = f"{CATALOG}.{SCHEMA}"
REPORTS_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{REPORTS_VOLUME}"
