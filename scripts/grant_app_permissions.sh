#!/usr/bin/env bash
# Grants UC + workspace privileges to the Databricks App's service principal so the
# app can read the pricing_workbench schema + reports volume + query the Claude
# foundation model. Run this once after `databricks apps create`. Re-runnable.
#
# Usage:
#   ./scripts/grant_app_permissions.sh pricing-workbench-v2
set -euo pipefail

APP_NAME="${1:-pricing-workbench-v2}"
CATALOG="${CATALOG_NAME:-lr_serverless_aws_us_catalog}"
SCHEMA="${SCHEMA_NAME:-pricing_workbench}"
WAREHOUSE="${WAREHOUSE_ID:-ab79eced8207d29b}"

echo "[grant] looking up service principal for $APP_NAME..."
SP=$(databricks apps get "$APP_NAME" --output json | python3 -c "import json,sys; print(json.load(sys.stdin).get('service_principal_client_id',''))")
if [ -z "$SP" ]; then
  echo "ERROR: could not find service principal for $APP_NAME" >&2
  exit 1
fi
echo "[grant] SP: $SP"

run_sql() {
  local stmt="$1"
  databricks api post /api/2.0/sql/statements --json "{
    \"warehouse_id\": \"$WAREHOUSE\",
    \"statement\": \"$stmt\",
    \"wait_timeout\": \"30s\"
  }" | python3 -c "
import json,sys
d=json.load(sys.stdin)
state=d.get('status',{}).get('state')
err=d.get('status',{}).get('error',{}).get('message','')
print(f'  → {state} {err}')
"
}

echo "[grant] catalog USE..."
run_sql "GRANT USE CATALOG ON CATALOG $CATALOG TO \`$SP\`"

echo "[grant] schema USE SCHEMA + SELECT + MODIFY + EXECUTE..."
run_sql "GRANT USE SCHEMA ON SCHEMA $CATALOG.$SCHEMA TO \`$SP\`"
run_sql "GRANT SELECT     ON SCHEMA $CATALOG.$SCHEMA TO \`$SP\`"
run_sql "GRANT MODIFY     ON SCHEMA $CATALOG.$SCHEMA TO \`$SP\`"
run_sql "GRANT EXECUTE    ON SCHEMA $CATALOG.$SCHEMA TO \`$SP\`"

echo "[grant] reports volume READ/WRITE..."
run_sql "GRANT READ VOLUME  ON VOLUME $CATALOG.$SCHEMA.reports TO \`$SP\`"

echo "[grant] done."
