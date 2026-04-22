"""Thin SQL wrapper around Databricks SQL Statement Execution API.

All queries go through a single helper so we can add caching, audit logging,
and rate limiting in one place later."""
from __future__ import annotations
from typing import Any
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from . import config

_client: WorkspaceClient | None = None

def get_client() -> WorkspaceClient:
    global _client
    if _client is None:
        _client = WorkspaceClient()
    return _client

def _wait_for_statement(stmt_id: str, timeout_s: int = 60) -> dict[str, Any]:
    wc = get_client()
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = wc.statement_execution.get_statement(stmt_id)
        state = r.status.state
        if state in (StatementState.SUCCEEDED, StatementState.FAILED,
                     StatementState.CANCELED, StatementState.CLOSED):
            return r.as_dict()
        time.sleep(0.5)
    raise TimeoutError(f"SQL statement {stmt_id} timed out")

def run_sql(sql: str, *, timeout_s: int = 60) -> list[dict[str, Any]]:
    """Execute a SQL statement and return a list of dict rows."""
    if not config.WAREHOUSE_ID:
        raise RuntimeError("WAREHOUSE_ID / DATABRICKS_WAREHOUSE_ID env var not set")
    wc = get_client()
    r = wc.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=config.WAREHOUSE_ID,
        catalog=config.CATALOG,
        schema=config.SCHEMA,
        wait_timeout="30s",
    ).as_dict()

    state = r.get("status", {}).get("state")
    if state == "PENDING" or state == "RUNNING":
        r = _wait_for_statement(r["statement_id"], timeout_s=timeout_s)
        state = r.get("status", {}).get("state")
    if state != "SUCCEEDED":
        err = r.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL failed: {err.get('message', state)}")

    manifest = r.get("manifest", {}) or {}
    cols = [c["name"] for c in (manifest.get("schema", {}) or {}).get("columns", [])]
    data = (r.get("result", {}) or {}).get("data_array", []) or []
    return [dict(zip(cols, row)) for row in data]

def table_exists(fqn: str) -> bool:
    try:
        run_sql(f"DESCRIBE TABLE {fqn}", timeout_s=10)
        return True
    except Exception:
        return False
