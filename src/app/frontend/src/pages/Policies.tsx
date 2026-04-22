import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card, Table, Badge } from "../components/Card";

export default function Policies() {
  const [rows, setRows] = useState<any[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [status, setStatus] = useState<string>("");

  useEffect(() => {
    const q = status ? `?status=${status}` : "";
    api<{ policies: any[] }>(`policies${q}`).then((r) => setRows(r.policies)).catch((e) => setErr(String(e)));
  }, [status]);

  return (
    <>
      <PageHeader
        title="Policies"
        subtitle="Every in-force or lapsed policy — one row per policy_id with its version count and company."
        actions={
          <select value={status} onChange={(e) => setStatus(e.target.value)}
            className="border border-slate-300 rounded px-2 py-1 text-sm">
            <option value="">All statuses</option>
            <option value="active">Active</option>
            <option value="lapsed">Lapsed</option>
            <option value="cancelled">Cancelled</option>
          </select>
        }
      />
      {err && <div className="text-red-600 text-sm mb-4">{err}</div>}
      <Card>
        <Table
          empty="Loading…"
          columns={[
            { key: "policy_id",      label: "Policy ID", render: (r: any) => (
              <Link to={`/policies/${r.policy_id}`} className="text-blue-600 hover:underline font-mono text-xs">{r.policy_id}</Link>
            ) },
            { key: "company_name",   label: "Company" },
            { key: "region",         label: "Region" },
            { key: "primary_sic_code", label: "SIC" },
            { key: "status",         label: "Status", render: (r: any) => (
              <Badge tone={r.status === "active" ? "green" : r.status === "lapsed" ? "amber" : "red"}>{r.status}</Badge>
            ) },
            { key: "inception_date", label: "Inception" },
            { key: "version_count",  label: "Versions" },
          ]}
          rows={rows}
        />
      </Card>
    </>
  );
}
