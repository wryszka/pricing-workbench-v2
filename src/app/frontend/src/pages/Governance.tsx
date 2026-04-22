import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card, Table, Badge } from "../components/Card";

export default function Governance() {
  const [summary, setSummary] = useState<any>(null);
  const [audit, setAudit] = useState<any[]>([]);
  const [lineage, setLineage] = useState<any>(null);
  const [tab, setTab] = useState<"overview" | "audit" | "fairness" | "lineage">("overview");
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    api<any>("governance").then(setSummary).catch((e) => setErr(String(e)));
    api<{ events: any[] }>("governance/audit?limit=200").then((r) => setAudit(r.events)).catch(() => {});
    api<any>("governance/lineage").then(setLineage).catch(() => {});
  }, []);

  if (err) return <div className="text-red-600 text-sm">{err}</div>;

  return (
    <>
      <PageHeader
        title="Governance"
        subtitle="Audit log, dataset approvals, fairness report, data lineage."
      />

      <div className="flex gap-1 mb-3 border-b border-slate-200">
        {(["overview", "audit", "fairness", "lineage"] as const).map((t) => (
          <button key={t} onClick={() => setTab(t)}
            className={`px-3 py-1.5 text-sm border-b-2 -mb-px capitalize ${
              tab === t ? "border-blue-600 text-blue-700 font-medium" : "border-transparent text-slate-600 hover:text-slate-900"
            }`}>{t}</button>
        ))}
      </div>

      {tab === "overview" && summary && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <Card title="Event counts">
            <Table
              columns={[
                { key: "event_type", label: "Event" },
                { key: "n",          label: "Count" },
                { key: "latest",     label: "Latest" },
              ]}
              rows={summary.event_counts ?? []}
            />
          </Card>
          <Card title="Dataset approvals">
            <Table
              columns={[
                { key: "decision", label: "Decision", render: (r: any) => (
                  <Badge tone={r.decision === "approved" ? "green" : "red"}>{r.decision}</Badge>
                ) },
                { key: "n",        label: "Count" },
              ]}
              rows={summary.dataset_approvals ?? []}
            />
          </Card>
          <Card title="Fairness checks" className="lg:col-span-2">
            <Table
              columns={[
                { key: "check",  label: "Check" },
                { key: "status", label: "Status", render: (r: any) => {
                  const v = r.status;
                  if (v === "PASS") return <Badge tone="green">PASS</Badge>;
                  if (v === "FAIL") return <Badge tone="red">FAIL</Badge>;
                  return <Badge tone="slate">{v}</Badge>;
                } },
                { key: "detail", label: "Detail" },
                { key: "_evaluated_at", label: "At" },
              ]}
              rows={summary.fairness ?? []}
            />
          </Card>
        </div>
      )}

      {tab === "audit" && (
        <Card title={`Audit log (${audit.length} events)`}>
          <Table
            columns={[
              { key: "timestamp",   label: "When" },
              { key: "event_type",  label: "Event", render: (r: any) => (
                <Badge tone={r.event_type?.includes("rejected") ? "red" : r.event_type?.includes("approved") ? "green" : "slate"}>{r.event_type}</Badge>
              ) },
              { key: "entity_type", label: "Type" },
              { key: "entity_id",   label: "Entity", className: "font-mono text-xs" },
              { key: "user_id",     label: "User" },
              { key: "source",      label: "Source" },
              { key: "details",     label: "Details", render: (r: any) => (
                <span className="text-xs text-slate-500 truncate max-w-xs inline-block">{r.details}</span>
              ) },
            ]}
            rows={audit}
          />
        </Card>
      )}

      {tab === "fairness" && summary && (
        <Card title="Fairness + robustness audit — from 09_fairness_and_robustness">
          <Table
            columns={[
              { key: "check",  label: "Check" },
              { key: "status", label: "Status", render: (r: any) => {
                if (r.status === "PASS") return <Badge tone="green">PASS</Badge>;
                if (r.status === "FAIL") return <Badge tone="red">FAIL</Badge>;
                return <Badge tone="slate">{r.status}</Badge>;
              } },
              { key: "detail", label: "Detail" },
            ]}
            rows={summary.fairness ?? []}
          />
        </Card>
      )}

      {tab === "lineage" && lineage && (
        <div className="grid grid-cols-1 gap-4">
          {(["feature_tables", "silver", "raw"] as const).map((bucket) => (
            <Card key={bucket} title={bucket.replace("_", " ")}>
              <Table
                columns={[
                  { key: "table",         label: "Table",   className: "font-mono text-sm" },
                  { key: "delta_version", label: "Delta version", render: (r: any) => <Badge tone="blue">v{r.delta_version ?? "?"}</Badge> },
                  { key: "row_count",     label: "Rows" },
                ]}
                rows={lineage[bucket] ?? []}
              />
            </Card>
          ))}
        </div>
      )}
    </>
  );
}
