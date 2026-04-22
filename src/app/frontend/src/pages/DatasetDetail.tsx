import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { api, formatNumber, formatPercent } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card, StatNumber, Table, Badge } from "../components/Card";
import { ArrowLeft, CheckCircle2, XCircle } from "lucide-react";

type Detail = {
  id: string; name: string; description: string; owner: string;
  raw: string; silver: string;
  raw_fqn: string; silver_fqn: string;
  raw_count: number; silver_count: number;
  rows_dropped_by_dq: number;
  dq_pass_rate: number | null;
  silver_schema: any[];
  silver_sample: any[];
};

export default function DatasetDetail() {
  const { id } = useParams<{ id: string }>();
  const [data, setData] = useState<Detail | null>(null);
  const [approvals, setApprovals] = useState<any[]>([]);
  const [tab, setTab] = useState<"schema" | "sample" | "approvals">("schema");
  const [decision, setDecision] = useState<"approved" | "rejected" | "">("");
  const [reviewer, setReviewer] = useState("actuary.demo");
  const [notes, setNotes] = useState("");
  const [flash, setFlash] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const reload = () => {
    api<Detail>(`datasets/${id}`).then(setData).catch((e) => setErr(String(e)));
    api<{ approvals: any[] }>(`datasets/${id}/approvals`).then((r) => setApprovals(r.approvals)).catch(() => {});
  };

  useEffect(reload, [id]);

  async function submitDecision() {
    if (!decision) return;
    try {
      await api(`datasets/${id}/approve`, {
        method: "POST",
        body: JSON.stringify({ decision, reviewer, notes }),
      });
      setFlash(`Recorded ${decision}`);
      setNotes(""); setDecision("");
      setTimeout(() => setFlash(null), 3000);
      reload();
    } catch (e) {
      setErr(String(e));
    }
  }

  if (err)  return <div className="text-red-600 text-sm">{err}</div>;
  if (!data) return <div className="text-slate-500 text-sm">Loading…</div>;

  return (
    <>
      <Link to="/external-data" className="text-sm text-slate-600 hover:underline flex items-center gap-1 mb-3">
        <ArrowLeft size={14} /> Back to datasets
      </Link>
      <PageHeader title={data.name} subtitle={data.description} />

      <div className="grid grid-cols-4 gap-4 mb-6">
        <Card><StatNumber value={formatNumber(data.raw_count)} label="Raw rows" hint={data.raw_fqn} /></Card>
        <Card><StatNumber value={formatNumber(data.silver_count)} label="Silver rows" hint={data.silver_fqn} /></Card>
        <Card><StatNumber value={formatNumber(data.rows_dropped_by_dq)} label="Dropped by DQ" /></Card>
        <Card><StatNumber value={formatPercent(data.dq_pass_rate, 2)} label="DQ pass rate" /></Card>
      </div>

      {flash && <div className="mb-4 p-3 bg-emerald-50 text-emerald-700 border border-emerald-200 rounded text-sm">{flash}</div>}

      <Card title="Actuary approval" className="mb-6">
        <div className="text-sm text-slate-600 mb-3">
          Record a review decision on the <strong>current Delta snapshot</strong> (raw v{data.raw_count} rows).
          Both the decision and an audit-log entry will be written.
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <div>
            <label className="text-xs text-slate-500 block">Reviewer</label>
            <input value={reviewer} onChange={(e) => setReviewer(e.target.value)}
              className="border border-slate-300 rounded px-2 py-1 text-sm w-48" />
          </div>
          <div className="flex-1 min-w-64">
            <label className="text-xs text-slate-500 block">Notes</label>
            <input value={notes} onChange={(e) => setNotes(e.target.value)} placeholder="Optional notes"
              className="border border-slate-300 rounded px-2 py-1 text-sm w-full" />
          </div>
          <button
            onClick={() => { setDecision("approved"); setTimeout(submitDecision, 0); }}
            className="bg-emerald-600 hover:bg-emerald-700 text-white px-3 py-1.5 rounded text-sm flex items-center gap-1"
          >
            <CheckCircle2 size={14} /> Approve
          </button>
          <button
            onClick={() => { setDecision("rejected"); setTimeout(submitDecision, 0); }}
            className="bg-red-600 hover:bg-red-700 text-white px-3 py-1.5 rounded text-sm flex items-center gap-1"
          >
            <XCircle size={14} /> Reject
          </button>
        </div>
      </Card>

      <div className="flex gap-1 mb-3 border-b border-slate-200">
        {(["schema", "sample", "approvals"] as const).map((t) => (
          <button key={t} onClick={() => setTab(t)}
            className={`px-3 py-1.5 text-sm border-b-2 -mb-px ${
              tab === t ? "border-blue-600 text-blue-700 font-medium" : "border-transparent text-slate-600 hover:text-slate-900"
            }`}>
            {t === "schema" ? "Silver schema" : t === "sample" ? "Sample rows" : `Approvals (${approvals.length})`}
          </button>
        ))}
      </div>

      <Card>
        {tab === "schema" && (
          <Table
            columns={[
              { key: "col_name",  label: "Column" },
              { key: "data_type", label: "Type" },
              { key: "comment",   label: "Comment" },
            ]}
            rows={data.silver_schema}
          />
        )}
        {tab === "sample" && (
          data.silver_sample.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="text-xs">
                <thead>
                  <tr>
                    {Object.keys(data.silver_sample[0]).map((k) => (
                      <th key={k} className="text-left bg-slate-100 px-2 py-1 border border-slate-200 font-medium whitespace-nowrap">{k}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.silver_sample.map((row, i) => (
                    <tr key={i}>
                      {Object.values(row).map((v: any, j) => (
                        <td key={j} className="px-2 py-1 border border-slate-200 whitespace-nowrap">{v === null ? "—" : String(v)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : <div className="text-sm text-slate-500 italic">No rows</div>
        )}
        {tab === "approvals" && (
          <Table
            empty="No approvals yet"
            columns={[
              { key: "decision",     label: "Decision", render: (r: any) => (
                <Badge tone={r.decision === "approved" ? "green" : "red"}>{r.decision}</Badge>
              ) },
              { key: "reviewer",      label: "Reviewer" },
              { key: "reviewer_notes",label: "Notes" },
              { key: "raw_row_count", label: "Raw rows" },
              { key: "silver_row_count", label: "Silver rows" },
              { key: "reviewed_at",   label: "At" },
            ]}
            rows={approvals}
          />
        )}
      </Card>
    </>
  );
}
