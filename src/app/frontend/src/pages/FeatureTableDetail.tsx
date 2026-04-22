import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { api } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card, Table, Badge } from "../components/Card";
import { ArrowLeft } from "lucide-react";

export default function FeatureTableDetail() {
  const { id } = useParams<{ id: string }>();
  const [data, setData] = useState<any>(null);
  const [tab, setTab] = useState<"schema" | "sample" | "history">("schema");
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    api<any>(`features/${id}`).then(setData).catch((e) => setErr(String(e)));
  }, [id]);

  if (err) return <div className="text-red-600 text-sm">{err}</div>;
  if (!data) return <div className="text-slate-500 text-sm">Loading…</div>;

  return (
    <>
      <Link to="/feature-table" className="text-sm text-slate-600 hover:underline flex items-center gap-1 mb-3">
        <ArrowLeft size={14} /> Back to feature tables
      </Link>
      <PageHeader title={data.fqn} />

      <div className="flex gap-1 mb-3 border-b border-slate-200">
        {(["schema", "sample", "history"] as const).map((t) => (
          <button key={t} onClick={() => setTab(t)}
            className={`px-3 py-1.5 text-sm border-b-2 -mb-px ${
              tab === t ? "border-blue-600 text-blue-700 font-medium" : "border-transparent text-slate-600 hover:text-slate-900"
            }`}>
            {t === "schema" ? `Schema (${data.schema?.length || 0})` : t === "sample" ? `Sample (${data.sample?.length || 0})` : `Delta history (${data.history?.length || 0})`}
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
            rows={data.schema ?? []}
          />
        )}

        {tab === "sample" && (
          data.sample?.length > 0 ? (
            <div className="overflow-x-auto max-h-[600px]">
              <table className="text-xs">
                <thead className="sticky top-0 bg-white z-10">
                  <tr>
                    {Object.keys(data.sample[0]).map((k) => (
                      <th key={k} className="text-left bg-slate-100 px-2 py-1 border border-slate-200 font-medium whitespace-nowrap">{k}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.sample.map((row: any, i: number) => (
                    <tr key={i}>
                      {Object.values(row).map((v: any, j: number) => (
                        <td key={j} className="px-2 py-1 border border-slate-200 whitespace-nowrap max-w-xs truncate">{v === null ? "—" : String(v)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : <div className="text-sm text-slate-500 italic">No rows</div>
        )}

        {tab === "history" && (
          <Table
            columns={[
              { key: "version",        label: "Version", render: (r: any) => <Badge tone="blue">v{r.version}</Badge> },
              { key: "timestamp",      label: "Committed" },
              { key: "operation",      label: "Operation" },
              { key: "operationMetrics",label:"Metrics", render: (r:any) => {
                const m = r.operationMetrics || {};
                return Object.entries(m).slice(0, 4).map(([k, v]) => (
                  <span key={k} className="mr-2 text-xs text-slate-600">{k}: {String(v)}</span>
                ));
              }},
              { key: "userName",       label: "User" },
            ]}
            rows={data.history ?? []}
          />
        )}
      </Card>
    </>
  );
}
