import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api, formatNumber } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card, Table, Badge } from "../components/Card";
import { ChevronRight } from "lucide-react";

type FT = { id: string; fqn: string; grain: string; description: string; used_by: string[]; row_count: number; delta_version: number | null };

export default function FeatureTable() {
  const [fts, setFts] = useState<FT[]>([]);
  const [catalog, setCatalog] = useState<any[]>([]);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    api<{ feature_tables: FT[] }>("features").then((r) => setFts(r.feature_tables)).catch((e) => setErr(String(e)));
    api<{ features: any[] }>("features/catalog").then((r) => setCatalog(r.features)).catch(() => {});
  }, []);

  return (
    <>
      <PageHeader
        title="Feature Tables"
        subtitle="Point-in-time joins live here. These are the tables every model consumes — versioned in Delta, tracked in MLflow."
      />
      {err && <div className="text-red-600 text-sm mb-4">{err}</div>}

      <div className="space-y-4 mb-8">
        {fts.map((ft) => (
          <Link key={ft.id} to={`/feature-table/${ft.id}`}
            className="block bg-white rounded-lg border border-slate-200 p-5 hover:border-blue-400 hover:shadow-sm transition">
            <div className="flex items-start justify-between gap-4">
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  <code className="text-blue-700 font-medium">{ft.fqn}</code>
                  {ft.delta_version !== null && <Badge tone="blue">Delta v{ft.delta_version}</Badge>}
                </div>
                <div className="text-xs text-slate-500 mt-0.5">Grain: {ft.grain}</div>
                <p className="text-sm text-slate-700 mt-2">{ft.description}</p>
                <div className="text-xs text-slate-500 mt-2">
                  Used by: {ft.used_by.map((u) => <Badge key={u} tone="slate">{u}</Badge>).reduce((p, c, i) => <>{p}{i > 0 && " "}{c}</>, <></>)}
                </div>
              </div>
              <div className="text-right">
                <div className="text-2xl font-semibold text-slate-900">{formatNumber(ft.row_count)}</div>
                <div className="text-xs text-slate-500 uppercase tracking-wide">rows</div>
                <ChevronRight size={14} className="text-slate-400 mt-1 inline" />
              </div>
            </div>
          </Link>
        ))}
      </div>

      <Card title={`Feature catalog (${catalog.length} features)`}>
        <Table
          empty="Loading…"
          columns={[
            { key: "feature_name",   label: "Feature", className: "font-medium" },
            { key: "feature_group",  label: "Group", render: (r: any) => <Badge tone="slate">{r.feature_group}</Badge> },
            { key: "source_tables",  label: "Source", render: (r: any) => Array.isArray(r.source_tables) ? r.source_tables.join(", ") : r.source_tables },
            { key: "transformation", label: "Transformation" },
            { key: "owner",          label: "Owner" },
            { key: "regulatory_sensitive", label: "Sensitive", render: (r: any) => r.regulatory_sensitive ? <Badge tone="amber">yes</Badge> : "" },
            { key: "pii",            label: "PII", render: (r: any) => r.pii ? <Badge tone="red">PII</Badge> : "" },
          ]}
          rows={catalog}
        />
      </Card>
    </>
  );
}
