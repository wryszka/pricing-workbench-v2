import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api, formatNumber } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card, Table, Badge } from "../components/Card";
import { Trophy, ChevronRight } from "lucide-react";

export default function ModelFactory() {
  const [models, setModels] = useState<any[]>([]);
  const [comparison, setComparison] = useState<any[]>([]);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    api<{ models: any[] }>("models").then((r) => setModels(r.models)).catch((e) => setErr(String(e)));
    api<{ rows: any[] }>("models/comparison").then((r) => setComparison(r.rows)).catch(() => {});
  }, []);

  return (
    <>
      <PageHeader
        title="Model Factory"
        subtitle="Every pricing candidate trained against feature_policy_year_training. Champion aliases are resolved on the Unity Catalog Model Registry."
      />
      {err && <div className="text-red-600 text-sm mb-4">{err}</div>}

      <Card title="Champion/challenger comparison" className="mb-6"
        right={<div className="text-xs text-slate-500">From <code>model_comparison</code> · 2024 test slice</div>}>
        <Table
          empty="No comparison data. Run the model_factory task."
          columns={[
            { key: "model",            label: "Candidate", className: "font-medium" },
            { key: "gini_on_incurred", label: "Gini on incurred", render: (r: any) => (
              <span className="font-mono text-sm">{Number(r.gini_on_incurred).toFixed(4)}</span>
            ) },
            { key: "rmse",             label: "RMSE", render: (r: any) => formatNumber(r.rmse) },
            { key: "mae",              label: "MAE",  render: (r: any) => formatNumber(r.mae) },
            { key: "total_ratio",      label: "Total ratio", render: (r: any) => (
              <span className={`font-mono text-sm ${
                r.total_ratio > 1.1 ? "text-amber-600" : r.total_ratio < 0.9 ? "text-red-600" : "text-slate-700"
              }`}>{Number(r.total_ratio).toFixed(4)}</span>
            ) },
            { key: "_evaluated_at",    label: "Evaluated" },
          ]}
          rows={comparison}
        />
      </Card>

      <h2 className="text-lg font-semibold text-slate-800 mb-3">Registered models</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {models.map((m) => (
          <Link to={`/model-factory/${m.name}`} key={m.full_name}
            className="block bg-white rounded-lg border border-slate-200 p-4 hover:border-blue-400 hover:shadow-sm transition">
            <div className="flex items-start justify-between gap-3">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 flex-wrap">
                  <span className="font-medium text-slate-900">{m.name}</span>
                  <Badge tone="blue">v{m.latest_version}</Badge>
                  {m.aliases.map((a: any) => (
                    <Badge key={a.alias} tone="green">
                      <Trophy size={10} className="mr-0.5" /> {a.alias}
                    </Badge>
                  ))}
                </div>
                <div className="text-xs text-slate-500 mt-1 truncate">{m.comment || m.full_name}</div>
              </div>
              <ChevronRight size={14} className="text-slate-400 mt-1" />
            </div>
          </Link>
        ))}
      </div>
    </>
  );
}
