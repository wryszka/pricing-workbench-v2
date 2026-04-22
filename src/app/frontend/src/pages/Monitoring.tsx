import { useEffect, useState } from "react";
import { api, formatNumber } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card, Table, Badge } from "../components/Card";
import { Activity } from "lucide-react";

export default function Monitoring() {
  const [comparison, setComparison] = useState<any[]>([]);

  useEffect(() => {
    api<{ rows: any[] }>("models/comparison").then((r) => setComparison(r.rows)).catch(() => {});
  }, []);

  return (
    <>
      <PageHeader
        title="Monitoring"
        subtitle="Live drift + endpoint latency dashboards are Phase 4 work. For now, this tab shows the latest model-factory champion/challenger comparison."
      />
      <Card>
        <div className="flex items-start gap-4 mb-4">
          <div className="p-3 rounded-full bg-amber-50 text-amber-600 shrink-0">
            <Activity size={20} />
          </div>
          <div className="text-sm text-slate-600">
            Full drift detection (population stability index per feature, Gini over rolling
            windows, endpoint p50/p95 latency) is deferred to Phase 4 when the serving
            endpoint is live. Model-factory test-set metrics below are the current proxy.
          </div>
        </div>
        <Table
          empty="No data"
          columns={[
            { key: "model", label: "Candidate", className: "font-medium" },
            { key: "gini_on_incurred", label: "Gini", render: (r: any) => (
              <span className="font-mono text-sm">{Number(r.gini_on_incurred).toFixed(4)}</span>
            ) },
            { key: "rmse",         label: "RMSE", render: (r: any) => formatNumber(r.rmse) },
            { key: "total_ratio",  label: "Total ratio", render: (r: any) => Number(r.total_ratio).toFixed(4) },
            { key: "_evaluated_at",label: "At" },
            { key: "status", label: "Status", render: (r: any) => {
              const tr = Number(r.total_ratio);
              if (Math.abs(tr - 1) < 0.05) return <Badge tone="green">healthy</Badge>;
              if (Math.abs(tr - 1) < 0.1) return <Badge tone="amber">drift</Badge>;
              return <Badge tone="red">miscalibrated</Badge>;
            } },
          ]}
          rows={comparison}
        />
      </Card>
    </>
  );
}
