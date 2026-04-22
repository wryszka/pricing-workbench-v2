import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api, formatNumber } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card, Table, Badge } from "../components/Card";
import { FileText, ChevronRight } from "lucide-react";

export default function Reports() {
  const [reports, setReports] = useState<any[]>([]);
  const [governance, setGovernance] = useState<any[]>([]);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    api<{ reports: any[] }>("reports").then((r) => setReports(r.reports)).catch((e) => setErr(String(e)));
    api<{ rows: any[] }>("reports/governance").then((r) => setGovernance(r.rows)).catch(() => {});
  }, []);

  // Merge governance metadata into the report listing
  const rows = reports.map((r) => {
    const gov = governance.find((g) =>
      g.model_name && r.model_name && g.model_name === r.model_name && String(g.version) === String(r.version));
    return { ...r, ...(gov || {}) };
  });

  return (
    <>
      <PageHeader
        title="Regulatory Reports"
        subtitle="One report per registered model — lineage, SHAP reason codes, PDP plots, fairness audit. Generated on every model_factory run."
      />
      {err && <div className="text-red-600 text-sm mb-4">{err}</div>}

      <Card>
        <Table
          empty="No reports generated yet. Run the generate_model_reports task."
          columns={[
            { key: "model_name",  label: "Model", render: (r: any) => (
              <Link to={`/reports/${r.id}`} className="text-blue-600 hover:underline font-medium flex items-center gap-1">
                <FileText size={14} /> {r.model_name || r.id}
              </Link>
            ) },
            { key: "version",     label: "Version", render: (r: any) => <Badge tone="blue">v{r.version}</Badge> },
            { key: "alias",       label: "Alias", render: (r: any) => (
              r.alias ? <Badge tone="green">{r.alias}</Badge> : ""
            ) },
            { key: "gini",        label: "Gini", render: (r: any) => (
              r.gini !== undefined ? <span className="font-mono text-sm">{Number(r.gini).toFixed(4)}</span> : "—"
            ) },
            { key: "total_ratio", label: "Total ratio", render: (r: any) => (
              r.total_ratio !== undefined ? <span className="font-mono text-sm">{Number(r.total_ratio).toFixed(4)}</span> : "—"
            ) },
            { key: "test_rows",   label: "Test rows", render: (r: any) => formatNumber(r.test_rows) },
            { key: "shap_method", label: "Explanation", render: (r: any) => (
              r.shap_method ? <span className="text-xs text-slate-600">{r.shap_method.split(" ")[0]}</span> : ""
            ) },
            { key: "open",        label: "", render: (r: any) => (
              <Link to={`/reports/${r.id}`} className="text-slate-400 hover:text-blue-600"><ChevronRight size={14} /></Link>
            ) },
          ]}
          rows={rows}
        />
      </Card>
    </>
  );
}
