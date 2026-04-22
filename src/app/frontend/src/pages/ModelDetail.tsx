import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { api } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card, Table, Badge } from "../components/Card";
import { ArrowLeft, Trophy, FileText } from "lucide-react";

export default function ModelDetail() {
  const { name } = useParams<{ name: string }>();
  const [data, setData] = useState<any>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    api<any>(`models/${name}`).then(setData).catch((e) => setErr(String(e)));
  }, [name]);

  if (err)  return <div className="text-red-600 text-sm">{err}</div>;
  if (!data) return <div className="text-slate-500 text-sm">Loading…</div>;

  const latest = data.versions?.[0]?.version;
  const reportId = latest ? `${name}_v${latest}` : null;

  return (
    <>
      <Link to="/model-factory" className="text-sm text-slate-600 hover:underline flex items-center gap-1 mb-3">
        <ArrowLeft size={14} /> Back to model factory
      </Link>
      <PageHeader title={data.name} subtitle={data.full_name}
        actions={reportId && (
          <Link to={`/reports/${reportId}`} className="bg-blue-600 hover:bg-blue-700 text-white px-3 py-1.5 rounded text-sm flex items-center gap-1">
            <FileText size={14} /> View regulatory report
          </Link>
        )} />

      {data.aliases?.length > 0 && (
        <Card className="mb-6" title="Champion aliases">
          <div className="flex gap-2 flex-wrap">
            {data.aliases.map((a: any) => (
              <Badge key={a.alias_name} tone="green">
                <Trophy size={10} className="mr-0.5" /> {a.alias_name} → v{a.version_num}
              </Badge>
            ))}
          </div>
        </Card>
      )}

      <Card title={`Registered versions (${data.versions?.length || 0})`}>
        <Table
          columns={[
            { key: "version",    label: "Version", render: (r: any) => <Badge tone="blue">v{r.version}</Badge> },
            { key: "status",     label: "Status" },
            { key: "created_at", label: "Created" },
            { key: "run_id",     label: "MLflow run", render: (r: any) => (
              <code className="text-xs bg-slate-100 px-1 rounded font-mono">{r.run_id?.slice(0, 12)}…</code>
            ) },
            { key: "report",     label: "Report", render: (r: any) => (
              <Link to={`/reports/${name}_v${r.version}`} className="text-blue-600 hover:underline text-xs">
                Open
              </Link>
            ) },
          ]}
          rows={data.versions ?? []}
        />
      </Card>
    </>
  );
}
