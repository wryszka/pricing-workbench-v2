import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { api } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card, Table, Badge } from "../components/Card";
import { ArrowLeft } from "lucide-react";

export default function PolicyDetail() {
  const { id } = useParams<{ id: string }>();
  const [data, setData] = useState<any>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    api<any>(`policies/${id}`).then(setData).catch((e) => setErr(String(e)));
  }, [id]);

  if (err) return <div className="text-red-600 text-sm">{err}</div>;
  if (!data) return <div className="text-slate-500 text-sm">Loading…</div>;

  const p = data.policy;

  return (
    <>
      <Link to="/policies" className="text-sm text-slate-600 hover:underline flex items-center gap-1 mb-3">
        <ArrowLeft size={14} /> Back to policies
      </Link>
      <PageHeader title={p.policy_id} subtitle={p.company_name} />

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <Card><div className="text-xs text-slate-500 uppercase">Status</div><Badge tone={p.status === "active" ? "green" : "amber"}>{p.status}</Badge></Card>
        <Card><div className="text-xs text-slate-500 uppercase">Region</div><div>{p.region}</div></Card>
        <Card><div className="text-xs text-slate-500 uppercase">SIC</div><div className="font-mono text-sm">{p.primary_sic_code}</div></Card>
        <Card><div className="text-xs text-slate-500 uppercase">Inception</div><div className="text-sm">{p.inception_date}</div></Card>
      </div>

      <Card title={`Policy versions (${data.versions?.length || 0})`} className="mb-6">
        <Table
          columns={[
            { key: "policy_version",  label: "Version", render: (r: any) => <Badge tone="blue">v{r.policy_version}</Badge> },
            { key: "effective_from",  label: "From" },
            { key: "effective_to",    label: "To" },
            { key: "sum_insured",     label: "Sum insured" },
            { key: "gross_premium",   label: "Premium" },
            { key: "construction_type", label: "Construction" },
            { key: "channel",         label: "Channel" },
            { key: "model_version_used", label: "Model", render: (r: any) => (
              <code className="text-xs bg-slate-100 px-1 rounded">{r.model_version_used || "—"}</code>
            ) },
          ]}
          rows={data.versions ?? []}
        />
      </Card>

      <Card title={`Claims (${data.claims?.length || 0})`}>
        <Table
          empty="No claims on this policy"
          columns={[
            { key: "claim_id",        label: "Claim ID", className: "font-mono text-xs" },
            { key: "policy_version",  label: "On ver" },
            { key: "loss_date",       label: "Loss date" },
            { key: "reported_date",   label: "Reported" },
            { key: "peril",           label: "Peril", render: (r: any) => <Badge tone="slate">{r.peril}</Badge> },
            { key: "status",          label: "Status" },
            { key: "incurred_amount", label: "Incurred" },
            { key: "paid_amount",     label: "Paid" },
          ]}
          rows={data.claims ?? []}
        />
      </Card>
    </>
  );
}
