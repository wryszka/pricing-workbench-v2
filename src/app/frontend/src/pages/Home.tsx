import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api, formatNumber } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card, StatNumber, Badge } from "../components/Card";
import { Database, FlaskConical, Boxes, FileText, Rocket, Shield, ArrowRight } from "lucide-react";

type HomeData = {
  entity: string;
  catalog: string;
  counts: Record<string, number>;
  pillars: { id: string; name: string; path: string }[];
};

const PILLAR_META: Record<string, { icon: any; description: string }> = {
  ingestion:  { icon: Database,    description: "4 vendor feeds → silver → gold. DLT data-quality gates, approval workflow." },
  features:   { icon: Boxes,       description: "Policy-year training + quote training + serving-time lookup tables." },
  modelling:  { icon: FlaskConical,description: "GLM + GBM + XGBoost + PyMC. Champion/challenger in UC model registry." },
  reports:    { icon: FileText,    description: "Per-model regulatory PDFs — lineage, SHAP reason codes, fairness audit." },
  serving:    { icon: Rocket,      description: "Model serving endpoint + online feature store. (Phase 4 — placeholder)" },
  governance: { icon: Shield,      description: "Audit log, dataset approvals, fairness report, model governance table." },
};

export default function Home() {
  const [data, setData] = useState<HomeData | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    api<HomeData>("home").then(setData).catch((e) => setErr(String(e)));
  }, []);

  if (err)  return <div className="text-red-600 text-sm">{err}</div>;
  if (!data) return <div className="text-slate-500 text-sm">Loading…</div>;

  const c = data.counts;

  return (
    <>
      <PageHeader
        title={`${data.entity} — Pricing Workbench`}
        subtitle={`Unity Catalog: ${data.catalog}`}
      />

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <Card><StatNumber value={formatNumber(c.dim_companies)} label="Companies" /></Card>
        <Card><StatNumber value={formatNumber(c.dim_policies)} label="Policies" hint={`${formatNumber(c.dim_policy_versions)} versions`} /></Card>
        <Card><StatNumber value={formatNumber(c.fact_quotes)} label="Quotes (4-year)" /></Card>
        <Card><StatNumber value={formatNumber(c.fact_claims)} label="Claims" /></Card>
        <Card><StatNumber value={formatNumber(c.feature_policy_year_training)} label="Training rows (policy-year)" /></Card>
        <Card><StatNumber value={formatNumber(c.feature_quote_training)} label="Training rows (quote)" /></Card>
        <Card><StatNumber value={formatNumber(c.model_governance)} label="Generated reports" /></Card>
        <Card><StatNumber value={formatNumber(c.app_audit_log)} label="Audit events" /></Card>
      </div>

      <h2 className="text-lg font-semibold text-slate-800 mb-3">Pillars</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {data.pillars.map((p) => {
          const meta = PILLAR_META[p.id];
          const Icon = meta?.icon || Shield;
          return (
            <Link
              key={p.id}
              to={p.path}
              className="block bg-white rounded-lg border border-slate-200 p-5 hover:border-blue-400 hover:shadow-sm transition"
            >
              <div className="flex items-start gap-3">
                <div className="p-2 rounded bg-blue-50 text-blue-600 shrink-0">
                  <Icon size={18} />
                </div>
                <div className="flex-1">
                  <div className="flex items-center justify-between">
                    <div className="font-medium text-slate-900">{p.name}</div>
                    <ArrowRight size={14} className="text-slate-400" />
                  </div>
                  <p className="text-sm text-slate-600 mt-1 leading-relaxed">{meta?.description}</p>
                </div>
              </div>
            </Link>
          );
        })}
      </div>

      <div className="mt-8">
        <Card title="About this demo">
          <div className="text-sm text-slate-700 space-y-2">
            <p>
              <strong>Bricksurance SE Pricing Workbench v2</strong> is a synthetic commercial-insurance
              pricing pipeline designed to demonstrate an actuary-accurate data model, a full model factory
              (GLM + GBM + XGBoost + PyMC), and regulator-ready explainability end-to-end.
            </p>
            <p>
              Every feature traces back to a source CSV → bronze → DLT silver → feature table → model
              via Delta versioning + MLflow dataset lineage. Every model gets a markdown report with
              SHAP reason codes, partial-dependence plots, and a protected-attribute audit.
            </p>
            <p className="text-xs text-slate-500">
              <Badge tone="amber">synthetic data</Badge>{" "}
              All data is simulated. No real Bricksurance or partner data is used.
            </p>
          </div>
        </Card>
      </div>
    </>
  );
}
