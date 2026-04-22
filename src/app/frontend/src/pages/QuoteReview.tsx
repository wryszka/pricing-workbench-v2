import { PageHeader } from "../components/PageHeader";
import { Card } from "../components/Card";
import { Zap } from "lucide-react";

export default function QuoteReview() {
  return (
    <>
      <PageHeader
        title="Quote Review"
        subtitle="Per-quote forensic drill-down with model replay and SHAP decomposition (bolt-on)."
      />
      <Card>
        <div className="flex items-start gap-4">
          <div className="p-3 rounded-full bg-indigo-50 text-indigo-600 shrink-0">
            <Zap size={20} />
          </div>
          <div>
            <h3 className="font-semibold text-slate-800">Placeholder</h3>
            <p className="text-sm text-slate-600 mt-2">
              This bolt-on will read saved quote payloads from the
              <code className="bg-slate-100 px-1 mx-1 rounded text-xs">saved_payloads</code>
              volume, rescore them against the current champion, and render per-feature SHAP
              contributions for any underwriter who asks "why did we price this way?".
            </p>
            <p className="text-xs text-slate-500 mt-2">
              The underlying infrastructure is already in place: <code className="bg-slate-100 px-1 rounded text-xs">feature_quote_training</code>
              {" "}has the historic quote stream, and <code className="bg-slate-100 px-1 rounded text-xs">demand_gbm</code>
              {" "}scores conversion probability. This tab only needs the UI.
            </p>
          </div>
        </div>
      </Card>
    </>
  );
}
