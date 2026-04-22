import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card } from "../components/Card";
import { Rocket, Clock } from "lucide-react";

export default function Serving() {
  const [data, setData] = useState<any>(null);

  useEffect(() => {
    api<any>("serving").then(setData).catch(() => {});
  }, []);

  return (
    <>
      <PageHeader title="Serving" subtitle="Model Serving endpoint + online feature store" />
      <Card>
        <div className="flex items-start gap-4">
          <div className="p-3 rounded-full bg-blue-50 text-blue-600 shrink-0">
            <Rocket size={20} />
          </div>
          <div className="flex-1">
            <h3 className="font-semibold text-slate-800 flex items-center gap-2">
              <Clock size={14} /> Phase 4 — placeholder
            </h3>
            <p className="text-sm text-slate-600 mt-2">
              {data?.message || "The serving endpoint is planned as a follow-up. The Model Factory is already producing UC-registered models with alias routing."}
            </p>
            {data?.plan?.length > 0 && (
              <ul className="list-disc pl-5 mt-3 space-y-1 text-sm text-slate-700">
                {data.plan.map((item: string, i: number) => <li key={i}>{item}</li>)}
              </ul>
            )}
          </div>
        </div>
      </Card>
    </>
  );
}
