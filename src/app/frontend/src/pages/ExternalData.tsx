import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api, formatNumber } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card, Table } from "../components/Card";
import { ChevronRight } from "lucide-react";

type Dataset = {
  id: string; name: string; description: string; owner: string;
  raw: string; silver: string;
  raw_count: number; silver_count: number;
};

export default function ExternalData() {
  const [data, setData] = useState<{ datasets: Dataset[] } | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    api<{ datasets: Dataset[] }>("datasets").then(setData).catch((e) => setErr(String(e)));
  }, []);

  return (
    <>
      <PageHeader
        title="External Data"
        subtitle="Four vendor feeds arrive as CSVs in the external_landing volume, are ingested to raw_* tables, then validated by DLT into silver_* tables."
      />
      {err && <div className="text-red-600 text-sm mb-4">{err}</div>}
      <Card>
        <Table
          empty={data ? "No datasets" : "Loading…"}
          columns={[
            { key: "name", label: "Dataset", render: (r: Dataset) => (
              <Link to={`/external-data/${r.id}`} className="text-blue-600 hover:underline font-medium flex items-center gap-1">
                {r.name} <ChevronRight size={14} />
              </Link>
            ) },
            { key: "description", label: "Description" },
            { key: "owner", label: "Owner" },
            { key: "raw_count",    label: "Raw rows",    render: (r: Dataset) => formatNumber(r.raw_count) },
            { key: "silver_count", label: "Silver rows", render: (r: Dataset) => formatNumber(r.silver_count) },
            { key: "dropped",      label: "DQ drops",    render: (r: Dataset) => formatNumber(Math.max(0, r.raw_count - r.silver_count)) },
          ]}
          rows={data?.datasets ?? []}
        />
      </Card>
    </>
  );
}
