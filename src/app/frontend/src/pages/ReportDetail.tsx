import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { api } from "../lib/api";
import { PageHeader } from "../components/PageHeader";
import { Card } from "../components/Card";
import { ArrowLeft, Download } from "lucide-react";

export default function ReportDetail() {
  const { id } = useParams<{ id: string }>();
  const [markdown, setMarkdown] = useState<string | null>(null);
  const [files, setFiles] = useState<{ plots: string[]; tables: string[] }>({ plots: [], tables: [] });
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    api<{ markdown: string }>(`reports/${encodeURIComponent(id)}`)
      .then((r) => setMarkdown(r.markdown))
      .catch((e) => setErr(String(e)));
    api<{ files: { plots: string[]; tables: string[] } }>(`reports/${encodeURIComponent(id)}/files`)
      .then((r) => setFiles(r.files))
      .catch(() => {});
  }, [id]);

  if (err) return <div className="text-red-600 text-sm">{err}</div>;
  if (!markdown) return <div className="text-slate-500 text-sm">Loading…</div>;

  // Rewrite image paths (e.g. "plots/calibration.png") to route through our backend
  // so the volume-backed PNGs render correctly in the browser.
  const rewritten = markdown.replace(/!\[([^\]]*)\]\(plots\/([^)]+)\)/g,
    (_, alt, name) => `![${alt}](/api/reports/${encodeURIComponent(id!)}/plot/${encodeURIComponent(name)})`);

  return (
    <>
      <Link to="/reports" className="text-sm text-slate-600 hover:underline flex items-center gap-1 mb-3">
        <ArrowLeft size={14} /> Back to reports
      </Link>
      <PageHeader title={id || ""}
        actions={
          <div className="flex gap-2">
            <a href={`/api/reports/${encodeURIComponent(id!)}`} target="_blank" rel="noreferrer"
              className="border border-slate-300 px-3 py-1.5 rounded text-sm flex items-center gap-1 hover:bg-slate-50">
              <Download size={14} /> Raw markdown
            </a>
          </div>
        }
      />

      <div className="grid grid-cols-1 lg:grid-cols-[1fr_260px] gap-6">
        <Card>
          <div className="md-report">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{rewritten}</ReactMarkdown>
          </div>
        </Card>

        <aside className="space-y-4">
          <Card title={`Plots (${files.plots.length})`}>
            {files.plots.length === 0 ? (
              <div className="text-xs text-slate-500 italic">None</div>
            ) : (
              <ul className="space-y-1">
                {files.plots.map((p) => (
                  <li key={p}>
                    <a href={`/api/reports/${encodeURIComponent(id!)}/plot/${encodeURIComponent(p)}`}
                      target="_blank" rel="noreferrer"
                      className="text-xs text-blue-600 hover:underline">{p}</a>
                  </li>
                ))}
              </ul>
            )}
          </Card>
          <Card title={`Tables (${files.tables.length})`}>
            {files.tables.length === 0 ? (
              <div className="text-xs text-slate-500 italic">None</div>
            ) : (
              <ul className="space-y-1">
                {files.tables.map((p) => (
                  <li key={p}>
                    <a href={`/api/reports/${encodeURIComponent(id!)}/table/${encodeURIComponent(p)}`}
                      target="_blank" rel="noreferrer"
                      className="text-xs text-blue-600 hover:underline">{p}</a>
                  </li>
                ))}
              </ul>
            )}
          </Card>
        </aside>
      </div>
    </>
  );
}
