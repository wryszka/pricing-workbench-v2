import type { ReactNode } from "react";

export function Card({
  title, right, children, className = "",
}: {
  title?: string;
  right?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={`bg-white rounded-lg border border-slate-200 ${className}`}>
      {(title || right) && (
        <div className="px-4 py-3 border-b border-slate-100 flex items-center justify-between gap-3">
          {title && <h3 className="font-medium text-slate-800">{title}</h3>}
          {right}
        </div>
      )}
      <div className="p-4">{children}</div>
    </div>
  );
}

export function StatNumber({ value, label, hint }: { value: ReactNode; label: string; hint?: string }) {
  return (
    <div>
      <div className="text-2xl font-semibold text-slate-900">{value}</div>
      <div className="text-xs uppercase tracking-wide text-slate-500 mt-1">{label}</div>
      {hint && <div className="text-xs text-slate-400 mt-0.5">{hint}</div>}
    </div>
  );
}

export function Badge({
  children, tone = "slate",
}: { children: ReactNode; tone?: "slate" | "green" | "amber" | "red" | "blue" }) {
  const tones = {
    slate: "bg-slate-100 text-slate-700 border-slate-200",
    green: "bg-emerald-50 text-emerald-700 border-emerald-200",
    amber: "bg-amber-50 text-amber-700 border-amber-200",
    red:   "bg-red-50 text-red-700 border-red-200",
    blue:  "bg-blue-50 text-blue-700 border-blue-200",
  }[tone];
  return (
    <span className={`inline-flex items-center px-2 py-0.5 text-[11px] rounded border ${tones}`}>
      {children}
    </span>
  );
}

export function Table({
  columns, rows, empty = "No data",
}: {
  columns: { key: string; label: string; render?: (row: any) => ReactNode; className?: string }[];
  rows: any[];
  empty?: string;
}) {
  if (!rows || rows.length === 0) {
    return <div className="text-sm text-slate-500 italic py-4">{empty}</div>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-slate-500 text-xs uppercase tracking-wide border-b border-slate-200">
            {columns.map((c) => (
              <th key={c.key} className={`font-medium py-2 pr-3 ${c.className || ""}`}>{c.label}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {rows.map((row, i) => (
            <tr key={i} className="hover:bg-slate-50">
              {columns.map((c) => (
                <td key={c.key} className={`py-2 pr-3 ${c.className || ""}`}>
                  {c.render ? c.render(row) : row[c.key]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
