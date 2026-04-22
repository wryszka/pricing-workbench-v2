import { NavLink } from "react-router-dom";
import {
  Database, FlaskConical, Boxes, FileText, Rocket, Activity,
  Shield, Zap, LineChart, BookOpen,
} from "lucide-react";

const NAV = [
  { to: "/",              label: "Home",               icon: LineChart },
  { to: "/external-data", label: "External Data",      icon: Database },
  { to: "/feature-table", label: "Feature Table",      icon: Boxes },
  { to: "/policies",      label: "Policies",           icon: BookOpen },
  { to: "/model-factory", label: "Model Factory",      icon: FlaskConical },
  { to: "/reports",       label: "Regulatory Reports", icon: FileText },
  { to: "/serving",       label: "Serving",            icon: Rocket },
  { to: "/monitoring",    label: "Monitoring",         icon: Activity },
  { to: "/governance",    label: "Governance",         icon: Shield },
  { to: "/quote-review",  label: "Quote Review",       icon: Zap },
];

export function Sidebar() {
  return (
    <aside className="w-56 shrink-0 bg-sidebar text-slate-200 flex flex-col min-h-screen">
      <div className="p-4 border-b border-slate-700">
        <div className="font-semibold text-white">Pricing Workbench</div>
        <div className="text-xs text-slate-400 mt-0.5">v2 — Bricksurance SE</div>
      </div>
      <nav className="flex-1 p-2 space-y-0.5">
        {NAV.map(({ to, label, icon: Icon }) => (
          <NavLink
            key={to}
            to={to}
            end={to === "/"}
            className={({ isActive }) =>
              `flex items-center gap-2 px-3 py-2 rounded text-sm transition ` +
              (isActive
                ? "bg-blue-600 text-white"
                : "hover:bg-sidebarAccent text-slate-300")
            }
          >
            <Icon size={16} />
            <span>{label}</span>
          </NavLink>
        ))}
      </nav>
      <div className="p-3 border-t border-slate-700 text-[11px] text-slate-500">
        Actuary-accurate pricing pipeline · regulator-ready reports
      </div>
    </aside>
  );
}
