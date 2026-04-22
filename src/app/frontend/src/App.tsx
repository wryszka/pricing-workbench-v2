import { Routes, Route } from "react-router-dom";
import { Sidebar } from "./components/Sidebar";

import Home from "./pages/Home";
import ExternalData from "./pages/ExternalData";
import DatasetDetail from "./pages/DatasetDetail";
import FeatureTable from "./pages/FeatureTable";
import FeatureTableDetail from "./pages/FeatureTableDetail";
import Policies from "./pages/Policies";
import PolicyDetail from "./pages/PolicyDetail";
import ModelFactory from "./pages/ModelFactory";
import ModelDetail from "./pages/ModelDetail";
import Reports from "./pages/Reports";
import ReportDetail from "./pages/ReportDetail";
import Serving from "./pages/Serving";
import Monitoring from "./pages/Monitoring";
import Governance from "./pages/Governance";
import QuoteReview from "./pages/QuoteReview";

export default function App() {
  return (
    <div className="flex min-h-screen bg-slate-50">
      <Sidebar />
      <main className="flex-1 overflow-x-hidden">
        <div className="max-w-7xl mx-auto px-6 py-8">
          <Routes>
            <Route path="/"                          element={<Home />} />
            <Route path="/external-data"             element={<ExternalData />} />
            <Route path="/external-data/:id"         element={<DatasetDetail />} />
            <Route path="/feature-table"             element={<FeatureTable />} />
            <Route path="/feature-table/:id"         element={<FeatureTableDetail />} />
            <Route path="/policies"                  element={<Policies />} />
            <Route path="/policies/:id"              element={<PolicyDetail />} />
            <Route path="/model-factory"             element={<ModelFactory />} />
            <Route path="/model-factory/:name"       element={<ModelDetail />} />
            <Route path="/reports"                   element={<Reports />} />
            <Route path="/reports/:id"               element={<ReportDetail />} />
            <Route path="/serving"                   element={<Serving />} />
            <Route path="/monitoring"                element={<Monitoring />} />
            <Route path="/governance"                element={<Governance />} />
            <Route path="/quote-review"              element={<QuoteReview />} />
          </Routes>
        </div>
      </main>
    </div>
  );
}
