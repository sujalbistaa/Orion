import { Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { Overview } from "./pages/Overview";
import { Clusters } from "./pages/Clusters";
import { Nodes } from "./pages/Nodes";
import { NodeDetail } from "./pages/NodeDetail";
import { Workloads } from "./pages/Workloads";
import { WorkloadDetail } from "./pages/WorkloadDetail";
import { Deployments } from "./pages/Deployments";
import { DeploymentDetail } from "./pages/DeploymentDetail";
import { Services } from "./pages/Services";
import { Events } from "./pages/Events";
import { Observability } from "./pages/Observability";
import { FaultInjection } from "./pages/FaultInjection";
import { FaultRunDetail } from "./pages/FaultRunDetail";
import { Settings } from "./pages/Settings";
import { NotFound } from "./pages/NotFound";

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Overview />} />
        <Route path="/clusters" element={<Clusters />} />
        <Route path="/nodes" element={<Nodes />} />
        <Route path="/nodes/:name" element={<NodeDetail />} />
        <Route path="/workloads" element={<Workloads />} />
        <Route path="/workloads/:name" element={<WorkloadDetail />} />
        <Route path="/deployments" element={<Deployments />} />
        <Route path="/deployments/:name" element={<DeploymentDetail />} />
        <Route path="/services" element={<Services />} />
        <Route path="/events" element={<Events />} />
        <Route path="/observability" element={<Observability />} />
        <Route path="/faults" element={<FaultInjection />} />
        <Route path="/faults/runs/:id" element={<FaultRunDetail />} />
        <Route path="/settings" element={<Settings />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
