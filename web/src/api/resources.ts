// One function per real Orion endpoint (pkg/apiserver/server.go routes()).
// Nothing here fabricates data; every function is a direct call through
// ./client.ts to the live API.
import { api } from "./client";
import type {
  ClusterResponse,
  ClusterSummary,
  Deployment,
  DeploymentDetail,
  DeploymentRevision,
  Event,
  ExperimentDescriptor,
  ExperimentRun,
  ListResponse,
  Node,
  NodeDetail,
  Service,
  Workload,
  WorkloadDetail,
} from "./types";

// --- Cluster ---------------------------------------------------------------

export const getCluster = () => api.get<ClusterResponse>("/api/v1/cluster");
export const getSummary = () => api.get<ClusterSummary>("/api/v1/summary");

// --- Nodes -------------------------------------------------------------

export const listNodes = () => api.get<ListResponse<Node>>("/api/v1/nodes");
export const getNode = (name: string) =>
  api.get<NodeDetail>(`/api/v1/nodes/${encodeURIComponent(name)}`);
export const cordonNode = (name: string) =>
  api.post(`/api/v1/nodes/${encodeURIComponent(name)}/cordon`);
export const uncordonNode = (name: string) =>
  api.post(`/api/v1/nodes/${encodeURIComponent(name)}/uncordon`);
export const drainNode = (name: string, force: boolean) =>
  api.post(
    `/api/v1/nodes/${encodeURIComponent(name)}/drain${force ? "?force=true" : ""}`,
  );
export const deleteNode = (name: string, force: boolean) =>
  api.delete(
    `/api/v1/nodes/${encodeURIComponent(name)}${force ? "?force=true" : ""}`,
  );

// --- Workloads ---------------------------------------------------------

export const listWorkloads = () =>
  api.get<ListResponse<Workload>>("/api/v1/workloads");
export const getWorkload = (name: string) =>
  api.get<WorkloadDetail>(`/api/v1/workloads/${encodeURIComponent(name)}`);
export const deleteWorkload = (
  name: string,
  opts: { force?: boolean; revision?: number } = {},
) => {
  const params = new URLSearchParams();
  if (opts.force) params.set("force", "true");
  if (opts.revision) params.set("revision", String(opts.revision));
  const qs = params.toString();
  return api.delete(
    `/api/v1/workloads/${encodeURIComponent(name)}${qs ? `?${qs}` : ""}`,
  );
};

export function workloadLogsUrl(name: string, tail = 200): string {
  return `/api/v1/workloads/${encodeURIComponent(name)}/logs?tail=${tail}`;
}

// --- Deployments ---------------------------------------------------------

export const listDeployments = () =>
  api.get<ListResponse<Deployment>>("/api/v1/deployments");
export const getDeployment = (name: string) =>
  api.get<DeploymentDetail>(`/api/v1/deployments/${encodeURIComponent(name)}`);
export const createDeployment = (body: unknown) =>
  api.post<Deployment>("/api/v1/deployments", body);
export const scaleDeployment = (name: string, replicas: number) =>
  api.post(`/api/v1/deployments/${encodeURIComponent(name)}/scale`, {
    replicas,
  });
export const rollbackDeployment = (name: string, revision: number) =>
  api.post<Deployment>(
    `/api/v1/deployments/${encodeURIComponent(name)}/rollback`,
    { revision },
  );
export const getDeploymentRevisions = (name: string) =>
  api.get<ListResponse<DeploymentRevision>>(
    `/api/v1/deployments/${encodeURIComponent(name)}/revisions`,
  );
export const deleteDeployment = (name: string) =>
  api.delete(`/api/v1/deployments/${encodeURIComponent(name)}`);

// --- Services ------------------------------------------------------------

export const listServices = () =>
  api.get<ListResponse<Service>>("/api/v1/services");
export const getService = (name: string) =>
  api.get<{ service: Service; events: Event[] }>(
    `/api/v1/services/${encodeURIComponent(name)}`,
  );
export const createService = (body: unknown) =>
  api.post<Service>("/api/v1/services", body);
export const deleteService = (name: string) =>
  api.delete(`/api/v1/services/${encodeURIComponent(name)}`);

// --- Events ----------------------------------------------------------------

export interface EventQuery {
  since?: number;
  before?: number;
  kind?: string;
  name?: string;
  severity?: string;
  limit?: number;
}

export function listEvents(q: EventQuery = {}) {
  const params = new URLSearchParams();
  if (q.since) params.set("since", String(q.since));
  if (q.before) params.set("before", String(q.before));
  if (q.kind) params.set("kind", q.kind);
  if (q.name) params.set("name", q.name);
  if (q.severity) params.set("severity", q.severity);
  if (q.limit) params.set("limit", String(q.limit));
  const qs = params.toString();
  return api.get<ListResponse<Event>>(`/api/v1/events${qs ? `?${qs}` : ""}`);
}

// --- Fault injection ---------------------------------------------------

export const listExperiments = () =>
  api.get<ListResponse<ExperimentDescriptor>>("/api/v1/faults/experiments");
export const listRuns = () =>
  api.get<ListResponse<ExperimentRun>>("/api/v1/faults/runs");
export const getRun = (id: string) =>
  api.get<ExperimentRun>(`/api/v1/faults/runs/${encodeURIComponent(id)}`);
export const startRun = (body: {
  kind: string;
  params: Record<string, string>;
  durationSeconds?: number;
}) => api.post<ExperimentRun>("/api/v1/faults/runs", body);
export const abortRun = (id: string) =>
  api.post<ExperimentRun>(
    `/api/v1/faults/runs/${encodeURIComponent(id)}/abort`,
  );
