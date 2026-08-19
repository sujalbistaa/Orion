// TypeScript mirrors of Orion's Go API types (pkg/api/v1, pkg/apiserver,
// pkg/store). Field names and shapes must match the JSON wire format exactly
// — see the Go source referenced above each section.
//
// Numeric wire types (from pkg/api/v1/resource.go):
//   MilliCPU and Bytes are both plain JSON numbers (milli-cores, bytes).
// Duration (pkg/api/v1/types.go) serializes as a Go duration STRING, e.g.
// "30s", "5m0s" — never as a bare number.

export type MilliCPU = number;
export type Bytes = number;
/** Wire form of pkg/api/v1.Duration: a Go duration string like "30s". */
export type GoDuration = string;

// ---------------------------------------------------------------------------
// ObjectMeta / common
// ---------------------------------------------------------------------------

export interface OwnerReference {
  kind: string;
  name: string;
  uid: string;
}

export interface Condition {
  type: string;
  status: boolean;
  reason: string;
  message?: string;
  since: string;
}

export interface ObjectMeta {
  name: string;
  uid: string;
  revision: number;
  generation: number;
  labels?: Record<string, string>;
  annotations?: Record<string, string>;
  createdAt: string;
  updatedAt: string;
  deletedAt?: string;
  ownerRef?: OwnerReference;
}

export interface Resources {
  cpu: MilliCPU;
  memory: Bytes;
}

export interface ResourceSpec {
  request: Resources;
  limit?: Resources;
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

export interface Member {
  id: string;
  address: string;
  role: "Leader" | "Follower" | "Candidate" | "Unknown" | string;
  reachable: boolean;
}

export interface Cluster {
  name: string;
  id: string;
  version: string;
  createdAt: string;
  controlPlane: Member[];
  leaderId: string;
  raftTerm: number;
  commitIndex: number;
  appliedIndex: number;
  quorum: number;
  quorumHealthy: boolean;
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

export type NodePhase =
  | "Registering"
  | "Ready"
  | "NotReady"
  | "Unreachable"
  | "Draining"
  | "Decommissioned";

export interface Taint {
  key: string;
  value?: string;
  effect: string;
}

export interface NodeSpec {
  address: string;
  unschedulable: boolean;
  taints?: Taint[];
}

export interface RuntimeInfo {
  name: string;
  version: string;
  os: string;
  arch: string;
  kernelVersion?: string;
}

export interface NodeStatus {
  phase: NodePhase;
  capacity: Resources;
  allocatable: Resources;
  allocated: Resources;
  usage: Resources;
  workloadCount: number;
  lastHeartbeat: string;
  agentStartedAt: string;
  runtime: RuntimeInfo;
  conditions?: Condition[];
}

export interface Node extends ObjectMeta {
  spec: NodeSpec;
  status: NodeStatus;
}

export interface NodeDetail {
  node: Node;
  workloads: Workload[];
  events: Event[];
}

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

export type WorkloadPhase =
  | "Pending"
  | "Scheduled"
  | "Starting"
  | "Running"
  | "Succeeded"
  | "Failed"
  | "Terminating"
  | "Terminated";

export type HealthState = "Unknown" | "Healthy" | "Unhealthy";

export type RestartPolicy = "Always" | "OnFailure" | "Never";

export type HealthCheckKind = "http" | "tcp" | "process";

export interface EnvVar {
  name: string;
  value: string;
}

export interface Port {
  name?: string;
  container: number;
  host?: number;
  protocol?: "tcp" | "udp" | string;
}

export interface HealthCheck {
  kind: HealthCheckKind;
  path?: string;
  port?: number;
  initialDelay: GoDuration;
  interval: GoDuration;
  timeout: GoDuration;
  failureThreshold: number;
  successThreshold: number;
}

export interface Toleration {
  key: string;
  value?: string;
}

export interface WorkloadSpec {
  image: string;
  command?: string[];
  args?: string[];
  env?: EnvVar[];
  ports?: Port[];
  resources: ResourceSpec;
  restartPolicy: RestartPolicy;
  terminationGracePeriod: GoDuration;
  healthCheck?: HealthCheck;
  priority: number;
  nodeSelector?: Record<string, string>;
  tolerations?: Toleration[];
}

export interface NodeScore {
  nodeName: string;
  score: number;
  breakdown?: Record<string, number>;
}

export interface NodeRejection {
  nodeName: string;
  filter: string;
  reason: string;
}

export interface PlacementDecision {
  workloadName: string;
  nodeName: string;
  decidedAt: string;
  score: number;
  reason: string;
  candidates?: NodeScore[];
  rejections?: NodeRejection[];
  latencyMicros: number;
}

export interface WorkloadStatus {
  phase: WorkloadPhase;
  health: HealthState;
  nodeName?: string;
  containerId?: string;
  message?: string;
  reason?: string;
  restartCount: number;
  exitCode?: number;
  startedAt?: string;
  finishedAt?: string;
  lastTransition: string;
  usage: Resources;
  hostPorts?: Record<number, number>;
  placement?: PlacementDecision;
  observedGeneration: number;
}

export interface Workload extends ObjectMeta {
  spec: WorkloadSpec;
  status: WorkloadStatus;
}

export interface WorkloadDetail {
  workload: Workload;
  node?: Node;
  events: Event[];
}

// ---------------------------------------------------------------------------
// Deployment
// ---------------------------------------------------------------------------

export type StrategyKind = "RollingUpdate" | "Recreate";

export interface Strategy {
  kind: StrategyKind;
  maxUnavailable: number;
  maxSurge: number;
}

export interface DeploymentSpec {
  replicas: number;
  template: WorkloadSpec;
  strategy: Strategy;
  progressDeadline?: GoDuration;
}

export type DeploymentPhase = "Progressing" | "Available" | "Degraded";

export interface DeploymentStatus {
  phase: DeploymentPhase;
  revision: number;
  desiredReplicas: number;
  currentReplicas: number;
  updatedReplicas: number;
  availableReplicas: number;
  unschedulableReplicas: number;
  conditions?: Condition[];
  observedGeneration: number;
  lastTransition: string;
}

export interface Deployment extends ObjectMeta {
  spec: DeploymentSpec;
  status: DeploymentStatus;
}

export interface DeploymentRevision {
  deployment: string;
  revision: number;
  template: WorkloadSpec;
  replicas: number;
  createdAt: string;
  templateHash: string;
}

export interface DeploymentDetail {
  deployment: Deployment;
  workloads: Workload[];
  revisions: DeploymentRevision[];
  events: Event[];
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

export type LoadBalanceStrategy = "RoundRobin" | "LeastConnections";

export interface ServiceSpec {
  selector: Record<string, string>;
  port: number;
  targetPort: number;
  strategy: LoadBalanceStrategy;
}

export interface Endpoint {
  workloadName: string;
  workloadUid: string;
  nodeName: string;
  address: string;
  port: number;
  health: HealthState;
  ready: boolean;
}

export interface ServiceStatus {
  endpoints: Endpoint[];
  healthyEndpoints: number;
  totalEndpoints: number;
  observedRevision: number;
  lastTransition: string;
}

export interface Service extends ObjectMeta {
  spec: ServiceSpec;
  status: ServiceStatus;
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

export type EventSeverity = "Info" | "Warning" | "Critical";

export interface Event {
  id: number;
  timestamp: string;
  severity: EventSeverity;
  source: string;
  reason: string;
  kind: string;
  name: string;
  message: string;
  actor?: string;
}

// ---------------------------------------------------------------------------
// Cluster summary (pkg/store/read.go ClusterSummary)
// ---------------------------------------------------------------------------

export interface ClusterSummary {
  nodes: {
    total: number;
    ready: number;
    notReady: number;
    unreachable: number;
    cordoned: number;
  };
  workloads: {
    total: number;
    running: number;
    pending: number;
    starting: number;
    failed: number;
    succeeded: number;
    unhealthy: number;
    unschedulable: number;
    restarts: number;
  };
  deployments: {
    total: number;
    available: number;
    progressing: number;
    degraded: number;
  };
  services: {
    total: number;
    withoutHealthyEndpoints: number;
  };
  capacity: {
    cpuAllocatable: MilliCPU;
    cpuAllocated: MilliCPU;
    cpuUsed: MilliCPU;
    memAllocatable: Bytes;
    memAllocated: Bytes;
    memUsed: Bytes;
  };
  appliedIndex: number;
}

export interface ClusterResponse {
  cluster: Cluster;
  summary: ClusterSummary;
}

// ---------------------------------------------------------------------------
// Fault injection (pkg/apiserver/faults.go)
// ---------------------------------------------------------------------------

export type ExperimentKind =
  | "node-failure"
  | "node-restart"
  | "workload-crash"
  | "controller-crash"
  | "leader-failure"
  | "network-partition"
  | "resource-exhaustion";

export type ParameterType =
  | "string"
  | "duration"
  | "int"
  | "node"
  | "workload"
  | "deployment";

export interface ParameterSpec {
  name: string;
  type: ParameterType;
  required: boolean;
  default?: string;
  help: string;
}

export interface ExperimentDescriptor {
  kind: ExperimentKind;
  name: string;
  description: string;
  hypothesis: string;
  invariants: string[];
  parameters: ParameterSpec[];
  destructive: boolean;
}

export type RunState =
  | "Pending"
  | "Injecting"
  | "Observing"
  | "Recovering"
  | "Succeeded"
  | "Failed"
  | "Aborted";

export interface TimelineEntry {
  at: string;
  elapsed: string;
  phase: RunState;
  message: string;
  level: "info" | "warn" | "error" | string;
}

export interface InvariantResult {
  name: string;
  held: boolean;
  detail: string;
  violations: number;
}

export interface ExperimentRun {
  id: string;
  kind: ExperimentKind;
  state: RunState;
  params: Record<string, string>;
  actor: string;
  startedAt: string;
  finishedAt?: string;
  affectedNodes?: string[];
  affectedWorkloads?: string[];
  timeline: TimelineEntry[];
  invariants: InvariantResult[];
  recoveryDuration?: string;
  error?: string;
}

export const RUN_TERMINAL_STATES: RunState[] = ["Succeeded", "Failed", "Aborted"];

// ---------------------------------------------------------------------------
// Envelopes
// ---------------------------------------------------------------------------

export interface ListResponse<T> {
  items: T[];
  revision: number;
  total: number;
}

export interface FieldError {
  field: string;
  detail: string;
}

export interface APIError {
  code: string;
  message: string;
  fields?: FieldError[];
}

export interface ErrorEnvelope {
  error: APIError;
}

// ---------------------------------------------------------------------------
// Watch / SSE (pkg/store/watch.go Change, pkg/apiserver/watch.go)
// ---------------------------------------------------------------------------

export interface WatchChange {
  revision: number;
  kind: string;
  op: "Created" | "Updated" | "Deleted" | string;
  name: string;
  object?: unknown;
}

export interface WatchSync {
  revision: number;
  leader: boolean;
}

export interface WatchResync {
  reason: string;
  revision: number;
}
