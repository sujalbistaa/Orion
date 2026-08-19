// Package v1 defines Orion's core domain model: the objects the control plane
// stores, the states they move through, and the invariants that must hold.
//
// Design notes that differ deliberately from Kubernetes:
//
//   - There are no namespaces. Orion targets a single administrative domain;
//     namespaces would add addressing complexity to every API path without
//     changing any of the interesting distributed-systems behaviour.
//   - A Workload is a single container, not a pod. Multi-container co-scheduling
//     is a real feature but an orthogonal one; keeping one container per
//     schedulable unit keeps the resource accounting exact and readable.
//   - Every object carries a Revision assigned from the Raft log index. It is
//     the cluster-wide ordering used for optimistic concurrency and for the
//     console's incremental updates. Clients never invent revisions.
package v1

import "time"

// ObjectMeta is embedded in every persisted object.
type ObjectMeta struct {
	// Name is the user-facing unique identifier within a kind. Immutable.
	Name string `json:"name"`
	// UID is assigned by the control plane at creation and never reused. It
	// distinguishes a recreated object from its predecessor of the same name.
	UID string `json:"uid"`
	// Revision is the Raft log index of the last write that changed this
	// object. Clients pass it back for compare-and-swap updates.
	Revision uint64 `json:"revision"`
	// Generation increments only when the *spec* changes. Controllers compare
	// it to Status.ObservedGeneration to know whether they have caught up.
	Generation int64 `json:"generation"`

	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`

	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	// DeletedAt marks the start of graceful deletion. While set, the object is
	// visible but is being torn down; controllers must not resurrect it.
	DeletedAt *time.Time `json:"deletedAt,omitempty"`

	// OwnerRef ties a workload to the deployment that created it. Deleting the
	// owner cascades to owned objects.
	OwnerRef *OwnerReference `json:"ownerRef,omitempty"`
}

type OwnerReference struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	UID  string `json:"uid"`
}

// Condition is an observation about an object at a point in time. Conditions
// accumulate detail that a single phase cannot express (why a node is
// NotReady, why a deployment is Degraded).
type Condition struct {
	Type    string    `json:"type"`
	Status  bool      `json:"status"`
	Reason  string    `json:"reason"`
	Message string    `json:"message,omitempty"`
	Since   time.Time `json:"since"`
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

// Cluster is the singleton describing this Orion installation. It is derived
// state plus a small amount of operator-set configuration.
type Cluster struct {
	Name          string    `json:"name"`
	ID            string    `json:"id"`
	Version       string    `json:"version"`
	CreatedAt     time.Time `json:"createdAt"`
	ControlPlane  []Member  `json:"controlPlane"`
	LeaderID      string    `json:"leaderId"`
	RaftTerm      uint64    `json:"raftTerm"`
	CommitIndex   uint64    `json:"commitIndex"`
	AppliedIndex  uint64    `json:"appliedIndex"`
	Quorum        int       `json:"quorum"`
	QuorumHealthy bool      `json:"quorumHealthy"`
}

// Member is a control-plane replica.
type Member struct {
	ID        string `json:"id"`
	Address   string `json:"address"`
	Role      string `json:"role"` // Leader | Follower | Candidate | Unknown
	Reachable bool   `json:"reachable"`
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

// Node is a machine running an Orion agent.
type Node struct {
	ObjectMeta `json:",inline"`
	Spec       NodeSpec   `json:"spec"`
	Status     NodeStatus `json:"status"`
}

type NodeSpec struct {
	// Address is the agent's reachable host:port for control-plane callbacks
	// and for service endpoint construction.
	Address string `json:"address"`
	// Unschedulable is operator-controlled (cordon). It is independent of
	// phase: a Ready node can still be unschedulable.
	Unschedulable bool `json:"unschedulable"`
	// Taints exclude workloads that do not tolerate them.
	Taints []Taint `json:"taints,omitempty"`
}

type Taint struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect"` // NoSchedule
}

type NodeStatus struct {
	Phase NodePhase `json:"phase"`
	// Capacity is the node's total resources as reported by the agent.
	Capacity Resources `json:"capacity"`
	// Allocatable is Capacity minus a system reservation. The scheduler uses
	// Allocatable, never Capacity.
	Allocatable Resources `json:"allocatable"`
	// Allocated is the sum of resource requests of active workloads bound to
	// this node, computed by the control plane rather than reported by the
	// agent so that scheduling decisions never trust a node's arithmetic.
	Allocated Resources `json:"allocated"`
	// Usage is live measured consumption from the runtime. Informational: the
	// scheduler does not place on measured usage, which would make placement
	// non-deterministic and oscillate.
	Usage Resources `json:"usage"`

	WorkloadCount int `json:"workloadCount"`

	LastHeartbeat time.Time `json:"lastHeartbeat"`
	// AgentStartedAt lets the control plane detect an agent restart (a new
	// start time on the same node) and re-sync workload state.
	AgentStartedAt time.Time   `json:"agentStartedAt"`
	Runtime        RuntimeInfo `json:"runtime"`
	Conditions     []Condition `json:"conditions,omitempty"`
}

type RuntimeInfo struct {
	Name          string `json:"name"`
	Version       string `json:"version"`
	OS            string `json:"os"`
	Arch          string `json:"arch"`
	KernelVersion string `json:"kernelVersion,omitempty"`
}

// Available returns the resources the scheduler may still commit on this node.
func (n *Node) Available() Resources {
	return n.Status.Allocatable.Sub(n.Status.Allocated)
}

// Schedulable reports whether the scheduler may consider this node at all.
func (n *Node) Schedulable() bool {
	return n.Status.Phase.Schedulable() && !n.Spec.Unschedulable && n.DeletedAt == nil
}

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

// Workload is one schedulable, running container instance.
type Workload struct {
	ObjectMeta `json:",inline"`
	Spec       WorkloadSpec   `json:"spec"`
	Status     WorkloadStatus `json:"status"`
}

type WorkloadSpec struct {
	Image     string       `json:"image"`
	Command   []string     `json:"command,omitempty"`
	Args      []string     `json:"args,omitempty"`
	Env       []EnvVar     `json:"env,omitempty"`
	Ports     []Port       `json:"ports,omitempty"`
	Resources ResourceSpec `json:"resources"`

	RestartPolicy RestartPolicy `json:"restartPolicy"`
	// TerminationGracePeriod is how long the agent waits after SIGTERM before
	// SIGKILL.
	TerminationGracePeriod Duration `json:"terminationGracePeriod"`

	HealthCheck *HealthCheck `json:"healthCheck,omitempty"`

	// Priority orders admission when resources are scarce. Higher wins.
	Priority int32 `json:"priority"`
	// NodeSelector constrains placement to nodes carrying all these labels.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	Tolerations  []Toleration      `json:"tolerations,omitempty"`
}

type Toleration struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Port struct {
	Name string `json:"name,omitempty"`
	// Container is the port the process listens on inside the container.
	Container int32 `json:"container"`
	// Host, when non-zero, publishes the port on the node. Zero means the
	// runtime picks an ephemeral host port, which Orion records in the status
	// so services can route to it.
	Host     int32  `json:"host,omitempty"`
	Protocol string `json:"protocol,omitempty"` // tcp (default) | udp
}

// HealthCheckKind selects the probe mechanism.
type HealthCheckKind string

const (
	HealthCheckHTTP HealthCheckKind = "http"
	HealthCheckTCP  HealthCheckKind = "tcp"
	// HealthCheckProcess treats "container is running" as healthy. It is the
	// implicit default and requires no probe traffic.
	HealthCheckProcess HealthCheckKind = "process"
)

type HealthCheck struct {
	Kind HealthCheckKind `json:"kind"`
	Path string          `json:"path,omitempty"`
	Port int32           `json:"port,omitempty"`

	InitialDelay Duration `json:"initialDelay"`
	Interval     Duration `json:"interval"`
	Timeout      Duration `json:"timeout"`
	// FailureThreshold consecutive failures mark the workload Unhealthy.
	FailureThreshold int32 `json:"failureThreshold"`
	// SuccessThreshold consecutive successes mark it Healthy again.
	SuccessThreshold int32 `json:"successThreshold"`
}

type WorkloadStatus struct {
	Phase  WorkloadPhase `json:"phase"`
	Health HealthState   `json:"health"`
	// NodeName is written exactly once, by the scheduler, on the
	// Pending -> Scheduled edge. A workload is never re-bound to a different
	// node; rescheduling creates a replacement workload with a new UID.
	NodeName string `json:"nodeName,omitempty"`
	// ContainerID is the runtime's identifier, set by the agent.
	ContainerID string `json:"containerId,omitempty"`

	Message      string     `json:"message,omitempty"`
	Reason       string     `json:"reason,omitempty"`
	RestartCount int32      `json:"restartCount"`
	ExitCode     *int32     `json:"exitCode,omitempty"`
	StartedAt    *time.Time `json:"startedAt,omitempty"`
	FinishedAt   *time.Time `json:"finishedAt,omitempty"`
	// LastTransition is when Phase last changed; used for timeout detection.
	LastTransition time.Time `json:"lastTransition"`

	// Usage is live measured consumption reported by the agent.
	Usage Resources `json:"usage"`
	// HostPorts maps container port -> actual published host port.
	HostPorts map[int32]int32 `json:"hostPorts,omitempty"`

	// Placement records why the scheduler chose this node. Kept on the object
	// so operators can answer "why is this here?" long after the decision.
	Placement *PlacementDecision `json:"placement,omitempty"`

	ObservedGeneration int64 `json:"observedGeneration"`
}

// Ready reports whether this workload should serve traffic.
func (w *Workload) Ready() bool {
	return w.Status.Phase == WorkloadRunning && w.Status.Health.Serving() && w.DeletedAt == nil
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------

// PlacementDecision is the scheduler's explainable output. Every field exists
// to answer an operator question, not for display decoration.
type PlacementDecision struct {
	WorkloadName string    `json:"workloadName"`
	NodeName     string    `json:"nodeName"`
	DecidedAt    time.Time `json:"decidedAt"`
	// Score of the winning node.
	Score int32 `json:"score"`
	// Reason is a one-line human summary, e.g.
	// "selected worker-02 (score 78): most free memory of 3 feasible nodes".
	Reason string `json:"reason"`
	// Feasible nodes with their score breakdown, winner first.
	Candidates []NodeScore `json:"candidates,omitempty"`
	// Rejections records why each infeasible node was filtered out.
	Rejections []NodeRejection `json:"rejections,omitempty"`
	// LatencyMicros is the wall-clock cost of this scheduling cycle.
	LatencyMicros int64 `json:"latencyMicros"`
}

type NodeScore struct {
	NodeName string `json:"nodeName"`
	Score    int32  `json:"score"`
	// Breakdown maps scorer name -> weighted contribution.
	Breakdown map[string]int32 `json:"breakdown,omitempty"`
}

type NodeRejection struct {
	NodeName string `json:"nodeName"`
	Filter   string `json:"filter"`
	Reason   string `json:"reason"`
}

// ---------------------------------------------------------------------------
// Deployment
// ---------------------------------------------------------------------------

// Deployment maintains N replicas of a workload template.
type Deployment struct {
	ObjectMeta `json:",inline"`
	Spec       DeploymentSpec   `json:"spec"`
	Status     DeploymentStatus `json:"status"`
}

type DeploymentSpec struct {
	Replicas int32        `json:"replicas"`
	Template WorkloadSpec `json:"template"`
	Strategy Strategy     `json:"strategy"`
	// ProgressDeadline: if the deployment has not converged within this window
	// it is marked Degraded. Zero uses the controller default.
	ProgressDeadline Duration `json:"progressDeadline,omitzero"`
}

// StrategyKind selects the rollout algorithm.
type StrategyKind string

const (
	// StrategyRolling replaces replicas incrementally, bounded by MaxUnavailable
	// and MaxSurge, so capacity is preserved during a rollout.
	StrategyRolling StrategyKind = "RollingUpdate"
	// StrategyRecreate tears down all old replicas before creating new ones.
	StrategyRecreate StrategyKind = "Recreate"
)

type Strategy struct {
	Kind StrategyKind `json:"kind"`
	// MaxUnavailable replicas below desired during a rolling update.
	MaxUnavailable int32 `json:"maxUnavailable"`
	// MaxSurge replicas above desired during a rolling update.
	MaxSurge int32 `json:"maxSurge"`
}

type DeploymentStatus struct {
	Phase DeploymentPhase `json:"phase"`
	// Revision increments on each template change and identifies which
	// workloads belong to which rollout.
	Revision int64 `json:"revision"`

	DesiredReplicas   int32 `json:"desiredReplicas"`
	CurrentReplicas   int32 `json:"currentReplicas"`
	UpdatedReplicas   int32 `json:"updatedReplicas"`
	AvailableReplicas int32 `json:"availableReplicas"`
	// UnschedulableReplicas is the count the scheduler could not place. It is
	// the signal an operator needs when a rollout stalls on capacity.
	UnschedulableReplicas int32 `json:"unschedulableReplicas"`

	Conditions         []Condition `json:"conditions,omitempty"`
	ObservedGeneration int64       `json:"observedGeneration"`
	LastTransition     time.Time   `json:"lastTransition"`
}

// DeploymentRevision is an immutable snapshot of a template, retained so a
// rollback is a real re-apply of a known-good spec rather than a guess.
type DeploymentRevision struct {
	Deployment string       `json:"deployment"`
	Revision   int64        `json:"revision"`
	Template   WorkloadSpec `json:"template"`
	Replicas   int32        `json:"replicas"`
	CreatedAt  time.Time    `json:"createdAt"`
	// TemplateHash identifies the template content; equal hashes mean no
	// rollout is required.
	TemplateHash string `json:"templateHash"`
}

// ---------------------------------------------------------------------------
// Service and endpoints
// ---------------------------------------------------------------------------

// Service is a stable name in front of a changing set of workload endpoints.
type Service struct {
	ObjectMeta `json:",inline"`
	Spec       ServiceSpec   `json:"spec"`
	Status     ServiceStatus `json:"status"`
}

type ServiceSpec struct {
	// Selector matches workload labels. Empty selector matches nothing, which
	// is safer than matching everything.
	Selector map[string]string `json:"selector"`
	// Port is the port clients connect to on the Orion service proxy.
	Port int32 `json:"port"`
	// TargetPort is the container port traffic is forwarded to.
	TargetPort int32 `json:"targetPort"`
	// Strategy selects endpoints per connection.
	Strategy LoadBalanceStrategy `json:"strategy"`
}

type LoadBalanceStrategy string

const (
	// LBRoundRobin distributes connections evenly across healthy endpoints.
	LBRoundRobin LoadBalanceStrategy = "RoundRobin"
	// LBLeastConnections favours the endpoint with the fewest in-flight
	// connections; useful when request durations vary widely.
	LBLeastConnections LoadBalanceStrategy = "LeastConnections"
)

type ServiceStatus struct {
	Endpoints []Endpoint `json:"endpoints"`
	// Healthy/Total are denormalized for cheap list rendering.
	HealthyEndpoints int       `json:"healthyEndpoints"`
	TotalEndpoints   int       `json:"totalEndpoints"`
	ObservedRevision uint64    `json:"observedRevision"`
	LastTransition   time.Time `json:"lastTransition"`
}

// Endpoint is one addressable, health-checked backend.
type Endpoint struct {
	WorkloadName string      `json:"workloadName"`
	WorkloadUID  string      `json:"workloadUid"`
	NodeName     string      `json:"nodeName"`
	Address      string      `json:"address"`
	Port         int32       `json:"port"`
	Health       HealthState `json:"health"`
	// Ready mirrors Workload.Ready() at the time endpoints were computed.
	Ready bool `json:"ready"`
}

func (e Endpoint) Target() string {
	return e.Address + ":" + itoa(int(e.Port))
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

// Event is an operational record. Events are the audit trail for every
// state-changing decision the control plane makes, including destructive ones.
type Event struct {
	// ID is monotonically increasing (Raft index based) and is the pagination
	// cursor.
	ID        uint64        `json:"id"`
	Timestamp time.Time     `json:"timestamp"`
	Severity  EventSeverity `json:"severity"`
	// Source is the component: scheduler, deployment-controller, node-agent...
	Source string `json:"source"`
	// Reason is a stable machine-readable code, e.g. "WorkloadScheduled".
	Reason string `json:"reason"`
	// Kind/Name identify the object the event is about.
	Kind    string `json:"kind"`
	Name    string `json:"name"`
	Message string `json:"message"`
	// Actor is set for operator-initiated actions (audit logging).
	Actor string `json:"actor,omitempty"`
}

// Duration is a JSON-friendly time.Duration serialized as a Go duration string
// ("30s", "5m"), so specs stay readable in YAML/JSON and in the console.
type Duration time.Duration

func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Duration(d).String() + `"`), nil
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	if len(b) >= 2 && b[0] == '"' {
		v, err := time.ParseDuration(string(b[1 : len(b)-1]))
		if err != nil {
			return err
		}
		*d = Duration(v)
		return nil
	}
	// Bare numbers are interpreted as nanoseconds, matching time.Duration.
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return &TransitionError{Kind: "duration", Name: string(b), From: "", To: ""}
		}
		n = n*10 + int64(c-'0')
	}
	*d = Duration(n)
	return nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
