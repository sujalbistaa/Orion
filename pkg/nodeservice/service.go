// Package nodeservice implements the control plane's side of the agent
// protocol: node registration and the periodic full-state sync.
//
// It is the trust boundary between the control plane and the machines it
// manages. Agents are authenticated but not trusted to be correct: a buggy or
// compromised agent must not be able to claim impossible capacity, adopt
// another node's workloads, or drive the control plane's state machine into a
// state the cluster's own rules forbid.
package nodeservice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/controlplane"
	orionv1 "github.com/sujalbistaa/orion/pkg/proto/orionv1"
	"github.com/sujalbistaa/orion/pkg/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Timings the control plane hands to every agent, so the failure-detection
// window is configured in one place.
type Timings struct {
	HeartbeatInterval time.Duration
	// SelfFenceTimeout must be shorter than HeartbeatTimeout+EvictionDelay in
	// the node lifecycle controller. That ordering is what makes eviction safe:
	// a partitioned agent stops its containers before the control plane starts
	// their replacements. New validates it rather than trusting configuration.
	SelfFenceTimeout time.Duration
	// EvictionDeadline is HeartbeatTimeout+EvictionDelay, used only to check
	// the above invariant.
	EvictionDeadline time.Duration
}

// Gate decides whether the control plane will talk to a node. It is how fault
// injection produces a real unreachable node rather than a simulated one: from
// every other component's perspective the heartbeats simply stop arriving.
// A nil Gate allows everything.
type Gate interface {
	Allowed(node string) bool
}

// Service implements orionv1.NodeServiceServer.
type Service struct {
	orionv1.UnimplementedNodeServiceServer

	cp      *controlplane.ControlPlane
	log     *slog.Logger
	timings Timings
	gate    Gate
	// clusterID identifies this cluster to agents, so an agent pointed at a
	// rebuilt cluster notices rather than silently joining it.
	clusterID string
}

func New(cp *controlplane.ControlPlane, clusterID string, timings Timings, gate Gate, log *slog.Logger) (*Service, error) {
	if timings.SelfFenceTimeout >= timings.EvictionDeadline {
		return nil, fmt.Errorf(
			"nodeservice: self-fence timeout (%s) must be shorter than the eviction deadline (%s), "+
				"otherwise a partitioned node could still be running containers when their replacements start",
			timings.SelfFenceTimeout, timings.EvictionDeadline)
	}
	if timings.HeartbeatInterval >= timings.SelfFenceTimeout {
		return nil, fmt.Errorf("nodeservice: heartbeat interval (%s) must be shorter than the self-fence timeout (%s)",
			timings.HeartbeatInterval, timings.SelfFenceTimeout)
	}
	return &Service{
		cp: cp, log: log.With("component", "nodeservice"),
		timings: timings, gate: gate, clusterID: clusterID,
	}, nil
}

// Register accepts a node into the cluster.
func (s *Service) Register(ctx context.Context, req *orionv1.RegisterRequest) (*orionv1.RegisterResponse, error) {
	if !s.allowed(req.GetNodeName()) {
		return nil, status.Error(codes.Unavailable, "node is unreachable")
	}
	node := &v1.Node{
		ObjectMeta: v1.ObjectMeta{Name: req.GetNodeName(), Labels: req.GetLabels()},
		Spec:       v1.NodeSpec{Address: req.GetAddress()},
		Status: v1.NodeStatus{
			Capacity:       fromProtoResources(req.GetCapacity()),
			Allocatable:    fromProtoResources(req.GetAllocatable()),
			Runtime:        fromProtoRuntime(req.GetRuntime()),
			AgentStartedAt: fromProtoTime(req.GetAgentStartedAt()),
		},
	}
	// The agent reports its own capacity, so it is validated like any other
	// untrusted input: a node claiming a petabyte of RAM would otherwise
	// attract every pending workload in the cluster.
	if err := v1.ValidateNodeRegistration(node); err != nil {
		s.log.Warn("rejecting node registration", "node", req.GetNodeName(), "err", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid node registration: %v", err)
	}

	res, err := s.cp.Apply(ctx, store.Command{
		Kind:      store.CmdRegisterNode,
		Node:      node,
		Actor:     "node/" + req.GetNodeName(),
		RequestID: fmt.Sprintf("register/%s/%d", req.GetNodeName(), fromProtoTime(req.GetAgentStartedAt()).UnixNano()),
	})
	if err != nil {
		return nil, s.mapError(err, "registering node")
	}

	s.log.Info("node registered",
		"node", node.Name, "uid", res.Node.UID, "address", node.Spec.Address,
		"allocatable", node.Status.Allocatable.String(),
		"runtime", node.Status.Runtime.Name+" "+node.Status.Runtime.Version,
		"agentVersion", req.GetAgentVersion())

	return &orionv1.RegisterResponse{
		NodeUid:             res.Node.UID,
		HeartbeatIntervalMs: s.timings.HeartbeatInterval.Milliseconds(),
		SelfFenceTimeoutMs:  s.timings.SelfFenceTimeout.Milliseconds(),
		ClusterId:           s.clusterID,
	}, nil
}

// Sync exchanges observed node state for the workloads that node should run.
func (s *Service) Sync(ctx context.Context, req *orionv1.SyncRequest) (*orionv1.SyncResponse, error) {
	nodeName := req.GetNodeName()
	if nodeName == "" {
		return nil, status.Error(codes.InvalidArgument, "node name is required")
	}
	if !s.allowed(nodeName) {
		// The gate is blocking this node, so the RPC fails exactly as it would
		// across a real partition. Unavailable is the right code: it tells the
		// agent to retry, which is what it would do against a broken network.
		return nil, status.Error(codes.Unavailable, "node is unreachable")
	}

	node, ok := s.cp.Store().Node(nodeName)
	if !ok {
		// Unknown node: tell it to re-register rather than failing the RPC, so
		// an agent whose record was deleted recovers on its own.
		return &orionv1.SyncResponse{
			Accepted:            false,
			RejectReason:        "this node is not registered",
			HeartbeatIntervalMs: s.timings.HeartbeatInterval.Milliseconds(),
			SelfFenceTimeoutMs:  s.timings.SelfFenceTimeout.Milliseconds(),
		}, nil
	}
	if req.GetNodeUid() != "" && req.GetNodeUid() != node.UID {
		// The node record was recreated; this agent's view belongs to a
		// previous incarnation and its workload reports must not be applied.
		return &orionv1.SyncResponse{
			Accepted:     false,
			RejectReason: "node identity has changed; re-register",
		}, nil
	}

	if err := s.applyNodeStatus(ctx, node, req); err != nil {
		return nil, err
	}
	if err := s.applyWorkloadStatuses(ctx, node, req.GetWorkloads()); err != nil {
		return nil, err
	}

	assigned := s.assignmentFor(nodeName)
	resp := &orionv1.SyncResponse{
		Workloads:           assigned,
		Accepted:            true,
		HeartbeatIntervalMs: s.timings.HeartbeatInterval.Milliseconds(),
		SelfFenceTimeoutMs:  s.timings.SelfFenceTimeout.Milliseconds(),
	}
	if addr, ok := s.cp.LeaderAddress(); ok && !s.cp.IsLeader() {
		resp.LeaderAddress = addr
	}
	return resp, nil
}

func (s *Service) applyNodeStatus(ctx context.Context, node *v1.Node, req *orionv1.SyncRequest) error {
	capacity := fromProtoResources(req.GetCapacity())
	allocatable := fromProtoResources(req.GetAllocatable())
	// Reject implausible capacity on every heartbeat, not just at registration:
	// otherwise a node could register honestly and then inflate itself.
	if allocatable.CPU > capacity.CPU || allocatable.Memory > capacity.Memory ||
		capacity.CPU <= 0 || capacity.Memory <= 0 {
		return status.Error(codes.InvalidArgument, "reported allocatable capacity exceeds total capacity")
	}

	newStatus := node.Status
	newStatus.Capacity = capacity
	newStatus.Allocatable = allocatable
	newStatus.Usage = fromProtoResources(req.GetUsage())
	newStatus.Runtime = fromProtoRuntime(req.GetRuntime())
	newStatus.Conditions = fromProtoConditions(req.GetConditions())

	_, err := s.cp.Apply(ctx, store.Command{
		Kind:       store.CmdUpdateNodeStatus,
		Name:       node.Name,
		UID:        node.UID,
		NodeStatus: &newStatus,
		Actor:      "node/" + node.Name,
	})
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return s.mapError(err, "updating node status")
	}
	return nil
}

func (s *Service) applyWorkloadStatuses(ctx context.Context, node *v1.Node, reports []*orionv1.WorkloadStatus) error {
	for _, r := range reports {
		current, ok := s.cp.Store().Workload(r.GetName())
		if !ok {
			continue // already purged; the assignment will tell the agent to remove it
		}
		// An agent may only report on workloads bound to it. Without this
		// check, any agent could drive any workload's state machine.
		if current.Status.NodeName != node.Name {
			s.log.Warn("ignoring a workload report from the wrong node",
				"workload", r.GetName(), "reportedBy", node.Name, "boundTo", current.Status.NodeName)
			continue
		}
		if r.GetUid() != "" && r.GetUid() != current.UID {
			continue // a report about a previous incarnation of this name
		}

		next := toWorkloadStatus(r, current.Status)
		if !workloadStatusChanged(current.Status, next) {
			// Most heartbeats report nothing new. Skipping the write keeps the
			// Raft log proportional to actual change rather than to node count.
			continue
		}

		_, err := s.cp.Apply(ctx, store.Command{
			Kind:           store.CmdUpdateWorkloadStatus,
			Name:           r.GetName(),
			UID:            current.UID,
			WorkloadStatus: &next,
			Actor:          "node/" + node.Name,
		})
		if err != nil {
			switch {
			case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrUIDMismatch):
				// The workload went away underneath us; the assignment will
				// clean it up.
			case errors.Is(err, store.ErrInvalidState):
				// A stale report the state machine correctly refused, e.g. a
				// Running report for a workload already terminated. Expected
				// during eviction, and not worth failing the heartbeat over.
				s.log.Debug("rejected a stale workload report",
					"workload", r.GetName(), "reportedPhase", r.GetPhase(), "err", err)
			default:
				return s.mapError(err, "updating workload status")
			}
		}
	}
	return nil
}

// assignmentFor builds the complete desired workload set for a node.
func (s *Service) assignmentFor(nodeName string) []*orionv1.AssignedWorkload {
	workloads := s.cp.Store().AssignedWorkloads(nodeName)
	out := make([]*orionv1.AssignedWorkload, 0, len(workloads))

	for _, w := range workloads {
		desired := "Running"
		if w.DeletedAt != nil || w.Status.Phase == v1.WorkloadTerminating {
			desired = "Terminated"
		}
		out = append(out, &orionv1.AssignedWorkload{
			Name:                     w.Name,
			Uid:                      w.UID,
			Image:                    w.Spec.Image,
			Command:                  w.Spec.Command,
			Args:                     w.Spec.Args,
			Env:                      toProtoEnv(w.Spec.Env),
			Ports:                    toProtoPorts(w.Spec.Ports),
			Request:                  toProtoResources(w.Spec.Resources.Request),
			Limit:                    toProtoResources(w.Spec.Resources.EffectiveLimit()),
			RestartPolicy:            string(w.Spec.RestartPolicy),
			TerminationGracePeriodMs: w.Spec.TerminationGracePeriod.Duration().Milliseconds(),
			HealthCheck:              toProtoHealthCheck(w.Spec.HealthCheck),
			DesiredState:             desired,
			Labels:                   w.Labels,
		})
	}
	return out
}

// Deregister handles a graceful agent shutdown.
func (s *Service) Deregister(ctx context.Context, req *orionv1.DeregisterRequest) (*orionv1.DeregisterResponse, error) {
	node, ok := s.cp.Store().Node(req.GetNodeName())
	if !ok {
		return &orionv1.DeregisterResponse{Accepted: true}, nil
	}
	if req.GetNodeUid() != "" && req.GetNodeUid() != node.UID {
		return &orionv1.DeregisterResponse{Accepted: false}, nil
	}

	// A shutting-down agent leaves its containers running so the next agent
	// process adopts them; that is why the node moves to NotReady rather than
	// Unreachable. NotReady stops new scheduling without triggering eviction.
	_, err := s.cp.Apply(ctx, store.Command{
		Kind:   store.CmdSetNodePhase,
		Name:   node.Name,
		Phase:  v1.NodeNotReady,
		Reason: "agent reported shutdown: " + req.GetReason(),
		Actor:  "node/" + node.Name,
	})
	if err != nil && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrInvalidState) {
		return nil, s.mapError(err, "deregistering node")
	}
	s.log.Info("node deregistered", "node", node.Name, "reason", req.GetReason())
	return &orionv1.DeregisterResponse{Accepted: true}, nil
}

// allowed consults the fault-injection gate.
func (s *Service) allowed(node string) bool {
	return s.gate == nil || s.gate.Allowed(node)
}

// mapError translates control-plane errors into gRPC status codes the agent
// can act on: Unavailable means retry, everything else means do not.
func (s *Service) mapError(err error, what string) error {
	switch {
	case errors.Is(err, controlplane.ErrNotLeader):
		return status.Errorf(codes.FailedPrecondition, "%s: this replica is not the leader", what)
	case errors.Is(err, controlplane.ErrUnavailable):
		return status.Errorf(codes.Unavailable, "%s: cluster is unavailable", what)
	case errors.Is(err, store.ErrInvalidState), errors.Is(err, store.ErrConflict):
		return status.Errorf(codes.FailedPrecondition, "%s: %v", what, err)
	case errors.Is(err, store.ErrNotFound):
		return status.Errorf(codes.NotFound, "%s: %v", what, err)
	default:
		s.log.Error("unexpected error serving an agent request", "what", what, "err", err)
		return status.Errorf(codes.Internal, "%s", what)
	}
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

func toWorkloadStatus(r *orionv1.WorkloadStatus, current v1.WorkloadStatus) v1.WorkloadStatus {
	out := current.DeepCopy()
	out.Phase = v1.WorkloadPhase(r.GetPhase())
	out.Health = v1.HealthState(r.GetHealth())
	out.ContainerID = r.GetContainerId()
	out.Reason = r.GetReason()
	out.Message = r.GetMessage()
	out.RestartCount = r.GetRestartCount()
	out.Usage = fromProtoResources(r.GetUsage())
	if len(r.GetHostPorts()) > 0 {
		out.HostPorts = r.GetHostPorts()
	}
	if r.GetHasExitCode() {
		code := r.GetExitCode()
		out.ExitCode = &code
	}
	if t := fromProtoTime(r.GetStartedAt()); !t.IsZero() {
		out.StartedAt = &t
	}
	if t := fromProtoTime(r.GetFinishedAt()); !t.IsZero() {
		out.FinishedAt = &t
	}
	return out
}

// workloadStatusChanged compares the fields an agent owns. Timestamps and
// resource usage are excluded from the comparison on purpose: usage changes on
// every sample, and writing it into the replicated log every few seconds for
// every workload would make the Raft log a metrics pipeline.
func workloadStatusChanged(a, b v1.WorkloadStatus) bool {
	if a.Phase != b.Phase || a.Health != b.Health || a.ContainerID != b.ContainerID ||
		a.Reason != b.Reason || a.Message != b.Message || a.RestartCount != b.RestartCount {
		return true
	}
	if (a.ExitCode == nil) != (b.ExitCode == nil) {
		return true
	}
	if a.ExitCode != nil && b.ExitCode != nil && *a.ExitCode != *b.ExitCode {
		return true
	}
	if len(a.HostPorts) != len(b.HostPorts) {
		return true
	}
	for k, v := range a.HostPorts {
		if b.HostPorts[k] != v {
			return true
		}
	}
	return false
}

func fromProtoResources(r *orionv1.Resources) v1.Resources {
	if r == nil {
		return v1.Resources{}
	}
	return v1.Resources{CPU: v1.MilliCPU(r.GetCpuMillis()), Memory: v1.Bytes(r.GetMemoryBytes())}
}

func toProtoResources(r v1.Resources) *orionv1.Resources {
	return &orionv1.Resources{CpuMillis: int64(r.CPU), MemoryBytes: int64(r.Memory)}
}

func fromProtoRuntime(r *orionv1.RuntimeInfo) v1.RuntimeInfo {
	if r == nil {
		return v1.RuntimeInfo{}
	}
	return v1.RuntimeInfo{
		Name: r.GetName(), Version: r.GetVersion(), OS: r.GetOs(),
		Arch: r.GetArch(), KernelVersion: r.GetKernelVersion(),
	}
}

func fromProtoTime(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}

func fromProtoConditions(in []*orionv1.NodeCondition) []v1.Condition {
	if len(in) == 0 {
		return nil
	}
	out := make([]v1.Condition, 0, len(in))
	now := time.Now().UTC()
	for _, c := range in {
		out = append(out, v1.Condition{
			Type: c.GetType(), Status: c.GetStatus(),
			Reason: c.GetReason(), Message: c.GetMessage(), Since: now,
		})
	}
	return out
}

func toProtoEnv(in []v1.EnvVar) []*orionv1.EnvVar {
	out := make([]*orionv1.EnvVar, 0, len(in))
	for _, e := range in {
		out = append(out, &orionv1.EnvVar{Name: e.Name, Value: e.Value})
	}
	return out
}

func toProtoPorts(in []v1.Port) []*orionv1.Port {
	out := make([]*orionv1.Port, 0, len(in))
	for _, p := range in {
		out = append(out, &orionv1.Port{
			Name: p.Name, Container: p.Container, Host: p.Host, Protocol: p.Protocol,
		})
	}
	return out
}

func toProtoHealthCheck(hc *v1.HealthCheck) *orionv1.HealthCheck {
	if hc == nil {
		return nil
	}
	return &orionv1.HealthCheck{
		Kind:             string(hc.Kind),
		Path:             hc.Path,
		Port:             hc.Port,
		InitialDelayMs:   hc.InitialDelay.Duration().Milliseconds(),
		IntervalMs:       hc.Interval.Duration().Milliseconds(),
		TimeoutMs:        hc.Timeout.Duration().Milliseconds(),
		FailureThreshold: hc.FailureThreshold,
		SuccessThreshold: hc.SuccessThreshold,
	}
}
