package agent

import (
	"context"
	"errors"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	orionv1 "github.com/sujalbistaa/orion/pkg/proto/orionv1"
	"github.com/sujalbistaa/orion/pkg/runtime"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// observe samples every managed workload and assembles the node's report.
//
// Sampling happens before the Sync call rather than during it so a slow engine
// delays the report by a bounded amount instead of consuming the RPC deadline.
func (a *Agent) observe(ctx context.Context) *orionv1.SyncRequest {
	a.mu.Lock()
	workloads := make([]*managedWorkload, 0, len(a.workloads))
	for _, w := range a.workloads {
		workloads = append(workloads, w)
	}
	a.mu.Unlock()

	var totalUsage v1.Resources
	statuses := make([]*orionv1.WorkloadStatus, 0, len(workloads))

	for _, w := range workloads {
		if w.containerID != "" {
			// Stats are best-effort: a sampling failure must not stop the
			// heartbeat, because a missed heartbeat is what gets a node evicted.
			statCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			if s, err := a.rt.Stats(statCtx, w.containerID); err == nil {
				usage := v1.Resources{CPU: v1.MilliCPU(s.CPUMillis), Memory: v1.Bytes(s.MemoryBytes)}
				w.mu.Lock()
				w.usage = usage
				w.mu.Unlock()
				totalUsage = totalUsage.Add(usage)
			} else if !errors.Is(err, runtime.ErrNotFound) {
				w.log.Debug("could not sample stats", "err", err)
			}
			cancel()
		}
		statuses = append(statuses, w.status())
	}

	uid, _ := a.nodeUID.Load().(string)
	return &orionv1.SyncRequest{
		NodeName:    a.cfg.NodeName,
		NodeUid:     uid,
		Capacity:    toProtoResources(a.capacity),
		Allocatable: toProtoResources(a.allocatable),
		Usage:       toProtoResources(totalUsage),
		Runtime:     toProtoRuntime(a.runtimeInfo),
		Conditions:  a.conditions(ctx),
		Workloads:   statuses,
	}
}

// conditions reports node-level health that the phase alone cannot express.
func (a *Agent) conditions(ctx context.Context) []*orionv1.NodeCondition {
	pingCtx, cancel := context.WithTimeout(ctx, time.Second)
	err := a.rt.Ping(pingCtx)
	cancel()

	runtimeReady := &orionv1.NodeCondition{
		Type:   "RuntimeReady",
		Status: err == nil,
		Reason: "RuntimeHealthy",
	}
	if err != nil {
		runtimeReady.Reason = "RuntimeUnavailable"
		runtimeReady.Message = err.Error()
	}

	conds := []*orionv1.NodeCondition{runtimeReady}
	if a.fenced.Load() {
		conds = append(conds, &orionv1.NodeCondition{
			Type:    "Fenced",
			Status:  true,
			Reason:  "ControlPlaneUnreachable",
			Message: "the agent stopped its workloads after losing contact with the control plane",
		})
	}
	return conds
}

func (a *Agent) sync(ctx context.Context, req *orionv1.SyncRequest) (*orionv1.SyncResponse, error) {
	resp, err := a.client.Sync(ctx, req)
	if err != nil {
		return nil, err
	}
	a.applyTimings(resp.GetHeartbeatIntervalMs(), resp.GetSelfFenceTimeoutMs())
	return resp, nil
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

func toProtoResources(r v1.Resources) *orionv1.Resources {
	return &orionv1.Resources{CpuMillis: int64(r.CPU), MemoryBytes: int64(r.Memory)}
}

func toProtoRuntime(r v1.RuntimeInfo) *orionv1.RuntimeInfo {
	return &orionv1.RuntimeInfo{
		Name: r.Name, Version: r.Version, Os: r.OS, Arch: r.Arch, KernelVersion: r.KernelVersion,
	}
}

func toProtoTime(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
