package store

import (
	"encoding/json"
	"fmt"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
)

// CommandKind identifies a replicated mutation.
type CommandKind string

const (
	// Node lifecycle.
	CmdRegisterNode     CommandKind = "RegisterNode"
	CmdUpdateNodeStatus CommandKind = "UpdateNodeStatus"
	CmdSetNodePhase     CommandKind = "SetNodePhase"
	CmdCordonNode       CommandKind = "CordonNode"
	CmdDeleteNode       CommandKind = "DeleteNode"

	// Workload lifecycle.
	CmdCreateWorkload       CommandKind = "CreateWorkload"
	CmdBindWorkload         CommandKind = "BindWorkload"
	CmdUpdateWorkloadStatus CommandKind = "UpdateWorkloadStatus"
	CmdDeleteWorkload       CommandKind = "DeleteWorkload"
	CmdPurgeWorkload        CommandKind = "PurgeWorkload"

	// Deployments.
	CmdCreateDeployment       CommandKind = "CreateDeployment"
	CmdUpdateDeploymentSpec   CommandKind = "UpdateDeploymentSpec"
	CmdUpdateDeploymentStatus CommandKind = "UpdateDeploymentStatus"
	CmdDeleteDeployment       CommandKind = "DeleteDeployment"
	CmdPurgeDeployment        CommandKind = "PurgeDeployment"
	CmdRollbackDeployment     CommandKind = "RollbackDeployment"

	// Services.
	CmdCreateService         CommandKind = "CreateService"
	CmdUpdateServiceSpec     CommandKind = "UpdateServiceSpec"
	CmdUpdateServiceEndpoint CommandKind = "UpdateServiceEndpoints"
	CmdDeleteService         CommandKind = "DeleteService"

	// Operational record.
	CmdRecordEvent CommandKind = "RecordEvent"
)

// Command is one replicated mutation.
//
// Two fields carry the weight of the design:
//
// Timestamp is set by the proposer and replicated. Apply must be deterministic
// across replicas, so it may never read the clock itself — a follower applying
// the same entry an hour later must produce byte-identical state.
//
// RequestID makes application idempotent. Raft can deliver a proposal that the
// client retried after a leader change, and controllers legitimately re-issue
// work. The store remembers recent request IDs and replays the original result
// instead of applying twice.
type Command struct {
	Kind      CommandKind `json:"kind"`
	Timestamp time.Time   `json:"ts"`
	// RequestID deduplicates retries. Empty means "not idempotent"; only
	// commands that are naturally idempotent (status updates) may omit it.
	RequestID string `json:"rid,omitempty"`
	// Actor is the authenticated principal, recorded in the audit trail for
	// destructive operations.
	Actor string `json:"actor,omitempty"`

	Node       *v1.Node       `json:"node,omitempty"`
	Workload   *v1.Workload   `json:"workload,omitempty"`
	Deployment *v1.Deployment `json:"deployment,omitempty"`
	Service    *v1.Service    `json:"service,omitempty"`
	Event      *v1.Event      `json:"event,omitempty"`

	// Name targets an existing object for commands that do not carry a body.
	Name string `json:"name,omitempty"`
	// UID guards against acting on a recreated object with the same name. A
	// mismatch makes the command a no-op rather than a corruption.
	UID string `json:"uid,omitempty"`
	// ExpectedRevision implements compare-and-swap. Zero disables the check.
	ExpectedRevision uint64 `json:"rev,omitempty"`

	NodeStatus     *v1.NodeStatus        `json:"nodeStatus,omitempty"`
	WorkloadStatus *v1.WorkloadStatus    `json:"workloadStatus,omitempty"`
	DeployStatus   *v1.DeploymentStatus  `json:"deployStatus,omitempty"`
	Endpoints      []v1.Endpoint         `json:"endpoints,omitempty"`
	Placement      *v1.PlacementDecision `json:"placement,omitempty"`

	Phase         v1.NodePhase `json:"phase,omitempty"`
	Unschedulable *bool        `json:"unschedulable,omitempty"`
	Reason        string       `json:"reason,omitempty"`
	// TargetRevision selects the deployment revision to roll back to.
	TargetRevision int64 `json:"targetRevision,omitempty"`
	// GraceSeconds overrides the workload's termination grace period.
	GraceSeconds *int32 `json:"graceSeconds,omitempty"`
}

// Encode serializes a command for the Raft log.
//
// JSON is used rather than a binary format because the control-plane write
// rate is measured in operations per second, not per microsecond, and a log an
// operator can read with `jq` during an incident is worth more than the bytes
// saved. The data plane (agent heartbeats, stats) uses protobuf, where the
// volume actually justifies it.
func (c *Command) Encode() ([]byte, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("store: encoding %s command: %w", c.Kind, err)
	}
	return b, nil
}

func DecodeCommand(data []byte) (*Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("store: decoding command: %w", err)
	}
	if c.Kind == "" {
		return nil, fmt.Errorf("store: command has no kind")
	}
	return &c, nil
}

// Result is what a command's proposer receives once the entry is applied.
type Result struct {
	// Revision is the log index the mutation was applied at.
	Revision uint64 `json:"revision"`
	// Err is non-nil when the command was rejected at apply time — a conflict,
	// a missing object, or an illegal state transition. Every replica computes
	// the same rejection, so this is agreed state, not a local failure.
	Err error `json:"-"`

	Node       *v1.Node       `json:"node,omitempty"`
	Workload   *v1.Workload   `json:"workload,omitempty"`
	Deployment *v1.Deployment `json:"deployment,omitempty"`
	Service    *v1.Service    `json:"service,omitempty"`

	// Duplicate reports that this RequestID had already been applied and the
	// stored result was replayed.
	Duplicate bool `json:"duplicate,omitempty"`
}
