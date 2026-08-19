// Package runtime abstracts the container engine the node agent drives.
//
// The interface is deliberately narrow: it is the set of operations the agent
// actually performs, expressed in Orion's terms rather than Docker's. That
// keeps the agent's reconciliation logic free of engine-specific detail and
// makes the containerd implementation a matter of writing one file rather than
// rewriting the agent.
//
// Two implementations exist:
//
//	Docker  the real one, driving the Docker Engine API.
//	Fake    an in-memory engine used by the agent's own tests. It is not a
//	        stand-in for Docker anywhere else and never runs in a cluster.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// Errors the agent distinguishes on.
var (
	// ErrNotFound means the container does not exist. It is not a failure: an
	// agent that restarts after a container was removed must treat this as
	// "already gone" and converge, not retry forever.
	ErrNotFound = errors.New("runtime: container not found")
	// ErrAlreadyExists means a container with that name already exists, which
	// happens when an agent restarts and re-creates what it already made.
	ErrAlreadyExists = errors.New("runtime: container already exists")
	// ErrImageNotFound means the image could not be pulled.
	ErrImageNotFound = errors.New("runtime: image not found")
	// ErrUnavailable means the engine itself is not reachable. The agent
	// reports this as a node condition rather than failing every workload.
	ErrUnavailable = errors.New("runtime: container engine is unavailable")
)

// ContainerSpec is everything needed to create a container. It is built by the
// agent from a workload spec and is intentionally flat: no engine-specific
// options leak through this boundary.
type ContainerSpec struct {
	// Name is the container's engine-visible name. Orion derives it from the
	// workload name and UID so a restarted agent can find what it created and
	// so a recreated workload never adopts its predecessor's container.
	Name  string
	Image string

	Command []string
	Args    []string
	Env     []string // "KEY=VALUE"

	// Labels are applied to the container so the agent can enumerate what it
	// owns without keeping local state that could drift.
	Labels map[string]string

	Ports []PortMapping

	// CPUMillis and MemoryBytes are hard limits enforced by the engine.
	CPUMillis   int64
	MemoryBytes int64

	// ReadOnlyRootFS and NoNewPrivileges are on by default for every Orion
	// workload. A container orchestrator that runs untrusted images with a
	// writable root and privilege escalation available is a liability.
	ReadOnlyRootFS  bool
	NoNewPrivileges bool
	// TmpfsPaths gives a read-only-root container somewhere to write.
	TmpfsPaths []string
}

// PortMapping publishes a container port on the host. HostPort zero asks the
// engine to allocate one, which is what lets several replicas of the same
// workload share a node.
type PortMapping struct {
	ContainerPort int32
	HostPort      int32
	Protocol      string // tcp | udp
}

// ContainerStatus is the engine's view of one container.
type ContainerStatus struct {
	ID    string
	Name  string
	Image string
	State ContainerState

	// ExitCode and FinishedAt are set once the container has stopped.
	ExitCode   int32
	StartedAt  time.Time
	FinishedAt time.Time

	// RestartCount is the engine's own restart counter, used to distinguish an
	// in-place restart from a fresh container.
	RestartCount int32

	// Ports maps container port -> the host port actually published. The agent
	// reports this upward so services can route to it.
	Ports map[int32]int32

	// Health is the engine's healthcheck verdict, if the image defines one.
	// Orion runs its own probes as well; this is supplementary.
	Health string

	Labels map[string]string
	// OOMKilled distinguishes a memory-limit kill from an ordinary failure,
	// which is the difference between "raise the limit" and "fix the bug".
	OOMKilled bool
	Error     string
}

// ContainerState is the engine's lifecycle state, kept separate from Orion's
// WorkloadPhase. The agent maps between them; conflating the two would mean
// the control plane's state machine is defined by Docker's vocabulary.
type ContainerState string

const (
	StateCreated    ContainerState = "created"
	StateRunning    ContainerState = "running"
	StatePaused     ContainerState = "paused"
	StateRestarting ContainerState = "restarting"
	StateExited     ContainerState = "exited"
	StateDead       ContainerState = "dead"
	StateRemoving   ContainerState = "removing"
	StateUnknown    ContainerState = "unknown"
)

// Terminal reports whether the container will not run again without an
// explicit start.
func (s ContainerState) Terminal() bool {
	return s == StateExited || s == StateDead
}

// Stats is a point-in-time resource sample.
type Stats struct {
	// CPUMillis is measured consumption, comparable to a spec's CPU request.
	CPUMillis int64
	// MemoryBytes is the working set: usage minus reclaimable page cache.
	// Reporting raw usage would make every container look like it is at its
	// limit, because the kernel fills the cache with whatever it reads.
	MemoryBytes int64
	MemoryLimit int64
	SampledAt   time.Time
}

// NodeInfo describes the engine and the machine it runs on. The agent reports
// it at registration so the scheduler knows the node's real capacity.
type NodeInfo struct {
	RuntimeName    string
	RuntimeVersion string
	OS             string
	Arch           string
	KernelVersion  string
	Hostname       string

	// CPUs and MemoryBytes are the machine's total resources.
	CPUs        int
	MemoryBytes int64
}

// LogOptions controls a log read.
type LogOptions struct {
	// Tail is how many trailing lines to return. Zero means all, which the
	// agent never asks for on an unbounded stream.
	Tail int
	// Since filters to entries after this time.
	Since time.Time
	// Follow streams new entries until the context is cancelled.
	Follow bool
	// Timestamps prefixes each line with its engine timestamp.
	Timestamps bool
}

// Runtime is the container engine.
//
// Every method must be safe for concurrent use and must respect context
// cancellation: the agent calls them from its reconciliation loop, which has
// to remain responsive while an image pull takes a minute.
type Runtime interface {
	// Name identifies the implementation, e.g. "docker".
	Name() string

	// Info returns engine and machine details.
	Info(ctx context.Context) (NodeInfo, error)

	// Ping verifies the engine is reachable. The agent uses it for the node's
	// RuntimeReady condition.
	Ping(ctx context.Context) error

	// PullImage fetches an image. It must be idempotent: pulling an image that
	// is already present succeeds without doing work.
	PullImage(ctx context.Context, image string) error

	// Create makes a container without starting it, returning its ID.
	Create(ctx context.Context, spec ContainerSpec) (string, error)

	// Start runs a created container.
	Start(ctx context.Context, id string) error

	// Stop sends SIGTERM, waits up to timeout, then SIGKILL. Stopping an
	// already-stopped container is not an error.
	Stop(ctx context.Context, id string, timeout time.Duration) error

	// Restart stops and starts a container in place, preserving its identity
	// and incrementing its restart count.
	Restart(ctx context.Context, id string, timeout time.Duration) error

	// Remove deletes a container. Removing one that does not exist returns
	// ErrNotFound, which callers treat as success.
	Remove(ctx context.Context, id string, force bool) error

	// Inspect returns one container's status.
	Inspect(ctx context.Context, id string) (ContainerStatus, error)

	// List returns containers carrying all the given labels, including stopped
	// ones. This is how a restarted agent rediscovers what it owns.
	List(ctx context.Context, labels map[string]string) ([]ContainerStatus, error)

	// Stats samples resource usage for one container.
	Stats(ctx context.Context, id string) (Stats, error)

	// Logs returns a container's log stream. The caller closes it.
	Logs(ctx context.Context, id string, opts LogOptions) (io.ReadCloser, error)

	// Close releases engine connections.
	Close() error
}

// OrionLabels are applied to every container Orion creates. They are the only
// record of ownership: the agent keeps no local database, so after a restart it
// asks the engine what it owns rather than trusting a file that may be stale.
const (
	LabelManagedBy    = "io.orion.managed-by"
	LabelWorkload     = "io.orion.workload"
	LabelWorkloadUID  = "io.orion.workload-uid"
	LabelNode         = "io.orion.node"
	LabelTemplateHash = "io.orion.template-hash"

	ManagedByValue = "orion"
)

// ContainerName derives an engine container name from a workload.
//
// Including the UID means a workload that was deleted and recreated with the
// same name gets a distinct container, so the agent can never adopt a
// predecessor's container and report it as the new workload.
func ContainerName(workloadName, uid string) string {
	short := uid
	if len(short) > 12 {
		short = short[len(short)-12:]
	}
	return fmt.Sprintf("orion-%s-%s", workloadName, short)
}
