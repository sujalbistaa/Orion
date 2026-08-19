package apiserver

import (
	"net/http"
	"time"
)

// The fault injection API. The types live here rather than in the faults
// package so that pkg/faults depends on nothing in the API server and can be
// driven from tests and from the CLI without an HTTP server present.

// ExperimentKind identifies a failure scenario.
type ExperimentKind string

const (
	// ExperimentNodeFailure stops a node's agent, so the control plane must
	// detect the failure and reschedule its workloads.
	ExperimentNodeFailure ExperimentKind = "node-failure"
	// ExperimentNodeRestart stops and restarts an agent, verifying it adopts
	// its containers rather than recreating them.
	ExperimentNodeRestart ExperimentKind = "node-restart"
	// ExperimentWorkloadCrash kills a container, so the restart policy and the
	// deployment controller must replace it.
	ExperimentWorkloadCrash ExperimentKind = "workload-crash"
	// ExperimentControllerCrash stops the controller manager, verifying that
	// reconciliation resumes without duplicating work.
	ExperimentControllerCrash ExperimentKind = "controller-crash"
	// ExperimentLeaderFailure forces a Raft leader change under load.
	ExperimentLeaderFailure ExperimentKind = "leader-failure"
	// ExperimentNetworkPartition isolates a node from the control plane,
	// exercising the self-fencing path.
	ExperimentNetworkPartition ExperimentKind = "network-partition"
	// ExperimentResourceExhaustion fills the cluster so scheduling fails,
	// verifying that pressure is reported rather than hidden.
	ExperimentResourceExhaustion ExperimentKind = "resource-exhaustion"
)

// ParameterSpec describes one experiment input, so the console can render a
// form without hard-coding knowledge of each experiment.
type ParameterSpec struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // string | duration | int | node | workload | deployment
	Required bool   `json:"required"`
	Default  string `json:"default,omitempty"`
	Help     string `json:"help"`
}

// ExperimentDescriptor documents an experiment and what it proves.
type ExperimentDescriptor struct {
	Kind ExperimentKind `json:"kind"`
	Name string         `json:"name"`
	// Description says what the experiment does.
	Description string `json:"description"`
	// Hypothesis is what the cluster is expected to do in response. An
	// experiment without a stated expectation is just breakage.
	Hypothesis string `json:"hypothesis"`
	// Invariants are the properties asserted throughout the run.
	Invariants []string        `json:"invariants"`
	Parameters []ParameterSpec `json:"parameters"`
	// Destructive marks experiments that stop real workloads.
	Destructive bool `json:"destructive"`
}

// ExperimentRequest starts a run.
type ExperimentRequest struct {
	Kind ExperimentKind `json:"kind"`
	// Params are experiment-specific, validated against the descriptor.
	Params map[string]string `json:"params"`
	// Duration is how long the fault is held. Zero uses the experiment default.
	Duration time.Duration `json:"-"`
	// DurationSeconds is the wire form of Duration.
	DurationSeconds int `json:"durationSeconds,omitempty"`
	// Actor is filled in by the server from the authenticated principal.
	Actor string `json:"-"`
}

// RunState is the lifecycle of an experiment run.
type RunState string

const (
	RunPending   RunState = "Pending"
	RunInjecting RunState = "Injecting"
	// RunObserving means the fault is active and invariants are being checked.
	RunObserving RunState = "Observing"
	// RunRecovering means the fault has been lifted and the cluster is being
	// watched until it converges.
	RunRecovering RunState = "Recovering"
	RunSucceeded  RunState = "Succeeded"
	RunFailed     RunState = "Failed"
	RunAborted    RunState = "Aborted"
)

// TimelineEntry is one observation during a run. The timeline is the artifact
// an engineer actually reads afterwards.
type TimelineEntry struct {
	At      time.Time `json:"at"`
	Elapsed string    `json:"elapsed"`
	Phase   RunState  `json:"phase"`
	Message string    `json:"message"`
	// Level is info, warn or error.
	Level string `json:"level"`
}

// InvariantResult records whether a property held.
type InvariantResult struct {
	Name string `json:"name"`
	Held bool   `json:"held"`
	// Detail explains a violation, or confirms what was checked.
	Detail string `json:"detail"`
	// Violations counts how many times the property was observed to fail.
	Violations int `json:"violations"`
}

// ExperimentRun is a single execution.
type ExperimentRun struct {
	ID    string         `json:"id"`
	Kind  ExperimentKind `json:"kind"`
	State RunState       `json:"state"`

	Params map[string]string `json:"params"`
	Actor  string            `json:"actor"`

	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`

	// AffectedNodes and AffectedWorkloads are what the fault actually touched.
	AffectedNodes     []string `json:"affectedNodes,omitempty"`
	AffectedWorkloads []string `json:"affectedWorkloads,omitempty"`

	Timeline   []TimelineEntry   `json:"timeline"`
	Invariants []InvariantResult `json:"invariants"`

	// RecoveryDuration is how long the cluster took to return to desired state
	// after the fault was lifted. It is the headline number of the run.
	RecoveryDuration string `json:"recoveryDuration,omitempty"`
	// Error explains why a run failed to execute (as opposed to an invariant
	// violation, which is a legitimate result).
	Error string `json:"error,omitempty"`
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func (s *Server) handleListExperiments(w http.ResponseWriter, r *http.Request) {
	writeList(w, s.faults.List(), s.store.AppliedIndex())
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	writeList(w, s.faults.Runs(), s.store.AppliedIndex())
}

func (s *Server) handleStartExperiment(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	var req ExperimentRequest
	if !decodeBody(w, r, s.maxRequestSize, &req) {
		return
	}
	if req.DurationSeconds > 0 {
		req.Duration = time.Duration(req.DurationSeconds) * time.Second
	}
	req.Actor = actorOf(r.Context())

	run, err := s.faults.Start(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "experiment_rejected", err.Error())
		return
	}

	// Deliberately breaking a cluster is exactly the kind of action someone
	// will need to account for later.
	s.recordAudit(r.Context(), "FaultInjected", "Experiment", string(req.Kind),
		"fault injection experiment "+run.ID+" started")

	writeJSON(w, http.StatusAccepted, run)
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	run, ok := s.faults.Get(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no experiment run with that id")
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleAbortExperiment(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	id := r.PathValue("id")
	if err := s.faults.Abort(r.Context(), id); err != nil {
		writeError(w, http.StatusConflict, "abort_failed", err.Error())
		return
	}
	s.recordAudit(r.Context(), "FaultAborted", "Experiment", id, "experiment aborted by operator")

	run, _ := s.faults.Get(id)
	writeJSON(w, http.StatusOK, run)
}
