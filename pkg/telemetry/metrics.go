// Package telemetry defines Orion's metrics and structured logging.
//
// Every metric here is derived from something the system actually does. There
// are no gauges that exist to make a dashboard look busy: if a number cannot
// change an operator's decision, it is not exported.
//
// Naming follows Prometheus convention — unit suffixes, base units, _total on
// counters — so the metrics compose with standard tooling instead of needing
// bespoke queries.
package telemetry

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every Orion collector.
type Metrics struct {
	registry *prometheus.Registry

	// --- API ---------------------------------------------------------------
	apiRequests *prometheus.CounterVec
	apiLatency  *prometheus.HistogramVec
	apiInFlight prometheus.Gauge

	// --- Consensus ---------------------------------------------------------
	raftTerm         prometheus.Gauge
	raftCommitIndex  prometheus.Gauge
	raftAppliedIndex prometheus.Gauge
	raftIsLeader     prometheus.Gauge
	raftProposals    *prometheus.CounterVec
	raftProposalTime prometheus.Histogram

	// --- Scheduler ---------------------------------------------------------
	scheduleAttempts *prometheus.CounterVec
	scheduleLatency  prometheus.Histogram
	pendingWorkloads prometheus.Gauge

	// --- Controllers -------------------------------------------------------
	reconcileTotal   *prometheus.CounterVec
	reconcileLatency *prometheus.HistogramVec
	replicasCreated  *prometheus.CounterVec
	replicasDeleted  *prometheus.CounterVec

	// --- Cluster state -----------------------------------------------------
	nodes          *prometheus.GaugeVec
	workloads      *prometheus.GaugeVec
	deployments    *prometheus.GaugeVec
	restartsTotal  prometheus.Gauge
	cpuAllocated   prometheus.Gauge
	cpuAllocatable prometheus.Gauge
	cpuUsed        prometheus.Gauge
	memAllocated   prometheus.Gauge
	memAllocatable prometheus.Gauge
	memUsed        prometheus.Gauge

	// --- Failure and recovery ----------------------------------------------
	nodeFailures     prometheus.Counter
	workloadsEvicted *prometheus.CounterVec
	recoveryDuration prometheus.Histogram

	// --- Agent -------------------------------------------------------------
	agentSyncs      *prometheus.CounterVec
	agentSyncTime   prometheus.Histogram
	containerOps    *prometheus.CounterVec
	containerOpTime *prometheus.HistogramVec
	agentFenced     prometheus.Gauge
	workloadRestart *prometheus.CounterVec

	// --- Service proxy -----------------------------------------------------
	proxyRequests  *prometheus.CounterVec
	proxyLatency   *prometheus.HistogramVec
	proxyEndpoints *prometheus.GaugeVec
	proxyRetries   *prometheus.CounterVec
}

// latencyBuckets span 1ms to ~16s. Control-plane operations that take longer
// than that are failures, not slow successes, and are counted as errors.
var latencyBuckets = prometheus.ExponentialBuckets(0.001, 2, 15)

// New builds the metric set and registers it on a private registry, so Orion's
// metrics never collide with a library that registered on the default one.
func New() *Metrics {
	r := prometheus.NewRegistry()
	m := &Metrics{registry: r}

	factory := promauto(r)

	m.apiRequests = factory.counterVec("orion_api_requests_total",
		"API requests by method, route and status class.", "method", "route", "status")
	m.apiLatency = factory.histogramVec("orion_api_request_duration_seconds",
		"API request latency.", latencyBuckets, "method", "route")
	m.apiInFlight = factory.gauge("orion_api_requests_in_flight",
		"API requests currently being served.")

	m.raftTerm = factory.gauge("orion_raft_term", "Current Raft term.")
	m.raftCommitIndex = factory.gauge("orion_raft_commit_index", "Highest committed Raft log index.")
	m.raftAppliedIndex = factory.gauge("orion_raft_applied_index",
		"Highest Raft log index applied to the state machine. The gap to commit index is apply lag.")
	m.raftIsLeader = factory.gauge("orion_raft_is_leader",
		"1 when this replica is the Raft leader, 0 otherwise.")
	m.raftProposals = factory.counterVec("orion_raft_proposals_total",
		"Raft proposals by outcome.", "result")
	m.raftProposalTime = factory.histogram("orion_raft_proposal_duration_seconds",
		"Time from proposing a command to it being applied. This is the real write latency of the cluster.",
		latencyBuckets)

	m.scheduleAttempts = factory.counterVec("orion_scheduler_attempts_total",
		"Scheduling attempts by outcome: bound, unschedulable, conflict, obsolete, error.", "result")
	m.scheduleLatency = factory.histogram("orion_scheduler_bind_duration_seconds",
		"Time from scheduling decision to the binding being committed.", latencyBuckets)
	m.pendingWorkloads = factory.gauge("orion_scheduler_pending_workloads",
		"Workloads awaiting placement. Sustained non-zero means the cluster is out of capacity.")

	m.reconcileTotal = factory.counterVec("orion_controller_reconcile_total",
		"Controller reconciliation passes by controller and outcome.", "controller", "result")
	m.reconcileLatency = factory.histogramVec("orion_controller_reconcile_duration_seconds",
		"Controller reconciliation pass duration.", latencyBuckets, "controller")
	m.replicasCreated = factory.counterVec("orion_deployment_replicas_created_total",
		"Replicas created by the deployment controller.", "deployment")
	m.replicasDeleted = factory.counterVec("orion_deployment_replicas_deleted_total",
		"Replicas deleted by the deployment controller.", "deployment", "reason")

	m.nodes = factory.gaugeVec("orion_nodes", "Nodes by phase.", "phase")
	m.workloads = factory.gaugeVec("orion_workloads", "Workloads by phase.", "phase")
	m.deployments = factory.gaugeVec("orion_deployments", "Deployments by phase.", "phase")
	m.restartsTotal = factory.gauge("orion_workload_restarts",
		"Sum of restart counts across all workloads.")

	m.cpuAllocatable = factory.gauge("orion_cluster_cpu_allocatable_millicores",
		"Allocatable CPU across Ready nodes.")
	m.cpuAllocated = factory.gauge("orion_cluster_cpu_allocated_millicores",
		"CPU committed to workload requests.")
	m.cpuUsed = factory.gauge("orion_cluster_cpu_used_millicores", "Measured CPU consumption.")
	m.memAllocatable = factory.gauge("orion_cluster_memory_allocatable_bytes",
		"Allocatable memory across Ready nodes.")
	m.memAllocated = factory.gauge("orion_cluster_memory_allocated_bytes",
		"Memory committed to workload requests.")
	m.memUsed = factory.gauge("orion_cluster_memory_used_bytes", "Measured memory consumption.")

	m.nodeFailures = factory.counter("orion_node_failures_total",
		"Nodes that became unreachable.")
	m.workloadsEvicted = factory.counterVec("orion_workloads_evicted_total",
		"Workloads evicted from failed or draining nodes.", "node")
	m.recoveryDuration = factory.histogram("orion_recovery_duration_seconds",
		"Time from a node becoming unreachable to its workloads being running again elsewhere.",
		prometheus.ExponentialBuckets(0.5, 2, 12))

	m.agentSyncs = factory.counterVec("orion_agent_syncs_total",
		"Agent sync calls by outcome.", "result")
	m.agentSyncTime = factory.histogram("orion_agent_sync_duration_seconds",
		"Agent sync round-trip time, including local observation.", latencyBuckets)
	m.containerOps = factory.counterVec("orion_container_operations_total",
		"Container engine operations by type and outcome.", "operation", "result")
	m.containerOpTime = factory.histogramVec("orion_container_operation_duration_seconds",
		"Container engine operation duration.", prometheus.ExponentialBuckets(0.005, 2, 14), "operation")
	m.agentFenced = factory.gauge("orion_agent_fenced",
		"1 when this agent has stopped its workloads after losing contact with the control plane.")
	m.workloadRestart = factory.counterVec("orion_workload_restarts_total",
		"In-place container restarts performed by the agent.", "workload")

	m.proxyRequests = factory.counterVec("orion_proxy_requests_total",
		"Service proxy requests by service and outcome.", "service", "result")
	m.proxyLatency = factory.histogramVec("orion_proxy_request_duration_seconds",
		"Service proxy request latency.", latencyBuckets, "service")
	m.proxyEndpoints = factory.gaugeVec("orion_proxy_healthy_endpoints",
		"Healthy endpoints per service as seen by the proxy.", "service")
	m.proxyRetries = factory.counterVec("orion_proxy_retries_total",
		"Requests retried against another endpoint after a backend failure.", "service")

	// Go runtime and process metrics: heap growth and goroutine leaks are the
	// two failure modes that show up first in a long-running control plane.
	r.MustRegister(collectors.NewGoCollector())
	r.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	return m
}

// Handler serves the Prometheus scrape endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// A failing collector should be visible in the scrape rather than
		// silently omitted.
		ErrorHandling: promhttp.ContinueOnError,
	})
}

// Registry exposes the registry for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// ---------------------------------------------------------------------------
// Recording
// ---------------------------------------------------------------------------

// ObserveAPIRequest records one API request. The route is the pattern, not the
// path, so a cluster with 10,000 workloads does not produce 10,000 label
// values and blow up the time series database.
func (m *Metrics) ObserveAPIRequest(method, route string, status int, d time.Duration) {
	m.apiRequests.WithLabelValues(method, route, statusClass(status)).Inc()
	m.apiLatency.WithLabelValues(method, route).Observe(d.Seconds())
}

func (m *Metrics) APIRequestStarted()  { m.apiInFlight.Inc() }
func (m *Metrics) APIRequestFinished() { m.apiInFlight.Dec() }

func statusClass(status int) string {
	switch {
	case status < 200:
		return "1xx"
	case status < 300:
		return "2xx"
	case status < 400:
		return "3xx"
	case status < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

// ObserveRaft publishes consensus state. Called on a timer rather than on every
// change: these are levels, not events.
func (m *Metrics) ObserveRaft(term, commit, applied uint64, isLeader bool) {
	m.raftTerm.Set(float64(term))
	m.raftCommitIndex.Set(float64(commit))
	m.raftAppliedIndex.Set(float64(applied))
	if isLeader {
		m.raftIsLeader.Set(1)
	} else {
		m.raftIsLeader.Set(0)
	}
}

func (m *Metrics) ObserveProposal(result string, d time.Duration) {
	m.raftProposals.WithLabelValues(result).Inc()
	m.raftProposalTime.Observe(d.Seconds())
}

// ScheduleAttempt implements controller.SchedulerMetrics.
func (m *Metrics) ScheduleAttempt(result string, latency time.Duration) {
	m.scheduleAttempts.WithLabelValues(result).Inc()
	if latency > 0 {
		m.scheduleLatency.Observe(latency.Seconds())
	}
}

func (m *Metrics) PendingWorkloads(n int) { m.pendingWorkloads.Set(float64(n)) }

// ReconcileFinished implements controller.Observer.
func (m *Metrics) ReconcileFinished(controller string, d time.Duration, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	m.reconcileTotal.WithLabelValues(controller, result).Inc()
	m.reconcileLatency.WithLabelValues(controller).Observe(d.Seconds())
}

// ReplicaCreated implements controller.DeploymentMetrics.
func (m *Metrics) ReplicaCreated(deployment string) {
	m.replicasCreated.WithLabelValues(deployment).Inc()
}

func (m *Metrics) ReplicaDeleted(deployment, reason string) {
	m.replicasDeleted.WithLabelValues(deployment, reason).Inc()
}

// NodeBecameUnreachable implements controller.NodeMetrics.
func (m *Metrics) NodeBecameUnreachable(string) { m.nodeFailures.Inc() }

func (m *Metrics) WorkloadEvicted(node string, count int) {
	m.workloadsEvicted.WithLabelValues(node).Add(float64(count))
}

func (m *Metrics) RecoveryObserved(_ string, d time.Duration) {
	m.recoveryDuration.Observe(d.Seconds())
}

// SyncCompleted implements agent.Metrics.
func (m *Metrics) SyncCompleted(err error, d time.Duration) {
	result := "success"
	if err != nil {
		result = "error"
	}
	m.agentSyncs.WithLabelValues(result).Inc()
	m.agentSyncTime.Observe(d.Seconds())
}

func (m *Metrics) ContainerOperation(op string, err error, d time.Duration) {
	result := "success"
	if err != nil {
		result = "error"
	}
	m.containerOps.WithLabelValues(op, result).Inc()
	m.containerOpTime.WithLabelValues(op).Observe(d.Seconds())
}

func (m *Metrics) WorkloadRestarted(workload string) {
	m.workloadRestart.WithLabelValues(workload).Inc()
}

func (m *Metrics) Fenced(active bool) {
	if active {
		m.agentFenced.Set(1)
	} else {
		m.agentFenced.Set(0)
	}
}

// ProxyRequest records a service proxy request.
func (m *Metrics) ProxyRequest(service, result string, d time.Duration) {
	m.proxyRequests.WithLabelValues(service, result).Inc()
	m.proxyLatency.WithLabelValues(service).Observe(d.Seconds())
}

func (m *Metrics) ProxyRetry(service string) { m.proxyRetries.WithLabelValues(service).Inc() }

func (m *Metrics) ProxyEndpoints(service string, healthy int) {
	m.proxyEndpoints.WithLabelValues(service).Set(float64(healthy))
}

// ClusterState publishes the cluster gauges. It is fed from the store's
// summary on a timer, so the numbers in Prometheus and in the console come
// from exactly the same computation.
type ClusterState struct {
	NodesByPhase       map[string]int
	WorkloadsByPhase   map[string]int
	DeploymentsByPhase map[string]int
	Restarts           int
	CPUAllocatable     int64
	CPUAllocated       int64
	CPUUsed            int64
	MemAllocatable     int64
	MemAllocated       int64
	MemUsed            int64
}

func (m *Metrics) ObserveClusterState(s ClusterState) {
	// Reset first so a phase that dropped to zero disappears rather than being
	// frozen at its last value forever.
	m.nodes.Reset()
	for phase, n := range s.NodesByPhase {
		m.nodes.WithLabelValues(phase).Set(float64(n))
	}
	m.workloads.Reset()
	for phase, n := range s.WorkloadsByPhase {
		m.workloads.WithLabelValues(phase).Set(float64(n))
	}
	m.deployments.Reset()
	for phase, n := range s.DeploymentsByPhase {
		m.deployments.WithLabelValues(phase).Set(float64(n))
	}
	m.restartsTotal.Set(float64(s.Restarts))

	m.cpuAllocatable.Set(float64(s.CPUAllocatable))
	m.cpuAllocated.Set(float64(s.CPUAllocated))
	m.cpuUsed.Set(float64(s.CPUUsed))
	m.memAllocatable.Set(float64(s.MemAllocatable))
	m.memAllocated.Set(float64(s.MemAllocated))
	m.memUsed.Set(float64(s.MemUsed))
}

// ---------------------------------------------------------------------------
// Registration helpers
// ---------------------------------------------------------------------------

type metricFactory struct{ r *prometheus.Registry }

func promauto(r *prometheus.Registry) metricFactory { return metricFactory{r: r} }

func (f metricFactory) counter(name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	f.r.MustRegister(c)
	return c
}

func (f metricFactory) counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	f.r.MustRegister(c)
	return c
}

func (f metricFactory) gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	f.r.MustRegister(g)
	return g
}

func (f metricFactory) gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	f.r.MustRegister(g)
	return g
}

func (f metricFactory) histogram(name, help string, buckets []float64) prometheus.Histogram {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets})
	f.r.MustRegister(h)
	return h
}

func (f metricFactory) histogramVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	f.r.MustRegister(h)
	return h
}
