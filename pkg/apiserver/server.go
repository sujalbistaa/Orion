// Package apiserver serves Orion's external HTTP API.
//
// It is the only thing the console and the CLI talk to. Nothing outside this
// package reaches into the scheduler, the controllers or Raft directly — the
// API is the contract, and keeping it the sole entry point is what allows the
// internals to change without breaking clients.
//
// Reads are served from this replica's applied state. Writes go through Raft
// and are refused with a redirect if this replica is not the leader.
package apiserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/controlplane"
	"github.com/sujalbistaa/orion/pkg/store"
	"github.com/sujalbistaa/orion/pkg/telemetry"
)

// Options configures the API server.
type Options struct {
	ControlPlane *controlplane.ControlPlane
	Metrics      *telemetry.Metrics
	Logger       *slog.Logger

	// Auth guards the API. A nil Authenticator means the API is open, which is
	// only acceptable for single-machine development and is logged loudly at
	// startup.
	Auth Authenticator

	// LogFetcher retrieves container logs from the node running a workload.
	LogFetcher LogFetcher

	// FaultInjector is optional and, when present, exposes the fault injection
	// API. It is nil unless explicitly enabled, because deliberately breaking
	// a cluster is not something that should be one stray HTTP call away in
	// production.
	FaultInjector FaultInjector

	// StaticFS serves the web console. Nil disables it.
	StaticFS http.FileSystem

	// ReadTimeout and WriteTimeout bound a request. WriteTimeout must be zero
	// for the server to support the event stream, so it is applied per-handler
	// instead.
	ReadTimeout    time.Duration
	MaxRequestSize int64
}

// LogFetcher reads a workload's container logs from the node running it.
type LogFetcher interface {
	Logs(ctx context.Context, node, workload string, tail int, follow bool) (LogStream, error)
}

// LogStream is a live log reader.
type LogStream interface {
	Read(p []byte) (int, error)
	Close() error
}

// FaultInjector runs controlled failure experiments.
type FaultInjector interface {
	List() []ExperimentDescriptor
	Start(ctx context.Context, req ExperimentRequest) (ExperimentRun, error)
	Get(id string) (ExperimentRun, bool)
	Runs() []ExperimentRun
	Abort(ctx context.Context, id string) error
}

// Server is the HTTP API.
type Server struct {
	cp      *controlplane.ControlPlane
	store   *store.Store
	metrics *telemetry.Metrics
	log     *slog.Logger
	auth    Authenticator
	logs    LogFetcher
	faults  FaultInjector
	static  http.FileSystem

	maxRequestSize int64
	writeLimiter   *rateLimiter
	mux            *http.ServeMux
}

const defaultMaxRequestSize = 1 << 20 // 1 MiB: specs are small; anything larger is abuse

func New(opts Options) (*Server, error) {
	if opts.ControlPlane == nil {
		return nil, errors.New("apiserver: ControlPlane is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.MaxRequestSize <= 0 {
		opts.MaxRequestSize = defaultMaxRequestSize
	}

	s := &Server{
		cp:             opts.ControlPlane,
		store:          opts.ControlPlane.Store(),
		metrics:        opts.Metrics,
		log:            opts.Logger.With("component", "apiserver"),
		auth:           opts.Auth,
		logs:           opts.LogFetcher,
		faults:         opts.FaultInjector,
		static:         opts.StaticFS,
		maxRequestSize: opts.MaxRequestSize,
		// 50 writes/second sustained with a burst of 100 per principal. A
		// human operator never approaches this; a stuck retry loop does.
		writeLimiter: newRateLimiter(50, 100),
		mux:          http.NewServeMux(),
	}
	if s.auth == nil {
		s.log.Warn("API authentication is disabled; every caller has full administrative access. " +
			"Set ORION_API_TOKEN before exposing this server on a network.")
		s.auth = openAccess{}
	}
	s.routes()
	return s, nil
}

// Handler returns the fully wrapped HTTP handler.
func (s *Server) Handler() http.Handler {
	// Middleware order matters: recovery outermost so a panic anywhere is
	// still logged and counted; request ID before logging so every line
	// correlates; auth before the handlers but after logging so rejected
	// requests are visible.
	var h http.Handler = s.mux
	h = s.withAuth(h)
	h = s.withMetrics(h)
	h = s.withLogging(h)
	h = s.withRequestID(h)
	h = s.withRecovery(h)
	return h
}

func (s *Server) routes() {
	m := s.mux

	// Health and readiness are deliberately different. Health means the process
	// is alive; readiness means it can serve correct answers, which requires
	// applied state that is not arbitrarily stale.
	m.HandleFunc("GET /healthz", s.handleHealthz)
	m.HandleFunc("GET /readyz", s.handleReadyz)

	m.HandleFunc("GET /api/v1/cluster", s.handleGetCluster)
	m.HandleFunc("GET /api/v1/summary", s.handleGetSummary)

	m.HandleFunc("GET /api/v1/nodes", s.handleListNodes)
	m.HandleFunc("GET /api/v1/nodes/{name}", s.handleGetNode)
	m.HandleFunc("POST /api/v1/nodes/{name}/cordon", s.handleCordonNode)
	m.HandleFunc("POST /api/v1/nodes/{name}/uncordon", s.handleUncordonNode)
	m.HandleFunc("POST /api/v1/nodes/{name}/drain", s.handleDrainNode)
	m.HandleFunc("DELETE /api/v1/nodes/{name}", s.handleDeleteNode)

	m.HandleFunc("GET /api/v1/workloads", s.handleListWorkloads)
	m.HandleFunc("POST /api/v1/workloads", s.handleCreateWorkload)
	m.HandleFunc("GET /api/v1/workloads/{name}", s.handleGetWorkload)
	m.HandleFunc("DELETE /api/v1/workloads/{name}", s.handleDeleteWorkload)
	m.HandleFunc("GET /api/v1/workloads/{name}/logs", s.handleWorkloadLogs)

	m.HandleFunc("GET /api/v1/deployments", s.handleListDeployments)
	m.HandleFunc("POST /api/v1/deployments", s.handleCreateDeployment)
	m.HandleFunc("GET /api/v1/deployments/{name}", s.handleGetDeployment)
	m.HandleFunc("PUT /api/v1/deployments/{name}", s.handleUpdateDeployment)
	m.HandleFunc("POST /api/v1/deployments/{name}/scale", s.handleScaleDeployment)
	m.HandleFunc("POST /api/v1/deployments/{name}/rollback", s.handleRollbackDeployment)
	m.HandleFunc("GET /api/v1/deployments/{name}/revisions", s.handleDeploymentRevisions)
	m.HandleFunc("DELETE /api/v1/deployments/{name}", s.handleDeleteDeployment)

	m.HandleFunc("GET /api/v1/services", s.handleListServices)
	m.HandleFunc("POST /api/v1/services", s.handleCreateService)
	m.HandleFunc("GET /api/v1/services/{name}", s.handleGetService)
	m.HandleFunc("DELETE /api/v1/services/{name}", s.handleDeleteService)

	m.HandleFunc("GET /api/v1/events", s.handleListEvents)
	// The change stream is how the console stays current without polling the
	// whole cluster state every second.
	m.HandleFunc("GET /api/v1/watch", s.handleWatch)

	if s.faults != nil {
		m.HandleFunc("GET /api/v1/faults/experiments", s.handleListExperiments)
		m.HandleFunc("GET /api/v1/faults/runs", s.handleListRuns)
		m.HandleFunc("POST /api/v1/faults/runs", s.handleStartExperiment)
		m.HandleFunc("GET /api/v1/faults/runs/{id}", s.handleGetRun)
		m.HandleFunc("POST /api/v1/faults/runs/{id}/abort", s.handleAbortExperiment)
	}

	if s.metrics != nil {
		m.Handle("GET /metrics", s.metrics.Handler())
	}

	if s.static != nil {
		m.Handle("/", s.spaHandler())
	}
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"version": versionInfo(),
	})
}

// maxAcceptableApplyLag is how far behind the commit index a replica may be and
// still call itself ready. A follower that is catching up would return stale
// data, so it removes itself from the load balancer instead.
const maxAcceptableApplyLag = 100

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	cluster := s.cp.ClusterStatus()
	lag := int64(cluster.CommitIndex) - int64(cluster.AppliedIndex)

	problems := []string{}
	if cluster.LeaderID == "0" {
		problems = append(problems, "no leader elected")
	}
	if lag > maxAcceptableApplyLag {
		problems = append(problems, "state machine is behind the commit index")
	}

	body := map[string]any{
		"leader":       cluster.LeaderID,
		"term":         cluster.RaftTerm,
		"commitIndex":  cluster.CommitIndex,
		"appliedIndex": cluster.AppliedIndex,
		"applyLag":     lag,
		"isLeader":     s.cp.IsLeader(),
	}
	if len(problems) > 0 {
		body["status"] = "not ready"
		body["problems"] = problems
		writeJSON(w, http.StatusServiceUnavailable, body)
		return
	}
	body["status"] = "ready"
	writeJSON(w, http.StatusOK, body)
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

func (s *Server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic while serving a request",
					"path", r.URL.Path, "method", r.Method, "panic", rec)
				// No panic detail in the response: it would leak internals to
				// a caller who may not be trusted.
				writeError(w, http.StatusInternalServerError, "internal", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if id == "" {
			id = controlplane.NewRequestID()[:16]
		}
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r.WithContext(telemetry.WithRequestID(r.Context(), id)))
	})
}

func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		// Successful reads are the overwhelming majority of traffic and are
		// logged at debug; anything that changed state or failed is logged at
		// info or above so the default log level is an audit trail, not noise.
		level := slog.LevelDebug
		switch {
		case rec.status >= 500:
			level = slog.LevelError
		case rec.status >= 400:
			level = slog.LevelWarn
		case r.Method != http.MethodGet:
			level = slog.LevelInfo
		}
		telemetry.LoggerFrom(r.Context(), s.log).Log(r.Context(), level, "http request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration", time.Since(start).Round(time.Microsecond),
			"bytes", rec.written, "remote", clientIP(r), "actor", actorOf(r.Context()))
	})
}

func (s *Server) withMetrics(next http.Handler) http.Handler {
	if s.metrics == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.metrics.APIRequestStarted()
		defer s.metrics.APIRequestFinished()

		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		// Label on the route pattern, never the path: a cluster with 10,000
		// workloads would otherwise create 10,000 time series.
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		s.metrics.ObserveAPIRequest(r.Method, route, rec.status, time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status  int
	written int
	wrote   bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.wrote {
		return
	}
	r.wrote = true
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wrote {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.written += n
	return n, err
}

// Flush forwards to the underlying writer so the event stream still works
// through the middleware chain.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func clientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		if i := strings.IndexByte(fwd, ','); i > 0 {
			return strings.TrimSpace(fwd[:i])
		}
		return strings.TrimSpace(fwd)
	}
	host := r.RemoteAddr
	if i := strings.LastIndexByte(host, ':'); i > 0 {
		return host[:i]
	}
	return host
}

func versionInfo() map[string]string {
	return map[string]string{"api": "v1"}
}

// requireLeader rejects a write on a follower, telling the caller where the
// leader is so the CLI and console can retry there rather than failing.
func (s *Server) requireLeader(w http.ResponseWriter) bool {
	if s.cp.IsLeader() {
		return true
	}
	if addr, ok := s.cp.LeaderAddress(); ok {
		w.Header().Set("X-Orion-Leader", addr)
		writeError(w, http.StatusServiceUnavailable, "not_leader",
			"this replica is not the leader; retry against "+addr)
		return false
	}
	writeError(w, http.StatusServiceUnavailable, "no_leader",
		"no leader is elected; the cluster cannot accept writes")
	return false
}

// applyWrite proposes a command and maps the outcome onto an HTTP response.
// Every write path goes through here, so status-code semantics are decided in
// one place instead of drifting between handlers.
func (s *Server) applyWrite(w http.ResponseWriter, r *http.Request, cmd store.Command, successStatus int) (store.Result, bool) {
	cmd.Actor = actorOf(r.Context())

	res, err := s.cp.Apply(r.Context(), cmd)
	if err == nil {
		if successStatus != 0 {
			writeResult(w, successStatus, res)
		}
		return res, true
	}

	switch {
	case errors.Is(err, controlplane.ErrNotLeader):
		if addr, ok := s.cp.LeaderAddress(); ok {
			w.Header().Set("X-Orion-Leader", addr)
		}
		writeError(w, http.StatusServiceUnavailable, "not_leader",
			"this replica is not the leader; retry against the leader")
	case errors.Is(err, controlplane.ErrUnavailable):
		writeError(w, http.StatusServiceUnavailable, "unavailable",
			"the cluster could not commit this change; it is safe to retry")
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, store.ErrAlreadyExists):
		writeError(w, http.StatusConflict, "already_exists", err.Error())
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, store.ErrInvalidState), errors.Is(err, store.ErrUIDMismatch):
		writeError(w, http.StatusConflict, "invalid_state", err.Error())
	default:
		telemetry.LoggerFrom(r.Context(), s.log).Error("write failed",
			"kind", cmd.Kind, "name", cmd.Name, "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "the change could not be applied")
	}
	return store.Result{}, false
}

// recordAudit writes an operational event for a destructive action. Deletions,
// drains and fault injection are exactly the operations someone will need to
// account for later.
func (s *Server) recordAudit(ctx context.Context, reason, kind, name, message string) {
	s.cp.RecordEvent(ctx, v1.Event{
		Severity: v1.SeverityWarning,
		Source:   "api",
		Reason:   reason,
		Kind:     kind,
		Name:     name,
		Message:  message,
		Actor:    actorOf(ctx),
	})
}
