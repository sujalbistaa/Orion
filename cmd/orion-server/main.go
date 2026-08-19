// Command orion-server runs an Orion control-plane replica: the Raft node, the
// cluster state machine, the scheduler, the controllers, the agent-facing gRPC
// service and the HTTP API.
//
// Running these in one process is a deliberate choice. Splitting them would
// mean the scheduler talks to the store over a network hop it does not need,
// and every deployment becomes a five-service orchestration problem before the
// first workload runs. They are separate packages with real boundaries; the
// process is just where those packages happen to be linked.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sujalbistaa/orion/internal/version"
	"github.com/sujalbistaa/orion/pkg/apiserver"
	"github.com/sujalbistaa/orion/pkg/controller"
	"github.com/sujalbistaa/orion/pkg/controlplane"
	"github.com/sujalbistaa/orion/pkg/faults"
	"github.com/sujalbistaa/orion/pkg/nodeservice"
	orionv1 "github.com/sujalbistaa/orion/pkg/proto/orionv1"
	"github.com/sujalbistaa/orion/pkg/proxy"
	"github.com/sujalbistaa/orion/pkg/raft"
	rafttransport "github.com/sujalbistaa/orion/pkg/raft/transport"
	"github.com/sujalbistaa/orion/pkg/scheduler"
	"github.com/sujalbistaa/orion/pkg/store"
	"github.com/sujalbistaa/orion/pkg/telemetry"
	"github.com/sujalbistaa/orion/web"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type config struct {
	nodeID    uint64
	dataDir   string
	peers     string
	apiAddr   string
	grpcAddr  string
	raftAddr  string
	proxyBind string

	heartbeatTimeout time.Duration
	evictionDelay    time.Duration
	agentHeartbeat   time.Duration

	enableProxy   bool
	enableFaults  bool
	enableConsole bool

	logLevel  string
	logFormat string

	showVersion bool
}

func main() {
	cfg := parseFlags()

	if cfg.showVersion {
		fmt.Println(version.Get().String())
		return
	}

	log := telemetry.NewLogger(cfg.logLevel, telemetry.LogFormat(cfg.logFormat), "orion-server")
	slog.SetDefault(log)

	if err := run(cfg, log); err != nil {
		log.Error("orion-server exited with an error", "err", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.Uint64Var(&cfg.nodeID, "id", envUint("ORION_NODE_ID", 1),
		"this replica's Raft node ID; must be unique and stable across restarts")
	flag.StringVar(&cfg.dataDir, "data-dir", env("ORION_DATA_DIR", "./.orion/server"),
		"directory for the Raft log and snapshots")
	flag.StringVar(&cfg.peers, "peers", env("ORION_PEERS", ""),
		"comma-separated id=host:port list of control-plane replicas; empty means single-node")
	flag.StringVar(&cfg.apiAddr, "api-addr", env("ORION_API_ADDR", ":7070"),
		"address for the HTTP API and web console")
	flag.StringVar(&cfg.grpcAddr, "grpc-addr", env("ORION_GRPC_ADDR", ":7071"),
		"address for the agent-facing gRPC service")
	flag.StringVar(&cfg.raftAddr, "raft-addr", env("ORION_RAFT_ADDR", ":7072"),
		"address for Raft peer traffic")
	flag.StringVar(&cfg.proxyBind, "proxy-bind", env("ORION_PROXY_BIND", "0.0.0.0"),
		"interface the service load balancer binds to")

	flag.DurationVar(&cfg.heartbeatTimeout, "heartbeat-timeout", envDuration("ORION_HEARTBEAT_TIMEOUT", 15*time.Second),
		"how long a node may go silent before it is presumed unreachable")
	flag.DurationVar(&cfg.evictionDelay, "eviction-delay", envDuration("ORION_EVICTION_DELAY", 15*time.Second),
		"additional grace period before an unreachable node's workloads are terminated")
	flag.DurationVar(&cfg.agentHeartbeat, "agent-heartbeat", envDuration("ORION_AGENT_HEARTBEAT", 3*time.Second),
		"how often agents report in")

	flag.BoolVar(&cfg.enableProxy, "enable-proxy", envBool("ORION_ENABLE_PROXY", true),
		"run the service load balancer in this process")
	flag.BoolVar(&cfg.enableFaults, "enable-fault-injection", envBool("ORION_ENABLE_FAULTS", false),
		"expose the fault injection API; keep this off in production")
	flag.BoolVar(&cfg.enableConsole, "enable-console", envBool("ORION_ENABLE_CONSOLE", true),
		"serve the embedded web console")

	flag.StringVar(&cfg.logLevel, "log-level", env("ORION_LOG_LEVEL", "info"), "debug, info, warn or error")
	flag.StringVar(&cfg.logFormat, "log-format", env("ORION_LOG_FORMAT", "json"), "json or text")
	flag.BoolVar(&cfg.showVersion, "version", false, "print version and exit")

	flag.Parse()
	return cfg
}

func run(cfg config, log *slog.Logger) error {
	log.Info("starting orion-server", "version", version.Get().Version, "commit", version.Get().Commit,
		"nodeId", cfg.nodeID, "dataDir", cfg.dataDir)

	// The safety ordering that makes eviction sound. Deriving the agent's
	// self-fence timeout from the eviction deadline here — rather than
	// configuring both independently — removes the possibility of an operator
	// setting them in a combination that permits split brain.
	evictionDeadline := cfg.heartbeatTimeout + cfg.evictionDelay
	selfFenceTimeout := evictionDeadline - 2*cfg.agentHeartbeat
	if selfFenceTimeout <= cfg.agentHeartbeat {
		return fmt.Errorf(
			"heartbeat-timeout (%s) + eviction-delay (%s) leaves no room for an agent to fence itself "+
				"at a %s heartbeat interval; increase the timeouts or shorten the heartbeat",
			cfg.heartbeatTimeout, cfg.evictionDelay, cfg.agentHeartbeat)
	}

	peers, err := parsePeers(cfg.peers, cfg.nodeID, cfg.raftAddr)
	if err != nil {
		return err
	}

	storage, err := raft.OpenFileStorage(cfg.dataDir, raft.FileStorageOptions{})
	if err != nil {
		return fmt.Errorf("opening raft storage: %w", err)
	}

	metrics := telemetry.New()
	clusterState := store.New()

	transport := rafttransport.NewHTTP(rafttransport.Options{
		SelfID:  cfg.nodeID,
		Logger:  log,
		AuthKey: os.Getenv("ORION_CLUSTER_KEY"),
	})

	voters := make([]uint64, 0, len(peers))
	for id := range peers {
		voters = append(voters, id)
	}

	cp, err := controlplane.New(controlplane.Options{
		NodeID: cfg.nodeID,
		Peers:  peers,
		Raft: raft.Config{
			ID:            cfg.nodeID,
			Peers:         voters,
			ElectionTick:  10,
			HeartbeatTick: 1,
			Storage:       storage,
			PreVote:       true,
			CheckQuorum:   true,
			Logger:        log,
		},
		Transport:         transport,
		Store:             clusterState,
		TickInterval:      100 * time.Millisecond,
		SnapshotThreshold: 8192,
		ProposeTimeout:    10 * time.Second,
		Logger:            log,
	})
	if err != nil {
		return err
	}
	// The transport needs the node to deliver into, and the node needs the
	// transport to send through; the indirection breaks the cycle.
	transport.SetDeliver(cp.Raft().Step)

	cp.Start()
	defer cp.Stop()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- controllers -------------------------------------------------------
	manager := controller.NewManager(controller.ManagerOptions{
		Logger:     log,
		Leadership: cp.LeadershipChanges(),
		IsLeader:   cp.IsLeader,
		Observer:   metrics,
	})
	nodeLifecycle := controller.NewNodeLifecycleController(cp, log, metrics)
	nodeLifecycle.HeartbeatTimeout = cfg.heartbeatTimeout
	nodeLifecycle.EvictionDelay = cfg.evictionDelay

	manager.Register(
		controller.NewSchedulingController(cp, scheduler.New(), log, metrics),
		controller.NewDeploymentController(cp, log, metrics),
		nodeLifecycle,
		controller.NewEndpointController(cp, log),
		controller.NewGarbageCollector(cp, log),
	)

	supervisor := &controllerSupervisor{manager: manager, log: log}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		supervisor.run(ctx)
	}()

	// --- fault injection ---------------------------------------------------
	gate := faults.NewGate()
	var injector *faults.Injector
	if cfg.enableFaults {
		injector, err = faults.New(faults.Options{
			ControlPlane:      cp,
			Gate:              gate,
			Logger:            log,
			ControllerControl: supervisor,
		})
		if err != nil {
			return err
		}
		defer injector.Close()
		log.Warn("fault injection is ENABLED; this server can deliberately break its own cluster")
	}

	// --- agent-facing gRPC -------------------------------------------------
	nodeSvc, err := nodeservice.New(cp, fmt.Sprintf("orion-%d", cfg.nodeID), nodeservice.Timings{
		HeartbeatInterval: cfg.agentHeartbeat,
		SelfFenceTimeout:  selfFenceTimeout,
		EvictionDeadline:  evictionDeadline,
	}, gate, log)
	if err != nil {
		return err
	}
	log.Info("failure detection configured",
		"heartbeat", cfg.agentHeartbeat, "heartbeatTimeout", cfg.heartbeatTimeout,
		"evictionDelay", cfg.evictionDelay, "selfFenceTimeout", selfFenceTimeout)

	grpcServer := grpc.NewServer(
		// Agents are long-lived and mostly idle between heartbeats; keepalive
		// keeps a NAT or load balancer from silently dropping the connection
		// and turning it into a heartbeat timeout.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 30 * time.Second, Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime: 10 * time.Second, PermitWithoutStream: true,
		}),
		grpc.MaxRecvMsgSize(4<<20),
	)
	orionv1.RegisterNodeServiceServer(grpcServer, nodeSvc)

	grpcLn, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		return fmt.Errorf("listening for agents on %s: %w", cfg.grpcAddr, err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("agent gRPC service listening", "addr", grpcLn.Addr().String())
		if err := grpcServer.Serve(grpcLn); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("gRPC server failed", "err", err)
		}
	}()

	// --- service proxy -----------------------------------------------------
	if cfg.enableProxy {
		p, err := proxy.New(proxy.Options{
			Store: clusterState, Logger: log, BindAddress: cfg.proxyBind, Metrics: metrics,
		})
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Run(ctx); err != nil {
				log.Error("service proxy failed", "err", err)
			}
		}()
	}

	// --- HTTP API ----------------------------------------------------------
	auth, err := buildAuth(log)
	if err != nil {
		return err
	}

	apiOpts := apiserver.Options{
		ControlPlane: cp,
		Metrics:      metrics,
		Logger:       log,
		Auth:         auth,
		LogFetcher:   newAgentLogFetcher(clusterState, os.Getenv("ORION_CLUSTER_KEY")),
	}
	if injector != nil {
		apiOpts.FaultInjector = injector
	}
	if cfg.enableConsole {
		if fsys, ok := web.ConsoleFS(); ok {
			apiOpts.StaticFS = fsys
		} else {
			log.Warn("web console assets are not embedded in this build; " +
				"run `make web-build` before building the server to include them")
		}
	}

	api, err := apiserver.New(apiOpts)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/raft/message", transport.Handler())
	mux.Handle("/", api.Handler())

	httpServer := &http.Server{
		Addr:    cfg.apiAddr,
		Handler: mux,
		// No WriteTimeout: the event stream and log following are long-lived.
		// ReadHeaderTimeout is what actually protects against slowloris.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("HTTP API listening", "addr", cfg.apiAddr,
			"console", cfg.enableConsole && apiOpts.StaticFS != nil,
			"faultInjection", cfg.enableFaults)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server failed", "err", err)
			stop()
		}
	}()

	// --- metrics publisher -------------------------------------------------
	wg.Add(1)
	go func() {
		defer wg.Done()
		publishClusterMetrics(ctx, cp, metrics)
	}()

	<-ctx.Done()
	log.Info("shutdown signal received; draining")

	// Stop accepting new work before tearing anything down, so in-flight
	// requests finish against a consistent system.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("HTTP server did not drain cleanly", "err", err)
	}
	grpcServer.GracefulStop()
	wg.Wait()

	if err := storage.Close(); err != nil {
		log.Warn("closing raft storage", "err", err)
	}
	log.Info("orion-server stopped")
	return nil
}

// controllerSupervisor owns the controller manager's lifecycle so that fault
// injection can stop and restart reconciliation for real.
type controllerSupervisor struct {
	manager *controller.Manager
	log     *slog.Logger

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *controllerSupervisor) run(parent context.Context) {
	s.mu.Lock()
	s.ctx = parent
	s.mu.Unlock()
	s.StartControllers()

	<-parent.Done()
	s.StopControllers()
}

func (s *controllerSupervisor) StartControllers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil || s.ctx == nil || s.ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithCancel(s.ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	done := s.done
	go func() {
		defer close(done)
		s.manager.Run(ctx)
	}()
	s.log.Info("controller manager started")
}

func (s *controllerSupervisor) StopControllers() {
	s.mu.Lock()
	cancel, done := s.cancel, s.done
	s.cancel, s.done = nil, nil
	s.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
	s.log.Warn("controller manager stopped; no reconciliation is running")
}

func (s *controllerSupervisor) ControllersRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancel != nil
}

// publishClusterMetrics feeds the store's summary into Prometheus, so the
// numbers on the console and in Grafana come from the same computation.
func publishClusterMetrics(ctx context.Context, cp *controlplane.ControlPlane, m *telemetry.Metrics) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s := cp.Store().Summary()
			cluster := cp.ClusterStatus()
			m.ObserveRaft(cluster.RaftTerm, cluster.CommitIndex, cluster.AppliedIndex, cp.IsLeader())
			m.ObserveClusterState(telemetry.ClusterState{
				NodesByPhase: map[string]int{
					"Ready": s.Nodes.Ready, "NotReady": s.Nodes.NotReady, "Unreachable": s.Nodes.Unreachable,
				},
				WorkloadsByPhase: map[string]int{
					"Running": s.Workloads.Running, "Pending": s.Workloads.Pending,
					"Starting": s.Workloads.Starting, "Failed": s.Workloads.Failed,
					"Succeeded": s.Workloads.Succeeded,
				},
				DeploymentsByPhase: map[string]int{
					"Available": s.Deployments.Available, "Progressing": s.Deployments.Progressing,
					"Degraded": s.Deployments.Degraded,
				},
				Restarts:       s.Workloads.Restarts,
				CPUAllocatable: int64(s.Capacity.CPUAllocatable),
				CPUAllocated:   int64(s.Capacity.CPUAllocated),
				CPUUsed:        int64(s.Capacity.CPUUsed),
				MemAllocatable: int64(s.Capacity.MemAllocatable),
				MemAllocated:   int64(s.Capacity.MemAllocated),
				MemUsed:        int64(s.Capacity.MemUsed),
			})
		}
	}
}

// buildAuth configures API authentication from the environment.
func buildAuth(log *slog.Logger) (apiserver.Authenticator, error) {
	operator := os.Getenv("ORION_API_TOKEN")
	viewer := os.Getenv("ORION_API_VIEWER_TOKEN")
	if operator == "" && viewer == "" {
		// Returning nil makes the API server log its own warning and run open,
		// which is the right behaviour for `make run` on a laptop and the wrong
		// one anywhere else.
		return nil, nil
	}

	auth := apiserver.NewTokenAuth()
	if operator != "" {
		if err := auth.AddToken(operator, "operator", apiserver.RoleOperator); err != nil {
			return nil, fmt.Errorf("ORION_API_TOKEN: %w", err)
		}
	}
	if viewer != "" {
		if err := auth.AddToken(viewer, "viewer", apiserver.RoleViewer); err != nil {
			return nil, fmt.Errorf("ORION_API_VIEWER_TOKEN: %w", err)
		}
	}
	log.Info("API authentication enabled",
		"operatorToken", operator != "", "viewerToken", viewer != "")
	return auth, nil
}

// parsePeers builds the replica table. A single-node cluster is the default so
// that `orion-server` with no flags does something useful.
func parsePeers(spec string, selfID uint64, selfRaftAddr string) (map[uint64]string, error) {
	peers := map[uint64]string{selfID: normalizeAddr(selfRaftAddr)}
	if strings.TrimSpace(spec) == "" {
		return peers, nil
	}
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		id, addr, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("peer %q must be in id=host:port form", entry)
		}
		n, err := strconv.ParseUint(strings.TrimSpace(id), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("peer %q has an invalid id: %w", entry, err)
		}
		if n == 0 {
			return nil, fmt.Errorf("peer id 0 is reserved")
		}
		peers[n] = strings.TrimSpace(addr)
	}
	if len(peers)%2 == 0 {
		return nil, fmt.Errorf(
			"an even number of control-plane replicas (%d) gains no fault tolerance over %d and "+
				"doubles the chance of losing quorum; use an odd number", len(peers), len(peers)-1)
	}
	return peers, nil
}

// normalizeAddr turns ":7072" into "127.0.0.1:7072" so a peer table entry is
// dialable rather than a bind spec.
func normalizeAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envUint(key string, def uint64) uint64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}
