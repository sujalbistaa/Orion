// Command orion-agent runs on every node. It reports the machine's capacity to
// the control plane and makes the containers on that machine match what the
// control plane says should be there.
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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sujalbistaa/orion/internal/version"
	"github.com/sujalbistaa/orion/pkg/agent"
	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	orionv1 "github.com/sujalbistaa/orion/pkg/proto/orionv1"
	"github.com/sujalbistaa/orion/pkg/runtime"
	"github.com/sujalbistaa/orion/pkg/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

func main() {
	var (
		nodeName    = flag.String("node-name", env("ORION_NODE_NAME", agent.Hostname()), "this node's cluster identity; must be stable across restarts")
		serverAddr  = flag.String("server", env("ORION_SERVER", "127.0.0.1:7071"), "control-plane gRPC address")
		advertise   = flag.String("advertise", env("ORION_ADVERTISE", ""), "host:port other components use to reach this agent; defaults to the local API address")
		localAddr   = flag.String("local-addr", env("ORION_AGENT_ADDR", ":7090"), "address for this agent's local API (logs, status)")
		metricsAddr = flag.String("metrics-addr", env("ORION_AGENT_METRICS_ADDR", ""), "address for the Prometheus endpoint; empty disables it")

		labelsFlag = flag.String("labels", env("ORION_NODE_LABELS", ""), "comma-separated key=value node labels used by scheduling constraints")

		reservedCPU = flag.String("reserved-cpu", env("ORION_RESERVED_CPU", "500m"), "CPU held back from Allocatable for the OS and the agent")
		reservedMem = flag.String("reserved-memory", env("ORION_RESERVED_MEMORY", "512Mi"), "memory held back from Allocatable")

		heartbeat = flag.Duration("heartbeat", envDuration("ORION_AGENT_HEARTBEAT", 3*time.Second), "initial heartbeat interval; the control plane may override it")

		logLevel  = flag.String("log-level", env("ORION_LOG_LEVEL", "info"), "debug, info, warn or error")
		logFormat = flag.String("log-format", env("ORION_LOG_FORMAT", "json"), "json or text")

		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println(version.Get().String())
		return
	}

	log := telemetry.NewLogger(*logLevel, telemetry.LogFormat(*logFormat), "orion-agent")
	slog.SetDefault(log)

	cfg := agentConfig{
		nodeName: *nodeName, serverAddr: *serverAddr, advertise: *advertise,
		localAddr: *localAddr, metricsAddr: *metricsAddr, labels: *labelsFlag,
		reservedCPU: *reservedCPU, reservedMem: *reservedMem, heartbeat: *heartbeat,
	}
	if err := run(cfg, log); err != nil {
		log.Error("orion-agent exited with an error", "err", err)
		os.Exit(1)
	}
}

type agentConfig struct {
	nodeName    string
	serverAddr  string
	advertise   string
	localAddr   string
	metricsAddr string
	labels      string
	reservedCPU string
	reservedMem string
	heartbeat   time.Duration
}

func run(cfg agentConfig, log *slog.Logger) error {
	labels, err := parseLabels(cfg.labels)
	if err != nil {
		return err
	}
	reservedCPU, err := v1.ParseMilliCPU(cfg.reservedCPU)
	if err != nil {
		return fmt.Errorf("reserved-cpu: %w", err)
	}
	reservedMem, err := v1.ParseBytes(cfg.reservedMem)
	if err != nil {
		return fmt.Errorf("reserved-memory: %w", err)
	}

	rt, err := runtime.NewDocker(cfg.nodeName)
	if err != nil {
		return fmt.Errorf("connecting to the container engine: %w", err)
	}
	defer rt.Close()

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	err = rt.Ping(pingCtx)
	cancelPing()
	if err != nil {
		return fmt.Errorf("the container engine is not reachable; is Docker running? %w", err)
	}

	// The agent's local API is how the control plane fetches logs, so the
	// address it advertises has to be the one that is actually reachable.
	advertise := cfg.advertise
	if advertise == "" {
		advertise = advertisableAddress(cfg.localAddr)
	}

	conn, err := grpc.NewClient(cfg.serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true,
		}),
		// Reconnect promptly after a control-plane restart rather than backing
		// off for the default 120s, which would look like a node failure.
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: 2 * time.Second,
		}),
	)
	if err != nil {
		return fmt.Errorf("connecting to the control plane at %s: %w", cfg.serverAddr, err)
	}
	defer conn.Close()

	a, err := agent.New(agent.Config{
		NodeName:          cfg.nodeName,
		Address:           advertise,
		Labels:            labels,
		ReservedCPU:       reservedCPU,
		ReservedMemory:    reservedMem,
		HeartbeatInterval: cfg.heartbeat,
		// The control plane overrides this at registration so the cluster
		// agrees on one failure-detection window.
		SelfFenceTimeout: 4 * cfg.heartbeat,
		SyncTimeout:      cfg.heartbeat / 2,
		Logger:           log,
	}, rt, orionv1.NewNodeServiceClient(conn))
	if err != nil {
		return err
	}

	metrics := telemetry.New()
	a.SetMetrics(metrics)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	localServer := &http.Server{
		Addr:              cfg.localAddr,
		Handler:           a.LocalAPI(os.Getenv("ORION_CLUSTER_KEY")).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("agent local API listening", "addr", cfg.localAddr, "advertise", advertise)
		if err := localServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("agent local API failed", "err", err)
			stop()
		}
	}()

	var metricsServer *http.Server
	if cfg.metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		metricsServer = &http.Server{Addr: cfg.metricsAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("agent metrics listening", "addr", cfg.metricsAddr)
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("agent metrics server failed", "err", err)
			}
		}()
	}

	log.Info("starting orion-agent",
		"version", version.Get().Version, "node", cfg.nodeName, "server", cfg.serverAddr,
		"reservedCPU", reservedCPU.String(), "reservedMemory", reservedMem.String())

	runErr := a.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = localServer.Shutdown(shutdownCtx)
	if metricsServer != nil {
		_ = metricsServer.Shutdown(shutdownCtx)
	}
	wg.Wait()

	log.Info("orion-agent stopped")
	return runErr
}

func parseLabels(spec string) (map[string]string, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(spec, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("label %q must be in key=value form", pair)
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out, nil
}

// advertisableAddress turns a bind address into one other machines can dial.
// ":7090" is a perfectly good thing to listen on and a useless thing to
// advertise, so the host part is resolved to a routable local address.
func advertisableAddress(bind string) string {
	host, port, err := net.SplitHostPort(bind)
	if err != nil {
		return bind
	}
	if host != "" && host != "0.0.0.0" && host != "::" {
		return bind
	}
	if ip := outboundIP(); ip != "" {
		return net.JoinHostPort(ip, port)
	}
	return net.JoinHostPort("127.0.0.1", port)
}

// outboundIP finds the address this machine would use to reach the network. No
// packets are sent; the kernel just resolves the route.
func outboundIP() string {
	conn, err := net.Dial("udp", "192.0.2.1:9") // TEST-NET-1, guaranteed unrouted
	if err != nil {
		return ""
	}
	defer conn.Close()
	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return ""
	}
	return addr.IP.String()
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
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
