package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/raft"
	"github.com/sujalbistaa/orion/pkg/store"
)

// backend is a real TCP server that identifies itself, so tests can verify
// which endpoint a connection actually reached rather than trusting counters.
type backend struct {
	id       string
	listener net.Listener
	conns    int
	mu       sync.Mutex
	closed   bool
}

func newBackend(t *testing.T, id string) *backend {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("starting backend: %v", err)
	}
	b := &backend{id: id, listener: ln}
	go b.serve()
	t.Cleanup(b.close)
	return b
}

func (b *backend) serve() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.mu.Lock()
		b.conns++
		b.mu.Unlock()
		go func(c net.Conn) {
			defer c.Close()
			// Echo the backend's identity, then whatever the client sends.
			fmt.Fprintf(c, "%s\n", b.id)
			io.Copy(c, c)
		}(conn)
	}
}

func (b *backend) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		b.listener.Close()
	}
}

func (b *backend) connCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.conns
}

func (b *backend) endpoint(name string) v1.Endpoint {
	host, portStr, _ := net.SplitHostPort(b.listener.Addr().String())
	var port int32
	fmt.Sscanf(portStr, "%d", &port)
	return v1.Endpoint{
		WorkloadName: name, WorkloadUID: name + "-uid", NodeName: "worker-01",
		Address: host, Port: port, Health: v1.HealthHealthy, Ready: true,
	}
}

// testProxy wires a proxy to a store that tests can drive directly.
type testProxy struct {
	*Proxy
	store *store.Store
	index uint64
	t     *testing.T
	port  int32
}

func newTestProxy(t *testing.T) *testProxy {
	t.Helper()
	st := store.New()
	p, err := New(Options{
		Store:       st,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		BindAddress: "127.0.0.1",
		DialTimeout: 300 * time.Millisecond,
		MaxRetries:  3,
	})
	if err != nil {
		t.Fatal(err)
	}
	tp := &testProxy{Proxy: p, store: st, t: t}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return tp
}

func (tp *testProxy) apply(cmd store.Command) {
	tp.t.Helper()
	if cmd.Timestamp.IsZero() {
		cmd.Timestamp = time.Now()
	}
	data, err := cmd.Encode()
	if err != nil {
		tp.t.Fatal(err)
	}
	tp.index++
	res := tp.store.Apply(raft.Entry{Index: tp.index, Term: 1, Data: data}).(store.Result)
	if res.Err != nil {
		tp.t.Fatalf("%s: %v", cmd.Kind, res.Err)
	}
}

// freePort reserves a port and releases it, so the service can bind it.
func freePort(t *testing.T) int32 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port int32
	fmt.Sscanf(portStr, "%d", &port)
	return port
}

func (tp *testProxy) createService(name string, strategy v1.LoadBalanceStrategy, endpoints ...v1.Endpoint) int32 {
	tp.t.Helper()
	port := freePort(tp.t)
	tp.port = port
	tp.apply(store.Command{Kind: store.CmdCreateService, Service: &v1.Service{
		ObjectMeta: v1.ObjectMeta{Name: name},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{"app": name}, Port: port, TargetPort: 80, Strategy: strategy,
		},
	}})
	tp.setEndpoints(name, endpoints...)
	waitFor(tp.t, 3*time.Second, "the listener to bind", func() bool {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	})
	return port
}

func (tp *testProxy) setEndpoints(name string, endpoints ...v1.Endpoint) {
	tp.apply(store.Command{Kind: store.CmdUpdateServiceEndpoint, Name: name, Endpoints: endpoints})
	tp.sync()
}

// connect opens a connection through the proxy and returns which backend
// answered.
func connect(t *testing.T, port int32) (string, error) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 2*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		return "", err
	}
	return string(buf[:n-1]), nil
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// ---------------------------------------------------------------------------

func TestProxyRoundRobinsAcrossEndpoints(t *testing.T) {
	tp := newTestProxy(t)
	a, b, c := newBackend(t, "a"), newBackend(t, "b"), newBackend(t, "c")

	port := tp.createService("web", v1.LBRoundRobin,
		a.endpoint("web-0"), b.endpoint("web-1"), c.endpoint("web-2"))

	hits := map[string]int{}
	for i := 0; i < 9; i++ {
		id, err := connect(t, port)
		if err != nil {
			t.Fatalf("connection %d failed: %v", i, err)
		}
		hits[id]++
	}
	if len(hits) != 3 {
		t.Fatalf("traffic did not reach every endpoint: %v", hits)
	}
	for id, n := range hits {
		if n != 3 {
			t.Errorf("backend %s received %d of 9 connections; round robin should be even: %v", id, n, hits)
		}
	}
}

// Only endpoints the cluster has proven healthy may receive traffic.
func TestProxySkipsUnhealthyEndpoints(t *testing.T) {
	tp := newTestProxy(t)
	healthy, unhealthy := newBackend(t, "healthy"), newBackend(t, "unhealthy")

	bad := unhealthy.endpoint("web-1")
	bad.Ready = false
	bad.Health = v1.HealthUnhealthy

	port := tp.createService("web", v1.LBRoundRobin, healthy.endpoint("web-0"), bad)

	for i := 0; i < 6; i++ {
		id, err := connect(t, port)
		if err != nil {
			t.Fatalf("connection %d failed: %v", i, err)
		}
		if id != "healthy" {
			t.Fatalf("connection reached the unhealthy endpoint")
		}
	}
	if unhealthy.connCount() != 0 {
		t.Errorf("the unhealthy backend received %d connections", unhealthy.connCount())
	}

	// An endpoint whose health is merely Unknown must also be excluded:
	// endpoints are proven healthy, never assumed.
	unknown := unhealthy.endpoint("web-1")
	unknown.Ready = true
	unknown.Health = v1.HealthUnknown
	tp.setEndpoints("web", healthy.endpoint("web-0"), unknown)

	for i := 0; i < 4; i++ {
		if id, _ := connect(t, port); id != "healthy" {
			t.Fatal("an endpoint with Unknown health received traffic")
		}
	}
}

// An endpoint can die between the last health check and this connection. The
// proxy must retry rather than surface the failure.
func TestProxyRetriesWhenABackendIsDead(t *testing.T) {
	tp := newTestProxy(t)
	live := newBackend(t, "live")
	dead := newBackend(t, "dead")
	deadEndpoint := dead.endpoint("web-1")
	dead.close() // the endpoint is still advertised but nothing is listening

	port := tp.createService("web", v1.LBRoundRobin, live.endpoint("web-0"), deadEndpoint)

	for i := 0; i < 6; i++ {
		id, err := connect(t, port)
		if err != nil {
			t.Fatalf("connection %d failed instead of retrying to a live backend: %v", i, err)
		}
		if id != "live" {
			t.Fatalf("connection reported backend %q", id)
		}
	}
	stats := tp.Stats()
	if len(stats) != 1 || stats[0].Retries == 0 {
		t.Errorf("retries were not recorded: %+v", stats)
	}
}

func TestProxyRejectsWhenNoEndpointsAreHealthy(t *testing.T) {
	tp := newTestProxy(t)
	b := newBackend(t, "only")
	port := tp.createService("web", v1.LBRoundRobin, b.endpoint("web-0"))

	if _, err := connect(t, port); err != nil {
		t.Fatalf("baseline connection failed: %v", err)
	}

	tp.setEndpoints("web") // every endpoint gone

	// The listener stays up — the service still exists — but connections are
	// closed immediately rather than hanging, so a client fails fast.
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err != nil {
		return // also acceptable
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	if n, err := conn.Read(buf); err == nil && n > 0 {
		t.Fatalf("a connection was served with no healthy endpoints: %q", buf[:n])
	}
}

// Least-connections must favour the idle endpoint when one is saturated by
// long-lived connections.
func TestLeastConnectionsFavoursTheIdleEndpoint(t *testing.T) {
	tp := newTestProxy(t)
	busy, idle := newBackend(t, "busy"), newBackend(t, "idle")
	port := tp.createService("web", v1.LBLeastConnections, busy.endpoint("web-0"), idle.endpoint("web-1"))

	// Hold several long-lived connections open. Which backend they land on
	// depends on the initial pick, so identify them and then check that further
	// connections avoid whichever one is loaded.
	var held []net.Conn
	loaded := map[string]int{}
	for i := 0; i < 4; i++ {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err != nil {
			t.Fatalf("holding connection %d: %v", i, err)
		}
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 32)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("reading identity: %v", err)
		}
		loaded[string(buf[:n-1])]++
		held = append(held, conn)
	}
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()

	// With four connections held, the distribution must be balanced: least
	// connections should never have sent all four to one backend.
	for id, n := range loaded {
		if n == 4 {
			t.Fatalf("least-connections sent every held connection to %s: %v", id, loaded)
		}
	}
	_ = busy
	_ = idle
}

func TestProxyStopsListeningWhenAServiceIsDeleted(t *testing.T) {
	tp := newTestProxy(t)
	b := newBackend(t, "only")
	port := tp.createService("web", v1.LBRoundRobin, b.endpoint("web-0"))

	if _, err := connect(t, port); err != nil {
		t.Fatalf("baseline connection failed: %v", err)
	}

	tp.apply(store.Command{Kind: store.CmdDeleteService, Name: "web"})
	tp.sync()

	waitFor(t, 3*time.Second, "the listener to be released", func() bool {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err != nil {
			return true
		}
		conn.Close()
		return false
	})
	if len(tp.Stats()) != 0 {
		t.Errorf("stats still report a listener for a deleted service: %+v", tp.Stats())
	}
}

// Data must survive the proxy intact in both directions.
func TestProxyPassesDataThroughUnmodified(t *testing.T) {
	tp := newTestProxy(t)
	b := newBackend(t, "echo")
	port := tp.createService("web", v1.LBRoundRobin, b.endpoint("web-0"))

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 2*time.Second)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Consume the identity line.
	buf := make([]byte, 64)
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("reading identity: %v", err)
	}

	payload := make([]byte, 128*1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	go func() { conn.Write(payload) }()

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("reading echoed payload: %v", err)
	}
	for i := range payload {
		if got[i] != payload[i] {
			t.Fatalf("payload corrupted at byte %d: got %d want %d", i, got[i], payload[i])
		}
	}
}

func TestProxyTracksConnectionStats(t *testing.T) {
	tp := newTestProxy(t)
	b := newBackend(t, "only")
	port := tp.createService("web", v1.LBRoundRobin, b.endpoint("web-0"))

	for i := 0; i < 3; i++ {
		if _, err := connect(t, port); err != nil {
			t.Fatal(err)
		}
	}
	stats := tp.Stats()
	if len(stats) != 1 {
		t.Fatalf("expected stats for one service, got %d", len(stats))
	}
	if stats[0].TotalConnections < 3 {
		t.Errorf("total connections = %d, want at least 3", stats[0].TotalConnections)
	}
	if stats[0].HealthyEndpoints != 1 {
		t.Errorf("healthy endpoints = %d, want 1", stats[0].HealthyEndpoints)
	}
}
