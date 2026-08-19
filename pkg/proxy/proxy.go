// Package proxy implements Orion's service load balancer.
//
// It is a layer-4 TCP proxy rather than an HTTP one. Orion has no business
// knowing whether a workload speaks HTTP, Postgres or Redis, and an L7 proxy
// would mean parsing, buffering and potentially corrupting protocols it does
// not understand. Balancing connections is enough to make a service a stable
// address in front of a changing set of backends, which is the actual problem.
//
// Endpoint health comes from the control plane, not from the proxy: a workload
// is a candidate only when the cluster has established that it is Running and
// its health check passes. The proxy adds one thing on top — a failing dial is
// retried against another endpoint, because an endpoint can die between the
// last health check and this connection.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/store"
)

// Metrics is implemented by the telemetry package; nil is allowed.
type Metrics interface {
	ProxyRequest(service, result string, d time.Duration)
	ProxyRetry(service string)
	ProxyEndpoints(service string, healthy int)
}

// Options configures the proxy.
type Options struct {
	Store  *store.Store
	Logger *slog.Logger

	// BindAddress is the interface to listen on. Defaults to all interfaces;
	// set to 127.0.0.1 on a developer machine so services are not exposed to
	// the local network by accident.
	BindAddress string

	// DialTimeout bounds connecting to a backend. It must be short: it is paid
	// on every failed endpoint before the retry.
	DialTimeout time.Duration
	// MaxRetries is how many other endpoints to try after a dial failure.
	MaxRetries int
	// IdleTimeout closes a connection with no traffic in either direction.
	// Zero disables it, which is correct for long-lived protocols.
	IdleTimeout time.Duration
	// MaxConnections per service bounds resource use from a single service.
	MaxConnections int

	Metrics Metrics
}

// Proxy runs one listener per service and keeps them in step with the cluster.
type Proxy struct {
	opts Options
	log  *slog.Logger

	mu        sync.Mutex
	listeners map[string]*serviceListener

	wg     sync.WaitGroup
	stopC  chan struct{}
	closed atomic.Bool
}

func New(opts Options) (*Proxy, error) {
	if opts.Store == nil {
		return nil, errors.New("proxy: Store is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 2 * time.Second
	}
	if opts.MaxRetries <= 0 {
		opts.MaxRetries = 2
	}
	if opts.MaxConnections <= 0 {
		opts.MaxConnections = 4096
	}
	return &Proxy{
		opts:      opts,
		log:       opts.Logger.With("component", "proxy"),
		listeners: map[string]*serviceListener{},
		stopC:     make(chan struct{}),
	}, nil
}

// Run keeps listeners in step with the services in the cluster until ctx ends.
func (p *Proxy) Run(ctx context.Context) error {
	watcher := p.opts.Store.Watch()
	defer watcher.Stop()

	p.sync()

	// The watch drives updates; the ticker is the backstop that guarantees
	// convergence if a notification is ever missed, matching how every other
	// Orion component reconciles.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.Close()
			return nil
		case <-p.stopC:
			return nil
		case <-ticker.C:
			p.sync()
		case change, ok := <-watcher.Changes():
			if !ok {
				// The watcher was dropped; a full sync recovers.
				p.sync()
				return p.Run(ctx)
			}
			if change.Kind == "Service" {
				p.sync()
			}
		}
	}
}

// sync creates, updates and removes listeners to match the cluster's services.
func (p *Proxy) sync() {
	services := p.opts.Store.Services()
	want := make(map[string]*v1.Service, len(services))
	for _, s := range services {
		want[s.Name] = s
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for name, l := range p.listeners {
		svc, ok := want[name]
		if !ok {
			l.close()
			delete(p.listeners, name)
			p.log.Info("stopped listener for a deleted service", "service", name)
			continue
		}
		if svc.Spec.Port != l.port {
			// The port changed; the old listener must go before the new one can
			// bind.
			l.close()
			delete(p.listeners, name)
			continue
		}
		l.update(svc)
		if p.opts.Metrics != nil {
			p.opts.Metrics.ProxyEndpoints(name, l.healthyCount())
		}
	}

	for name, svc := range want {
		if _, ok := p.listeners[name]; ok {
			continue
		}
		l, err := p.startListener(svc)
		if err != nil {
			// A port conflict is an operator problem, not a crash. Log it once
			// per sync and keep the rest of the proxy working.
			p.log.Error("could not listen for service",
				"service", name, "port", svc.Spec.Port, "err", err)
			continue
		}
		p.listeners[name] = l
	}
}

func (p *Proxy) startListener(svc *v1.Service) (*serviceListener, error) {
	addr := net.JoinHostPort(p.opts.BindAddress, fmt.Sprintf("%d", svc.Spec.Port))
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return nil, err
	}

	l := &serviceListener{
		proxy:    p,
		name:     svc.Name,
		port:     svc.Spec.Port,
		listener: ln,
		log:      p.log.With("service", svc.Name, "port", svc.Spec.Port),
		sem:      make(chan struct{}, p.opts.MaxConnections),
		done:     make(chan struct{}),
	}
	l.update(svc)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		l.serve()
	}()
	p.log.Info("listening for service", "service", svc.Name, "addr", ln.Addr().String(),
		"strategy", svc.Spec.Strategy, "endpoints", l.healthyCount())
	return l, nil
}

// Close stops every listener and waits for in-flight connections to finish.
func (p *Proxy) Close() {
	if p.closed.Swap(true) {
		return
	}
	close(p.stopC)

	p.mu.Lock()
	for name, l := range p.listeners {
		l.close()
		delete(p.listeners, name)
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// Stats reports per-service proxy state for the console.
type Stats struct {
	Service           string `json:"service"`
	Port              int32  `json:"port"`
	HealthyEndpoints  int    `json:"healthyEndpoints"`
	ActiveConnections int64  `json:"activeConnections"`
	TotalConnections  int64  `json:"totalConnections"`
	FailedConnections int64  `json:"failedConnections"`
	Retries           int64  `json:"retries"`
}

func (p *Proxy) Stats() []Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Stats, 0, len(p.listeners))
	for _, l := range p.listeners {
		out = append(out, l.stats())
	}
	return out
}

// ---------------------------------------------------------------------------
// Per-service listener
// ---------------------------------------------------------------------------

type serviceListener struct {
	proxy    *Proxy
	name     string
	port     int32
	listener net.Listener
	log      *slog.Logger

	// sem bounds concurrent connections so one service cannot exhaust the
	// process's file descriptors.
	sem  chan struct{}
	done chan struct{}
	wg   sync.WaitGroup

	mu       sync.RWMutex
	strategy v1.LoadBalanceStrategy
	// endpoints holds only endpoints the cluster considers ready.
	endpoints []v1.Endpoint
	// next drives round-robin.
	next uint64
	// inflight counts connections per endpoint target, for least-connections.
	inflight map[string]int64

	activeConns atomic.Int64
	totalConns  atomic.Int64
	failedConns atomic.Int64
	retries     atomic.Int64
	closeOnce   sync.Once
}

func (l *serviceListener) update(svc *v1.Service) {
	ready := make([]v1.Endpoint, 0, len(svc.Status.Endpoints))
	for _, e := range svc.Status.Endpoints {
		// Only proven-healthy endpoints receive traffic. An endpoint whose
		// health is Unknown is one the cluster cannot vouch for.
		if e.Ready && e.Health.Serving() && e.Address != "" && e.Port > 0 {
			ready = append(ready, e)
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.strategy = svc.Spec.Strategy
	l.endpoints = ready
	if l.inflight == nil {
		l.inflight = map[string]int64{}
	}
	// Drop counters for endpoints that no longer exist so the map does not grow
	// forever across rollouts.
	live := make(map[string]bool, len(ready))
	for _, e := range ready {
		live[e.Target()] = true
	}
	for target := range l.inflight {
		if !live[target] {
			delete(l.inflight, target)
		}
	}
}

func (l *serviceListener) healthyCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.endpoints)
}

func (l *serviceListener) serve() {
	defer close(l.done)
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				l.wg.Wait()
				return
			}
			// A transient accept error (fd exhaustion) must not spin the CPU.
			l.log.Warn("accept failed", "err", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		select {
		case l.sem <- struct{}{}:
		default:
			// At the connection cap. Closing immediately is the honest
			// response: queueing would just move the failure later and make it
			// look like a latency problem.
			l.log.Warn("connection limit reached; rejecting", "limit", cap(l.sem))
			conn.Close()
			l.failedConns.Add(1)
			continue
		}

		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			defer func() { <-l.sem }()
			l.handle(conn)
		}()
	}
}

func (l *serviceListener) handle(client net.Conn) {
	start := time.Now()
	l.totalConns.Add(1)
	l.activeConns.Add(1)
	defer l.activeConns.Add(-1)
	defer client.Close()

	backend, endpoint, err := l.dialBackend()
	if err != nil {
		l.failedConns.Add(1)
		if l.proxy.opts.Metrics != nil {
			l.proxy.opts.Metrics.ProxyRequest(l.name, "no_backend", time.Since(start))
		}
		l.log.Warn("no backend available", "err", err, "client", client.RemoteAddr().String())
		return
	}
	defer backend.Close()

	target := endpoint.Target()
	l.trackInflight(target, 1)
	defer l.trackInflight(target, -1)

	l.pipe(client, backend)

	if l.proxy.opts.Metrics != nil {
		l.proxy.opts.Metrics.ProxyRequest(l.name, "ok", time.Since(start))
	}
}

// dialBackend picks an endpoint and connects, retrying against others on
// failure. An endpoint can die between the last health check and this
// connection; retrying turns that into a slower request rather than an error
// the client sees.
func (l *serviceListener) dialBackend() (net.Conn, v1.Endpoint, error) {
	tried := map[string]bool{}

	for attempt := 0; attempt <= l.proxy.opts.MaxRetries; attempt++ {
		endpoint, ok := l.pick(tried)
		if !ok {
			if attempt == 0 {
				return nil, v1.Endpoint{}, errors.New("the service has no healthy endpoints")
			}
			return nil, v1.Endpoint{}, fmt.Errorf("every healthy endpoint refused the connection (%d tried)", len(tried))
		}
		tried[endpoint.Target()] = true

		conn, err := net.DialTimeout("tcp", endpoint.Target(), l.proxy.opts.DialTimeout)
		if err == nil {
			if attempt > 0 && l.proxy.opts.Metrics != nil {
				l.proxy.opts.Metrics.ProxyRetry(l.name)
			}
			return conn, endpoint, nil
		}
		l.retries.Add(1)
		l.log.Debug("backend refused connection, trying another",
			"endpoint", endpoint.Target(), "workload", endpoint.WorkloadName, "err", err)
	}
	return nil, v1.Endpoint{}, errors.New("exhausted retries connecting to backends")
}

// pick selects an endpoint according to the service's strategy, skipping any
// already tried in this connection attempt.
func (l *serviceListener) pick(exclude map[string]bool) (v1.Endpoint, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	candidates := l.endpoints[:0:0]
	for _, e := range l.endpoints {
		if !exclude[e.Target()] {
			candidates = append(candidates, e)
		}
	}
	if len(candidates) == 0 {
		return v1.Endpoint{}, false
	}

	switch l.strategy {
	case v1.LBLeastConnections:
		// Favour the endpoint with the fewest in-flight connections. This
		// matters when request durations vary widely: round-robin would keep
		// feeding an endpoint that is already saturated by long requests.
		best := candidates[0]
		bestLoad := l.inflight[best.Target()]
		for _, e := range candidates[1:] {
			if load := l.inflight[e.Target()]; load < bestLoad {
				best, bestLoad = e, load
			}
		}
		return best, true

	default: // round robin
		idx := l.next % uint64(len(candidates))
		l.next++
		return candidates[idx], true
	}
}

func (l *serviceListener) trackInflight(target string, delta int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inflight[target] += delta
	if l.inflight[target] <= 0 {
		delete(l.inflight, target)
	}
}

// pipe copies bytes in both directions until either side closes.
//
// The half-close handling is deliberate: many protocols (and plain HTTP with
// Connection: close) signal end-of-request by shutting down the write side.
// Tearing the whole connection down on the first EOF would truncate the
// response.
func (l *serviceListener) pipe(client, backend net.Conn) {
	idle := l.proxy.opts.IdleTimeout
	var wg sync.WaitGroup
	wg.Add(2)

	copyDir := func(dst, src net.Conn) {
		defer wg.Done()
		if idle > 0 {
			_ = src.SetReadDeadline(time.Now().Add(idle))
		}
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}
				if idle > 0 {
					_ = src.SetReadDeadline(time.Now().Add(idle))
				}
			}
			if err != nil {
				break
			}
		}
		// Half-close: tell the peer this direction is finished but let the
		// other direction drain.
		if tcp, ok := dst.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		} else {
			_ = dst.Close()
		}
	}

	go copyDir(backend, client)
	go copyDir(client, backend)
	wg.Wait()
}

func (l *serviceListener) stats() Stats {
	return Stats{
		Service:           l.name,
		Port:              l.port,
		HealthyEndpoints:  l.healthyCount(),
		ActiveConnections: l.activeConns.Load(),
		TotalConnections:  l.totalConns.Load(),
		FailedConnections: l.failedConns.Load(),
		Retries:           l.retries.Load(),
	}
}

func (l *serviceListener) close() {
	l.closeOnce.Do(func() {
		_ = l.listener.Close()
		<-l.done
	})
}
