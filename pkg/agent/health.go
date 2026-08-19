package agent

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	orionv1 "github.com/sujalbistaa/orion/pkg/proto/orionv1"
)

// prober runs a workload's health check on its own goroutine.
//
// Probes run out of band rather than inside the sync loop for one reason: a
// probe with a three-second timeout would otherwise stall every other
// workload's reconciliation on the node. The prober writes health state that
// the sync loop reads; it never touches the container.
type prober struct {
	workload *managedWorkload
	check    *orionv1.HealthCheck
	// target is the address to probe, resolved from the published host port.
	target string

	interval         time.Duration
	timeout          time.Duration
	failures         int32
	successes        int32
	failureThreshold int32
	successThreshold int32

	client *http.Client

	stopOnce sync.Once
	stopC    chan struct{}
	doneC    chan struct{}
}

// ensureProbe starts or updates the health probe for a running workload.
func (a *Agent) ensureProbe(w *managedWorkload, spec *orionv1.AssignedWorkload) {
	check := spec.GetHealthCheck()

	// No check, or a process check: liveness is "the container is running",
	// which the reconciler already observes. Starting a goroutine to
	// re-discover that would be waste.
	if check == nil || check.GetKind() == "" || check.GetKind() == string(v1.HealthCheckProcess) {
		w.mu.Lock()
		if w.health != v1.HealthHealthy {
			w.health = v1.HealthHealthy
		}
		w.mu.Unlock()
		return
	}

	w.mu.Lock()
	existing := w.probe
	hostPorts := w.hostPorts
	w.mu.Unlock()

	target, ok := probeTarget(check, hostPorts)
	if !ok {
		// The port is not published yet; the next tick will retry. Health stays
		// Unknown, which correctly keeps the workload out of service endpoints.
		return
	}
	if existing != nil && existing.target == target {
		return
	}
	if existing != nil {
		existing.stop()
	}

	p := newProber(w, check, target)
	w.mu.Lock()
	w.probe = p
	w.mu.Unlock()
	go p.run()
}

// probeTarget resolves where to send the probe. Probes go to the published host
// port on loopback: the agent shares a network namespace with the host, not
// with the container, so the container's own IP is not reachable from here.
func probeTarget(check *orionv1.HealthCheck, hostPorts map[int32]int32) (string, bool) {
	want := check.GetPort()
	if want == 0 {
		return "", false
	}
	hostPort, ok := hostPorts[want]
	if !ok || hostPort == 0 {
		return "", false
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(int(hostPort))), true
}

func newProber(w *managedWorkload, check *orionv1.HealthCheck, target string) *prober {
	interval := time.Duration(check.GetIntervalMs()) * time.Millisecond
	if interval <= 0 {
		interval = v1.DefaultHealthInterval
	}
	timeout := time.Duration(check.GetTimeoutMs()) * time.Millisecond
	if timeout <= 0 || timeout > interval {
		timeout = min(v1.DefaultHealthTimeout, interval)
	}
	failureThreshold := check.GetFailureThreshold()
	if failureThreshold <= 0 {
		failureThreshold = v1.DefaultHealthFailureThreshold
	}
	successThreshold := check.GetSuccessThreshold()
	if successThreshold <= 0 {
		successThreshold = v1.DefaultHealthSuccessThreshold
	}

	return &prober{
		workload:         w,
		check:            check,
		target:           target,
		interval:         interval,
		timeout:          timeout,
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		stopC:            make(chan struct{}),
		doneC:            make(chan struct{}),
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DisableKeepAlives: true, // a probe must test a fresh connection
				DialContext:       (&net.Dialer{Timeout: timeout}).DialContext,
			},
			// A redirect is not a health signal; treat the first response as
			// the answer.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (p *prober) run() {
	defer close(p.doneC)

	// Wait out the initial delay before the first probe, so a container that
	// legitimately takes time to bind its port is not marked unhealthy for
	// starting up.
	if d := time.Duration(p.check.GetInitialDelayMs()) * time.Millisecond; d > 0 {
		select {
		case <-time.After(d):
		case <-p.stopC:
			return
		}
	}

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	p.probeOnce()
	for {
		select {
		case <-p.stopC:
			return
		case <-ticker.C:
			p.probeOnce()
		}
	}
}

func (p *prober) probeOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	err := p.execute(ctx)
	w := p.workload

	w.mu.Lock()
	defer w.mu.Unlock()

	if err == nil {
		p.failures = 0
		p.successes++
		if p.successes >= p.successThreshold && w.health != v1.HealthHealthy {
			w.health = v1.HealthHealthy
			w.message = ""
			w.log.Info("workload is healthy", "target", p.target)
		}
		return
	}

	p.successes = 0
	p.failures++
	if p.failures >= p.failureThreshold && w.health != v1.HealthUnhealthy {
		w.health = v1.HealthUnhealthy
		w.message = fmt.Sprintf("health check failed %d times: %v", p.failures, err)
		w.log.Warn("workload is unhealthy", "target", p.target, "failures", p.failures, "err", err)
	}
}

func (p *prober) execute(ctx context.Context) error {
	switch p.check.GetKind() {
	case string(v1.HealthCheckTCP):
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", p.target)
		if err != nil {
			return err
		}
		return conn.Close()

	case string(v1.HealthCheckHTTP):
		path := p.check.GetPath()
		if path == "" {
			path = "/"
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+p.target+path, nil)
		if err != nil {
			return err
		}
		resp, err := p.client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		// 2xx and 3xx are healthy; anything else, including 401 and 500, is not.
		if resp.StatusCode < 200 || resp.StatusCode >= 400 {
			return fmt.Errorf("http status %d", resp.StatusCode)
		}
		return nil

	default:
		return fmt.Errorf("unsupported health check kind %q", p.check.GetKind())
	}
}

func (p *prober) stop() {
	p.stopOnce.Do(func() { close(p.stopC) })
	<-p.doneC
}
