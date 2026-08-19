// Package e2e drives the real orion-server and orion-agent binaries against a
// real Docker daemon and a real client, exercising the same path an operator
// would: build, start, deploy, watch it converge, tear down. Nothing here is
// wired in-process; if this test passes, the system works, not just its
// components in isolation.
package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/apiserver"
	"github.com/sujalbistaa/orion/pkg/client"
)

const (
	apiAddr   = "127.0.0.1:18070"
	grpcAddr  = "127.0.0.1:18071"
	raftAddr  = "127.0.0.1:18072"
	agentAddr = "127.0.0.1:18090"
)

func requireDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping: needs a Docker daemon (-short)")
	}
	cmd := exec.Command("docker", "info")
	if err := cmd.Run(); err != nil {
		t.Skipf("skipping: docker daemon not reachable: %v", err)
	}
}

// buildBinaries compiles orion-server and orion-agent into a temp dir so the
// test exercises exactly what a release build produces, not an in-process
// shortcut.
func buildBinaries(t *testing.T) (server, agent string) {
	t.Helper()
	dir := t.TempDir()
	root := repoRoot(t)

	server = filepath.Join(dir, "orion-server")
	agent = filepath.Join(dir, "orion-agent")

	build := func(out, pkg string) {
		cmd := exec.Command("go", "build", "-o", out, pkg)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("building %s: %v\n%s", pkg, err, out)
		}
	}
	build(server, "./cmd/orion-server")
	build(agent, "./cmd/orion-agent")
	return server, agent
}

func repoRoot(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("locating module root: %v", err)
	}
	return string(trimNewline(out))
}

func trimNewline(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// process wraps a running binary and makes sure it actually stops, rather
// than leaking a background orion-server/orion-agent into the next test run.
type process struct {
	cmd  *exec.Cmd
	name string
}

func startProcess(t *testing.T, name string, args ...string) *process {
	t.Helper()
	cmd := exec.Command(name, args...)
	logPath := filepath.Join(t.TempDir(), filepath.Base(name)+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("creating log file for %s: %v", name, err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s: %v", name, err)
	}
	p := &process{cmd: cmd, name: filepath.Base(name)}
	t.Cleanup(func() {
		p.stop(t)
		logFile.Close()
		if t.Failed() {
			if data, err := os.ReadFile(logPath); err == nil {
				t.Logf("--- %s log ---\n%s", p.name, data)
			}
		}
	})
	return p
}

func (p *process) stop(t *testing.T) {
	t.Helper()
	if p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	}
}

func waitForPort(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("nothing listening on %s after %s", addr, timeout)
}

func waitFor(t *testing.T, timeout time.Duration, what string, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := check()
		if err != nil {
			lastErr = err
		} else if ok {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s (last error: %v)", what, lastErr)
}

// TestClusterLifecycle brings up a real single-node control plane and one
// real agent, then walks a full deployment through it: create, converge to
// Available with real running containers, confirm the service actually
// reports healthy endpoints, then delete and confirm the workloads are torn
// down. This is the scenario the README's quickstart describes; this test is
// what keeps that description honest.
func TestClusterLifecycle(t *testing.T) {
	requireDocker(t)

	serverBin, agentBin := buildBinaries(t)
	dataDir := t.TempDir()

	startProcess(t, serverBin,
		"-api-addr", apiAddr,
		"-grpc-addr", grpcAddr,
		"-raft-addr", raftAddr,
		"-data-dir", dataDir,
		"-log-format", "text",
	)
	waitForPort(t, apiAddr, 10*time.Second)

	startProcess(t, agentBin,
		"-node-name", "e2e-node-1",
		"-server", grpcAddr,
		"-local-addr", agentAddr,
		"-log-format", "text",
	)

	c := client.New("http://" + apiAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	waitFor(t, 15*time.Second, "the agent to register as Ready", func() (bool, error) {
		nodes, err := c.ListNodes(ctx)
		if err != nil {
			return false, err
		}
		for _, n := range nodes {
			if n.Name == "e2e-node-1" && n.Status.Phase == v1.NodeReady {
				return true, nil
			}
		}
		return false, nil
	})

	deployment := &v1.Deployment{
		ObjectMeta: v1.ObjectMeta{Name: "e2e-web", Labels: map[string]string{"app": "e2e-web"}},
		Spec: v1.DeploymentSpec{
			Replicas: 2,
			Template: v1.WorkloadSpec{
				Image:         "nginx:1.27-alpine",
				Ports:         []v1.Port{{Container: 80}},
				RestartPolicy: v1.RestartAlways,
				Resources: v1.ResourceSpec{
					Request: v1.Resources{CPU: 100, Memory: 64 << 20},
					Limit:   v1.Resources{CPU: 250, Memory: 128 << 20},
				},
			},
			Strategy: v1.Strategy{Kind: v1.StrategyRolling, MaxUnavailable: 1, MaxSurge: 1},
		},
	}
	created, err := c.CreateDeployment(ctx, deployment)
	if err != nil {
		t.Fatalf("creating deployment: %v", err)
	}
	if created.Name != "e2e-web" {
		t.Fatalf("created deployment name = %q, want e2e-web", created.Name)
	}

	svc := &v1.Service{
		ObjectMeta: v1.ObjectMeta{Name: "e2e-web-svc"},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{"app": "e2e-web"},
			Port:       18080,
			TargetPort: 80,
			Strategy:   v1.LBRoundRobin,
		},
	}
	if _, err := c.CreateService(ctx, svc); err != nil {
		t.Fatalf("creating service: %v", err)
	}

	waitFor(t, 30*time.Second, "the deployment to become Available 2/2", func() (bool, error) {
		d, err := c.GetDeployment(ctx, "e2e-web")
		if err != nil {
			return false, err
		}
		return d.Deployment.Status.Phase == v1.DeploymentAvailable &&
			d.Deployment.Status.AvailableReplicas == 2, nil
	})

	waitFor(t, 15*time.Second, "the service to report 2 healthy endpoints", func() (bool, error) {
		svcs, err := c.ListServices(ctx)
		if err != nil {
			return false, err
		}
		for _, s := range svcs {
			if s.Name == "e2e-web-svc" {
				return s.Status.HealthyEndpoints == 2, nil
			}
		}
		return false, errors.New("service not found")
	})

	workloads, err := c.ListWorkloads(ctx, client.WorkloadFilter{})
	if err != nil {
		t.Fatalf("listing workloads: %v", err)
	}
	running := 0
	for _, w := range workloads {
		if w.OwnerRef != nil && w.OwnerRef.Name == "e2e-web" && w.Status.Phase == v1.WorkloadRunning {
			running++
			if w.Status.NodeName != "e2e-node-1" {
				t.Errorf("workload %s bound to unexpected node %q", w.Name, w.Status.NodeName)
			}
		}
	}
	if running != 2 {
		t.Fatalf("expected 2 running replicas, found %d", running)
	}

	if err := c.DeleteService(ctx, "e2e-web-svc"); err != nil {
		t.Fatalf("deleting service: %v", err)
	}
	if err := c.DeleteDeployment(ctx, "e2e-web"); err != nil {
		t.Fatalf("deleting deployment: %v", err)
	}

	waitFor(t, 20*time.Second, "all replicas to be torn down", func() (bool, error) {
		workloads, err := c.ListWorkloads(ctx, client.WorkloadFilter{})
		if err != nil {
			return false, err
		}
		for _, w := range workloads {
			if w.OwnerRef != nil && w.OwnerRef.Name == "e2e-web" && w.Status.Phase.Active() {
				return false, nil
			}
		}
		return true, nil
	})
}

// TestNodeFailureRecovery runs the real node-failure fault experiment
// against a two-agent cluster and asserts it reports Succeeded — the same
// scenario documented in FAILURES.md, run for real rather than asserted in
// prose.
func TestNodeFailureRecovery(t *testing.T) {
	requireDocker(t)

	serverBin, agentBin := buildBinaries(t)
	dataDir := t.TempDir()

	const (
		api    = "127.0.0.1:18170"
		grpc   = "127.0.0.1:18171"
		raftP  = "127.0.0.1:18172"
		agentA = "127.0.0.1:18190"
		agentB = "127.0.0.1:18191"
	)

	startProcess(t, serverBin,
		"-api-addr", api, "-grpc-addr", grpc, "-raft-addr", raftP,
		"-data-dir", dataDir, "-log-format", "text", "-enable-fault-injection",
	)
	waitForPort(t, api, 10*time.Second)

	startProcess(t, agentBin, "-node-name", "e2e-a", "-server", grpc, "-local-addr", agentA, "-log-format", "text")
	startProcess(t, agentBin, "-node-name", "e2e-b", "-server", grpc, "-local-addr", agentB, "-log-format", "text")

	c := client.New("http://" + api)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	waitFor(t, 15*time.Second, "both agents to register as Ready", func() (bool, error) {
		nodes, err := c.ListNodes(ctx)
		if err != nil {
			return false, err
		}
		ready := 0
		for _, n := range nodes {
			if n.Status.Phase == v1.NodeReady {
				ready++
			}
		}
		return ready == 2, nil
	})

	deployment := &v1.Deployment{
		ObjectMeta: v1.ObjectMeta{Name: "e2e-web", Labels: map[string]string{"app": "e2e-web"}},
		Spec: v1.DeploymentSpec{
			Replicas: 3,
			Template: v1.WorkloadSpec{
				Image:         "nginx:1.27-alpine",
				RestartPolicy: v1.RestartAlways,
				Resources: v1.ResourceSpec{
					Request: v1.Resources{CPU: 100, Memory: 64 << 20},
				},
			},
			Strategy: v1.Strategy{Kind: v1.StrategyRolling, MaxUnavailable: 1, MaxSurge: 1},
		},
	}
	if _, err := c.CreateDeployment(ctx, deployment); err != nil {
		t.Fatalf("creating deployment: %v", err)
	}
	waitFor(t, 30*time.Second, "the deployment to become Available 3/3", func() (bool, error) {
		d, err := c.GetDeployment(ctx, "e2e-web")
		if err != nil {
			return false, err
		}
		return d.Deployment.Status.Phase == v1.DeploymentAvailable &&
			d.Deployment.Status.AvailableReplicas == 3, nil
	})

	run, err := c.StartExperiment(ctx, apiserver.ExperimentRequest{
		Kind:   apiserver.ExperimentNodeFailure,
		Params: map[string]string{"node": "e2e-b"},
	})
	if err != nil {
		t.Fatalf("starting node-failure experiment: %v", err)
	}

	waitFor(t, 60*time.Second, "the node-failure experiment to finish", func() (bool, error) {
		r, err := c.GetRun(ctx, run.ID)
		if err != nil {
			return false, err
		}
		switch r.State {
		case apiserver.RunSucceeded:
			return true, nil
		case apiserver.RunFailed, apiserver.RunAborted:
			return false, fmt.Errorf("experiment ended in state %s", r.State)
		default:
			return false, nil
		}
	})

	_ = c.DeleteDeployment(ctx, "e2e-web")
}
