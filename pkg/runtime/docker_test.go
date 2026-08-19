package runtime

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// These are conformance tests against a real Docker daemon. They are the only
// place Orion verifies its assumptions about the engine — everything else uses
// the Fake — so they exercise the specific behaviours the agent depends on:
// port publication, exit codes, restart counts, label filtering and log
// demultiplexing.
//
// They skip when Docker is unavailable so `go test ./...` works on a machine
// without it, and are always run in CI.

// testImage is tiny and has a shell, so a test can control exactly when the
// container exits.
const testImage = "busybox:1.36"

func dockerOrSkip(t *testing.T) *Docker {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Docker conformance test in short mode")
	}
	if os.Getenv("ORION_SKIP_DOCKER_TESTS") != "" {
		t.Skip("ORION_SKIP_DOCKER_TESTS is set")
	}
	d, err := NewDocker("test-node")
	if err != nil {
		t.Skipf("docker client unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.Ping(ctx); err != nil {
		d.Close()
		t.Skipf("docker daemon unreachable: %v", err)
	}
	t.Cleanup(func() { d.Close() })

	pullCtx, pullCancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer pullCancel()
	if err := d.PullImage(pullCtx, testImage); err != nil {
		t.Skipf("cannot pull %s: %v", testImage, err)
	}
	return d
}

// cleanup removes a container even if the test failed partway through, so a
// failing run does not leave containers behind on a developer's machine.
func cleanup(t *testing.T, d *Docker, id string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := d.Remove(ctx, id, true); err != nil && !errors.Is(err, ErrNotFound) {
			t.Logf("cleanup: removing %s: %v", id, err)
		}
	})
}

func TestDockerContainerLifecycle(t *testing.T) {
	d := dockerOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	name := ContainerName("lifecycle", "uid-"+randomSuffix())
	id, err := d.Create(ctx, ContainerSpec{
		Name:    name,
		Image:   testImage,
		Command: []string{"sh", "-c"},
		Args:    []string{"sleep 300"},
		Labels: map[string]string{
			LabelWorkload:    "lifecycle",
			LabelWorkloadUID: "uid-test",
		},
		CPUMillis:       500,
		MemoryBytes:     64 << 20,
		NoNewPrivileges: true,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	cleanup(t, d, id)

	st, err := d.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("inspect after create: %v", err)
	}
	if st.State != StateCreated {
		t.Errorf("state after create = %s, want created", st.State)
	}
	if st.Labels[LabelManagedBy] != ManagedByValue {
		t.Errorf("ownership label missing: %v", st.Labels)
	}

	if err := d.Start(ctx, id); err != nil {
		t.Fatalf("start: %v", err)
	}
	st, _ = d.Inspect(ctx, id)
	if st.State != StateRunning {
		t.Fatalf("state after start = %s, want running", st.State)
	}
	if st.StartedAt.IsZero() {
		t.Error("running container has no start time")
	}

	// Starting an already-running container is what a duplicated instruction
	// looks like, and the agent relies on it being a no-op.
	if err := d.Start(ctx, id); err != nil {
		t.Errorf("starting an already-running container should be a no-op, got %v", err)
	}

	stats, err := d.Stats(ctx, id)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.MemoryBytes <= 0 {
		t.Errorf("stats reported %d bytes of memory for a running container", stats.MemoryBytes)
	}
	if stats.MemoryLimit != 64<<20 {
		t.Errorf("memory limit = %d, want the 64Mi we set", stats.MemoryLimit)
	}

	before, _ := d.Inspect(ctx, id)
	if err := d.Restart(ctx, id, 5*time.Second); err != nil {
		t.Fatalf("restart: %v", err)
	}
	after, _ := d.Inspect(ctx, id)
	if after.RestartCount <= before.RestartCount && !after.StartedAt.After(before.StartedAt) {
		t.Error("restart did not produce a new start time or restart count")
	}

	if err := d.Stop(ctx, id, 5*time.Second); err != nil {
		t.Fatalf("stop: %v", err)
	}
	st, _ = d.Inspect(ctx, id)
	if !st.State.Terminal() {
		t.Errorf("state after stop = %s, want a terminal state", st.State)
	}
	// Stopping twice must be safe: the agent retries on a lost response.
	if err := d.Stop(ctx, id, 5*time.Second); err != nil {
		t.Errorf("stopping an already-stopped container should be a no-op, got %v", err)
	}

	if err := d.Remove(ctx, id, false); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := d.Inspect(ctx, id); !errors.Is(err, ErrNotFound) {
		t.Errorf("inspect after remove = %v, want ErrNotFound", err)
	}
	// A second removal must report ErrNotFound rather than a generic failure,
	// because the agent treats that as "already converged".
	if err := d.Remove(ctx, id, false); !errors.Is(err, ErrNotFound) {
		t.Errorf("removing a missing container = %v, want ErrNotFound", err)
	}
}

// The agent maps container exit codes onto workload phases, so the exit code
// must survive intact — including 137 for an OOM or SIGKILL.
func TestDockerReportsExitCode(t *testing.T) {
	d := dockerOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	name := ContainerName("exitcode", randomSuffix())
	id, err := d.Create(ctx, ContainerSpec{
		Name:        name,
		Image:       testImage,
		Command:     []string{"sh", "-c"},
		Args:        []string{"exit 42"},
		CPUMillis:   100,
		MemoryBytes: 16 << 20,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	cleanup(t, d, id)

	if err := d.Start(ctx, id); err != nil {
		t.Fatalf("start: %v", err)
	}

	var st ContainerStatus
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		st, err = d.Inspect(ctx, id)
		if err != nil {
			t.Fatalf("inspect: %v", err)
		}
		if st.State.Terminal() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !st.State.Terminal() {
		t.Fatalf("container did not exit within 30s, state is %s", st.State)
	}
	if st.ExitCode != 42 {
		t.Errorf("exit code = %d, want 42", st.ExitCode)
	}
	if st.FinishedAt.IsZero() {
		t.Error("exited container has no finish time")
	}
}

// Two replicas of one workload can land on the same node, so the engine must
// allocate distinct host ports when none is requested — and Orion must be able
// to read back which one it got.
func TestDockerPublishesEphemeralHostPorts(t *testing.T) {
	d := dockerOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var ports []int32
	for i := 0; i < 2; i++ {
		name := ContainerName("ports", randomSuffix())
		id, err := d.Create(ctx, ContainerSpec{
			Name:        name,
			Image:       testImage,
			Command:     []string{"sh", "-c"},
			Args:        []string{"sleep 60"},
			Ports:       []PortMapping{{ContainerPort: 8080, Protocol: "tcp"}},
			CPUMillis:   100,
			MemoryBytes: 16 << 20,
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		cleanup(t, d, id)
		if err := d.Start(ctx, id); err != nil {
			t.Fatalf("start: %v", err)
		}
		st, err := d.Inspect(ctx, id)
		if err != nil {
			t.Fatalf("inspect: %v", err)
		}
		host, ok := st.Ports[8080]
		if !ok || host == 0 {
			t.Fatalf("container %d published no host port: %v", i, st.Ports)
		}
		ports = append(ports, host)
	}
	if ports[0] == ports[1] {
		t.Fatalf("both replicas were given the same host port %d", ports[0])
	}
}

// A restarted agent rediscovers what it owns by asking the engine, so label
// filtering has to be exact.
func TestDockerListFiltersByLabel(t *testing.T) {
	d := dockerOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	uid := randomSuffix()
	mine, err := d.Create(ctx, ContainerSpec{
		Name:        ContainerName("mine", uid),
		Image:       testImage,
		Command:     []string{"sh", "-c"},
		Args:        []string{"sleep 60"},
		Labels:      map[string]string{LabelWorkload: "mine", LabelWorkloadUID: uid},
		CPUMillis:   100,
		MemoryBytes: 16 << 20,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	cleanup(t, d, mine)

	other, err := d.Create(ctx, ContainerSpec{
		Name:        ContainerName("other", randomSuffix()),
		Image:       testImage,
		Command:     []string{"sh", "-c"},
		Args:        []string{"sleep 60"},
		Labels:      map[string]string{LabelWorkload: "other"},
		CPUMillis:   100,
		MemoryBytes: 16 << 20,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	cleanup(t, d, other)

	found, err := d.List(ctx, map[string]string{LabelWorkloadUID: uid})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(found) != 1 || found[0].ID != mine {
		t.Fatalf("label filter returned %d containers, want exactly the one we labelled", len(found))
	}
	// Stopped containers must still be listed: that is how an agent discovers
	// a workload that died while it was down.
	if err := d.Stop(ctx, mine, 5*time.Second); err != nil {
		t.Fatalf("stop: %v", err)
	}
	found, err = d.List(ctx, map[string]string{LabelWorkloadUID: uid})
	if err != nil {
		t.Fatalf("list after stop: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("a stopped container was not listed; a restarted agent would miss it")
	}
}

// Logs arrive multiplexed with an 8-byte frame header per chunk. Orion strips
// it, and a bug here shows up as binary noise in the console.
func TestDockerLogsAreDemultiplexed(t *testing.T) {
	d := dockerOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	id, err := d.Create(ctx, ContainerSpec{
		Name:        ContainerName("logs", randomSuffix()),
		Image:       testImage,
		Command:     []string{"sh", "-c"},
		Args:        []string{"echo orion-stdout-line; echo orion-stderr-line 1>&2"},
		CPUMillis:   100,
		MemoryBytes: 16 << 20,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	cleanup(t, d, id)
	if err := d.Start(ctx, id); err != nil {
		t.Fatalf("start: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := d.Inspect(ctx, id)
		if st.State.Terminal() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	rc, err := d.Logs(ctx, id, LogOptions{Tail: 100})
	if err != nil {
		t.Fatalf("logs: %v", err)
	}
	defer rc.Close()
	out, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading logs: %v", err)
	}
	text := string(out)
	if !strings.Contains(text, "orion-stdout-line") {
		t.Errorf("stdout missing from logs: %q", text)
	}
	if !strings.Contains(text, "orion-stderr-line") {
		t.Errorf("stderr missing from logs: %q", text)
	}
	// The frame header's first byte is the stream type (1 or 2); if it leaked
	// through, the output would start with a control byte.
	for _, line := range strings.Split(text, "\n") {
		if line == "" {
			continue
		}
		if line[0] < 0x20 && line[0] != '\t' {
			t.Fatalf("log line still carries stream framing: %q", line)
		}
	}
}

func TestDockerRejectsMissingImage(t *testing.T) {
	d := dockerOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	err := d.PullImage(ctx, "orion-nonexistent-image-for-tests:v0")
	if err == nil {
		t.Fatal("expected pulling a nonexistent image to fail")
	}
}

func TestDockerInfoReportsMachineCapacity(t *testing.T) {
	d := dockerOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	info, err := d.Info(ctx)
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if info.CPUs <= 0 || info.MemoryBytes <= 0 {
		t.Fatalf("engine reported implausible capacity: %+v", info)
	}
	if info.RuntimeVersion == "" {
		t.Error("engine version is empty")
	}
}

func randomSuffix() string {
	return strings.ReplaceAll(time.Now().Format("150405.000000"), ".", "")
}
