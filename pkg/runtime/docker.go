package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// Docker drives the Docker Engine API.
type Docker struct {
	cli *client.Client
	// nodeName labels every container so a shared engine (a developer laptop
	// running several simulated nodes) keeps each node's containers separate.
	nodeName string
}

var _ Runtime = (*Docker)(nil)

// NewDocker connects to the engine. API version is negotiated rather than
// pinned, so the agent works against whatever engine the operator is running
// instead of failing on a version mismatch.
func NewDocker(nodeName string) (*Docker, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("runtime: connecting to docker: %w", err)
	}
	return &Docker{cli: cli, nodeName: nodeName}, nil
}

func (d *Docker) Name() string { return "docker" }

func (d *Docker) Close() error { return d.cli.Close() }

func (d *Docker) Ping(ctx context.Context) error {
	if _, err := d.cli.Ping(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return nil
}

func (d *Docker) Info(ctx context.Context) (NodeInfo, error) {
	info, err := d.cli.Info(ctx)
	if err != nil {
		return NodeInfo{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	v, err := d.cli.ServerVersion(ctx)
	if err != nil {
		return NodeInfo{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return NodeInfo{
		RuntimeName:    "docker",
		RuntimeVersion: v.Version,
		OS:             info.OSType,
		Arch:           info.Architecture,
		KernelVersion:  info.KernelVersion,
		Hostname:       info.Name,
		CPUs:           info.NCPU,
		MemoryBytes:    info.MemTotal,
	}, nil
}

func (d *Docker) PullImage(ctx context.Context, ref string) error {
	// Check locally first. Pulling on every workload start would make the
	// agent's reconciliation depend on registry availability, so a registry
	// outage would stop the cluster from recovering from unrelated failures.
	if _, err := d.cli.ImageInspect(ctx, ref); err == nil {
		return nil
	}

	rc, err := d.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		if errdefs.IsNotFound(err) {
			return fmt.Errorf("%w: %s", ErrImageNotFound, ref)
		}
		return fmt.Errorf("runtime: pulling %s: %w", ref, err)
	}
	defer rc.Close()

	// The pull only completes when its progress stream is drained; abandoning
	// the reader leaves a half-finished layer download.
	dec := json.NewDecoder(rc)
	for {
		var msg struct {
			Error string `json:"error"`
		}
		if err := dec.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("runtime: reading pull progress for %s: %w", ref, err)
		}
		if msg.Error != "" {
			return fmt.Errorf("runtime: pulling %s: %s", ref, msg.Error)
		}
	}
	return nil
}

func (d *Docker) Create(ctx context.Context, spec ContainerSpec) (string, error) {
	exposed, bindings, err := portConfig(spec.Ports)
	if err != nil {
		return "", err
	}

	labels := map[string]string{LabelManagedBy: ManagedByValue, LabelNode: d.nodeName}
	for k, v := range spec.Labels {
		labels[k] = v
	}

	cfg := &container.Config{
		Image:        spec.Image,
		Env:          spec.Env,
		Labels:       labels,
		ExposedPorts: exposed,
	}
	// An empty Entrypoint/Cmd must stay nil so the image's own defaults apply;
	// sending an empty slice overrides them with nothing and the container
	// exits immediately.
	if len(spec.Command) > 0 {
		cfg.Entrypoint = spec.Command
	}
	if len(spec.Args) > 0 {
		cfg.Cmd = spec.Args
	}

	host := &container.HostConfig{
		PortBindings: bindings,
		Resources: container.Resources{
			// NanoCPUs is a hard ceiling on CPU time. Orion also uses shares so
			// that a container under contention gets a fraction proportional to
			// its request rather than an equal share.
			NanoCPUs:   spec.CPUMillis * 1_000_000,
			CPUShares:  spec.CPUMillis,
			Memory:     spec.MemoryBytes,
			MemorySwap: spec.MemoryBytes, // disable swap: swapping hides OOM
		},
		// Orion restarts containers itself, through the workload state machine.
		// An engine-level restart policy would race with the control plane and
		// resurrect containers the cluster believes are gone.
		RestartPolicy:  container.RestartPolicy{Name: container.RestartPolicyDisabled},
		ReadonlyRootfs: spec.ReadOnlyRootFS,
		// Log rotation is mandatory: an unbounded json-file driver is one of
		// the most reliable ways to fill a node's disk.
		LogConfig: container.LogConfig{
			Type:   "json-file",
			Config: map[string]string{"max-size": "10m", "max-file": "3"},
		},
	}
	if spec.NoNewPrivileges {
		host.SecurityOpt = append(host.SecurityOpt, "no-new-privileges:true")
	}
	// Drop the capabilities that Docker's default set still grants but that a
	// server workload never needs. Dropping ALL is tempting and wrong: it
	// removes CHOWN and SETUID, which nginx, postgres and most other images
	// use to drop privilege at startup, so every one of them crash-loops. What
	// is left here is the set with real lateral-movement value:
	//
	//   NET_RAW      packet crafting and ARP spoofing from inside a container
	//   MKNOD        creating device nodes
	//   AUDIT_WRITE  writing to the host audit log
	//
	// A workload that genuinely needs one of these is a deliberate exception,
	// not a default.
	host.CapDrop = []string{"NET_RAW", "MKNOD", "AUDIT_WRITE"}
	if len(spec.TmpfsPaths) > 0 {
		host.Tmpfs = map[string]string{}
		for _, p := range spec.TmpfsPaths {
			host.Tmpfs[p] = "rw,noexec,nosuid,size=64m"
		}
	}

	resp, err := d.cli.ContainerCreate(ctx, cfg, host, nil, nil, spec.Name)
	if err != nil {
		switch {
		case errdefs.IsConflict(err):
			return "", fmt.Errorf("%w: %s", ErrAlreadyExists, spec.Name)
		case errdefs.IsNotFound(err):
			return "", fmt.Errorf("%w: %s", ErrImageNotFound, spec.Image)
		}
		return "", fmt.Errorf("runtime: creating container %s: %w", spec.Name, err)
	}
	return resp.ID, nil
}

func (d *Docker) Start(ctx context.Context, id string) error {
	err := d.cli.ContainerStart(ctx, id, container.StartOptions{})
	if err == nil {
		return nil
	}
	if errdefs.IsNotFound(err) {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	// Starting an already-running container is what a duplicated instruction
	// looks like; it is success, not failure.
	if strings.Contains(strings.ToLower(err.Error()), "already started") {
		return nil
	}
	return fmt.Errorf("runtime: starting container %s: %w", id, err)
}

func (d *Docker) Stop(ctx context.Context, id string, timeout time.Duration) error {
	secs := int(timeout.Seconds())
	err := d.cli.ContainerStop(ctx, id, container.StopOptions{Timeout: &secs})
	if err == nil {
		return nil
	}
	if errdefs.IsNotFound(err) {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return fmt.Errorf("runtime: stopping container %s: %w", id, err)
}

func (d *Docker) Restart(ctx context.Context, id string, timeout time.Duration) error {
	secs := int(timeout.Seconds())
	err := d.cli.ContainerRestart(ctx, id, container.StopOptions{Timeout: &secs})
	if err == nil {
		return nil
	}
	if errdefs.IsNotFound(err) {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return fmt.Errorf("runtime: restarting container %s: %w", id, err)
}

func (d *Docker) Remove(ctx context.Context, id string, force bool) error {
	err := d.cli.ContainerRemove(ctx, id, container.RemoveOptions{
		Force: force,
		// Anonymous volumes belong to the container's lifetime; leaving them
		// behind leaks disk on every workload replacement.
		RemoveVolumes: true,
	})
	if err == nil {
		return nil
	}
	if errdefs.IsNotFound(err) {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return fmt.Errorf("runtime: removing container %s: %w", id, err)
}

func (d *Docker) Inspect(ctx context.Context, id string) (ContainerStatus, error) {
	c, err := d.cli.ContainerInspect(ctx, id)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return ContainerStatus{}, fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		return ContainerStatus{}, fmt.Errorf("runtime: inspecting container %s: %w", id, err)
	}

	st := ContainerStatus{
		ID:           c.ID,
		Name:         strings.TrimPrefix(c.Name, "/"),
		Labels:       c.Config.Labels,
		RestartCount: int32(c.RestartCount),
		Ports:        map[int32]int32{},
	}
	if c.Config != nil {
		st.Image = c.Config.Image
	}
	if c.State != nil {
		st.State = mapState(c.State.Status)
		st.ExitCode = int32(c.State.ExitCode)
		st.OOMKilled = c.State.OOMKilled
		st.Error = c.State.Error
		st.StartedAt = parseDockerTime(c.State.StartedAt)
		st.FinishedAt = parseDockerTime(c.State.FinishedAt)
		if c.State.Health != nil {
			st.Health = c.State.Health.Status
		}
	}
	if c.NetworkSettings != nil {
		for port, bindings := range c.NetworkSettings.Ports {
			if len(bindings) == 0 {
				continue
			}
			hostPort, err := strconv.Atoi(bindings[0].HostPort)
			if err != nil {
				continue
			}
			st.Ports[int32(port.Int())] = int32(hostPort)
		}
	}
	return st, nil
}

func (d *Docker) List(ctx context.Context, labels map[string]string) ([]ContainerStatus, error) {
	f := filters.NewArgs()
	f.Add("label", LabelManagedBy+"="+ManagedByValue)
	for k, v := range labels {
		f.Add("label", k+"="+v)
	}

	list, err := d.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}

	out := make([]ContainerStatus, 0, len(list))
	for _, c := range list {
		// Inspect gives exit codes and port bindings that the list summary
		// omits, and the agent needs both to decide what to do.
		st, err := d.Inspect(ctx, c.ID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue // removed between listing and inspecting
			}
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

func (d *Docker) Stats(ctx context.Context, id string) (Stats, error) {
	resp, err := d.cli.ContainerStatsOneShot(ctx, id)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return Stats{}, fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		return Stats{}, fmt.Errorf("runtime: sampling stats for %s: %w", id, err)
	}
	defer resp.Body.Close()

	var s container.StatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return Stats{}, fmt.Errorf("runtime: decoding stats for %s: %w", id, err)
	}
	return Stats{
		CPUMillis:   cpuMillis(s),
		MemoryBytes: workingSet(s),
		MemoryLimit: int64(s.MemoryStats.Limit),
		SampledAt:   time.Now(),
	}, nil
}

// cpuMillis converts Docker's cumulative CPU counters into a rate.
//
// A one-shot stats call returns the current and previous samples, so the delta
// between them is a real rate rather than an average since container start —
// which is what makes a container that was busy an hour ago look busy now.
func cpuMillis(s container.StatsResponse) int64 {
	cpuDelta := float64(s.CPUStats.CPUUsage.TotalUsage) - float64(s.PreCPUStats.CPUUsage.TotalUsage)
	systemDelta := float64(s.CPUStats.SystemUsage) - float64(s.PreCPUStats.SystemUsage)
	if systemDelta <= 0 || cpuDelta < 0 {
		return 0
	}
	cpus := float64(s.CPUStats.OnlineCPUs)
	if cpus == 0 {
		cpus = float64(len(s.CPUStats.CPUUsage.PercpuUsage))
	}
	if cpus == 0 {
		cpus = 1
	}
	return int64((cpuDelta / systemDelta) * cpus * 1000)
}

// workingSet is usage minus reclaimable page cache.
//
// Raw memory usage includes the page cache, which the kernel grows to fill
// whatever is available. Reporting it would make every container look like it
// is at its memory limit and would make the cluster's memory graphs useless.
func workingSet(s container.StatsResponse) int64 {
	usage := int64(s.MemoryStats.Usage)
	if v, ok := s.MemoryStats.Stats["inactive_file"]; ok {
		usage -= int64(v)
	} else if v, ok := s.MemoryStats.Stats["total_inactive_file"]; ok {
		usage -= int64(v)
	} else if s.MemoryStats.Stats["cache"] > 0 {
		usage -= int64(s.MemoryStats.Stats["cache"])
	}
	if usage < 0 {
		return 0
	}
	return usage
}

func (d *Docker) Logs(ctx context.Context, id string, opts LogOptions) (io.ReadCloser, error) {
	o := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     opts.Follow,
		Timestamps: opts.Timestamps,
	}
	if opts.Tail > 0 {
		o.Tail = strconv.Itoa(opts.Tail)
	}
	if !opts.Since.IsZero() {
		o.Since = opts.Since.Format(time.RFC3339Nano)
	}

	rc, err := d.cli.ContainerLogs(ctx, id, o)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		return nil, fmt.Errorf("runtime: reading logs for %s: %w", id, err)
	}
	// Containers without a TTY produce a multiplexed stream with an 8-byte
	// header per frame. Callers want plain text, so demultiplex here rather
	// than leaking the framing to every consumer.
	return &demuxReader{src: rc}, nil
}

func portConfig(ports []PortMapping) (nat.PortSet, nat.PortMap, error) {
	exposed := nat.PortSet{}
	bindings := nat.PortMap{}
	for _, p := range ports {
		proto := p.Protocol
		if proto == "" {
			proto = "tcp"
		}
		port, err := nat.NewPort(proto, strconv.Itoa(int(p.ContainerPort)))
		if err != nil {
			return nil, nil, fmt.Errorf("runtime: invalid port %d/%s: %w", p.ContainerPort, proto, err)
		}
		exposed[port] = struct{}{}
		hostPort := ""
		if p.HostPort > 0 {
			hostPort = strconv.Itoa(int(p.HostPort))
		}
		// An empty HostPort asks the engine to pick a free one, which is what
		// lets two replicas of the same workload run on one node.
		bindings[port] = []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: hostPort}}
	}
	return exposed, bindings, nil
}

func mapState(s string) ContainerState {
	switch s {
	case "created":
		return StateCreated
	case "running":
		return StateRunning
	case "paused":
		return StatePaused
	case "restarting":
		return StateRestarting
	case "exited":
		return StateExited
	case "dead":
		return StateDead
	case "removing":
		return StateRemoving
	default:
		return StateUnknown
	}
}

func parseDockerTime(s string) time.Time {
	if s == "" || strings.HasPrefix(s, "0001-01-01") {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// demuxReader strips Docker's stream multiplexing framing, merging stdout and
// stderr into one plain-text stream.
type demuxReader struct {
	src io.ReadCloser
	buf []byte
	// remaining is how many payload bytes are left in the current frame.
	remaining int
	// plain latches on when the stream turns out not to be multiplexed, which
	// happens for containers created with a TTY.
	plain   bool
	checked bool
}

func (r *demuxReader) Read(p []byte) (int, error) {
	if r.plain {
		return r.src.Read(p)
	}
	for r.remaining == 0 {
		var header [8]byte
		if _, err := io.ReadFull(r.src, header[:]); err != nil {
			return 0, err
		}
		if !r.checked {
			r.checked = true
			// Stream type is 0-2 and the three bytes after it are always zero
			// in a valid frame header. Anything else means the stream is raw.
			if header[0] > 2 || header[1] != 0 || header[2] != 0 || header[3] != 0 {
				r.plain = true
				r.buf = append(r.buf, header[:]...)
				n := copy(p, r.buf)
				r.buf = r.buf[n:]
				return n, nil
			}
		}
		size := int(header[4])<<24 | int(header[5])<<16 | int(header[6])<<8 | int(header[7])
		if size < 0 {
			return 0, fmt.Errorf("runtime: invalid log frame size %d", size)
		}
		r.remaining = size
	}

	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.src.Read(p)
	r.remaining -= n
	return n, err
}

func (r *demuxReader) Close() error { return r.src.Close() }
