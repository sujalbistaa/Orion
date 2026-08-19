package controller

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/controlplane"
	"github.com/sujalbistaa/orion/pkg/store"
)

// EndpointController keeps each service's endpoint list in step with the
// workloads that match its selector.
//
// An endpoint is only marked ready when its workload is Running *and* its
// health check is passing. A workload whose health is Unknown does not serve:
// endpoints are proven healthy, never assumed, because sending traffic to a
// container that has started but not finished initializing is one of the most
// common causes of deploy-time errors.
type EndpointController struct {
	cp  *controlplane.ControlPlane
	log *slog.Logger

	Interval time.Duration
}

func NewEndpointController(cp *controlplane.ControlPlane, log *slog.Logger) *EndpointController {
	return &EndpointController{
		cp:       cp,
		log:      log.With("controller", "endpoints"),
		Interval: 2 * time.Second,
	}
}

func (c *EndpointController) Name() string                  { return "endpoints" }
func (c *EndpointController) ResyncInterval() time.Duration { return c.Interval }

func (c *EndpointController) Reconcile(ctx context.Context) error {
	st := c.cp.Store()

	nodes := map[string]*v1.Node{}
	for _, n := range st.Nodes() {
		nodes[n.Name] = n
	}

	for _, svc := range st.Services() {
		if err := ctx.Err(); err != nil {
			return err
		}
		endpoints := c.buildEndpoints(svc, st.WorkloadsMatching(svc.Spec.Selector), nodes)

		_, err := c.cp.Apply(ctx, store.Command{
			Kind:      store.CmdUpdateServiceEndpoint,
			Name:      svc.Name,
			Endpoints: endpoints,
		})
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return nil
}

func (c *EndpointController) buildEndpoints(svc *v1.Service, workloads []*v1.Workload, nodes map[string]*v1.Node) []v1.Endpoint {
	out := make([]v1.Endpoint, 0, len(workloads))

	for _, w := range workloads {
		if w.Status.NodeName == "" || w.DeletedAt != nil {
			continue
		}
		// Terminated workloads are not endpoints even briefly; a terminating
		// one is dropped immediately so traffic drains before the container
		// stops.
		if !w.Status.Phase.Active() {
			continue
		}
		node, ok := nodes[w.Status.NodeName]
		if !ok || node.Status.Phase == v1.NodeUnreachable {
			// A workload on an unreachable node cannot be verified as serving.
			continue
		}
		port, ok := resolvePort(w, svc.Spec.TargetPort)
		if !ok {
			// The container has not published its port yet (still starting).
			continue
		}
		host, err := hostOf(node.Spec.Address)
		if err != nil {
			c.log.Warn("node address is not host:port; skipping endpoint",
				"node", node.Name, "address", node.Spec.Address, "err", err)
			continue
		}

		out = append(out, v1.Endpoint{
			WorkloadName: w.Name,
			WorkloadUID:  w.UID,
			NodeName:     node.Name,
			Address:      host,
			Port:         port,
			Health:       w.Status.Health,
			Ready:        w.Ready(),
		})
	}
	return out
}

// resolvePort finds the host port a service should route to. The agent
// publishes container ports on the node and records the mapping, because two
// replicas of the same workload can land on one node and cannot share a port.
func resolvePort(w *v1.Workload, targetPort int32) (int32, bool) {
	if hostPort, ok := w.Status.HostPorts[targetPort]; ok && hostPort > 0 {
		return hostPort, true
	}
	// A statically declared host port is usable as soon as the workload is
	// running, without waiting for the agent to report the mapping.
	for _, p := range w.Spec.Ports {
		if p.Container == targetPort && p.Host > 0 {
			return p.Host, true
		}
	}
	return 0, false
}

func hostOf(address string) (string, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		// Tolerate a bare host with no port, which is a reasonable thing for an
		// operator to have configured.
		if address != "" && !containsColon(address) {
			return address, nil
		}
		return "", err
	}
	return host, nil
}

func containsColon(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return true
		}
	}
	return false
}
