package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/sujalbistaa/orion/pkg/apiserver"
	"github.com/sujalbistaa/orion/pkg/store"
)

// agentLogFetcher retrieves container logs by proxying to the agent on the node
// running the workload.
//
// Logs deliberately do not travel through the replicated log or the heartbeat
// stream. They are high-volume, they are only wanted when someone asks, and
// routing them through consensus would mean a single `logs -f` on a chatty
// container could dominate the cluster's write path. Fetching on demand keeps
// the cost proportional to the request.
type agentLogFetcher struct {
	store  *store.Store
	client *http.Client
	// clusterKey authenticates the control plane to the agent, so an arbitrary
	// caller on the network cannot read container output.
	clusterKey string
}

func newAgentLogFetcher(st *store.Store, clusterKey string) *agentLogFetcher {
	return &agentLogFetcher{
		store:      st,
		clusterKey: clusterKey,
		client: &http.Client{
			// No overall timeout: following a log is unbounded by design. The
			// dial and response-header timeouts bound the parts that can hang.
			Transport: &http.Transport{
				DialContext:           (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
				ResponseHeaderTimeout: 10 * time.Second,
				MaxIdleConnsPerHost:   2,
			},
		},
	}
}

func (f *agentLogFetcher) Logs(ctx context.Context, node, workload string, tail int, follow bool) (apiserver.LogStream, error) {
	n, ok := f.store.Node(node)
	if !ok {
		return nil, fmt.Errorf("node %s is not registered", node)
	}
	if n.Spec.Address == "" {
		return nil, fmt.Errorf("node %s did not report an address to fetch logs from", node)
	}

	q := url.Values{}
	q.Set("workload", workload)
	q.Set("tail", strconv.Itoa(tail))
	if follow {
		q.Set("follow", "true")
	}
	endpoint := "http://" + n.Spec.Address + "/logs?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if f.clusterKey != "" {
		req.Header.Set("X-Orion-Cluster-Key", f.clusterKey)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("agent on %s is unreachable: %w", node, err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("agent on %s returned %d: %s", node, resp.StatusCode, string(body))
	}
	return resp.Body, nil
}

var _ apiserver.LogFetcher = (*agentLogFetcher)(nil)
