// Package client is the Go client for Orion's HTTP API. It backs orionctl and
// is the supported way for external tools to drive a cluster.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/apiserver"
	"github.com/sujalbistaa/orion/pkg/store"
)

// Client talks to an Orion API server.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

type Option func(*Client)

func WithToken(token string) Option { return func(c *Client) { c.token = token } }

func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.http.Timeout = d }
}

// WithHTTPClient replaces the transport, for tests and for callers that need
// their own TLS configuration.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

func New(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		http: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
				MaxIdleConnsPerHost: 4,
			},
		},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// APIError is a structured failure from the server. Callers can match on Code
// rather than parsing messages.
type APIError struct {
	Status  int
	Code    string
	Message string
	Fields  []v1.FieldError
	// LeaderAddress is set when the request reached a follower.
	LeaderAddress string
}

func (e *APIError) Error() string {
	if len(e.Fields) > 0 {
		parts := make([]string, 0, len(e.Fields))
		for _, f := range e.Fields {
			parts = append(parts, f.Field+": "+f.Detail)
		}
		return fmt.Sprintf("%s (%s)", e.Message, strings.Join(parts, "; "))
	}
	return e.Message
}

// NotFound reports whether an error is a 404, so callers can branch without
// string matching.
func NotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encoding request: %w", err)
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("contacting %s: %w", c.baseURL, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
	}()

	if resp.StatusCode >= 400 {
		return parseError(resp)
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decoding response from %s %s: %w", method, path, err)
	}
	return nil
}

func parseError(resp *http.Response) error {
	apiErr := &APIError{
		Status:        resp.StatusCode,
		Code:          "unknown",
		Message:       http.StatusText(resp.StatusCode),
		LeaderAddress: resp.Header.Get("X-Orion-Leader"),
	}
	var envelope struct {
		Error struct {
			Code    string          `json:"code"`
			Message string          `json:"message"`
			Fields  []v1.FieldError `json:"fields"`
		} `json:"error"`
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Error.Code != "" {
		apiErr.Code = envelope.Error.Code
		apiErr.Message = envelope.Error.Message
		apiErr.Fields = envelope.Error.Fields
	} else if len(body) > 0 {
		apiErr.Message = strings.TrimSpace(string(body))
	}
	return apiErr
}

type listEnvelope[T any] struct {
	Items    []T    `json:"items"`
	Revision uint64 `json:"revision"`
	Total    int    `json:"total"`
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

func (c *Client) Cluster(ctx context.Context) (*apiserver.ClusterResponse, error) {
	var out apiserver.ClusterResponse
	return &out, c.do(ctx, http.MethodGet, "/api/v1/cluster", nil, &out)
}

func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/healthz", nil, nil)
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

func (c *Client) ListNodes(ctx context.Context) ([]*v1.Node, error) {
	var out listEnvelope[*v1.Node]
	return out.Items, c.do(ctx, http.MethodGet, "/api/v1/nodes", nil, &out)
}

func (c *Client) GetNode(ctx context.Context, name string) (*apiserver.NodeDetail, error) {
	var out apiserver.NodeDetail
	return &out, c.do(ctx, http.MethodGet, "/api/v1/nodes/"+url.PathEscape(name), nil, &out)
}

func (c *Client) CordonNode(ctx context.Context, name string, cordon bool) error {
	action := "cordon"
	if !cordon {
		action = "uncordon"
	}
	return c.do(ctx, http.MethodPost, "/api/v1/nodes/"+url.PathEscape(name)+"/"+action, nil, nil)
}

func (c *Client) DrainNode(ctx context.Context, name string, force bool) error {
	path := "/api/v1/nodes/" + url.PathEscape(name) + "/drain"
	if force {
		path += "?force=true"
	}
	return c.do(ctx, http.MethodPost, path, nil, nil)
}

func (c *Client) DeleteNode(ctx context.Context, name string, force bool) error {
	path := "/api/v1/nodes/" + url.PathEscape(name)
	if force {
		path += "?force=true"
	}
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

// WorkloadFilter narrows a workload listing server-side, so a large cluster
// does not ship its entire workload set to render one node's page.
type WorkloadFilter struct {
	Node       string
	Deployment string
	Phase      string
}

func (c *Client) ListWorkloads(ctx context.Context, f WorkloadFilter) ([]*v1.Workload, error) {
	q := url.Values{}
	if f.Node != "" {
		q.Set("node", f.Node)
	}
	if f.Deployment != "" {
		q.Set("deployment", f.Deployment)
	}
	if f.Phase != "" {
		q.Set("phase", f.Phase)
	}
	path := "/api/v1/workloads"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	var out listEnvelope[*v1.Workload]
	return out.Items, c.do(ctx, http.MethodGet, path, nil, &out)
}

func (c *Client) GetWorkload(ctx context.Context, name string) (*apiserver.WorkloadDetail, error) {
	var out apiserver.WorkloadDetail
	return &out, c.do(ctx, http.MethodGet, "/api/v1/workloads/"+url.PathEscape(name), nil, &out)
}

func (c *Client) CreateWorkload(ctx context.Context, w *v1.Workload) (*v1.Workload, error) {
	var out v1.Workload
	return &out, c.do(ctx, http.MethodPost, "/api/v1/workloads", w, &out)
}

func (c *Client) DeleteWorkload(ctx context.Context, name string, force bool) error {
	path := "/api/v1/workloads/" + url.PathEscape(name)
	if force {
		path += "?force=true"
	}
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

// WorkloadLogs streams a workload's container output. The caller closes it.
func (c *Client) WorkloadLogs(ctx context.Context, name string, tail int, follow bool) (io.ReadCloser, error) {
	q := url.Values{}
	q.Set("tail", strconv.Itoa(tail))
	if follow {
		q.Set("follow", "true")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/api/v1/workloads/"+url.PathEscape(name)+"/logs?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	// Following a log has no natural end, so it must not inherit the client's
	// request timeout.
	httpClient := c.http
	if follow {
		clone := *c.http
		clone.Timeout = 0
		httpClient = &clone
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		return nil, parseError(resp)
	}
	return resp.Body, nil
}

// ---------------------------------------------------------------------------
// Deployments
// ---------------------------------------------------------------------------

func (c *Client) ListDeployments(ctx context.Context) ([]*v1.Deployment, error) {
	var out listEnvelope[*v1.Deployment]
	return out.Items, c.do(ctx, http.MethodGet, "/api/v1/deployments", nil, &out)
}

func (c *Client) GetDeployment(ctx context.Context, name string) (*apiserver.DeploymentDetail, error) {
	var out apiserver.DeploymentDetail
	return &out, c.do(ctx, http.MethodGet, "/api/v1/deployments/"+url.PathEscape(name), nil, &out)
}

func (c *Client) CreateDeployment(ctx context.Context, d *v1.Deployment) (*v1.Deployment, error) {
	var out v1.Deployment
	return &out, c.do(ctx, http.MethodPost, "/api/v1/deployments", d, &out)
}

func (c *Client) UpdateDeployment(ctx context.Context, d *v1.Deployment) (*v1.Deployment, error) {
	var out v1.Deployment
	return &out, c.do(ctx, http.MethodPut, "/api/v1/deployments/"+url.PathEscape(d.Name), d, &out)
}

func (c *Client) ScaleDeployment(ctx context.Context, name string, replicas int32) error {
	return c.do(ctx, http.MethodPost, "/api/v1/deployments/"+url.PathEscape(name)+"/scale",
		apiserver.ScaleRequest{Replicas: replicas}, nil)
}

func (c *Client) RollbackDeployment(ctx context.Context, name string, revision int64) (*v1.Deployment, error) {
	var out v1.Deployment
	return &out, c.do(ctx, http.MethodPost, "/api/v1/deployments/"+url.PathEscape(name)+"/rollback",
		apiserver.RollbackRequest{Revision: revision}, &out)
}

func (c *Client) DeleteDeployment(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/api/v1/deployments/"+url.PathEscape(name), nil, nil)
}

// ---------------------------------------------------------------------------
// Services
// ---------------------------------------------------------------------------

func (c *Client) ListServices(ctx context.Context) ([]*v1.Service, error) {
	var out listEnvelope[*v1.Service]
	return out.Items, c.do(ctx, http.MethodGet, "/api/v1/services", nil, &out)
}

func (c *Client) CreateService(ctx context.Context, s *v1.Service) (*v1.Service, error) {
	var out v1.Service
	return &out, c.do(ctx, http.MethodPost, "/api/v1/services", s, &out)
}

func (c *Client) DeleteService(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/api/v1/services/"+url.PathEscape(name), nil, nil)
}

// ---------------------------------------------------------------------------
// Events and faults
// ---------------------------------------------------------------------------

func (c *Client) ListEvents(ctx context.Context, q store.EventQuery) ([]v1.Event, error) {
	values := url.Values{}
	if q.Kind != "" {
		values.Set("kind", q.Kind)
	}
	if q.Name != "" {
		values.Set("name", q.Name)
	}
	if q.Severity != "" {
		values.Set("severity", string(q.Severity))
	}
	if q.Since > 0 {
		values.Set("since", strconv.FormatUint(q.Since, 10))
	}
	if q.Limit > 0 {
		values.Set("limit", strconv.Itoa(q.Limit))
	}
	path := "/api/v1/events"
	if len(values) > 0 {
		path += "?" + values.Encode()
	}
	var out listEnvelope[v1.Event]
	return out.Items, c.do(ctx, http.MethodGet, path, nil, &out)
}

func (c *Client) ListExperiments(ctx context.Context) ([]apiserver.ExperimentDescriptor, error) {
	var out listEnvelope[apiserver.ExperimentDescriptor]
	return out.Items, c.do(ctx, http.MethodGet, "/api/v1/faults/experiments", nil, &out)
}

func (c *Client) ListRuns(ctx context.Context) ([]apiserver.ExperimentRun, error) {
	var out listEnvelope[apiserver.ExperimentRun]
	return out.Items, c.do(ctx, http.MethodGet, "/api/v1/faults/runs", nil, &out)
}

func (c *Client) StartExperiment(ctx context.Context, req apiserver.ExperimentRequest) (*apiserver.ExperimentRun, error) {
	var out apiserver.ExperimentRun
	return &out, c.do(ctx, http.MethodPost, "/api/v1/faults/runs", req, &out)
}

func (c *Client) GetRun(ctx context.Context, id string) (*apiserver.ExperimentRun, error) {
	var out apiserver.ExperimentRun
	return &out, c.do(ctx, http.MethodGet, "/api/v1/faults/runs/"+url.PathEscape(id), nil, &out)
}

func (c *Client) AbortExperiment(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/api/v1/faults/runs/"+url.PathEscape(id)+"/abort", nil, nil)
}
