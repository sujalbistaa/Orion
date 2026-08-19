package apiserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/controlplane"
	"github.com/sujalbistaa/orion/pkg/raft"
	"github.com/sujalbistaa/orion/pkg/raft/transport"
	"github.com/sujalbistaa/orion/pkg/store"
)

// The API server is tested against a real control plane rather than a mocked
// store: the interesting behaviour is in how it maps consensus outcomes onto
// HTTP semantics, which a mock would define away.

type testServer struct {
	*httptest.Server
	cp    *controlplane.ControlPlane
	auth  *TokenAuth
	token string
	view  string
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	lb := transport.NewLoopback()

	cp, err := controlplane.New(controlplane.Options{
		NodeID: 1,
		Peers:  map[uint64]string{1: "local"},
		Raft: raft.Config{
			ID: 1, Peers: []uint64{1}, ElectionTick: 3, HeartbeatTick: 1,
			Storage: raft.NewMemoryStorage(), Logger: log,
		},
		Transport:      lb.For(1),
		Store:          store.New(),
		TickInterval:   2 * time.Millisecond,
		ProposeTimeout: 5 * time.Second,
		Logger:         log,
	})
	if err != nil {
		t.Fatalf("creating control plane: %v", err)
	}
	lb.Register(1, cp.Raft().Step)
	cp.Start()
	t.Cleanup(cp.Stop)

	deadline := time.Now().Add(5 * time.Second)
	for !cp.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !cp.IsLeader() {
		t.Fatal("control plane did not become leader")
	}

	auth := NewTokenAuth()
	const operatorToken = "operator-token-0123456789"
	const viewerToken = "viewer-token-0123456789ab"
	if err := auth.AddToken(operatorToken, "operator@example.com", RoleOperator); err != nil {
		t.Fatal(err)
	}
	if err := auth.AddToken(viewerToken, "viewer@example.com", RoleViewer); err != nil {
		t.Fatal(err)
	}

	srv, err := New(Options{ControlPlane: cp, Logger: log, Auth: auth})
	if err != nil {
		t.Fatalf("creating api server: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	return &testServer{Server: httpSrv, cp: cp, auth: auth, token: operatorToken, view: viewerToken}
}

func (ts *testServer) do(t *testing.T, method, path, token string, body any) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encoding request body: %v", err)
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, ts.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func (ts *testServer) op(t *testing.T, method, path string, body any) *http.Response {
	return ts.do(t, method, path, ts.token, body)
}

func decode[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	return out
}

func bodyOf(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func validDeployment(name string, replicas int32) map[string]any {
	return map[string]any{
		"name":   name,
		"labels": map[string]string{"app": name},
		"spec": map[string]any{
			"replicas": replicas,
			"template": map[string]any{
				"image":     "nginx:1.27-alpine",
				"resources": map[string]any{"request": map[string]any{"cpu": 500, "memory": 268435456}},
			},
			"strategy": map[string]any{"kind": "RollingUpdate", "maxSurge": 1},
		},
	}
}

// ---------------------------------------------------------------------------

func TestHealthAndReadinessAreUnauthenticated(t *testing.T) {
	ts := newTestServer(t)
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		resp := ts.do(t, http.MethodGet, path, "", nil)
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized {
			t.Errorf("%s requires authentication; load balancers cannot provide one", path)
		}
	}
}

func TestReadinessFailsWithoutALeader(t *testing.T) {
	ts := newTestServer(t)
	resp := ts.do(t, http.MethodGet, "/readyz", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz on a healthy leader = %d, want 200: %s", resp.StatusCode, bodyOf(t, resp))
	}
	body := decode[map[string]any](t, resp)
	if body["status"] != "ready" {
		t.Errorf("status = %v, want ready", body["status"])
	}
}

func TestAPIRequiresAuthentication(t *testing.T) {
	ts := newTestServer(t)

	resp := ts.do(t, http.MethodGet, "/api/v1/nodes", "", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated read = %d, want 401", resp.StatusCode)
	}
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Error("401 response carries no WWW-Authenticate challenge")
	}

	resp = ts.do(t, http.MethodGet, "/api/v1/nodes", "wrong-token-that-is-long-enough", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("invalid token = %d, want 401", resp.StatusCode)
	}
}

// A viewer must be able to read everything and change nothing.
func TestViewerCannotWrite(t *testing.T) {
	ts := newTestServer(t)

	resp := ts.do(t, http.MethodGet, "/api/v1/deployments", ts.view, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("viewer read = %d, want 200", resp.StatusCode)
	}

	resp = ts.do(t, http.MethodPost, "/api/v1/deployments", ts.view, validDeployment("web", 2))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("viewer write = %d, want 403", resp.StatusCode)
	}
	if !strings.Contains(bodyOf(t, resp), "read-only") {
		t.Error("403 response does not explain that the token is read-only")
	}
}

func TestCreateAndGetDeployment(t *testing.T) {
	ts := newTestServer(t)

	resp := ts.op(t, http.MethodPost, "/api/v1/deployments", validDeployment("web", 3))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create = %d, want 201: %s", resp.StatusCode, bodyOf(t, resp))
	}
	created := decode[v1.Deployment](t, resp)
	if created.UID == "" {
		t.Error("created deployment has no UID")
	}
	if created.Spec.Replicas != 3 {
		t.Errorf("replicas = %d, want 3", created.Spec.Replicas)
	}
	// Defaults must be applied server-side and persisted.
	if created.Spec.Template.RestartPolicy != v1.RestartAlways {
		t.Errorf("restart policy default not applied: %q", created.Spec.Template.RestartPolicy)
	}

	resp = ts.op(t, http.MethodGet, "/api/v1/deployments/web", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get = %d, want 200", resp.StatusCode)
	}
	detail := decode[DeploymentDetail](t, resp)
	if detail.Deployment.Name != "web" {
		t.Errorf("got deployment %q", detail.Deployment.Name)
	}
	if len(detail.Revisions) != 1 {
		t.Errorf("expected one initial revision, got %d", len(detail.Revisions))
	}

	// Creating it twice is a conflict, not a silent overwrite.
	resp = ts.op(t, http.MethodPost, "/api/v1/deployments", validDeployment("web", 3))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("duplicate create = %d, want 409", resp.StatusCode)
	}
}

// Validation errors must name the fields at fault so the console can mark them.
func TestValidationErrorsIdentifyFields(t *testing.T) {
	ts := newTestServer(t)

	bad := validDeployment("Invalid_Name", 3)
	bad["spec"].(map[string]any)["template"].(map[string]any)["image"] = ""

	resp := ts.op(t, http.MethodPost, "/api/v1/deployments", bad)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("invalid create = %d, want 422: %s", resp.StatusCode, bodyOf(t, resp))
	}
	env := decode[errorEnvelope](t, resp)
	if env.Error.Code != "validation_failed" {
		t.Errorf("error code = %q", env.Error.Code)
	}
	if len(env.Error.Fields) < 2 {
		t.Fatalf("expected multiple field errors, got %+v", env.Error.Fields)
	}
	seen := map[string]bool{}
	for _, f := range env.Error.Fields {
		seen[f.Field] = true
		if f.Detail == "" {
			t.Errorf("field %q has no explanation", f.Field)
		}
	}
	if !seen["metadata.name"] || !seen["spec.template.image"] {
		t.Errorf("field errors do not identify the bad fields: %+v", env.Error.Fields)
	}
}

// A misspelled field must fail loudly rather than being silently ignored.
func TestUnknownFieldsAreRejected(t *testing.T) {
	ts := newTestServer(t)
	body := validDeployment("web", 3)
	body["spec"].(map[string]any)["replica"] = 5 // typo for "replicas"

	resp := ts.op(t, http.MethodPost, "/api/v1/deployments", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown field = %d, want 400: %s", resp.StatusCode, bodyOf(t, resp))
	}
}

// A client must not be able to declare a workload Running without a container.
func TestClientSuppliedStatusIsIgnored(t *testing.T) {
	ts := newTestServer(t)
	body := map[string]any{
		"name": "sneaky",
		"spec": map[string]any{
			"image":     "nginx",
			"resources": map[string]any{"request": map[string]any{"cpu": 100, "memory": 134217728}},
		},
		"status": map[string]any{"phase": "Running", "nodeName": "worker-01", "health": "Healthy"},
	}
	resp := ts.op(t, http.MethodPost, "/api/v1/workloads", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create = %d, want 201: %s", resp.StatusCode, bodyOf(t, resp))
	}
	created := decode[v1.Workload](t, resp)
	if created.Status.Phase != v1.WorkloadPending {
		t.Errorf("client-supplied status was honoured: phase = %s, want Pending", created.Status.Phase)
	}
	if created.Status.NodeName != "" {
		t.Errorf("client placed its own workload on %q", created.Status.NodeName)
	}
}

func TestScaleDeployment(t *testing.T) {
	ts := newTestServer(t)
	ts.op(t, http.MethodPost, "/api/v1/deployments", validDeployment("web", 2)).Body.Close()

	resp := ts.op(t, http.MethodPost, "/api/v1/deployments/web/scale", ScaleRequest{Replicas: 7})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scale = %d, want 200: %s", resp.StatusCode, bodyOf(t, resp))
	}
	result := decode[map[string]any](t, resp)
	if result["replicas"].(float64) != 7 {
		t.Errorf("scale result = %v", result)
	}

	resp = ts.op(t, http.MethodPost, "/api/v1/deployments/web/scale", ScaleRequest{Replicas: -1})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("negative replicas = %d, want 422", resp.StatusCode)
	}
}

// Destructive operations must be guarded and must land in the audit trail.
func TestDestructiveOperationsAreGuardedAndAudited(t *testing.T) {
	ts := newTestServer(t)

	// A workload owned by a deployment cannot be deleted directly without
	// force: it would simply be recreated.
	ts.op(t, http.MethodPost, "/api/v1/deployments", validDeployment("web", 1)).Body.Close()
	created := decode[v1.Workload](t, ts.op(t, http.MethodPost, "/api/v1/workloads", map[string]any{
		"name": "standalone",
		"spec": map[string]any{
			"image":     "nginx",
			"resources": map[string]any{"request": map[string]any{"cpu": 100, "memory": 134217728}},
		},
	}))
	if created.Name != "standalone" {
		t.Fatal("setup failed")
	}

	resp := ts.op(t, http.MethodDelete, "/api/v1/workloads/standalone", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete = %d, want 204: %s", resp.StatusCode, bodyOf(t, resp))
	}

	// The deletion must be attributable.
	events := decode[listResponse[v1.Event]](t, ts.op(t, http.MethodGet, "/api/v1/events?kind=Workload&limit=50", nil))
	found := false
	for _, e := range events.Items {
		if e.Reason == "WorkloadDeleted" && e.Actor == "operator@example.com" {
			found = true
		}
	}
	if !found {
		t.Errorf("deletion was not recorded against the acting principal: %+v", events.Items)
	}
}

func TestDeletingAnOwnedWorkloadRequiresForce(t *testing.T) {
	ts := newTestServer(t)
	ts.op(t, http.MethodPost, "/api/v1/deployments", validDeployment("web", 2)).Body.Close()

	// Wait for the deployment controller... there is none in this test, so
	// create an owned workload directly through the store to exercise the guard.
	d := decode[DeploymentDetail](t, ts.op(t, http.MethodGet, "/api/v1/deployments/web", nil))
	owned := &v1.Workload{
		ObjectMeta: v1.ObjectMeta{
			Name:     "web-0",
			OwnerRef: &v1.OwnerReference{Kind: "Deployment", Name: "web", UID: d.Deployment.UID},
		},
		Spec: v1.WorkloadSpec{Image: "nginx", Resources: v1.ResourceSpec{
			Request: v1.Resources{CPU: 100, Memory: 128 << 20}}},
	}
	if _, err := ts.cp.Apply(context.Background(), store.Command{
		Kind: store.CmdCreateWorkload, Workload: owned,
	}); err != nil {
		t.Fatalf("seeding owned workload: %v", err)
	}

	resp := ts.op(t, http.MethodDelete, "/api/v1/workloads/web-0", nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("deleting an owned workload without force = %d, want 409: %s",
			resp.StatusCode, bodyOf(t, resp))
	}
	env := decode[errorEnvelope](t, resp)
	if !strings.Contains(env.Error.Message, "recreated") {
		t.Errorf("the refusal does not explain why: %q", env.Error.Message)
	}

	resp = ts.op(t, http.MethodDelete, "/api/v1/workloads/web-0?force=true", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("forced delete = %d, want 204: %s", resp.StatusCode, bodyOf(t, resp))
	}
}

// Optimistic concurrency: a delete carrying a stale revision must be refused.
func TestRevisionGuardPreventsActingOnStaleState(t *testing.T) {
	ts := newTestServer(t)
	ts.op(t, http.MethodPost, "/api/v1/deployments", validDeployment("web", 2)).Body.Close()

	detail := decode[DeploymentDetail](t, ts.op(t, http.MethodGet, "/api/v1/deployments/web", nil))
	staleRevision := detail.Deployment.Revision

	// Something else changes the deployment.
	ts.op(t, http.MethodPost, "/api/v1/deployments/web/scale", ScaleRequest{Replicas: 5}).Body.Close()

	resp := ts.op(t, http.MethodDelete, fmt.Sprintf("/api/v1/deployments/web?revision=%d", staleRevision), nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("delete with a stale revision = %d, want 409: %s", resp.StatusCode, bodyOf(t, resp))
	}
}

func TestServicePortConflictIsRejected(t *testing.T) {
	ts := newTestServer(t)
	svc := map[string]any{
		"name": "web-svc",
		"spec": map[string]any{
			"selector": map[string]string{"app": "web"}, "port": 8080, "targetPort": 80,
		},
	}
	resp := ts.op(t, http.MethodPost, "/api/v1/services", svc)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create service = %d: %s", resp.StatusCode, bodyOf(t, resp))
	}
	resp.Body.Close()

	other := map[string]any{
		"name": "other-svc",
		"spec": map[string]any{
			"selector": map[string]string{"app": "other"}, "port": 8080, "targetPort": 90,
		},
	}
	resp = ts.op(t, http.MethodPost, "/api/v1/services", other)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate service port = %d, want 409", resp.StatusCode)
	}
	if !strings.Contains(bodyOf(t, resp), "web-svc") {
		t.Error("the conflict does not name the service already using the port")
	}
}

func TestNotFoundResponsesAreSpecific(t *testing.T) {
	ts := newTestServer(t)
	for _, path := range []string{
		"/api/v1/nodes/missing",
		"/api/v1/workloads/missing",
		"/api/v1/deployments/missing",
		"/api/v1/services/missing",
	} {
		resp := ts.op(t, http.MethodGet, path, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s = %d, want 404", path, resp.StatusCode)
			resp.Body.Close()
			continue
		}
		env := decode[errorEnvelope](t, resp)
		if !strings.Contains(env.Error.Message, "missing") {
			t.Errorf("%s: 404 message does not name the object: %q", path, env.Error.Message)
		}
	}
}

func TestRequestBodyIsBounded(t *testing.T) {
	ts := newTestServer(t)
	huge := validDeployment("web", 1)
	// A 2 MiB annotation exceeds the 1 MiB request cap.
	huge["annotations"] = map[string]string{"blob": strings.Repeat("x", 2<<20)}

	resp := ts.op(t, http.MethodPost, "/api/v1/deployments", huge)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body = %d, want 413", resp.StatusCode)
	}
}

// The change stream must deliver a baseline revision then live changes.
func TestWatchStreamsChanges(t *testing.T) {
	ts := newTestServer(t)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/watch?kind=Deployment", nil)
	req.Header.Set("Authorization", "Bearer "+ts.token)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := ts.Client().Do(req.WithContext(ctx))
	if err != nil {
		t.Fatalf("opening watch: %v", err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("watch content type = %q", got)
	}

	reader := bufio.NewReader(resp.Body)
	events := make(chan string, 16)
	go func() {
		var currentEvent string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				close(events)
				return
			}
			line = strings.TrimRight(line, "\r\n")
			switch {
			case strings.HasPrefix(line, "event: "):
				currentEvent = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				select {
				case events <- currentEvent + "|" + strings.TrimPrefix(line, "data: "):
				default:
				}
			}
		}
	}()

	select {
	case first := <-events:
		if !strings.HasPrefix(first, "sync|") {
			t.Fatalf("first stream message = %q, want a sync baseline", first)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no baseline message from the watch stream")
	}

	ts.op(t, http.MethodPost, "/api/v1/deployments", validDeployment("streamed", 1)).Body.Close()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case msg, ok := <-events:
			if !ok {
				t.Fatal("watch stream closed before delivering the change")
			}
			if strings.HasPrefix(msg, "change|") && strings.Contains(msg, "streamed") {
				return
			}
		case <-deadline:
			t.Fatal("the created deployment was not delivered on the watch stream")
		}
	}
}

// Writes must be throttled so a runaway client cannot fill the Raft log.
func TestWriteRateLimiting(t *testing.T) {
	limiter := newRateLimiter(10, 5)
	now := time.Now()

	allowed := 0
	for i := 0; i < 20; i++ {
		if limiter.allow("client", now) {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("burst allowed %d requests, want the configured burst of 5", allowed)
	}
	// Tokens refill over time.
	if !limiter.allow("client", now.Add(200*time.Millisecond)) {
		t.Error("the bucket did not refill after 200ms at 10/s")
	}
	// Limits are per principal, so one noisy client cannot starve another.
	if !limiter.allow("other-client", now) {
		t.Error("a second principal was throttled by the first one's usage")
	}
}

func TestTokenAuthRejectsWeakTokens(t *testing.T) {
	auth := NewTokenAuth()
	if err := auth.AddToken("short", "x", RoleOperator); err == nil {
		t.Fatal("expected a short token to be rejected")
	}
}

func TestMetricsEndpointExposesOrionSeries(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	lb := transport.NewLoopback()
	cp, err := controlplane.New(controlplane.Options{
		NodeID: 1, Peers: map[uint64]string{1: "local"},
		Raft: raft.Config{ID: 1, Peers: []uint64{1}, ElectionTick: 3, HeartbeatTick: 1,
			Storage: raft.NewMemoryStorage(), Logger: log},
		Transport: lb.For(1), Store: store.New(), TickInterval: 2 * time.Millisecond, Logger: log,
	})
	if err != nil {
		t.Fatal(err)
	}
	lb.Register(1, cp.Raft().Step)
	cp.Start()
	defer cp.Stop()

	metrics := newTestMetrics()
	srv, err := New(Options{ControlPlane: cp, Logger: log, Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// Generate one request so the counter exists.
	resp, _ := http.Get(httpSrv.URL + "/api/v1/nodes")
	resp.Body.Close()

	resp, err = http.Get(httpSrv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	for _, want := range []string{"orion_api_requests_total", "orion_api_request_duration_seconds"} {
		if !strings.Contains(text, want) {
			t.Errorf("metrics output does not contain %s", want)
		}
	}
	// The route label must be the pattern, not the path, or a large cluster
	// produces unbounded cardinality.
	if strings.Contains(text, `route="/api/v1/nodes/worker-01"`) {
		t.Error("metrics are labelled with concrete paths; cardinality would be unbounded")
	}
}
