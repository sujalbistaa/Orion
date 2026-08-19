package v1

import (
	"strings"
	"testing"
)

func validSpec() WorkloadSpec {
	return WorkloadSpec{
		Image:     "nginx:1.27-alpine",
		Resources: ResourceSpec{Request: Resources{CPU: 500, Memory: 128 << 20}},
	}
}

func fieldsOf(t *testing.T, err error) map[string]string {
	t.Helper()
	ve, ok := AsValidationError(err)
	if !ok {
		t.Fatalf("expected *ValidationError, got %T (%v)", err, err)
	}
	out := map[string]string{}
	for _, e := range ve.Errors {
		out[e.Field] = e.Detail
	}
	return out
}

func TestValidateWorkloadAcceptsMinimalValidSpec(t *testing.T) {
	w := &Workload{ObjectMeta: ObjectMeta{Name: "api-server"}, Spec: validSpec()}
	if err := ValidateWorkload(w); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Image references reach the container runtime, so anything that could be
// interpreted as a flag or shell metacharacter must be rejected at the edge.
func TestValidateWorkloadRejectsHostileImageReferences(t *testing.T) {
	hostile := []string{
		"nginx; rm -rf /",
		"--privileged",
		"nginx$(whoami)",
		"nginx`id`",
		"nginx|cat",
		"nginx\nFROM scratch",
		"",
	}
	for _, img := range hostile {
		w := &Workload{ObjectMeta: ObjectMeta{Name: "x"}, Spec: validSpec()}
		w.Spec.Image = img
		if err := ValidateWorkload(w); err == nil {
			t.Errorf("expected image %q to be rejected", img)
		}
	}
}

func TestValidateWorkloadRejectsEnvInjection(t *testing.T) {
	w := &Workload{ObjectMeta: ObjectMeta{Name: "x"}, Spec: validSpec()}
	w.Spec.Env = []EnvVar{{Name: "FOO=BAR", Value: "x"}}
	if err := ValidateWorkload(w); err == nil {
		t.Fatal("expected env name containing '=' to be rejected")
	}

	w.Spec.Env = []EnvVar{{Name: "FOO", Value: "a"}, {Name: "FOO", Value: "b"}}
	fields := fieldsOf(t, ValidateWorkload(w))
	if _, ok := fields["spec.env[1].name"]; !ok {
		t.Errorf("expected duplicate env var to be rejected, got %v", fields)
	}
}

func TestValidateWorkloadResourceInvariants(t *testing.T) {
	w := &Workload{ObjectMeta: ObjectMeta{Name: "x"}, Spec: validSpec()}
	w.Spec.Resources.Limit = Resources{CPU: 100, Memory: 64 << 20}
	fields := fieldsOf(t, ValidateWorkload(w))
	if _, ok := fields["spec.resources.limit.cpu"]; !ok {
		t.Errorf("expected limit below request to be rejected, got %v", fields)
	}
	if _, ok := fields["spec.resources.limit.memory"]; !ok {
		t.Errorf("expected memory limit below request to be rejected, got %v", fields)
	}

	w = &Workload{ObjectMeta: ObjectMeta{Name: "x"}, Spec: validSpec()}
	w.Spec.Resources.Request.CPU = 0
	if err := ValidateWorkload(w); err == nil {
		t.Error("expected zero CPU request to be rejected")
	}
}

// All problems must be reported in a single pass; fixing errors serially is a
// bad API experience and hides interacting failures.
func TestValidationReportsAllErrorsAtOnce(t *testing.T) {
	w := &Workload{
		ObjectMeta: ObjectMeta{Name: "Invalid_Name"},
		Spec:       WorkloadSpec{Image: "", RestartPolicy: "Sometimes"},
	}
	ve, ok := AsValidationError(ValidateWorkload(w))
	if !ok {
		t.Fatal("expected validation error")
	}
	if len(ve.Errors) < 4 {
		t.Errorf("expected at least 4 aggregated errors, got %d: %v", len(ve.Errors), ve.Errors)
	}
}

func TestValidateNameRules(t *testing.T) {
	bad := []string{"", "UPPER", "-leading", "trailing-", "under_score", strings.Repeat("a", 64), "has space"}
	for _, name := range bad {
		w := &Workload{ObjectMeta: ObjectMeta{Name: name}, Spec: validSpec()}
		if err := ValidateWorkload(w); err == nil {
			t.Errorf("expected name %q to be rejected", name)
		}
	}
	good := []string{"a", "web-01", "api-server-v2", strings.Repeat("a", 63)}
	for _, name := range good {
		w := &Workload{ObjectMeta: ObjectMeta{Name: name}, Spec: validSpec()}
		if err := ValidateWorkload(w); err != nil {
			t.Errorf("expected name %q to be accepted: %v", name, err)
		}
	}
}

func TestValidateDeploymentStrategy(t *testing.T) {
	d := &Deployment{
		ObjectMeta: ObjectMeta{Name: "web"},
		Spec: DeploymentSpec{
			Replicas: 3,
			Template: validSpec(),
			Strategy: Strategy{Kind: StrategyRolling, MaxUnavailable: 0, MaxSurge: 0},
		},
	}
	if err := ValidateDeployment(d); err == nil {
		t.Error("expected rolling update with no surge and no unavailability to be rejected")
	}

	d.Spec.Strategy.MaxSurge = 1
	if err := ValidateDeployment(d); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	d.Spec.Replicas = MaxReplicas + 1
	if err := ValidateDeployment(d); err == nil {
		t.Error("expected replica count above the cap to be rejected")
	}
}

// An empty service selector must be rejected rather than silently matching
// every workload in the cluster.
func TestValidateServiceRejectsEmptySelector(t *testing.T) {
	s := &Service{
		ObjectMeta: ObjectMeta{Name: "web"},
		Spec:       ServiceSpec{Port: 8080, TargetPort: 80},
	}
	if err := ValidateService(s); err == nil {
		t.Fatal("expected empty selector to be rejected")
	}
	s.Spec.Selector = map[string]string{"app": "web"}
	if err := ValidateService(s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateNodeRegistrationRejectsImpossibleCapacity(t *testing.T) {
	n := &Node{
		ObjectMeta: ObjectMeta{Name: "worker-01"},
		Spec:       NodeSpec{Address: "10.0.0.4:9100"},
		Status: NodeStatus{
			Capacity:    Resources{CPU: 4000, Memory: 8 << 30},
			Allocatable: Resources{CPU: 8000, Memory: 8 << 30},
		},
	}
	if err := ValidateNodeRegistration(n); err == nil {
		t.Fatal("expected allocatable exceeding capacity to be rejected")
	}
}

func TestHashWorkloadSpecIsStableAndSensitive(t *testing.T) {
	a := validSpec()
	a.NodeSelector = map[string]string{"zone": "a", "tier": "b"}
	b := validSpec()
	b.NodeSelector = map[string]string{"tier": "b", "zone": "a"}

	// Map iteration order must not affect the hash, or rollouts would trigger
	// spuriously on every controller restart.
	for i := 0; i < 50; i++ {
		if HashWorkloadSpec(&a) != HashWorkloadSpec(&b) {
			t.Fatal("hash is not stable across map iteration order")
		}
	}

	c := validSpec()
	c.Image = "nginx:1.28-alpine"
	if HashWorkloadSpec(&a) == HashWorkloadSpec(&c) {
		t.Fatal("hash did not change when the image changed")
	}
}

func TestParseQuantities(t *testing.T) {
	cpu := []struct {
		in   string
		want MilliCPU
	}{{"500m", 500}, {"2", 2000}, {"1.5", 1500}}
	for _, tc := range cpu {
		got, err := ParseMilliCPU(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("ParseMilliCPU(%q) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
	}

	mem := []struct {
		in   string
		want Bytes
	}{{"512Mi", 512 << 20}, {"2Gi", 2 << 30}, {"1024", 1024}}
	for _, tc := range mem {
		got, err := ParseBytes(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("ParseBytes(%q) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
	}

	for _, bad := range []string{"", "abc", "5x", "-"} {
		if _, err := ParseMilliCPU(bad); err == nil {
			t.Errorf("ParseMilliCPU(%q) should fail", bad)
		}
	}
}

func TestToleratesTaints(t *testing.T) {
	taints := []Taint{{Key: "dedicated", Value: "db", Effect: "NoSchedule"}}
	if ToleratesTaints(nil, taints) {
		t.Error("workload with no tolerations must not tolerate a NoSchedule taint")
	}
	if !ToleratesTaints([]Toleration{{Key: "dedicated", Value: "db"}}, taints) {
		t.Error("exact toleration should match")
	}
	if !ToleratesTaints([]Toleration{{Key: "dedicated"}}, taints) {
		t.Error("wildcard-value toleration should match")
	}
	if ToleratesTaints([]Toleration{{Key: "dedicated", Value: "cache"}}, taints) {
		t.Error("mismatched toleration value must not match")
	}
	// Non-NoSchedule effects are advisory and must not block scheduling.
	if !ToleratesTaints(nil, []Taint{{Key: "x", Effect: "PreferNoSchedule"}}) {
		t.Error("non-NoSchedule taints must not block placement")
	}
}
