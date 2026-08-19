package v1

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Limits that bound untrusted input. They exist to keep a single API request
// from being able to exhaust control-plane memory or the Raft log.
const (
	MaxNameLength      = 63
	MaxLabels          = 32
	MaxEnvVars         = 64
	MaxPorts           = 16
	MaxCommandArgs     = 64
	MaxEnvValueLength  = 4096
	MaxReplicas        = 1000
	MaxImageLength     = 512
	MaxAnnotationBytes = 16384
)

// nameRE mirrors DNS label rules: lowercase alphanumeric and '-', starting and
// ending alphanumeric. Names appear in container names and DNS-ish contexts, so
// the restriction is functional rather than stylistic.
var nameRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// labelKeyRE allows an optional prefix, e.g. "orion.io/managed-by".
var labelKeyRE = regexp.MustCompile(`^([a-z0-9]([-a-z0-9.]*[a-z0-9])?/)?[a-zA-Z0-9]([-a-zA-Z0-9_.]*[a-zA-Z0-9])?$`)

// imageRE is intentionally strict: image references are passed to the container
// runtime, so anything that could be read as a flag or shell metacharacter is
// rejected at the API boundary rather than sanitized later.
var imageRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/@-]*$`)

// FieldError identifies exactly which field failed and why, so the console and
// CLI can point at the problem instead of printing a generic failure.
type FieldError struct {
	Field  string `json:"field"`
	Detail string `json:"detail"`
}

func (e FieldError) Error() string { return e.Field + ": " + e.Detail }

// ValidationError aggregates all field errors from one validation pass. All
// problems are reported at once; users should not have to fix errors serially.
type ValidationError struct {
	Errors []FieldError `json:"errors"`
}

func (v *ValidationError) Error() string {
	parts := make([]string, len(v.Errors))
	for i, e := range v.Errors {
		parts[i] = e.Error()
	}
	return strings.Join(parts, "; ")
}

func (v *ValidationError) add(field, format string, args ...any) {
	v.Errors = append(v.Errors, FieldError{Field: field, Detail: fmt.Sprintf(format, args...)})
}

func (v *ValidationError) orNil() error {
	if len(v.Errors) == 0 {
		return nil
	}
	return v
}

// AsValidationError extracts a *ValidationError from an error chain.
func AsValidationError(err error) (*ValidationError, bool) {
	var ve *ValidationError
	ok := errors.As(err, &ve)
	return ve, ok
}

func validateName(v *ValidationError, field, name string) {
	switch {
	case name == "":
		v.add(field, "must not be empty")
	case len(name) > MaxNameLength:
		v.add(field, "must be at most %d characters", MaxNameLength)
	case !nameRE.MatchString(name):
		v.add(field, "must be lowercase alphanumeric with '-' separators (RFC 1123 label)")
	}
}

func validateLabels(v *ValidationError, field string, labels map[string]string) {
	if len(labels) > MaxLabels {
		v.add(field, "at most %d labels allowed, got %d", MaxLabels, len(labels))
		return
	}
	for k, val := range labels {
		if !labelKeyRE.MatchString(k) {
			v.add(field+"["+k+"]", "invalid label key")
		}
		if len(val) > MaxNameLength {
			v.add(field+"["+k+"]", "value must be at most %d characters", MaxNameLength)
		}
	}
}

func validateMeta(v *ValidationError, m *ObjectMeta) {
	validateName(v, "metadata.name", m.Name)
	validateLabels(v, "metadata.labels", m.Labels)
	total := 0
	for k, val := range m.Annotations {
		total += len(k) + len(val)
	}
	if total > MaxAnnotationBytes {
		v.add("metadata.annotations", "total size must be at most %d bytes", MaxAnnotationBytes)
	}
}

// ValidateWorkloadSpec checks a workload template. It is shared by the workload
// and deployment paths so both reject exactly the same inputs.
func ValidateWorkloadSpec(v *ValidationError, prefix string, s *WorkloadSpec) {
	f := func(name string) string { return prefix + name }

	switch {
	case s.Image == "":
		v.add(f("image"), "must not be empty")
	case len(s.Image) > MaxImageLength:
		v.add(f("image"), "must be at most %d characters", MaxImageLength)
	case !imageRE.MatchString(s.Image):
		v.add(f("image"), "contains characters not permitted in an image reference")
	}

	if len(s.Command)+len(s.Args) > MaxCommandArgs {
		v.add(f("command"), "command and args must total at most %d entries", MaxCommandArgs)
	}

	if len(s.Env) > MaxEnvVars {
		v.add(f("env"), "at most %d environment variables allowed", MaxEnvVars)
	}
	seenEnv := make(map[string]bool, len(s.Env))
	for i, e := range s.Env {
		if e.Name == "" {
			v.add(fmt.Sprintf("%senv[%d].name", prefix, i), "must not be empty")
		}
		if strings.ContainsAny(e.Name, "= \t\n\x00") {
			v.add(fmt.Sprintf("%senv[%d].name", prefix, i), "must not contain '=', whitespace or NUL")
		}
		if seenEnv[e.Name] {
			v.add(fmt.Sprintf("%senv[%d].name", prefix, i), "duplicate environment variable %q", e.Name)
		}
		seenEnv[e.Name] = true
		if len(e.Value) > MaxEnvValueLength {
			v.add(fmt.Sprintf("%senv[%d].value", prefix, i), "must be at most %d bytes", MaxEnvValueLength)
		}
		if strings.ContainsRune(e.Value, 0) {
			v.add(fmt.Sprintf("%senv[%d].value", prefix, i), "must not contain NUL")
		}
	}

	if len(s.Ports) > MaxPorts {
		v.add(f("ports"), "at most %d ports allowed", MaxPorts)
	}
	seenPort := make(map[int32]bool, len(s.Ports))
	for i, p := range s.Ports {
		if p.Container < 1 || p.Container > 65535 {
			v.add(fmt.Sprintf("%sports[%d].container", prefix, i), "must be between 1 and 65535")
		}
		if p.Host != 0 && (p.Host < 1024 || p.Host > 65535) {
			// Below 1024 would require the agent to hold extra privileges.
			v.add(fmt.Sprintf("%sports[%d].host", prefix, i), "must be 0 (auto) or between 1024 and 65535")
		}
		if seenPort[p.Container] {
			v.add(fmt.Sprintf("%sports[%d].container", prefix, i), "duplicate container port %d", p.Container)
		}
		seenPort[p.Container] = true
		if p.Protocol != "" && p.Protocol != "tcp" && p.Protocol != "udp" {
			v.add(fmt.Sprintf("%sports[%d].protocol", prefix, i), "must be tcp or udp")
		}
	}

	r := s.Resources
	if r.Request.CPU <= 0 {
		v.add(f("resources.request.cpu"), "must be greater than 0")
	}
	if r.Request.Memory <= 0 {
		v.add(f("resources.request.memory"), "must be greater than 0")
	}
	// The scheduler commits Request; a Limit below it would let the runtime
	// kill a workload that the scheduler considers within its guarantee.
	if r.Limit.CPU != 0 && r.Limit.CPU < r.Request.CPU {
		v.add(f("resources.limit.cpu"), "must be greater than or equal to request")
	}
	if r.Limit.Memory != 0 && r.Limit.Memory < r.Request.Memory {
		v.add(f("resources.limit.memory"), "must be greater than or equal to request")
	}
	// Docker refuses memory limits below 6MiB.
	if r.Request.Memory > 0 && r.Request.Memory < 6*1024*1024 {
		v.add(f("resources.request.memory"), "must be at least 6Mi")
	}

	if s.RestartPolicy != "" && !s.RestartPolicy.Valid() {
		v.add(f("restartPolicy"), "must be one of Always, OnFailure, Never")
	}
	if s.TerminationGracePeriod < 0 {
		v.add(f("terminationGracePeriod"), "must not be negative")
	}
	if s.TerminationGracePeriod.Duration() > 10*time.Minute {
		v.add(f("terminationGracePeriod"), "must be at most 10m")
	}

	validateLabels(v, f("nodeSelector"), s.NodeSelector)

	if hc := s.HealthCheck; hc != nil {
		switch hc.Kind {
		case HealthCheckHTTP:
			if hc.Port < 1 || hc.Port > 65535 {
				v.add(f("healthCheck.port"), "must be between 1 and 65535 for an http check")
			}
			if hc.Path == "" || !strings.HasPrefix(hc.Path, "/") {
				v.add(f("healthCheck.path"), "must be an absolute path for an http check")
			}
		case HealthCheckTCP:
			if hc.Port < 1 || hc.Port > 65535 {
				v.add(f("healthCheck.port"), "must be between 1 and 65535 for a tcp check")
			}
		case HealthCheckProcess, "":
		default:
			v.add(f("healthCheck.kind"), "must be one of http, tcp, process")
		}
		if hc.Interval < 0 || hc.Timeout < 0 || hc.InitialDelay < 0 {
			v.add(f("healthCheck"), "durations must not be negative")
		}
		if hc.Timeout > 0 && hc.Interval > 0 && hc.Timeout > hc.Interval {
			v.add(f("healthCheck.timeout"), "must not exceed interval")
		}
		if hc.FailureThreshold < 0 || hc.SuccessThreshold < 0 {
			v.add(f("healthCheck"), "thresholds must not be negative")
		}
	}
}

// ValidateWorkload validates a user-submitted workload.
func ValidateWorkload(w *Workload) error {
	v := &ValidationError{}
	validateMeta(v, &w.ObjectMeta)
	ValidateWorkloadSpec(v, "spec.", &w.Spec)
	return v.orNil()
}

// ValidateDeployment validates a user-submitted deployment.
func ValidateDeployment(d *Deployment) error {
	v := &ValidationError{}
	validateMeta(v, &d.ObjectMeta)
	if d.Spec.Replicas < 0 {
		v.add("spec.replicas", "must not be negative")
	}
	if d.Spec.Replicas > MaxReplicas {
		v.add("spec.replicas", "must be at most %d", MaxReplicas)
	}
	ValidateWorkloadSpec(v, "spec.template.", &d.Spec.Template)

	switch d.Spec.Strategy.Kind {
	case "", StrategyRolling:
		if d.Spec.Strategy.MaxUnavailable < 0 {
			v.add("spec.strategy.maxUnavailable", "must not be negative")
		}
		if d.Spec.Strategy.MaxSurge < 0 {
			v.add("spec.strategy.maxSurge", "must not be negative")
		}
		// Both zero would make a rolling update unable to make progress.
		if d.Spec.Strategy.Kind == StrategyRolling &&
			d.Spec.Strategy.MaxUnavailable == 0 && d.Spec.Strategy.MaxSurge == 0 {
			v.add("spec.strategy", "maxUnavailable and maxSurge must not both be 0")
		}
	case StrategyRecreate:
	default:
		v.add("spec.strategy.kind", "must be RollingUpdate or Recreate")
	}
	return v.orNil()
}

// ValidateService validates a user-submitted service.
func ValidateService(s *Service) error {
	v := &ValidationError{}
	validateMeta(v, &s.ObjectMeta)
	if len(s.Spec.Selector) == 0 {
		v.add("spec.selector", "must not be empty; an empty selector would match no workloads")
	}
	validateLabels(v, "spec.selector", s.Spec.Selector)
	if s.Spec.Port < 1024 || s.Spec.Port > 65535 {
		v.add("spec.port", "must be between 1024 and 65535")
	}
	if s.Spec.TargetPort < 1 || s.Spec.TargetPort > 65535 {
		v.add("spec.targetPort", "must be between 1 and 65535")
	}
	switch s.Spec.Strategy {
	case "", LBRoundRobin, LBLeastConnections:
	default:
		v.add("spec.strategy", "must be RoundRobin or LeastConnections")
	}
	return v.orNil()
}

// ValidateNodeRegistration checks what an agent reports at registration. Agents
// are semi-trusted: they are authenticated, but a buggy agent must not be able
// to poison scheduling with impossible capacity.
func ValidateNodeRegistration(n *Node) error {
	v := &ValidationError{}
	validateName(v, "name", n.Name)
	validateLabels(v, "labels", n.Labels)
	if n.Spec.Address == "" {
		v.add("spec.address", "must not be empty")
	}
	if n.Status.Capacity.CPU <= 0 {
		v.add("status.capacity.cpu", "must be greater than 0")
	}
	if n.Status.Capacity.Memory <= 0 {
		v.add("status.capacity.memory", "must be greater than 0")
	}
	if n.Status.Allocatable.CPU > n.Status.Capacity.CPU {
		v.add("status.allocatable.cpu", "must not exceed capacity")
	}
	if n.Status.Allocatable.Memory > n.Status.Capacity.Memory {
		v.add("status.allocatable.memory", "must not exceed capacity")
	}
	return v.orNil()
}
