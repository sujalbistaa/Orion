package v1

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"
)

// Defaults applied at admission. They are applied once, on write, and persisted
// — never re-derived at read time, so a change to these constants cannot
// silently alter the meaning of an object already in the store.
const (
	DefaultTerminationGracePeriod = 30 * time.Second
	DefaultHealthInterval         = 10 * time.Second
	DefaultHealthTimeout          = 3 * time.Second
	DefaultHealthFailureThreshold = 3
	DefaultHealthSuccessThreshold = 1
	DefaultProgressDeadline       = 5 * time.Minute
)

// SetWorkloadSpecDefaults fills unset optional fields.
func SetWorkloadSpecDefaults(s *WorkloadSpec) {
	if s.RestartPolicy == "" {
		s.RestartPolicy = RestartAlways
	}
	if s.TerminationGracePeriod == 0 {
		s.TerminationGracePeriod = Duration(DefaultTerminationGracePeriod)
	}
	for i := range s.Ports {
		if s.Ports[i].Protocol == "" {
			s.Ports[i].Protocol = "tcp"
		}
	}
	if hc := s.HealthCheck; hc != nil {
		if hc.Kind == "" {
			hc.Kind = HealthCheckProcess
		}
		if hc.Interval == 0 {
			hc.Interval = Duration(DefaultHealthInterval)
		}
		if hc.Timeout == 0 {
			hc.Timeout = Duration(DefaultHealthTimeout)
		}
		if hc.FailureThreshold == 0 {
			hc.FailureThreshold = DefaultHealthFailureThreshold
		}
		if hc.SuccessThreshold == 0 {
			hc.SuccessThreshold = DefaultHealthSuccessThreshold
		}
	}
}

// SetDeploymentDefaults fills unset optional fields on a deployment.
func SetDeploymentDefaults(d *Deployment) {
	SetWorkloadSpecDefaults(&d.Spec.Template)
	if d.Spec.Strategy.Kind == "" {
		d.Spec.Strategy.Kind = StrategyRolling
	}
	if d.Spec.Strategy.Kind == StrategyRolling &&
		d.Spec.Strategy.MaxUnavailable == 0 && d.Spec.Strategy.MaxSurge == 0 {
		// Surge-by-one keeps full capacity during a rollout, which is the safe
		// default for a service; operators who prefer no extra capacity set
		// MaxUnavailable explicitly.
		d.Spec.Strategy.MaxSurge = 1
	}
	if d.Spec.ProgressDeadline == 0 {
		d.Spec.ProgressDeadline = Duration(DefaultProgressDeadline)
	}
}

// SetServiceDefaults fills unset optional fields on a service.
func SetServiceDefaults(s *Service) {
	if s.Spec.Strategy == "" {
		s.Spec.Strategy = LBRoundRobin
	}
	if s.Spec.TargetPort == 0 {
		s.Spec.TargetPort = s.Spec.Port
	}
}

// HashWorkloadSpec produces a stable content hash of a workload template.
// Rollouts are triggered by a hash change, so this must be deterministic across
// processes and Go versions: JSON field order comes from the struct definition,
// and maps are the only source of nondeterminism, so they are sorted explicitly.
func HashWorkloadSpec(s *WorkloadSpec) string {
	h := sha256.New()
	enc := func(v any) {
		b, _ := json.Marshal(v)
		_ = binary.Write(h, binary.LittleEndian, uint32(len(b)))
		h.Write(b)
	}
	normalized := *s
	normalized.NodeSelector = nil
	enc(normalized)
	enc(sortedPairs(s.NodeSelector))
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func sortedPairs(m map[string]string) [][2]string {
	if len(m) == 0 {
		return nil
	}
	out := make([][2]string, 0, len(m))
	for k, v := range m {
		out = append(out, [2]string{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

// SelectorMatches reports whether labels satisfy every key/value in selector.
// An empty selector matches nothing (see ServiceSpec.Selector).
func SelectorMatches(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// ToleratesTaints reports whether a workload's tolerations cover every
// NoSchedule taint on a node.
func ToleratesTaints(tolerations []Toleration, taints []Taint) bool {
	for _, t := range taints {
		if t.Effect != "NoSchedule" {
			continue
		}
		if !tolerated(tolerations, t) {
			return false
		}
	}
	return true
}

func tolerated(tolerations []Toleration, t Taint) bool {
	for _, tol := range tolerations {
		if tol.Key != t.Key {
			continue
		}
		// An empty toleration value matches any value for that key.
		if tol.Value == "" || tol.Value == t.Value {
			return true
		}
	}
	return false
}

// SetCondition inserts or updates a condition, preserving Since when the status
// has not changed so that "how long has this been true" stays meaningful.
func SetCondition(conds []Condition, c Condition) []Condition {
	for i := range conds {
		if conds[i].Type != c.Type {
			continue
		}
		if conds[i].Status == c.Status {
			c.Since = conds[i].Since
		}
		conds[i] = c
		return conds
	}
	return append(conds, c)
}
