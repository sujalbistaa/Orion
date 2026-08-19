package v1

import (
	"fmt"
	"strconv"
	"strings"
)

// MilliCPU is CPU expressed in thousandths of a core. 1000 == one core.
// Integer arithmetic keeps scheduler arithmetic exact and free of float drift.
type MilliCPU int64

// Bytes is a memory quantity in bytes.
type Bytes int64

func (m MilliCPU) String() string {
	if m%1000 == 0 {
		return strconv.FormatInt(int64(m)/1000, 10)
	}
	return strconv.FormatInt(int64(m), 10) + "m"
}

// Cores returns the fractional core count, for display and metrics only.
func (m MilliCPU) Cores() float64 { return float64(m) / 1000 }

func (b Bytes) String() string {
	const unit = 1024
	if b < unit {
		return strconv.FormatInt(int64(b), 10)
	}
	div, exp := int64(unit), 0
	for n := int64(b) / unit; n >= unit && exp < 4; n /= unit {
		div *= unit
		exp++
	}
	v := float64(b) / float64(div)
	return strings.TrimSuffix(strconv.FormatFloat(v, 'f', 1, 64), ".0") + string("KMGTP"[exp]) + "i"
}

// ParseMilliCPU accepts "500m", "2", "1.5".
func ParseMilliCPU(s string) (MilliCPU, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty cpu quantity")
	}
	if strings.HasSuffix(s, "m") {
		n, err := strconv.ParseInt(strings.TrimSuffix(s, "m"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid cpu quantity %q: %w", s, err)
		}
		return MilliCPU(n), nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid cpu quantity %q: %w", s, err)
	}
	return MilliCPU(f * 1000), nil
}

var byteSuffixes = []struct {
	suffix string
	mult   int64
}{
	{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40},
	{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12},
	{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}, {"T", 1 << 40},
}

// ParseBytes accepts "512Mi", "2Gi", "1024", "1GB".
func ParseBytes(s string) (Bytes, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty memory quantity")
	}
	for _, sfx := range byteSuffixes {
		if strings.HasSuffix(s, sfx.suffix) {
			n, err := strconv.ParseFloat(strings.TrimSuffix(s, sfx.suffix), 64)
			if err != nil {
				return 0, fmt.Errorf("invalid memory quantity %q: %w", s, err)
			}
			return Bytes(n * float64(sfx.mult)), nil
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory quantity %q: %w", s, err)
	}
	return Bytes(n), nil
}

// Resources is a CPU/memory vector. The zero value means "no resources".
type Resources struct {
	CPU    MilliCPU `json:"cpu"`
	Memory Bytes    `json:"memory"`
}

func (r Resources) Add(o Resources) Resources {
	return Resources{CPU: r.CPU + o.CPU, Memory: r.Memory + o.Memory}
}

func (r Resources) Sub(o Resources) Resources {
	return Resources{CPU: r.CPU - o.CPU, Memory: r.Memory - o.Memory}
}

// Fits reports whether r can be carved out of capacity without going negative.
func (r Resources) Fits(capacity Resources) bool {
	return r.CPU <= capacity.CPU && r.Memory <= capacity.Memory
}

func (r Resources) IsZero() bool { return r.CPU == 0 && r.Memory == 0 }

func (r Resources) String() string {
	return fmt.Sprintf("cpu=%s memory=%s", r.CPU, r.Memory)
}

// ResourceRequest is the guaranteed reservation used for scheduling.
// ResourceLimit is the hard cap enforced by the container runtime.
//
// Orion requires Request <= Limit and schedules exclusively on Request, so a
// node is never oversubscribed on the guaranteed portion. Limits above
// requests are burstable and explicitly not accounted by the scheduler.
type ResourceSpec struct {
	Request Resources `json:"request"`
	Limit   Resources `json:"limit,omitzero"`
}

// EffectiveLimit returns the limit, defaulting to the request when unset.
func (rs ResourceSpec) EffectiveLimit() Resources {
	l := rs.Limit
	if l.CPU == 0 {
		l.CPU = rs.Request.CPU
	}
	if l.Memory == 0 {
		l.Memory = rs.Request.Memory
	}
	return l
}
