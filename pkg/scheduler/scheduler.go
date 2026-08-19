package scheduler

import (
	"fmt"
	"sort"
	"strings"
	"time"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
)

// Scheduler makes placement decisions. It holds no cluster state of its own:
// every cycle runs against a Snapshot supplied by the caller, which keeps it a
// pure function of (workload, snapshot) and therefore trivially testable.
type Scheduler struct {
	filters []Filter
	scorers []Scorer
	// now is injected so decision timestamps are controllable in tests.
	now func() time.Time
}

type Option func(*Scheduler)

func WithFilters(f ...Filter) Option { return func(s *Scheduler) { s.filters = f } }
func WithScorers(s2 ...Scorer) Option {
	return func(s *Scheduler) { s.scorers = s2 }
}
func WithClock(now func() time.Time) Option { return func(s *Scheduler) { s.now = now } }

func New(opts ...Option) *Scheduler {
	s := &Scheduler{
		filters: DefaultFilters(),
		scorers: DefaultScorers(),
		now:     time.Now,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// ErrUnschedulable is reported when no node can run a workload. It carries the
// per-node rejections so the operator sees why, not just that it failed.
type ErrUnschedulable struct {
	Workload   string
	Rejections []v1.NodeRejection
	NodesTried int
}

func (e *ErrUnschedulable) Error() string {
	if e.NodesTried == 0 {
		return fmt.Sprintf("workload %s is unschedulable: the cluster has no nodes accepting work", e.Workload)
	}
	// Summarize by reason so 40 identical rejections read as one line.
	counts := map[string]int{}
	order := []string{}
	for _, r := range e.Rejections {
		if _, seen := counts[r.Filter]; !seen {
			order = append(order, r.Filter)
		}
		counts[r.Filter]++
	}
	parts := make([]string, 0, len(order))
	for _, f := range order {
		parts = append(parts, fmt.Sprintf("%d %s", counts[f], f))
	}
	return fmt.Sprintf("workload %s is unschedulable: %d nodes rejected (%s)",
		e.Workload, e.NodesTried, strings.Join(parts, ", "))
}

// Schedule picks a node for one workload.
//
// On success the returned decision records the winner, its score breakdown, the
// runners-up and every rejection. Orion stores that on the workload, so months
// later an operator can still answer "why did this land here?" without
// re-deriving cluster state that no longer exists.
func (s *Scheduler) Schedule(w *v1.Workload, snap *Snapshot) (*v1.PlacementDecision, error) {
	start := time.Now()

	feasible := make([]*v1.Node, 0, len(snap.Nodes))
	var rejections []v1.NodeRejection

	for _, n := range snap.Nodes {
		if reason, filter := s.firstFailure(w, n, snap); reason != "" {
			rejections = append(rejections, v1.NodeRejection{NodeName: n.Name, Filter: filter, Reason: reason})
			continue
		}
		feasible = append(feasible, n)
	}

	if len(feasible) == 0 {
		return nil, &ErrUnschedulable{Workload: w.Name, Rejections: rejections, NodesTried: len(snap.Nodes)}
	}

	candidates := make([]v1.NodeScore, 0, len(feasible))
	for _, n := range feasible {
		breakdown := make(map[string]int32, len(s.scorers))
		var total int32
		for _, sc := range s.scorers {
			raw := clamp(sc.Score(w, n, snap), 0, 100)
			weighted := raw * sc.Weight()
			breakdown[sc.Name()] = weighted
			total += weighted
		}
		candidates = append(candidates, v1.NodeScore{NodeName: n.Name, Score: total, Breakdown: breakdown})
	}

	// Highest score wins; ties break on node name. Deterministic tie-breaking
	// means a scheduler restart does not reshuffle placement, and tests do not
	// depend on map ordering.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		return candidates[i].NodeName < candidates[j].NodeName
	})

	winner := candidates[0]
	snap.Reserve(winner.NodeName, w.Spec.Resources.Request)

	// Cap the recorded detail: a 500-node cluster would otherwise attach a
	// kilobyte of scoring noise to every workload in the Raft log.
	const maxRecorded = 5
	recordedCandidates := candidates
	if len(recordedCandidates) > maxRecorded {
		recordedCandidates = recordedCandidates[:maxRecorded]
	}
	recordedRejections := rejections
	if len(recordedRejections) > maxRecorded {
		recordedRejections = recordedRejections[:maxRecorded]
	}

	return &v1.PlacementDecision{
		WorkloadName:  w.Name,
		NodeName:      winner.NodeName,
		DecidedAt:     s.now(),
		Score:         winner.Score,
		Reason:        s.explain(winner, candidates, len(rejections)),
		Candidates:    recordedCandidates,
		Rejections:    recordedRejections,
		LatencyMicros: time.Since(start).Microseconds(),
	}, nil
}

// firstFailure returns the reason and filter name of the first rejection.
func (s *Scheduler) firstFailure(w *v1.Workload, n *v1.Node, snap *Snapshot) (string, string) {
	for _, f := range s.filters {
		if reason := f.Check(w, n, snap); reason != "" {
			return reason, f.Name()
		}
	}
	return "", ""
}

// explain produces the one-line summary an operator reads first.
func (s *Scheduler) explain(winner v1.NodeScore, all []v1.NodeScore, rejected int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "selected %s (score %d of %d feasible node", winner.NodeName, winner.Score, len(all))
	if len(all) != 1 {
		b.WriteByte('s')
	}
	if rejected > 0 {
		fmt.Fprintf(&b, ", %d rejected", rejected)
	}
	b.WriteByte(')')

	// Name the scorer that actually decided it, which is the one where the
	// winner most out-scored the runner-up.
	if len(all) > 1 {
		runnerUp := all[1]
		var bestName string
		var bestDelta int32
		for name, v := range winner.Breakdown {
			delta := v - runnerUp.Breakdown[name]
			if delta > bestDelta {
				bestDelta, bestName = delta, name
			}
		}
		if bestName != "" {
			fmt.Fprintf(&b, ": %s over %s on %s", winner.NodeName, runnerUp.NodeName, bestName)
		} else {
			fmt.Fprintf(&b, ": tied with %s, broken by name", runnerUp.NodeName)
		}
	}
	return b.String()
}

// ScheduleBatch places a queue of workloads against one snapshot.
//
// Running a batch against a single snapshot is deliberate: it lets the
// scheduler reserve capacity as it goes, so scheduling ten replicas produces
// ten different placements instead of ten identical ones that all but one get
// rejected at commit time.
func (s *Scheduler) ScheduleBatch(workloads []*v1.Workload, snap *Snapshot) []BatchResult {
	out := make([]BatchResult, 0, len(workloads))
	for _, w := range workloads {
		decision, err := s.Schedule(w, snap)
		out = append(out, BatchResult{Workload: w, Decision: decision, Err: err})
	}
	return out
}

// BatchResult pairs a workload with its outcome.
type BatchResult struct {
	Workload *v1.Workload
	Decision *v1.PlacementDecision
	Err      error
}

func clamp(v, lo, hi int32) int32 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
