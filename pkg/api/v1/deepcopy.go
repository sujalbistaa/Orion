package v1

// Deep copies exist so the store can hand callers objects they may freely
// mutate while the apply loop keeps writing to the originals. The cluster state
// is small enough that copying is cheaper than the bugs shared pointers cause.

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copySlice[T any](s []T) []T {
	if s == nil {
		return nil
	}
	out := make([]T, len(s))
	copy(out, s)
	return out
}

func (m ObjectMeta) DeepCopy() ObjectMeta {
	out := m
	out.Labels = copyMap(m.Labels)
	out.Annotations = copyMap(m.Annotations)
	if m.DeletedAt != nil {
		t := *m.DeletedAt
		out.DeletedAt = &t
	}
	if m.OwnerRef != nil {
		ref := *m.OwnerRef
		out.OwnerRef = &ref
	}
	return out
}

func (s WorkloadSpec) DeepCopy() WorkloadSpec {
	out := s
	out.Command = copySlice(s.Command)
	out.Args = copySlice(s.Args)
	out.Env = copySlice(s.Env)
	out.Ports = copySlice(s.Ports)
	out.NodeSelector = copyMap(s.NodeSelector)
	out.Tolerations = copySlice(s.Tolerations)
	if s.HealthCheck != nil {
		hc := *s.HealthCheck
		out.HealthCheck = &hc
	}
	return out
}

func (s WorkloadStatus) DeepCopy() WorkloadStatus {
	out := s
	if s.ExitCode != nil {
		v := *s.ExitCode
		out.ExitCode = &v
	}
	if s.StartedAt != nil {
		t := *s.StartedAt
		out.StartedAt = &t
	}
	if s.FinishedAt != nil {
		t := *s.FinishedAt
		out.FinishedAt = &t
	}
	if s.HostPorts != nil {
		out.HostPorts = make(map[int32]int32, len(s.HostPorts))
		for k, v := range s.HostPorts {
			out.HostPorts[k] = v
		}
	}
	if s.Placement != nil {
		p := s.Placement.DeepCopy()
		out.Placement = &p
	}
	return out
}

func (p PlacementDecision) DeepCopy() PlacementDecision {
	out := p
	out.Rejections = copySlice(p.Rejections)
	if p.Candidates != nil {
		out.Candidates = make([]NodeScore, len(p.Candidates))
		for i, c := range p.Candidates {
			out.Candidates[i] = c
			if c.Breakdown != nil {
				b := make(map[string]int32, len(c.Breakdown))
				for k, v := range c.Breakdown {
					b[k] = v
				}
				out.Candidates[i].Breakdown = b
			}
		}
	}
	return out
}

func (w *Workload) DeepCopy() *Workload {
	if w == nil {
		return nil
	}
	return &Workload{
		ObjectMeta: w.ObjectMeta.DeepCopy(),
		Spec:       w.Spec.DeepCopy(),
		Status:     w.Status.DeepCopy(),
	}
}

func (n *Node) DeepCopy() *Node {
	if n == nil {
		return nil
	}
	out := &Node{
		ObjectMeta: n.ObjectMeta.DeepCopy(),
		Spec:       n.Spec,
		Status:     n.Status,
	}
	out.Spec.Taints = copySlice(n.Spec.Taints)
	out.Status.Conditions = copySlice(n.Status.Conditions)
	return out
}

func (d *Deployment) DeepCopy() *Deployment {
	if d == nil {
		return nil
	}
	out := &Deployment{
		ObjectMeta: d.ObjectMeta.DeepCopy(),
		Spec:       d.Spec,
		Status:     d.Status,
	}
	out.Spec.Template = d.Spec.Template.DeepCopy()
	out.Status.Conditions = copySlice(d.Status.Conditions)
	return out
}

func (s *Service) DeepCopy() *Service {
	if s == nil {
		return nil
	}
	out := &Service{
		ObjectMeta: s.ObjectMeta.DeepCopy(),
		Spec:       s.Spec,
		Status:     s.Status,
	}
	out.Spec.Selector = copyMap(s.Spec.Selector)
	out.Status.Endpoints = copySlice(s.Status.Endpoints)
	return out
}

func (r *DeploymentRevision) DeepCopy() *DeploymentRevision {
	if r == nil {
		return nil
	}
	out := *r
	out.Template = r.Template.DeepCopy()
	return &out
}
