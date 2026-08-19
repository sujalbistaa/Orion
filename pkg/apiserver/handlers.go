package apiserver

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	v1 "github.com/sujalbistaa/orion/pkg/api/v1"
	"github.com/sujalbistaa/orion/pkg/store"
)

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

// ClusterResponse combines consensus state with a cluster summary, because the
// Overview page needs both and two round trips would let them disagree.
type ClusterResponse struct {
	Cluster v1.Cluster           `json:"cluster"`
	Summary store.ClusterSummary `json:"summary"`
}

func (s *Server) handleGetCluster(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, ClusterResponse{
		Cluster: s.cp.ClusterStatus(),
		Summary: s.store.Summary(),
	})
}

func (s *Server) handleGetSummary(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.store.Summary())
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	nodes := s.store.Nodes()
	if phase := r.URL.Query().Get("phase"); phase != "" {
		filtered := nodes[:0]
		for _, n := range nodes {
			if string(n.Status.Phase) == phase {
				filtered = append(filtered, n)
			}
		}
		nodes = filtered
	}
	writeList(w, nodes, s.store.AppliedIndex())
}

// NodeDetail is a node plus everything the node page shows, assembled under one
// read so the workload list cannot disagree with the resource totals above it.
type NodeDetail struct {
	Node      *v1.Node       `json:"node"`
	Workloads []*v1.Workload `json:"workloads"`
	Events    []v1.Event     `json:"events"`
}

func (s *Server) handleGetNode(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	node, ok := s.store.Node(name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no node named "+name)
		return
	}
	writeJSON(w, http.StatusOK, NodeDetail{
		Node:      node,
		Workloads: s.store.WorkloadsOnNode(name),
		Events:    s.store.Events(store.EventQuery{Kind: "Node", Name: name, Limit: 50}),
	})
}

func (s *Server) handleCordonNode(w http.ResponseWriter, r *http.Request) { s.setCordon(w, r, true) }

func (s *Server) handleUncordonNode(w http.ResponseWriter, r *http.Request) { s.setCordon(w, r, false) }

func (s *Server) setCordon(w http.ResponseWriter, r *http.Request, unschedulable bool) {
	if !s.requireLeader(w) {
		return
	}
	name := r.PathValue("name")
	s.applyWrite(w, r, store.Command{
		Kind:          store.CmdCordonNode,
		Name:          name,
		Unschedulable: &unschedulable,
	}, http.StatusOK)
}

// handleDrainNode moves a node to Draining, which stops new scheduling and asks
// the controller to evict its workloads in a controlled fashion. It is
// deliberately a distinct operation from cordon: cordon is reversible and
// harmless, drain moves running work.
func (s *Server) handleDrainNode(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	name := r.PathValue("name")
	node, ok := s.store.Node(name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no node named "+name)
		return
	}
	running := 0
	for _, wl := range s.store.WorkloadsOnNode(name) {
		if wl.Status.Phase.Active() {
			running++
		}
	}
	// Draining the last Ready node would leave nowhere for the evicted work to
	// go. Refuse unless the caller says they meant it.
	if !queryBool(r, "force") && s.readyNodeCount() <= 1 && running > 0 {
		writeError(w, http.StatusConflict, "would_lose_capacity",
			fmt.Sprintf("%s is the only Ready node and is running %d workloads; "+
				"draining it would leave them nowhere to go. Retry with ?force=true to proceed.",
				name, running))
		return
	}

	if _, ok := s.applyWrite(w, r, store.Command{
		Kind:   store.CmdSetNodePhase,
		Name:   name,
		Phase:  v1.NodeDraining,
		Reason: "drained by operator",
	}, 0); !ok {
		return
	}
	s.recordAudit(r.Context(), "NodeDrained", "Node", name,
		fmt.Sprintf("operator drained the node; %d workloads will be evicted", running))
	writeJSON(w, http.StatusOK, map[string]any{
		"node": name, "phase": v1.NodeDraining, "workloadsToEvict": running, "uid": node.UID,
	})
}

func (s *Server) readyNodeCount() int {
	n := 0
	for _, node := range s.store.Nodes() {
		if node.Status.Phase == v1.NodeReady {
			n++
		}
	}
	return n
}

func (s *Server) handleDeleteNode(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	name := r.PathValue("name")
	node, ok := s.store.Node(name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no node named "+name)
		return
	}
	// Removing a node that is still running work terminates that work. That is
	// occasionally what an operator wants, but never by accident.
	if node.Status.Phase == v1.NodeReady && !queryBool(r, "force") {
		writeError(w, http.StatusConflict, "node_is_ready",
			"this node is Ready and may still be running workloads. Drain it first, "+
				"or retry with ?force=true to remove it and terminate its workloads.")
		return
	}
	if _, ok := s.applyWrite(w, r, store.Command{Kind: store.CmdDeleteNode, Name: name}, 0); !ok {
		return
	}
	s.recordAudit(r.Context(), "NodeDeleted", "Node", name, "operator removed the node from the cluster")
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

func (s *Server) handleListWorkloads(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var workloads []*v1.Workload

	switch {
	case q.Get("node") != "":
		workloads = s.store.WorkloadsOnNode(q.Get("node"))
	case q.Get("deployment") != "":
		d, ok := s.store.Deployment(q.Get("deployment"))
		if !ok {
			writeList(w, []*v1.Workload{}, s.store.AppliedIndex())
			return
		}
		workloads = s.store.WorkloadsOwnedBy(d.UID)
	default:
		workloads = s.store.Workloads()
	}

	if phase := q.Get("phase"); phase != "" {
		filtered := workloads[:0]
		for _, wl := range workloads {
			if string(wl.Status.Phase) == phase {
				filtered = append(filtered, wl)
			}
		}
		workloads = filtered
	}
	writeList(w, workloads, s.store.AppliedIndex())
}

func (s *Server) handleCreateWorkload(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	var wl v1.Workload
	if !decodeBody(w, r, s.maxRequestSize, &wl) {
		return
	}
	// Status is server-owned. Accepting it from a client would let a caller
	// declare a workload Running without a container existing.
	wl.Status = v1.WorkloadStatus{}
	wl.UID, wl.Revision, wl.Generation = "", 0, 0
	wl.OwnerRef = nil // ownership is established by controllers, not by clients

	v1.SetWorkloadSpecDefaults(&wl.Spec)
	if err := v1.ValidateWorkload(&wl); err != nil {
		writeValidationError(w, err)
		return
	}
	s.applyWrite(w, r, store.Command{Kind: store.CmdCreateWorkload, Workload: &wl}, http.StatusCreated)
}

// WorkloadDetail is everything the workload page shows.
type WorkloadDetail struct {
	Workload *v1.Workload `json:"workload"`
	Node     *v1.Node     `json:"node,omitempty"`
	Events   []v1.Event   `json:"events"`
}

func (s *Server) handleGetWorkload(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	wl, ok := s.store.Workload(name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no workload named "+name)
		return
	}
	detail := WorkloadDetail{
		Workload: wl,
		Events:   s.store.Events(store.EventQuery{Kind: "Workload", Name: name, Limit: 50}),
	}
	if wl.Status.NodeName != "" {
		if node, ok := s.store.Node(wl.Status.NodeName); ok {
			detail.Node = node
		}
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleDeleteWorkload(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	name := r.PathValue("name")
	wl, ok := s.store.Workload(name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no workload named "+name)
		return
	}
	// A workload owned by a deployment will be recreated immediately; deleting
	// it directly is almost always a mistake for which the fix is to scale or
	// delete the deployment.
	if wl.OwnerRef != nil && !queryBool(r, "force") {
		writeError(w, http.StatusConflict, "owned_workload",
			fmt.Sprintf("%s is managed by %s %q and will be recreated. "+
				"Scale or delete the %s instead, or retry with ?force=true.",
				name, wl.OwnerRef.Kind, wl.OwnerRef.Name, strings.ToLower(wl.OwnerRef.Kind)))
		return
	}

	cmd := store.Command{Kind: store.CmdDeleteWorkload, Name: name, Reason: "deleted by operator"}
	if rev := queryUint64(r, "revision"); rev > 0 {
		// Optimistic concurrency: the console sends the revision it displayed,
		// so a delete cannot act on a workload that changed underneath it.
		cmd.ExpectedRevision = rev
	}
	if _, ok := s.applyWrite(w, r, cmd, 0); !ok {
		return
	}
	s.recordAudit(r.Context(), "WorkloadDeleted", "Workload", name, "operator deleted the workload")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleWorkloadLogs(w http.ResponseWriter, r *http.Request) {
	if s.logs == nil {
		writeError(w, http.StatusNotImplemented, "logs_unavailable",
			"this server is not configured to fetch container logs")
		return
	}
	name := r.PathValue("name")
	wl, ok := s.store.Workload(name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no workload named "+name)
		return
	}
	if wl.Status.NodeName == "" {
		writeError(w, http.StatusConflict, "not_scheduled",
			"this workload has not been placed on a node yet, so it has no logs")
		return
	}

	tail := queryInt(r, "tail", 200, 1, 10000)
	follow := queryBool(r, "follow")

	stream, err := s.logs.Logs(r.Context(), wl.Status.NodeName, name, tail, follow)
	if err != nil {
		writeError(w, http.StatusBadGateway, "log_fetch_failed",
			fmt.Sprintf("could not read logs from node %s: %v", wl.Status.NodeName, err))
		return
	}
	defer stream.Close()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// Proxies buffer text/plain by default, which makes a followed log appear
	// frozen.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return // client went away
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			return
		}
		if r.Context().Err() != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Deployments
// ---------------------------------------------------------------------------

func (s *Server) handleListDeployments(w http.ResponseWriter, r *http.Request) {
	writeList(w, s.store.Deployments(), s.store.AppliedIndex())
}

func (s *Server) handleCreateDeployment(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	var d v1.Deployment
	if !decodeBody(w, r, s.maxRequestSize, &d) {
		return
	}
	d.Status = v1.DeploymentStatus{}
	d.UID, d.Revision, d.Generation = "", 0, 0

	v1.SetDeploymentDefaults(&d)
	if err := v1.ValidateDeployment(&d); err != nil {
		writeValidationError(w, err)
		return
	}
	s.applyWrite(w, r, store.Command{Kind: store.CmdCreateDeployment, Deployment: &d}, http.StatusCreated)
}

// DeploymentDetail is everything the deployment page shows.
type DeploymentDetail struct {
	Deployment *v1.Deployment           `json:"deployment"`
	Workloads  []*v1.Workload           `json:"workloads"`
	Revisions  []*v1.DeploymentRevision `json:"revisions"`
	Events     []v1.Event               `json:"events"`
}

func (s *Server) handleGetDeployment(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	d, ok := s.store.Deployment(name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no deployment named "+name)
		return
	}
	writeJSON(w, http.StatusOK, DeploymentDetail{
		Deployment: d,
		Workloads:  s.store.WorkloadsOwnedBy(d.UID),
		Revisions:  s.store.DeploymentRevisions(name),
		Events:     s.store.Events(store.EventQuery{Kind: "Deployment", Name: name, Limit: 50}),
	})
}

func (s *Server) handleUpdateDeployment(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	name := r.PathValue("name")
	existing, ok := s.store.Deployment(name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no deployment named "+name)
		return
	}

	var d v1.Deployment
	if !decodeBody(w, r, s.maxRequestSize, &d) {
		return
	}
	d.Name = name
	d.Status = existing.Status
	v1.SetDeploymentDefaults(&d)
	if err := v1.ValidateDeployment(&d); err != nil {
		writeValidationError(w, err)
		return
	}

	cmd := store.Command{Kind: store.CmdUpdateDeploymentSpec, Name: name, Deployment: &d}
	if rev := queryUint64(r, "revision"); rev > 0 {
		cmd.ExpectedRevision = rev
	}
	s.applyWrite(w, r, cmd, http.StatusOK)
}

// ScaleRequest changes replica count without resending the whole spec, which
// keeps the common operation from being able to accidentally change the image.
type ScaleRequest struct {
	Replicas int32 `json:"replicas"`
}

func (s *Server) handleScaleDeployment(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	name := r.PathValue("name")
	d, ok := s.store.Deployment(name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no deployment named "+name)
		return
	}

	var req ScaleRequest
	if !decodeBody(w, r, s.maxRequestSize, &req) {
		return
	}
	if req.Replicas < 0 || req.Replicas > v1.MaxReplicas {
		writeError(w, http.StatusUnprocessableEntity, "invalid_replicas",
			fmt.Sprintf("replicas must be between 0 and %d", v1.MaxReplicas))
		return
	}

	updated := d.DeepCopy()
	updated.Spec.Replicas = req.Replicas
	if _, ok := s.applyWrite(w, r, store.Command{
		Kind: store.CmdUpdateDeploymentSpec, Name: name, Deployment: updated,
	}, 0); !ok {
		return
	}
	if req.Replicas == 0 {
		s.recordAudit(r.Context(), "DeploymentScaledToZero", "Deployment", name,
			"operator scaled the deployment to zero replicas")
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"deployment": name, "previousReplicas": d.Spec.Replicas, "replicas": req.Replicas,
	})
}

// RollbackRequest selects the revision to return to. Zero means "the previous
// one".
type RollbackRequest struct {
	Revision int64 `json:"revision"`
}

func (s *Server) handleRollbackDeployment(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	name := r.PathValue("name")

	var req RollbackRequest
	if r.ContentLength > 0 && !decodeBody(w, r, s.maxRequestSize, &req) {
		return
	}
	if _, ok := s.applyWrite(w, r, store.Command{
		Kind: store.CmdRollbackDeployment, Name: name, TargetRevision: req.Revision,
	}, 0); !ok {
		return
	}
	s.recordAudit(r.Context(), "DeploymentRolledBack", "Deployment", name,
		fmt.Sprintf("operator rolled back to revision %d", req.Revision))

	d, _ := s.store.Deployment(name)
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) handleDeploymentRevisions(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, ok := s.store.Deployment(name); !ok {
		writeError(w, http.StatusNotFound, "not_found", "no deployment named "+name)
		return
	}
	writeList(w, s.store.DeploymentRevisions(name), s.store.AppliedIndex())
}

func (s *Server) handleDeleteDeployment(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	name := r.PathValue("name")
	d, ok := s.store.Deployment(name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no deployment named "+name)
		return
	}
	cmd := store.Command{Kind: store.CmdDeleteDeployment, Name: name}
	if rev := queryUint64(r, "revision"); rev > 0 {
		cmd.ExpectedRevision = rev
	}
	if _, ok := s.applyWrite(w, r, cmd, 0); !ok {
		return
	}
	s.recordAudit(r.Context(), "DeploymentDeleted", "Deployment", name,
		fmt.Sprintf("operator deleted the deployment and its %d replicas", d.Status.CurrentReplicas))
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Services
// ---------------------------------------------------------------------------

func (s *Server) handleListServices(w http.ResponseWriter, r *http.Request) {
	writeList(w, s.store.Services(), s.store.AppliedIndex())
}

func (s *Server) handleCreateService(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	var svc v1.Service
	if !decodeBody(w, r, s.maxRequestSize, &svc) {
		return
	}
	svc.Status = v1.ServiceStatus{}
	svc.UID, svc.Revision, svc.Generation = "", 0, 0

	v1.SetServiceDefaults(&svc)
	if err := v1.ValidateService(&svc); err != nil {
		writeValidationError(w, err)
		return
	}
	// A second service on the same port would make routing ambiguous.
	for _, existing := range s.store.Services() {
		if existing.Spec.Port == svc.Spec.Port {
			writeError(w, http.StatusConflict, "port_in_use",
				fmt.Sprintf("port %d is already served by %q", svc.Spec.Port, existing.Name))
			return
		}
	}
	s.applyWrite(w, r, store.Command{Kind: store.CmdCreateService, Service: &svc}, http.StatusCreated)
}

func (s *Server) handleGetService(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	svc, ok := s.store.Service(name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no service named "+name)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service": svc,
		"events":  s.store.Events(store.EventQuery{Kind: "Service", Name: name, Limit: 50}),
	})
}

func (s *Server) handleDeleteService(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	name := r.PathValue("name")
	if _, ok := s.applyWrite(w, r, store.Command{Kind: store.CmdDeleteService, Name: name}, 0); !ok {
		return
	}
	s.recordAudit(r.Context(), "ServiceDeleted", "Service", name, "operator deleted the service")
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	events := s.store.Events(store.EventQuery{
		Since:    queryUint64(r, "since"),
		Before:   queryUint64(r, "before"),
		Kind:     q.Get("kind"),
		Name:     q.Get("name"),
		Severity: v1.EventSeverity(q.Get("severity")),
		Limit:    queryInt(r, "limit", 100, 1, 1000),
	})
	// Newest first is what the store returns and what the console renders; make
	// the ordering explicit so a future change to the store cannot silently
	// reverse the page.
	sort.SliceStable(events, func(i, j int) bool { return events[i].ID > events[j].ID })
	writeList(w, events, s.store.AppliedIndex())
}

// ---------------------------------------------------------------------------
// Static console
// ---------------------------------------------------------------------------

// spaHandler serves the built console, falling back to index.html for client
// routes so a deep link like /nodes/worker-01 loads instead of 404ing.
func (s *Server) spaHandler() http.Handler {
	fileServer := http.FileServer(s.static)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if f, err := s.static.Open(path); err == nil {
			f.Close()
			// Hashed assets are immutable; index.html must never be cached or
			// a deploy leaves browsers on the previous build indefinitely.
			if strings.HasPrefix(path, "/assets/") {
				w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
			} else {
				w.Header().Set("Cache-Control", "no-cache")
			}
			fileServer.ServeHTTP(w, r)
			return
		}
		index, err := s.static.Open("/index.html")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer index.Close()
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = io.Copy(w, index)
	})
}
