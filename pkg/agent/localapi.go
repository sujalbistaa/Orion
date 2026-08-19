package agent

import (
	"crypto/subtle"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/sujalbistaa/orion/pkg/runtime"
)

// LocalAPI is the small HTTP surface the agent exposes on the node.
//
// It exists for one reason: logs. Container output is high-volume and only
// wanted on demand, so it is fetched directly from the node rather than pushed
// through heartbeats or the replicated log — where a single `logs -f` on a
// chatty container could dominate the cluster's write path.
//
// The endpoint is authenticated with the cluster key. Container logs routinely
// contain credentials and personal data; an unauthenticated port that streams
// them would be a worse vulnerability than anything else in Orion.
type LocalAPI struct {
	agent      *Agent
	clusterKey string
}

func (a *Agent) LocalAPI(clusterKey string) *LocalAPI {
	return &LocalAPI{agent: a, clusterKey: clusterKey}
}

func (l *LocalAPI) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /logs", l.authenticated(l.handleLogs))
	mux.HandleFunc("GET /healthz", l.handleHealth)
	mux.HandleFunc("GET /status", l.authenticated(l.handleStatus))
	return mux
}

func (l *LocalAPI) authenticated(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if l.clusterKey != "" {
			got := r.Header.Get("X-Orion-Cluster-Key")
			if subtle.ConstantTimeCompare([]byte(got), []byte(l.clusterKey)) != 1 {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
		}
		next(w, r)
	}
}

// handleHealth is unauthenticated so a process supervisor can use it. It
// reveals only liveness.
func (l *LocalAPI) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	if l.agent.Fenced() {
		// A fenced agent is running but is deliberately not doing its job.
		// Reporting it healthy would hide an active incident.
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("fenced: no contact with the control plane\n"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (l *LocalAPI) handleStatus(w http.ResponseWriter, r *http.Request) {
	a := l.agent
	a.mu.Lock()
	workloads := len(a.workloads)
	a.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"node":"` + a.cfg.NodeName +
		`","workloads":` + strconv.Itoa(workloads) +
		`,"fenced":` + strconv.FormatBool(a.Fenced()) +
		`,"lastContact":"` + a.LastContact().UTC().Format(time.RFC3339) + `"}`))
}

func (l *LocalAPI) handleLogs(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("workload")
	if name == "" {
		http.Error(w, "workload is required", http.StatusBadRequest)
		return
	}

	l.agent.mu.Lock()
	managed, ok := l.agent.workloads[name]
	l.agent.mu.Unlock()
	if !ok {
		http.Error(w, "this node is not running that workload", http.StatusNotFound)
		return
	}
	managed.mu.Lock()
	containerID := managed.containerID
	managed.mu.Unlock()
	if containerID == "" {
		http.Error(w, "the workload has no container yet", http.StatusConflict)
		return
	}

	tail := 200
	if v := r.URL.Query().Get("tail"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 10000 {
			tail = n
		}
	}
	follow := r.URL.Query().Get("follow") == "true"

	stream, err := l.agent.rt.Logs(r.Context(), containerID, runtime.LogOptions{
		Tail: tail, Follow: follow, Timestamps: true,
	})
	if err != nil {
		http.Error(w, "could not read logs: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer stream.Close()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			if err != io.EOF {
				return
			}
			return
		}
		if r.Context().Err() != nil {
			return
		}
	}
}
