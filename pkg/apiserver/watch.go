package apiserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// handleWatch streams cluster changes as Server-Sent Events.
//
// SSE rather than WebSockets: the traffic is one-directional, SSE is plain
// HTTP so it works through any proxy without an upgrade negotiation, and the
// browser reconnects on its own. A WebSocket would buy bidirectionality the
// console does not need and cost a second protocol to operate.
//
// The stream carries changes, not state. A client lists once to get a baseline
// and then applies deltas, which is what keeps a console with 5,000 workloads
// from re-fetching the entire cluster every second.
func (s *Server) handleWatch(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming_unsupported",
			"this server cannot stream responses")
		return
	}

	kinds := map[string]bool{}
	for _, k := range r.URL.Query()["kind"] {
		kinds[k] = true
	}

	watcher := s.store.Watch()
	defer watcher.Stop()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Without this, nginx and friends buffer the stream and the console appears
	// to hang.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// The first message carries the revision the client should consider its
	// baseline, so it knows whether its initial list is older or newer than the
	// stream it just joined.
	writeSSE(w, "sync", map[string]any{
		"revision": s.store.AppliedIndex(),
		"leader":   s.cp.IsLeader(),
	})
	flusher.Flush()

	// Heartbeats keep intermediaries from closing an idle connection and let
	// the client distinguish "nothing is happening" from "the link is dead".
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return

		case <-heartbeat.C:
			// A comment frame is a valid SSE keepalive and is ignored by
			// EventSource, so it never reaches application code.
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()

		case change, open := <-watcher.Changes():
			if !open {
				// The watcher fell behind and was dropped. Telling the client
				// to resync is honest; silently continuing would leave it with
				// a view that is missing changes it will never see.
				writeSSE(w, "resync", map[string]any{
					"reason":   "this stream fell behind and was dropped; re-list to resynchronize",
					"revision": s.store.AppliedIndex(),
				})
				flusher.Flush()
				return
			}
			if len(kinds) > 0 && !kinds[change.Kind] {
				continue
			}
			if !writeSSE(w, "change", change) {
				return
			}
			// Drain anything already queued before flushing, so a burst costs
			// one syscall rather than one per change.
			drained := 0
			for drained < 64 {
				select {
				case next, stillOpen := <-watcher.Changes():
					if !stillOpen {
						flusher.Flush()
						return
					}
					if len(kinds) > 0 && !kinds[next.Kind] {
						continue
					}
					if !writeSSE(w, "change", next) {
						return
					}
					drained++
				default:
					drained = 64
				}
			}
			flusher.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, event string, payload any) bool {
	body, err := json.Marshal(payload)
	if err != nil {
		return true // skip an unencodable change rather than killing the stream
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, body); err != nil {
		return false
	}
	return true
}
