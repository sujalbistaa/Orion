package store

import (
	"sync"
	"sync/atomic"
)

// Change is a single observed mutation, delivered to watchers so the console
// and controllers can react without polling the whole cluster state.
type Change struct {
	Revision uint64 `json:"revision"`
	Kind     string `json:"kind"`
	Op       string `json:"op"` // Created | Updated | Deleted
	Name     string `json:"name"`
	Object   any    `json:"object,omitempty"`
}

// Watcher receives changes. A watcher that cannot keep up is marked stale and
// closed rather than allowed to grow without bound or to block the apply loop —
// the consumer's job is then to resync from a full list, which is cheap because
// cluster state is small.
type Watcher struct {
	ch      chan Change
	stale   atomic.Bool
	closeMu sync.Mutex
	closed  bool

	registry *watchRegistry
	id       uint64
}

// Changes is the stream of mutations. It is closed when the watcher is stopped
// or has fallen too far behind.
func (w *Watcher) Changes() <-chan Change { return w.ch }

// Stale reports whether this watcher missed changes and must resync from a
// full list before trusting further events.
func (w *Watcher) Stale() bool { return w.stale.Load() }

// Stop unregisters the watcher. It is safe to call more than once.
func (w *Watcher) Stop() { w.registry.remove(w) }

func (w *Watcher) close() {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()
	if !w.closed {
		w.closed = true
		close(w.ch)
	}
}

// watchBuffer is how many changes a slow watcher may fall behind before it is
// dropped. Sized for a console tab that briefly stalls, not for one that has
// gone away.
const watchBuffer = 256

type watchRegistry struct {
	mu       sync.RWMutex
	watchers map[uint64]*Watcher
	nextID   uint64

	// pending accumulates changes produced while the store lock is held. They
	// are broadcast after the lock is released so a slow watcher can never
	// stall the Raft apply loop.
	pending []Change
}

func newWatchRegistry() *watchRegistry {
	return &watchRegistry{watchers: map[uint64]*Watcher{}}
}

// queue is called with the store lock held.
func (r *watchRegistry) queue(c Change) { r.pending = append(r.pending, c) }

// drainPending is called with the store lock held.
func (r *watchRegistry) drainPending() []Change {
	if len(r.pending) == 0 {
		return nil
	}
	out := r.pending
	r.pending = nil
	return out
}

func (r *watchRegistry) broadcast(changes []Change) {
	if len(changes) == 0 {
		return
	}
	r.mu.RLock()
	targets := make([]*Watcher, 0, len(r.watchers))
	for _, w := range r.watchers {
		targets = append(targets, w)
	}
	r.mu.RUnlock()

	var dropped []*Watcher
	for _, w := range targets {
		for _, c := range changes {
			select {
			case w.ch <- c:
			default:
				w.stale.Store(true)
				dropped = append(dropped, w)
			}
			if w.stale.Load() {
				break
			}
		}
	}
	for _, w := range dropped {
		r.remove(w)
	}
}

func (r *watchRegistry) add() *Watcher {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	w := &Watcher{ch: make(chan Change, watchBuffer), registry: r, id: r.nextID}
	r.watchers[w.id] = w
	return w
}

func (r *watchRegistry) remove(w *Watcher) {
	r.mu.Lock()
	_, ok := r.watchers[w.id]
	delete(r.watchers, w.id)
	r.mu.Unlock()
	if ok {
		w.close()
	}
}

// Watch registers a change stream. The caller must call Stop.
func (st *Store) Watch() *Watcher { return st.watchers.add() }

// WatcherCount reports active watchers, exported for the metrics endpoint.
func (st *Store) WatcherCount() int {
	st.watchers.mu.RLock()
	defer st.watchers.mu.RUnlock()
	return len(st.watchers.watchers)
}
