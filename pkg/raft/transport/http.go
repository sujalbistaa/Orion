// Package transport carries Raft messages between control-plane replicas.
//
// The wire protocol is a POST of one binary-encoded message to /raft/message.
// HTTP was chosen over gRPC here deliberately: Raft already tolerates loss,
// delay, duplication and reordering, so the streaming, flow control and
// deadline machinery gRPC provides would be paid for and then ignored. Agent
// traffic, which is high-volume and needs streaming, does use gRPC.
package transport

import (
	"bytes"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sujalbistaa/orion/pkg/raft"
)

// maxMessageBytes bounds an inbound frame. A snapshot is the largest legitimate
// message; anything past this is either a bug or an attack.
const maxMessageBytes = 256 << 20

// sendQueueDepth is how many messages may be outstanding to one peer before
// new ones are dropped. Dropping is the correct behaviour: Raft retries, and a
// queue that grows without bound turns one slow peer into a memory leak.
const sendQueueDepth = 512

// HTTPTransport implements raft.Transport.
type HTTPTransport struct {
	selfID  uint64
	client  *http.Client
	log     *slog.Logger
	deliver func(raft.Message)
	authKey string

	mu    sync.RWMutex
	peers map[uint64]*peer

	closed atomic.Bool
	wg     sync.WaitGroup
}

type peer struct {
	id      uint64
	addr    string
	queue   chan raft.Message
	stop    chan struct{}
	dropped atomic.Uint64
	sent    atomic.Uint64
	failed  atomic.Uint64
}

// Options configures the transport.
type Options struct {
	SelfID uint64
	// Deliver is called for every inbound message. It must not block.
	Deliver func(raft.Message)
	Logger  *slog.Logger
	// AuthKey is a shared secret required on every peer request. Raft messages
	// can force a leader change and rewrite the log, so the peer port must not
	// be open to anything that can reach it on the network.
	AuthKey string
	// DialTimeout and RequestTimeout bound peer calls. They must stay well
	// under the election timeout, or a hung TCP connection would look like a
	// dead leader.
	DialTimeout    time.Duration
	RequestTimeout time.Duration
}

func NewHTTP(opts Options) *HTTPTransport {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 500 * time.Millisecond
	}
	if opts.RequestTimeout == 0 {
		opts.RequestTimeout = 2 * time.Second
	}
	return &HTTPTransport{
		selfID:  opts.SelfID,
		deliver: opts.Deliver,
		log:     opts.Logger.With("component", "raft-transport", "node", opts.SelfID),
		authKey: opts.AuthKey,
		peers:   map[uint64]*peer{},
		client: &http.Client{
			Timeout: opts.RequestTimeout,
			Transport: &http.Transport{
				DialContext:         (&net.Dialer{Timeout: opts.DialTimeout}).DialContext,
				MaxIdleConnsPerHost: 4,
				IdleConnTimeout:     60 * time.Second,
				// Peer traffic is many small messages; disabling Nagle keeps
				// heartbeat latency at the wire minimum.
				DisableCompression: true,
			},
		},
	}
}

// Send queues a message. It never blocks: a peer that cannot keep up loses
// messages, which Raft treats as ordinary network loss.
func (t *HTTPTransport) Send(m raft.Message) {
	if t.closed.Load() {
		return
	}
	t.mu.RLock()
	p, ok := t.peers[m.To]
	t.mu.RUnlock()
	if !ok {
		return
	}
	select {
	case p.queue <- m:
	default:
		if n := p.dropped.Add(1); n%100 == 1 {
			t.log.Warn("dropping raft message: peer send queue is full",
				"peer", m.To, "type", m.Type, "dropped", n)
		}
	}
}

func (t *HTTPTransport) AddPeer(id uint64, addr string) {
	if id == t.selfID || addr == "" {
		return
	}
	t.mu.Lock()
	if existing, ok := t.peers[id]; ok {
		if existing.addr == addr {
			t.mu.Unlock()
			return
		}
		// The address changed; retire the old sender before starting a new one.
		close(existing.stop)
		delete(t.peers, id)
	}
	p := &peer{id: id, addr: addr, queue: make(chan raft.Message, sendQueueDepth), stop: make(chan struct{})}
	t.peers[id] = p
	t.mu.Unlock()

	t.wg.Add(1)
	go t.runPeer(p)
	t.log.Info("peer added", "peer", id, "addr", addr)
}

func (t *HTTPTransport) RemovePeer(id uint64) {
	t.mu.Lock()
	p, ok := t.peers[id]
	delete(t.peers, id)
	t.mu.Unlock()
	if ok {
		close(p.stop)
		t.log.Info("peer removed", "peer", id)
	}
}

// runPeer owns all sending to one peer, so ordering to a given peer is
// preserved and a slow peer cannot block any other.
func (t *HTTPTransport) runPeer(p *peer) {
	defer t.wg.Done()
	for {
		select {
		case <-p.stop:
			return
		case m := <-p.queue:
			t.post(p, m)
		}
	}
}

func (t *HTTPTransport) post(p *peer, m raft.Message) {
	body := raft.EncodeMessage(m)
	url := "http://" + p.addr + "/raft/message"

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.log.Error("building raft request", "peer", p.id, "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if t.authKey != "" {
		req.Header.Set("X-Orion-Cluster-Key", t.authKey)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		// Unreachable peers are the normal state during a partition; log at a
		// low rate rather than on every heartbeat.
		if n := p.failed.Add(1); n%200 == 1 {
			t.log.Warn("raft peer unreachable", "peer", p.id, "addr", p.addr, "failures", n, "err", err)
		}
		return
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		if n := p.failed.Add(1); n%200 == 1 {
			t.log.Warn("raft peer rejected message", "peer", p.id, "status", resp.StatusCode, "failures", n)
		}
		return
	}
	p.sent.Add(1)
	p.failed.Store(0)
}

// Handler serves inbound peer messages. Mount it at /raft/message.
func (t *HTTPTransport) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if t.authKey != "" && r.Header.Get("X-Orion-Cluster-Key") != t.authKey {
			// No detail in the response: an unauthenticated caller learns
			// nothing about whether the key was close.
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		t.mu.RLock()
		deliver := t.deliver
		t.mu.RUnlock()
		if deliver == nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxMessageBytes))
		if err != nil {
			http.Error(w, "message too large or unreadable", http.StatusBadRequest)
			return
		}
		m, err := raft.DecodeMessage(body)
		if err != nil {
			t.log.Warn("discarding malformed raft message", "remote", r.RemoteAddr, "err", err)
			http.Error(w, "malformed message", http.StatusBadRequest)
			return
		}
		if m.To != t.selfID {
			// Misrouted: a stale peer table somewhere. Say so rather than
			// stepping a message meant for a different server.
			http.Error(w, "message addressed to a different node", http.StatusConflict)
			return
		}
		deliver(m)
		w.WriteHeader(http.StatusOK)
	})
}

// PeerStats is per-peer transport health, surfaced on the cluster page so an
// operator can see which replica link is failing.
type PeerStats struct {
	ID      uint64 `json:"id"`
	Address string `json:"address"`
	Sent    uint64 `json:"sent"`
	Dropped uint64 `json:"dropped"`
	// ConsecutiveFailures is zero when the last send succeeded.
	ConsecutiveFailures uint64 `json:"consecutiveFailures"`
	Reachable           bool   `json:"reachable"`
}

func (t *HTTPTransport) Stats() []PeerStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]PeerStats, 0, len(t.peers))
	for _, p := range t.peers {
		failures := p.failed.Load()
		out = append(out, PeerStats{
			ID:                  p.id,
			Address:             p.addr,
			Sent:                p.sent.Load(),
			Dropped:             p.dropped.Load(),
			ConsecutiveFailures: failures,
			Reachable:           failures == 0,
		})
	}
	return out
}

// PeerAddress returns a peer's address.
func (t *HTTPTransport) PeerAddress(id uint64) (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	p, ok := t.peers[id]
	if !ok {
		return "", false
	}
	return p.addr, true
}

func (t *HTTPTransport) Close() error {
	if t.closed.Swap(true) {
		return nil
	}
	t.mu.Lock()
	for id, p := range t.peers {
		close(p.stop)
		delete(t.peers, id)
	}
	t.mu.Unlock()
	t.wg.Wait()
	t.client.CloseIdleConnections()
	return nil
}

var _ raft.Transport = (*HTTPTransport)(nil)

// Loopback is an in-process transport used by integration tests that need real
// goroutines and real timing but no sockets.
type Loopback struct {
	mu    sync.RWMutex
	nodes map[uint64]func(raft.Message)
	// Drop, when set, decides whether a message is discarded. It is the hook
	// integration tests use to partition a cluster.
	Drop func(from, to uint64) bool
}

func NewLoopback() *Loopback { return &Loopback{nodes: map[uint64]func(raft.Message){}} }

// Register connects a node's Step function to the loopback fabric.
func (l *Loopback) Register(id uint64, step func(raft.Message)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.nodes[id] = step
}

// For returns a raft.Transport view for one node.
func (l *Loopback) For(id uint64) raft.Transport { return &loopbackPort{fabric: l, self: id} }

type loopbackPort struct {
	fabric *Loopback
	self   uint64
}

func (p *loopbackPort) Send(m raft.Message) {
	p.fabric.mu.RLock()
	step, ok := p.fabric.nodes[m.To]
	drop := p.fabric.Drop
	p.fabric.mu.RUnlock()
	if !ok {
		return
	}
	if drop != nil && drop(m.From, m.To) {
		return
	}
	// Delivered on a fresh goroutine so Send never blocks the sender's
	// consensus loop, matching the real transport's contract.
	go step(m)
}

func (p *loopbackPort) AddPeer(uint64, string) {}
func (p *loopbackPort) RemovePeer(uint64)      {}
func (p *loopbackPort) Close() error           { return nil }

var _ raft.Transport = (*loopbackPort)(nil)

// SetDeliver installs the inbound message handler after construction. The
// transport and the Raft node each need a reference to the other; this breaks
// the cycle without a package-level registry.
func (t *HTTPTransport) SetDeliver(f func(raft.Message)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deliver = f
}
