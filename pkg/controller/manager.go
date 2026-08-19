// Package controller holds Orion's reconciliation loops: the components that
// drive actual cluster state toward desired state.
//
// Every controller here follows the same contract:
//
//   - Reconcile is level-triggered. It reads the world as it is now and issues
//     whatever writes close the gap. It never assumes it saw the previous
//     event, because after a leader change it did not.
//   - Reconcile is idempotent. Running it twice in a row changes nothing the
//     second time.
//   - Reconcile is safe to interrupt. A controller killed halfway through
//     leaves consistent state, because each write it makes is individually
//     valid; the next pass finishes the job.
//
// Controllers run only on the Raft leader. Running them on every replica would
// mean three schedulers racing to place the same workload.
package controller

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// Controller is one reconciliation loop.
type Controller interface {
	// Name identifies the controller in logs, metrics and events.
	Name() string
	// Reconcile performs one full pass. It must be idempotent and must respect
	// context cancellation.
	Reconcile(ctx context.Context) error
	// ResyncInterval is the maximum time between passes. Controllers are also
	// woken by state changes; the interval is the backstop that guarantees
	// convergence even if a notification is missed.
	ResyncInterval() time.Duration
}

// Observer receives per-pass outcomes, used for metrics.
type Observer interface {
	ReconcileFinished(controller string, duration time.Duration, err error)
}

// ManagerOptions configures the controller manager.
type ManagerOptions struct {
	Logger *slog.Logger
	// Leadership signals when this replica gains or loses leadership.
	Leadership <-chan bool
	// IsLeader is consulted at startup, since the channel only reports changes.
	IsLeader func() bool
	// Trigger, when closed or written to, wakes every controller. The manager
	// coalesces bursts so a thousand state changes cause one extra pass, not a
	// thousand.
	Trigger <-chan struct{}

	Observer Observer

	// BaseBackoff and MaxBackoff bound the retry delay after a failed pass.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// Manager runs controllers while this replica holds leadership.
type Manager struct {
	opts        ManagerOptions
	log         *slog.Logger
	controllers []Controller

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
	wg      sync.WaitGroup

	// wake carries coalesced trigger notifications to each controller loop.
	wakeMu sync.Mutex
	wakes  []chan struct{}
}

func NewManager(opts ManagerOptions) *Manager {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.BaseBackoff == 0 {
		opts.BaseBackoff = 200 * time.Millisecond
	}
	if opts.MaxBackoff == 0 {
		opts.MaxBackoff = 30 * time.Second
	}
	return &Manager{opts: opts, log: opts.Logger.With("component", "controller-manager")}
}

func (m *Manager) Register(c ...Controller) { m.controllers = append(m.controllers, c...) }

// Run blocks until ctx is cancelled, starting and stopping the controllers as
// leadership comes and goes.
func (m *Manager) Run(ctx context.Context) {
	if m.opts.IsLeader != nil && m.opts.IsLeader() {
		m.startControllers(ctx)
	}

	fanout := make(chan struct{}, 1)
	if m.opts.Trigger != nil {
		go m.fanTriggers(ctx, fanout)
	}

	for {
		select {
		case <-ctx.Done():
			m.stopControllers()
			return

		case isLeader, ok := <-m.opts.Leadership:
			if !ok {
				m.stopControllers()
				return
			}
			if isLeader {
				m.log.Info("acquired leadership, starting controllers", "count", len(m.controllers))
				m.startControllers(ctx)
			} else {
				m.log.Info("lost leadership, stopping controllers")
				m.stopControllers()
			}

		case <-fanout:
			m.broadcastWake()
		}
	}
}

// fanTriggers coalesces a burst of state changes into a single wake-up. A
// deployment rollout produces dozens of changes in a few milliseconds; each one
// does not deserve its own full reconcile pass.
func (m *Manager) fanTriggers(ctx context.Context, out chan<- struct{}) {
	const debounce = 50 * time.Millisecond
	var timer *time.Timer
	var timerC <-chan time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.opts.Trigger:
			if timer == nil {
				timer = time.NewTimer(debounce)
				timerC = timer.C
			}
		case <-timerC:
			timer, timerC = nil, nil
			select {
			case out <- struct{}{}:
			default:
			}
		}
	}
}

func (m *Manager) startControllers(parent context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	m.cancel = cancel
	m.running = true

	m.wakeMu.Lock()
	m.wakes = m.wakes[:0]
	for range m.controllers {
		m.wakes = append(m.wakes, make(chan struct{}, 1))
	}
	wakes := append([]chan struct{}(nil), m.wakes...)
	m.wakeMu.Unlock()

	for i, c := range m.controllers {
		m.wg.Add(1)
		go func(c Controller, wake <-chan struct{}) {
			defer m.wg.Done()
			m.runController(ctx, c, wake)
		}(c, wakes[i])
	}
}

func (m *Manager) stopControllers() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	cancel := m.cancel
	m.running = false
	m.mu.Unlock()

	cancel()
	m.wg.Wait()
}

func (m *Manager) broadcastWake() {
	m.wakeMu.Lock()
	defer m.wakeMu.Unlock()
	for _, ch := range m.wakes {
		select {
		case ch <- struct{}{}:
		default: // a wake is already pending; one is enough
		}
	}
}

// runController is the retry loop for a single controller.
func (m *Manager) runController(ctx context.Context, c Controller, wake <-chan struct{}) {
	log := m.log.With("controller", c.Name())
	backoff := m.opts.BaseBackoff
	interval := c.ResyncInterval()

	// Stagger the first pass so that N controllers do not all hammer the store
	// in the same millisecond after a leader election.
	jitter := time.Duration(rand.Int63n(int64(200 * time.Millisecond)))
	select {
	case <-time.After(jitter):
	case <-ctx.Done():
		return
	}

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

		start := time.Now()
		err := c.Reconcile(ctx)
		elapsed := time.Since(start)

		if m.opts.Observer != nil {
			m.opts.Observer.ReconcileFinished(c.Name(), elapsed, err)
		}

		next := interval
		switch {
		case err == nil:
			backoff = m.opts.BaseBackoff
		case errors.Is(err, context.Canceled):
			return
		default:
			// Exponential backoff with jitter. Without jitter, several
			// controllers failing on the same downstream problem would retry in
			// lockstep and keep it saturated.
			log.Warn("reconcile failed", "err", err, "retryIn", backoff, "duration", elapsed)
			next = backoff + time.Duration(rand.Int63n(int64(backoff/2+1)))
			if backoff < m.opts.MaxBackoff {
				backoff *= 2
				if backoff > m.opts.MaxBackoff {
					backoff = m.opts.MaxBackoff
				}
			}
		}
		if next <= 0 {
			next = time.Second
		}
		timer.Reset(next)
	}
}

// Running reports whether controllers are currently active on this replica.
func (m *Manager) Running() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}
