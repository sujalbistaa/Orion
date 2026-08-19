package apiserver

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Role is a coarse permission level. Orion has two, because a control plane
// with a fine-grained permission model nobody configures correctly is worse
// than one with a boundary people actually understand.
type Role string

const (
	// RoleViewer may read everything and change nothing.
	RoleViewer Role = "viewer"
	// RoleOperator may read and write, including destructive operations.
	RoleOperator Role = "operator"
)

// Principal is an authenticated caller.
type Principal struct {
	Name string
	Role Role
}

// CanWrite reports whether this principal may change cluster state.
func (p Principal) CanWrite() bool { return p.Role == RoleOperator }

// Authenticator resolves a request to a principal.
type Authenticator interface {
	Authenticate(r *http.Request) (Principal, error)
}

// ErrUnauthenticated is returned when no valid credential was presented.
var ErrUnauthenticated = errors.New("apiserver: unauthenticated")

// TokenAuth authenticates bearer tokens against a static table.
//
// Static tokens are the right level of mechanism here: Orion is an
// infrastructure control plane, not a consumer product, and an OIDC
// integration would be a large amount of code protecting the same boundary. The
// tokens are compared in constant time and stored hashed, so a heap dump or a
// timing measurement does not hand them over.
type TokenAuth struct {
	mu sync.RWMutex
	// byHash maps sha256(token) -> principal.
	byHash map[string]Principal
}

func NewTokenAuth() *TokenAuth {
	return &TokenAuth{byHash: map[string]Principal{}}
}

// AddToken registers a token. The plaintext is not retained.
func (a *TokenAuth) AddToken(token, name string, role Role) error {
	if len(token) < 16 {
		// A short token is brute-forceable over a network in minutes.
		return errors.New("apiserver: API tokens must be at least 16 characters")
	}
	sum := sha256.Sum256([]byte(token))
	a.mu.Lock()
	defer a.mu.Unlock()
	a.byHash[hex.EncodeToString(sum[:])] = Principal{Name: name, Role: role}
	return nil
}

func (a *TokenAuth) Authenticate(r *http.Request) (Principal, error) {
	token := bearerToken(r)
	if token == "" {
		return Principal{}, ErrUnauthenticated
	}
	sum := sha256.Sum256([]byte(token))
	want := hex.EncodeToString(sum[:])

	a.mu.RLock()
	defer a.mu.RUnlock()
	// Compare every entry in constant time. Returning early on the first
	// mismatch would leak how many tokens share a prefix.
	var found Principal
	matched := 0
	for hash, principal := range a.byHash {
		if subtle.ConstantTimeCompare([]byte(hash), []byte(want)) == 1 {
			found = principal
			matched = 1
		}
	}
	if matched == 0 {
		return Principal{}, ErrUnauthenticated
	}
	return found, nil
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		return ""
	}
	const prefix = "Bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}

// openAccess is used when no authenticator is configured. It grants operator
// rights to everyone and is only reachable after a startup warning.
type openAccess struct{}

func (openAccess) Authenticate(*http.Request) (Principal, error) {
	return Principal{Name: "anonymous", Role: RoleOperator}, nil
}

// principalKey carries the authenticated caller through a request context.
type principalKey struct{}

func withPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalOf returns the authenticated caller.
func PrincipalOf(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

func actorOf(ctx context.Context) string {
	if p, ok := PrincipalOf(ctx); ok {
		return p.Name
	}
	return "unknown"
}

// unauthenticatedPaths are reachable without a credential. Health checks are
// consumed by load balancers and orchestrators that cannot hold one; metrics
// are exposed because a Prometheus scrape config with a bearer token is a
// common operational footgun and the endpoint reveals no secrets.
var unauthenticatedPaths = map[string]bool{
	"/healthz": true,
	"/readyz":  true,
	"/metrics": true,
}

func (s *Server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if unauthenticatedPaths[r.URL.Path] || !strings.HasPrefix(r.URL.Path, "/api/") {
			// Static console assets are public; every byte they contain is
			// already shipped to any browser that loads the page.
			next.ServeHTTP(w, r)
			return
		}

		principal, err := s.auth.Authenticate(r)
		if err != nil {
			// The challenge tells a client how to authenticate without
			// revealing whether a token was close to valid.
			w.Header().Set("WWW-Authenticate", `Bearer realm="orion"`)
			writeError(w, http.StatusUnauthorized, "unauthenticated",
				"a valid bearer token is required")
			return
		}

		if isWrite(r.Method) {
			if !principal.CanWrite() {
				writeError(w, http.StatusForbidden, "forbidden",
					"this token has read-only access")
				return
			}
			// Writes go into the Raft log, so a runaway client can degrade the
			// whole cluster rather than just itself. Reads are unlimited: they
			// are served from local memory and cost nothing to agree on.
			if !s.writeLimiter.allow(principal.Name, time.Now()) {
				w.Header().Set("Retry-After", "1")
				writeError(w, http.StatusTooManyRequests, "rate_limited",
					"too many write requests; this limit protects the replicated log")
				return
			}
		}

		next.ServeHTTP(w, r.WithContext(withPrincipal(r.Context(), principal)))
	})
}

func isWrite(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	default:
		return true
	}
}

// rateLimiter bounds write requests per principal.
//
// It exists to stop a runaway client from filling the Raft log, not to enforce
// a quota. Reads are not limited: they are served from local memory and cost
// nothing the cluster has to agree on.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	rate    float64
	burst   float64
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(perSecond, burst float64) *rateLimiter {
	return &rateLimiter{buckets: map[string]*tokenBucket{}, rate: perSecond, burst: burst}
}

func (l *rateLimiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		b = &tokenBucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
