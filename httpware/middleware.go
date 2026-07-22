// Package httpware provides small, optional HTTP middleware helpers for agent
// servers (remote, a2a, or custom). It does not depend on tRPC or cloud SDKs.
package httpware

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"
)

// BearerAuth rejects requests missing a matching Authorization bearer token.
// Empty token disables the middleware (always allows).
func BearerAuth(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if strings.TrimSpace(token) == "" {
			return next
		}
		want := "Bearer " + token
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Header.Get("Authorization") != want {
				http.Error(writer, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(writer, request)
		})
	}
}

// SessionFromHeader copies a header value into request context via a callback
// key function. Prefer application-specific context keys in real services.
type contextKey string

// SessionIDHeader is the default session correlation header.
const SessionIDHeader = "X-Session-ID"

// SessionIDKey is the context key for the session id middleware.
const SessionIDKey contextKey = "langgraph-go/session-id"

// WithSessionID stores request.Header[SessionIDHeader] on the context when set.
func WithSessionID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if id := strings.TrimSpace(request.Header.Get(SessionIDHeader)); id != "" {
			request = request.WithContext(context.WithValue(request.Context(), SessionIDKey, id))
		}
		next.ServeHTTP(writer, request)
	})
}

// RateLimit is a trivial token-bucket per remote IP for demos and local gateways.
// It is not a distributed limiter.
type RateLimit struct {
	mu       sync.Mutex
	interval time.Duration
	burst    int
	clients  map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

// NewRateLimit allows burst events, refilling one token every interval.
func NewRateLimit(interval time.Duration, burst int) *RateLimit {
	if interval <= 0 {
		interval = time.Second
	}
	if burst <= 0 {
		burst = 1
	}
	return &RateLimit{interval: interval, burst: burst, clients: make(map[string]*bucket)}
}

// Middleware enforces the limit.
func (r *RateLimit) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if r == nil {
			next.ServeHTTP(writer, request)
			return
		}
		key := request.RemoteAddr
		if !r.allow(key, time.Now()) {
			http.Error(writer, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func (r *RateLimit) allow(key string, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.clients[key]
	if b == nil {
		r.clients[key] = &bucket{tokens: float64(r.burst - 1), last: now}
		return true
	}
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed / r.interval.Seconds()
	if b.tokens > float64(r.burst) {
		b.tokens = float64(r.burst)
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Chain applies middlewares in order (first is outermost).
func Chain(handler http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		if middlewares[i] != nil {
			handler = middlewares[i](handler)
		}
	}
	return handler
}
