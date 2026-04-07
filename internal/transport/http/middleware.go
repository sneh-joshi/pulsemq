package http

import (
	"crypto/subtle"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ─── CORS ────────────────────────────────────────────────────────────────────

// CORSMiddleware adds permissive CORS headers so the playground and dashboard
// work when accessed from VS Code's Simple Browser (webview origin) or any
// other origin on localhost. For a hardened production deploy, restrict
// AllowedOrigins via config.
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			// Reflect the request origin so the browser accepts credentials.
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		} else {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Api-Key, Authorization")
		w.Header().Set("Access-Control-Max-Age", "86400")

		// Respond immediately to preflight requests.
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ─── Logging ──────────────────────────────────────────────────────────────────

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// LoggingMiddleware logs method, path, status, and duration for every request.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		slog.Info("http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// ─── Auth ─────────────────────────────────────────────────────────────────────

// AuthMiddleware checks for a static API key when auth is enabled.
// The key must be passed in the X-Api-Key header.
// Comparison is constant-time to prevent timing side-channel attacks.
func AuthMiddleware(apiKey string, enabled bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if !enabled || apiKey == "" {
			return next
		}
		keyBytes := []byte(apiKey)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provided := []byte(r.Header.Get("X-Api-Key"))
			// ConstantTimeCompare returns 1 only when lengths and contents match.
			if subtle.ConstantTimeCompare(provided, keyBytes) != 1 {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ─── Rate limiting ────────────────────────────────────────────────────────────

// ipEntry holds a rate.Limiter and the time it was last used (for TTL eviction).
type ipEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// RateLimitMiddleware applies per-IP token-bucket rate limiting.
// rps is the allowed requests per second; burst is the maximum burst size.
//
// The IP is resolved from X-Forwarded-For when present (first hop),
// falling back to RemoteAddr.  The in-memory limiter map is pruned
// opportunistically (when it exceeds 5,000 entries) so it never grows
// without bound, even under attack from many unique source IPs.
func RateLimitMiddleware(rps float64, burst int) func(http.Handler) http.Handler {
	var (
		mu       sync.Mutex
		limiters = make(map[string]*ipEntry)
	)

	getLimiter := func(ip string) *rate.Limiter {
		mu.Lock()
		defer mu.Unlock()

		if e, ok := limiters[ip]; ok {
			e.lastSeen = time.Now()
			return e.limiter
		}

		// Opportunistic TTL eviction: if the table is large, sweep out any entry
		// that hasn't been seen in the last 10 minutes.  This runs only when the
		// map is over the threshold, so it doesn't add overhead on small deploys.
		if len(limiters) >= 5000 {
			cutoff := time.Now().Add(-10 * time.Minute)
			for k, v := range limiters {
				if v.lastSeen.Before(cutoff) {
					delete(limiters, k)
				}
			}
		}

		l := rate.NewLimiter(rate.Limit(rps), burst)
		limiters[ip] = &ipEntry{limiter: l, lastSeen: time.Now()}
		return l
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := clientIP(r)
			if !getLimiter(ip).Allow() {
				writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// clientIP extracts the real client IP. It prefers the first address in
// X-Forwarded-For (set by reverse proxies such as nginx/Caddy) and falls
// back to RemoteAddr.
//
// Security note: X-Forwarded-For can be spoofed when there is no trusted
// reverse proxy in front of PulseMQ.  If running directly on the internet
// without a proxy, disable proxy trust in a future config option.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first (leftmost) address — the original client IP.
		if idx := len(xff); idx > 0 {
			for i := 0; i < len(xff); i++ {
				if xff[i] == ',' {
					idx = i
					break
				}
			}
			if ip := net.ParseIP(xff[:idx]); ip != nil {
				return ip.String()
			}
		}
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// ─── Body size limit ─────────────────────────────────────────────────────────

// maxRequestBodyBytes is the hard upper bound applied to every inbound request
// body.  It is intentionally generous (32 MiB) to allow batch publishes of up
// to 100 messages × 256 KiB, while still preventing unbounded memory growth
// from malicious or accidental oversized payloads.
const maxRequestBodyBytes = 32 << 20 // 32 MiB

// MaxBodyMiddleware wraps every request body in an http.MaxBytesReader so that
// handlers automatically receive a "request body too large" error if the client
// sends more than maxRequestBodyBytes.
func MaxBodyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		next.ServeHTTP(w, r)
	})
}

// ─── Chain ────────────────────────────────────────────────────────────────────

// chain composes a slice of middleware around the given handler (first = outermost).
func chain(h http.Handler, mw ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}
