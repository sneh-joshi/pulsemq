package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	transphttp "github.com/sneh-joshi/pulsemq/internal/transport/http"
)

// ─── CORS ────────────────────────────────────────────────────────────────────

func TestCORSMiddleware_WithOrigin(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := transphttp.CORSMiddleware(next)

	req := httptest.NewRequest("GET", "/health", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:3000" {
		t.Errorf("CORS origin: want %q, got %q", "http://localhost:3000", got)
	}
}

func TestCORSMiddleware_NoOrigin(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := transphttp.CORSMiddleware(next)

	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("CORS no-origin: want *, got %q", got)
	}
}

func TestCORSMiddleware_Preflight(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next handler should not be called for OPTIONS preflight")
	})
	h := transphttp.CORSMiddleware(next)

	req := httptest.NewRequest("OPTIONS", "/health", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("CORS preflight: want 204, got %d", rr.Code)
	}
}

// ─── Logging ─────────────────────────────────────────────────────────────────

func TestLoggingMiddleware(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := transphttp.LoggingMiddleware(next)

	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("logging passthrough: want 200, got %d", rr.Code)
	}
}

// ─── Auth ─────────────────────────────────────────────────────────────────────

func TestAuthMiddleware_Disabled(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	h := transphttp.AuthMiddleware("secret", false)(next)

	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if !called {
		t.Error("auth disabled: next handler should be called")
	}
}

func TestAuthMiddleware_EmptyKey(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	// enabled=true but key="" → behaves as disabled
	h := transphttp.AuthMiddleware("", true)(next)

	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if !called {
		t.Error("auth with empty key: next handler should be called")
	}
}

func TestAuthMiddleware_ValidKey(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	h := transphttp.AuthMiddleware("mysecret", true)(next)

	req := httptest.NewRequest("GET", "/health", nil)
	req.Header.Set("X-Api-Key", "mysecret")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if !called {
		t.Error("auth valid key: next handler should be called")
	}
	if rr.Code != http.StatusOK {
		t.Errorf("auth valid key: want 200, got %d", rr.Code)
	}
}

func TestAuthMiddleware_InvalidKey(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next handler should not be called with wrong API key")
	})
	h := transphttp.AuthMiddleware("mysecret", true)(next)

	req := httptest.NewRequest("GET", "/health", nil)
	req.Header.Set("X-Api-Key", "wrongkey")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("auth invalid key: want 401, got %d", rr.Code)
	}
}

func TestAuthMiddleware_MissingKey(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next handler should not be called without API key")
	})
	h := transphttp.AuthMiddleware("mysecret", true)(next)

	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("auth missing key: want 401, got %d", rr.Code)
	}
}

// ─── Rate limiting ────────────────────────────────────────────────────────────

func TestRateLimitMiddleware_AllowsUnderLimit(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// High limit so test requests always pass.
	h := transphttp.RateLimitMiddleware(1000, 1000)(next)

	req := httptest.NewRequest("GET", "/health", nil)
	req.RemoteAddr = "1.2.3.4:5678"
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("rate limit allow: want 200, got %d", rr.Code)
	}
}

func TestRateLimitMiddleware_BlocksOverLimit(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// rps=1, burst=1: second request from same IP should be rejected.
	h := transphttp.RateLimitMiddleware(1, 1)(next)

	makeReq := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/health", nil)
		req.RemoteAddr = "10.0.0.1:1234"
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}

	makeReq() // consume the burst
	rr := makeReq()
	if rr.Code != http.StatusTooManyRequests {
		t.Errorf("rate limit block: want 429, got %d", rr.Code)
	}
}

func TestRateLimitMiddleware_XForwardedFor(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := transphttp.RateLimitMiddleware(1000, 1000)(next)

	req := httptest.NewRequest("GET", "/health", nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.5, 10.0.0.1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("rate limit XFF: want 200, got %d", rr.Code)
	}
}

// ─── Max body ─────────────────────────────────────────────────────────────────

func TestMaxBodyMiddleware_SmallBody(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := transphttp.MaxBodyMiddleware(next)

	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"body":"hello"}`))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("max body small: want 200, got %d", rr.Code)
	}
}
