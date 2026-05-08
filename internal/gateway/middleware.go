package gateway

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"apnv.id/apex/api-gateway/internal/models"
	"apnv.id/apex/api-gateway/internal/store"
	"apnv.id/apex/api-gateway/internal/logger"
	
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	circuitBreakerThreshold = 10  // failures within window before opening circuit
)

// responseCapture wraps ResponseWriter to capture status code and body size.
type responseCapture struct {
	http.ResponseWriter
	statusCode int
	written    int64
	body       *bytes.Buffer
	capture    bool
}

func newResponseCapture(w http.ResponseWriter, capture bool) *responseCapture {
	rc := &responseCapture{ResponseWriter: w, statusCode: http.StatusOK, capture: capture}
	if capture {
		rc.body = &bytes.Buffer{}
	}
	return rc
}

func (rc *responseCapture) WriteHeader(code int) {
	rc.statusCode = code
	rc.ResponseWriter.WriteHeader(code)
}

func (rc *responseCapture) Write(b []byte) (int, error) {
	n, err := rc.ResponseWriter.Write(b)
	rc.written += int64(n)
	if rc.capture && rc.body != nil && rc.body.Len() < 64*1024 { // cap at 64KB
		rc.body.Write(b[:n])
	}
	return n, err
}

// ── Middleware ─────────────────────────────────────────────────────────────

// MiddlewareChain assembles the gateway middleware stack.
type MiddlewareChain struct {
	cache  *store.Cache
	tlog   *logger.TrafficLogger
}

// NewMiddlewareChain creates a MiddlewareChain.
func NewMiddlewareChain(cache *store.Cache, tlog *logger.TrafficLogger) *MiddlewareChain {
	return &MiddlewareChain{cache: cache, tlog: tlog}
}

// Wrap applies middleware to the given route handler in order:
//  1. Request ID injection
//  2. Rate limiting
//  3. Circuit breaker check
//  4. Traffic logging
func (mc *MiddlewareChain) Wrap(route *models.Route, next http.Handler) http.Handler {
	h := mc.logging(route, next)
	h = mc.circuitBreaker(route, h)
	h = mc.rateLimit(route, h)
	h = mc.requestID(h)
	return h
}

// requestID injects a unique X-Request-ID header into every request.
func (mc *MiddlewareChain) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = uuid.New().String()
		}
		r.Header.Set("X-Request-ID", reqID)
		w.Header().Set("X-Request-ID", reqID)
		next.ServeHTTP(w, r)
	})
}

// rateLimit enforces per-IP request rate limits using Redis sliding window.
func (mc *MiddlewareChain) rateLimit(route *models.Route, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if route.RateLimit <= 0 {
			next.ServeHTTP(w, r)
			return
		}
		clientIP := extractClientIP(r)
		allowed, count, err := mc.cache.AllowRequest(r.Context(), route.ID, clientIP, route.RateLimit)
		if err != nil {
			log.Warn().Err(err).Str("route", route.ID).Msg("rate limit check error, failing open")
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("X-RateLimit-Limit", intToStr(route.RateLimit))
		w.Header().Set("X-RateLimit-Remaining", intToStr(route.RateLimit-int(count)))
		if !allowed {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// circuitBreaker opens the circuit when upstream error counts exceed threshold.
func (mc *MiddlewareChain) circuitBreaker(route *models.Route, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failures, err := mc.cache.GetFailureCount(r.Context(), route.ID)
		if err == nil && failures >= circuitBreakerThreshold {
			log.Warn().Str("route", route.ID).Int64("failures", failures).Msg("circuit open")
			w.Header().Set("X-Circuit-Breaker", "open")
			http.Error(w, "Service Temporarily Unavailable", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// logging captures request/response metadata and ships it to the TrafficLogger.
func (mc *MiddlewareChain) logging(route *models.Route, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Optionally buffer the request body for payload logging
		var reqBody []byte
		if route.LogPayload && r.Body != nil {
			const maxBody = 64 * 1024 // 64KB
			buf, _ := io.ReadAll(io.LimitReader(r.Body, maxBody))
			reqBody = buf
			r.Body = io.NopCloser(bytes.NewReader(buf))
		}

		rc := newResponseCapture(w, route.LogPayload)
		next.ServeHTTP(rc, r)

		entry := &models.TrafficLog{
			ID:           uuid.New().String(),
			RouteID:      route.ID,
			RouteName:    route.Name,
			Timestamp:    start.UTC(),
			Method:       r.Method,
			Path:         r.URL.Path,
			RawQuery:     r.URL.RawQuery,
			Protocol:     route.Protocol,
			ClientIP:     extractClientIP(r),
			StatusCode:   rc.statusCode,
			LatencyMs:    time.Since(start).Milliseconds(),
			RequestSize:  r.ContentLength,
			ResponseSize: rc.written,
		}
		if route.LogPayload {
			entry.RequestBody = reqBody
			if rc.body != nil {
				entry.ResponseBody = rc.body.Bytes()
			}
			entry.RequestHeaders = flattenHeaders(r.Header)
			entry.ResponseHeaders = flattenHeaders(w.Header())
		}
		if rc.statusCode >= 500 {
			entry.Error = http.StatusText(rc.statusCode)
			mc.cache.RecordFailure(r.Context(), route.ID)
		} else {
			mc.cache.ResetFailures(r.Context(), route.ID)
		}

		mc.tlog.Log(entry)

		log.Debug().
			Str("route", route.Name).
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", rc.statusCode).
			Int64("latency_ms", entry.LatencyMs).
			Msg("proxied request")
	})
}

// ── Helpers ────────────────────────────────────────────────────────────────

func extractClientIP(r *http.Request) string {
	for _, hdr := range []string{"X-Forwarded-For", "X-Real-IP"} {
		if v := r.Header.Get(hdr); v != "" {
			return strings.Split(v, ",")[0]
		}
	}
	ip := r.RemoteAddr
	if colon := strings.LastIndex(ip, ":"); colon != -1 {
		ip = ip[:colon]
	}
	return ip
}

func flattenHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, vs := range h {
		out[k] = strings.Join(vs, ", ")
	}
	return out
}

func intToStr(n int) string {
	return fmt.Sprintf("%d", n)
}
