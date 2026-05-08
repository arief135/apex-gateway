package admin

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"apnv.id/apex/api-gateway/internal/config"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

// Server is the admin management API HTTP server.
type Server struct {
	cfg      *config.AdminConfig
	handlers *Handlers
	srv      *http.Server
}

// NewServer wires up the admin API routes and returns a ready Server.
func NewServer(cfg *config.AdminConfig, handlers *Handlers) *Server {
	s := &Server{cfg: cfg, handlers: handlers}
	s.srv = &http.Server{
		Addr:         cfg.Addr,
		Handler:      s.buildRouter(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

// Start begins listening. Blocks until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	log.Info().Str("addr", s.cfg.Addr).Msg("admin API listening")
	errCh := make(chan error, 1)
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.srv.Shutdown(shutCtx)
	}
}

// ── Router ─────────────────────────────────────────────────────────────────

func (s *Server) buildRouter() http.Handler {
	r := mux.NewRouter()
	r.Use(corsMiddleware)
	r.Use(requestLogMiddleware)

	// Public
	r.HandleFunc("/admin/health", s.handlers.Health).Methods(http.MethodGet)

	// Authenticated routes
	api := r.PathPrefix("/admin").Subrouter()
	api.Use(s.jwtMiddleware)

	// Routes
	api.HandleFunc("/routes",              s.handlers.ListRoutes).Methods(http.MethodGet)
	api.HandleFunc("/routes",              s.handlers.CreateRoute).Methods(http.MethodPost)
	api.HandleFunc("/routes/{id}",         s.handlers.GetRoute).Methods(http.MethodGet)
	api.HandleFunc("/routes/{id}",         s.handlers.UpdateRoute).Methods(http.MethodPut)
	api.HandleFunc("/routes/{id}",         s.handlers.DeleteRoute).Methods(http.MethodDelete)
	api.HandleFunc("/routes/{id}/status",  s.handlers.SetRouteStatus).Methods(http.MethodPatch)

	// Observability
	api.HandleFunc("/logs",  s.handlers.ListLogs).Methods(http.MethodGet)
	api.HandleFunc("/stats", s.handlers.GetStats).Methods(http.MethodGet)

	return r
}

// ── Middleware ─────────────────────────────────────────────────────────────

// jwtMiddleware validates Bearer tokens on protected admin endpoints.
func (s *Server) jwtMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			w.Header().Set("WWW-Authenticate", `Bearer realm="admin"`)
			http.Error(w, `{"error":"missing or invalid Authorization header"}`, http.StatusUnauthorized)
			return
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return []byte(s.cfg.JWTSecret), nil
		})
		if err != nil || !token.Valid {
			http.Error(w, `{"error":"invalid token"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// corsMiddleware adds permissive CORS headers for the dashboard SPA.
// Tighten AllowedOrigins in production.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Request-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requestLogMiddleware logs every admin API request at INFO level.
func requestLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rc := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rc, r)
		log.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", rc.statusCode).
			Dur("latency", time.Since(start)).
			Msg("admin request")
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.statusCode = code
	sr.ResponseWriter.WriteHeader(code)
}
