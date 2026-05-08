package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"apnv.id/apex/api-gateway/internal/gateway"
	"apnv.id/apex/api-gateway/internal/logger"
	"apnv.id/apex/api-gateway/internal/models"
	"apnv.id/apex/api-gateway/internal/store"
	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

// Handlers holds all admin API handler dependencies.
type Handlers struct {
	routeStore *store.RouteStore
	cache      *store.Cache
	router     *gateway.Router
	tlog       *logger.TrafficLogger
}

// NewHandlers creates an Handlers instance.
func NewHandlers(
	routeStore *store.RouteStore,
	cache *store.Cache,
	router *gateway.Router,
	tlog *logger.TrafficLogger,
) *Handlers {
	return &Handlers{routeStore: routeStore, cache: cache, router: router, tlog: tlog}
}

// ── Routes API ─────────────────────────────────────────────────────────────

// ListRoutes   GET /admin/routes
func (h *Handlers) ListRoutes(w http.ResponseWriter, r *http.Request) {
	routes, err := h.routeStore.ListRoutes(r.Context())
	if err != nil {
		jsonError(w, "failed to list routes", http.StatusInternalServerError)
		return
	}
	jsonOK(w, routes)
}

// GetRoute     GET /admin/routes/{id}
func (h *Handlers) GetRoute(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	route, err := h.routeStore.GetRoute(r.Context(), id)
	if err == store.ErrNotFound {
		jsonError(w, "route not found", http.StatusNotFound)
		return
	}
	if err != nil {
		jsonError(w, "failed to get route", http.StatusInternalServerError)
		return
	}
	jsonOK(w, route)
}

// CreateRoute  POST /admin/routes
func (h *Handlers) CreateRoute(w http.ResponseWriter, r *http.Request) {
	var req models.Route
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := validateRoute(&req); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	created, err := h.routeStore.CreateRoute(r.Context(), &req)
	if err != nil {
		jsonError(w, "failed to create route", http.StatusInternalServerError)
		return
	}

	h.invalidateAndReload(r.Context())
	log.Info().Str("id", created.ID).Str("name", created.Name).Msg("route created")
	w.WriteHeader(http.StatusCreated)
	jsonOK(w, created)
}

// UpdateRoute  PUT /admin/routes/{id}
func (h *Handlers) UpdateRoute(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	var req models.Route
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.ID = id
	if err := validateRoute(&req); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	updated, err := h.routeStore.UpdateRoute(r.Context(), &req)
	if err == store.ErrNotFound {
		jsonError(w, "route not found", http.StatusNotFound)
		return
	}
	if err != nil {
		jsonError(w, "failed to update route", http.StatusInternalServerError)
		return
	}

	h.invalidateAndReload(r.Context())
	log.Info().Str("id", updated.ID).Msg("route updated")
	jsonOK(w, updated)
}

// DeleteRoute  DELETE /admin/routes/{id}
func (h *Handlers) DeleteRoute(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if err := h.routeStore.DeleteRoute(r.Context(), id); err == store.ErrNotFound {
		jsonError(w, "route not found", http.StatusNotFound)
		return
	} else if err != nil {
		jsonError(w, "failed to delete route", http.StatusInternalServerError)
		return
	}
	h.invalidateAndReload(r.Context())
	log.Info().Str("id", id).Msg("route deleted")
	w.WriteHeader(http.StatusNoContent)
}

// SetRouteStatus  PATCH /admin/routes/{id}/status
func (h *Handlers) SetRouteStatus(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	var body struct {
		Status models.RouteStatus `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	switch body.Status {
	case models.RouteStatusActive, models.RouteStatusInactive, models.RouteStatusDegraded:
	default:
		jsonError(w, "invalid status value", http.StatusBadRequest)
		return
	}
	if err := h.routeStore.SetRouteStatus(r.Context(), id, body.Status); err == store.ErrNotFound {
		jsonError(w, "route not found", http.StatusNotFound)
		return
	} else if err != nil {
		jsonError(w, "failed to update status", http.StatusInternalServerError)
		return
	}
	h.invalidateAndReload(r.Context())
	jsonOK(w, map[string]string{"status": string(body.Status)})
}

// ── Traffic Logs API ───────────────────────────────────────────────────────

// ListLogs  GET /admin/logs?route_id=xxx&limit=100
func (h *Handlers) ListLogs(w http.ResponseWriter, r *http.Request) {
	routeID := r.URL.Query().Get("route_id")
	limit := 200
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	// Serve from in-memory ring buffer first (live data)
	logs := h.tlog.Recent(limit)

	// Filter by route if requested
	if routeID != "" {
		var filtered []*models.TrafficLog
		for _, l := range logs {
			if l.RouteID == routeID {
				filtered = append(filtered, l)
			}
		}
		logs = filtered
	}

	jsonOK(w, map[string]any{
		"logs":  logs,
		"count": len(logs),
	})
}

// ── Stats API ──────────────────────────────────────────────────────────────

// GetStats  GET /admin/stats
func (h *Handlers) GetStats(w http.ResponseWriter, r *http.Request) {
	routes := h.router.Routes()
	logs := h.tlog.Recent(1000)

	// Aggregate per-route stats from recent logs
	statsMap := make(map[string]*models.RouteStats)
	for _, route := range routes {
		statsMap[route.ID] = &models.RouteStats{RouteID: route.ID}
	}

	var totalLatency int64
	var totalErrors int64
	now := time.Now()

	for _, l := range logs {
		s, ok := statsMap[l.RouteID]
		if !ok {
			continue
		}
		s.TotalReqs++
		totalLatency += l.LatencyMs
		s.P50LatencyMs = float64(totalLatency) / float64(s.TotalReqs)
		if l.StatusCode >= 500 {
			totalErrors++
		}
		// RPS approximation over last 60s
		if now.Sub(l.Timestamp) < 60*time.Second {
			s.RPS += 1.0 / 60.0
		}
	}

	for _, s := range statsMap {
		if s.TotalReqs > 0 {
			s.ErrorRate = float64(totalErrors) / float64(s.TotalReqs) * 100
		}
	}

	out := make([]*models.RouteStats, 0, len(statsMap))
	for _, s := range statsMap {
		out = append(out, s)
	}

	jsonOK(w, map[string]any{
		"routes": out,
		"summary": map[string]any{
			"total_routes":  len(routes),
			"active_routes": countByStatus(routes, models.RouteStatusActive),
			"total_logs":    len(logs),
		},
	})
}

// ── Health ─────────────────────────────────────────────────────────────────

// Health  GET /admin/health
func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	jsonOK(w, map[string]string{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
}

// ── Helpers ────────────────────────────────────────────────────────────────

func (h *Handlers) invalidateAndReload(ctx context.Context) {
	if err := h.cache.InvalidateRoutes(ctx); err != nil {
		log.Warn().Err(err).Msg("cache invalidation failed")
	}
	if err := h.router.Reload(ctx); err != nil {
		log.Error().Err(err).Msg("router reload failed after mutation")
	}
}

func validateRoute(r *models.Route) error {
	var errs []string
	if strings.TrimSpace(r.Name) == "" {
		errs = append(errs, "name is required")
	}
	if strings.TrimSpace(r.PathPattern) == "" {
		errs = append(errs, "path_pattern is required")
	}
	if strings.TrimSpace(r.Destination) == "" {
		errs = append(errs, "destination is required")
	}
	if len(r.Methods) == 0 {
		r.Methods = []string{"ANY"}
	}
	if r.Protocol == "" {
		r.Protocol = models.ProtocolHTTP
	}
	if len(errs) > 0 {
		return fmt.Errorf("validation errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func countByStatus(routes []*models.Route, status models.RouteStatus) int {
	n := 0
	for _, r := range routes {
		if r.Status == status {
			n++
		}
	}
	return n
}

func jsonOK(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
