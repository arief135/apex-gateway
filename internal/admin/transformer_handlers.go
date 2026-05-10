package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"apnv.id/apex/api-gateway/internal/models"
	"apnv.id/apex/api-gateway/internal/store"
	"apnv.id/apex/api-gateway/internal/transform"
	"github.com/gorilla/mux"
)

// ── Transformer CRUD ───────────────────────────────────────────────────────

// ListTransformers  GET /admin/routes/{id}/transformers
func (h *Handlers) ListTransformers(w http.ResponseWriter, r *http.Request) {
	routeID := mux.Vars(r)["id"]
	ts, err := h.routeStore.ListTransformers(r.Context(), routeID)
	if err != nil {
		jsonError(w, "failed to list transformers", http.StatusInternalServerError)
		return
	}
	jsonOK(w, ts)
}

// CreateTransformer  POST /admin/routes/{id}/transformers
func (h *Handlers) CreateTransformer(w http.ResponseWriter, r *http.Request) {
	routeID := mux.Vars(r)["id"]

	// Verify the route exists
	if _, err := h.routeStore.GetRoute(r.Context(), routeID); err == store.ErrNotFound {
		jsonError(w, "route not found", http.StatusNotFound)
		return
	}

	var t models.Transformer
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		jsonError(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	t.RouteID = routeID
	if err := validateTransformer(&t); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	created, err := h.routeStore.CreateTransformer(r.Context(), &t)
	if err != nil {
		jsonError(w, "failed to create transformer", http.StatusInternalServerError)
		return
	}

	// Invalidate transformer cache for this route
	h.cache.InvalidateTransformers(r.Context(), routeID)

	w.WriteHeader(http.StatusCreated)
	jsonOK(w, created)
}

// GetTransformer  GET /admin/transformers/{tid}
func (h *Handlers) GetTransformer(w http.ResponseWriter, r *http.Request) {
	tid := mux.Vars(r)["tid"]
	t, err := h.routeStore.GetTransformer(r.Context(), tid)
	if err == store.ErrNotFound {
		jsonError(w, "transformer not found", http.StatusNotFound)
		return
	}
	if err != nil {
		jsonError(w, "failed to get transformer", http.StatusInternalServerError)
		return
	}
	jsonOK(w, t)
}

// UpdateTransformer  PUT /admin/transformers/{tid}
// The script is hot-swapped: the compiled cache entry is evicted automatically.
func (h *Handlers) UpdateTransformer(w http.ResponseWriter, r *http.Request) {
	tid := mux.Vars(r)["tid"]

	existing, err := h.routeStore.GetTransformer(r.Context(), tid)
	if err == store.ErrNotFound {
		jsonError(w, "transformer not found", http.StatusNotFound)
		return
	}

	var t models.Transformer
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		jsonError(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	t.ID = tid
	t.RouteID = existing.RouteID
	if err := validateTransformer(&t); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	updated, err := h.routeStore.UpdateTransformer(r.Context(), &t)
	if err != nil {
		jsonError(w, "failed to update transformer", http.StatusInternalServerError)
		return
	}

	// UpdateTransformer already evicts the compiled program via transform.InvalidateCache.
	// Also evict the Redis cache so next request reloads the new script.
	h.cache.InvalidateTransformers(r.Context(), t.RouteID)

	jsonOK(w, updated)
}

// DeleteTransformer  DELETE /admin/transformers/{tid}
func (h *Handlers) DeleteTransformer(w http.ResponseWriter, r *http.Request) {
	tid := mux.Vars(r)["tid"]

	existing, err := h.routeStore.GetTransformer(r.Context(), tid)
	if err == store.ErrNotFound {
		jsonError(w, "transformer not found", http.StatusNotFound)
		return
	}

	if err := h.routeStore.DeleteTransformer(r.Context(), tid); err != nil {
		jsonError(w, "failed to delete transformer", http.StatusInternalServerError)
		return
	}
	h.cache.InvalidateTransformers(r.Context(), existing.RouteID)
	w.WriteHeader(http.StatusNoContent)
}

// ── Dry-run Test Endpoint ──────────────────────────────────────────────────

// TestTransformer  POST /admin/transformers/test
//
// Compiles and executes a script against a provided payload without persisting
// anything. Returns the transformed output or a detailed error. Ideal for the
// dashboard's live script editor.
//
// Request body:
//
//	{
//	  "script":    "function transform(payload, ctx) { ... }",
//	  "direction": "request",
//	  "payload":   { "user_id": 1, "first_name": "Jane" },
//	  "context":   { "method": "POST", "path": "/api/users" }
//	}
func (h *Handlers) TestTransformer(w http.ResponseWriter, r *http.Request) {
	var req models.TransformTestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Script) == "" {
		jsonError(w, "script is required", http.StatusBadRequest)
		return
	}

	// Serialise the payload field back to JSON for the engine
	payloadBytes, err := json.Marshal(req.Payload)
	if err != nil {
		jsonError(w, "invalid payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Fill in default ctx if not provided
	tctx := req.Context
	if tctx == nil {
		tctx = &models.TransformContext{
			Method:    "POST",
			Path:      "/test",
			Headers:   map[string]string{"Content-Type": "application/json"},
			RouteID:   "test",
			RouteName: "Test Route",
			Direction: string(req.Direction),
		}
	}

	eng := transform.NewEngine()
	out, elapsed, err := eng.Test(req.Script, payloadBytes, tctx)

	resp := models.TransformTestResponse{ElapsedMs: elapsed.Milliseconds()}
	if err != nil {
		resp.Error = err.Error()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Deserialise output so it's embedded as proper JSON (not a string)
	var outputVal any
	json.Unmarshal(out, &outputVal)
	resp.Output = outputVal

	jsonOK(w, resp)
}

// GetTransformerMetrics  GET /admin/transformers/{tid}/metrics
func (h *Handlers) GetTransformerMetrics(w http.ResponseWriter, r *http.Request) {
	tid := mux.Vars(r)["tid"]
	m := transform.GetMetrics(tid)
	if m == nil {
		jsonOK(w, map[string]any{
			"transformer_id": tid,
			"executions":     0,
			"errors":         0,
			"note":           "no executions recorded yet",
		})
		return
	}
	m2 := struct {
		TransformerID string    `json:"transformer_id"`
		Executions    int64     `json:"executions"`
		Errors        int64     `json:"errors"`
		AvgLatencyMs  float64   `json:"avg_latency_ms"`
		LastError     string    `json:"last_error,omitempty"`
		LastErrorAt   time.Time `json:"last_error_at,omitempty"`
	}{
		TransformerID: tid,
		Executions:    m.Executions,
		Errors:        m.Errors,
		LastError:     m.LastError,
		LastErrorAt:   m.LastErrorAt,
	}
	if m.Executions > 0 {
		m2.AvgLatencyMs = float64(m.TotalMs) / float64(m.Executions)
	}
	jsonOK(w, m2)
}

// ── Validation ─────────────────────────────────────────────────────────────

func validateTransformer(t *models.Transformer) error {
	var errs []string
	if strings.TrimSpace(t.Name) == "" {
		errs = append(errs, "name is required")
	}
	if strings.TrimSpace(t.Script) == "" {
		errs = append(errs, "script is required")
	}
	switch t.Direction {
	case models.DirectionRequest, models.DirectionResponse:
	default:
		errs = append(errs, "direction must be 'request' or 'response'")
	}
	if !strings.Contains(t.Script, "function transform") {
		errs = append(errs, "script must define `function transform(payload, ctx) { ... }`")
	}
	if len(errs) > 0 {
		return fmt.Errorf("validation: %s", strings.Join(errs, "; "))
	}
	return nil
}

var _ = fmt.Sprintf // keep fmt imported
