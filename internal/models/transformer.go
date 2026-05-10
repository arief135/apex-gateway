package models

import "time"

// TransformDirection controls which side of the proxy a transformer runs on.
type TransformDirection string

const (
	DirectionRequest  TransformDirection = "request"  // runs before body is sent upstream
	DirectionResponse TransformDirection = "response" // runs before body is sent to client
)

// Transformer holds a user-defined JavaScript script that rewrites a JSON payload.
//
// The JS script must expose a single function with this signature:
//
//	function transform(payload, ctx) {
//	    // payload : parsed JSON body (object, array, string, or null)
//	    // ctx     : { method, path, headers, routeId, routeName, direction }
//	    // return  : transformed payload (will be JSON-serialised)
//	    return payload;
//	}
//
// Scripts are compiled once and cached keyed by ID+UpdatedAt.
// Updating a transformer automatically invalidates the compiled cache entry,
// so changes take effect on the very next request — no restart required.
type Transformer struct {
	ID        string             `json:"id"         db:"id"`
	RouteID   string             `json:"route_id"   db:"route_id"`
	Name      string             `json:"name"       db:"name"`
	Direction TransformDirection `json:"direction"  db:"direction"` // "request" | "response"
	Script    string             `json:"script"     db:"script"`    // JavaScript source
	Enabled   bool               `json:"enabled"    db:"enabled"`
	Order     int                `json:"order"      db:"order"` // execution order within same route+direction
	CreatedAt time.Time          `json:"created_at" db:"created_at"`
	UpdatedAt time.Time          `json:"updated_at" db:"updated_at"`
}

// TransformContext is passed to every script as the second argument `ctx`.
// It exposes read-only metadata about the current request.
type TransformContext struct {
	Method    string            `json:"method"`
	Path      string            `json:"path"`
	Headers   map[string]string `json:"headers"`
	RouteID   string            `json:"routeId"`
	RouteName string            `json:"routeName"`
	Direction string            `json:"direction"`
}

// TransformTestRequest is the body for POST /admin/transformers/test
type TransformTestRequest struct {
	Script    string             `json:"script"`
	Direction TransformDirection `json:"direction"`
	Payload   any                `json:"payload"` // raw JSON to transform
	Context   *TransformContext  `json:"context"` // optional — defaults are filled in
}

// TransformTestResponse is the result of a dry-run script execution.
type TransformTestResponse struct {
	Output  any    `json:"output,omitempty"`
	Error   string `json:"error,omitempty"`
	ElapsedMs int64 `json:"elapsed_ms"`
}
