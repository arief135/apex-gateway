package models

import (
	"time"
)

// RouteStatus represents the lifecycle state of a route.
type RouteStatus string

const (
	RouteStatusActive   RouteStatus = "active"
	RouteStatusInactive RouteStatus = "inactive"
	RouteStatusDegraded RouteStatus = "degraded"
)

// Protocol defines the upstream communication protocol.
type Protocol string

const (
	ProtocolHTTP      Protocol = "HTTP"
	ProtocolHTTPS     Protocol = "HTTPS"
	ProtocolWebSocket Protocol = "WebSocket"
	ProtocolGRPC      Protocol = "gRPC"
	ProtocolMQTT      Protocol = "MQTT"
)

// Route is a single API gateway routing rule.
type Route struct {
	ID          string            `json:"id"           db:"id"`
	Name        string            `json:"name"         db:"name"`
	PathPattern string            `json:"path_pattern" db:"path_pattern"`
	Methods     []string          `json:"methods"      db:"methods"`        // ["GET","POST"] or ["ANY"]
	Protocol    Protocol          `json:"protocol"     db:"protocol"`
	Destination string            `json:"destination"  db:"destination"`    // e.g. http://svc:3001 or grpc://svc:50051
	StripPrefix string            `json:"strip_prefix" db:"strip_prefix"`   // prefix removed before forwarding
	Headers     map[string]string `json:"headers"      db:"headers"`        // extra headers injected upstream
	Status      RouteStatus       `json:"status"       db:"status"`
	LogPayload  bool              `json:"log_payload"  db:"log_payload"`
	RateLimit   int               `json:"rate_limit"   db:"rate_limit"`     // req/s per IP, 0 = unlimited
	TimeoutMs   int               `json:"timeout_ms"   db:"timeout_ms"`     // upstream timeout, 0 = 30s default
	CreatedAt   time.Time         `json:"created_at"   db:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"   db:"updated_at"`
}

// TrafficLog is a single captured request/response record.
type TrafficLog struct {
	ID           string            `json:"id"`
	RouteID      string            `json:"route_id"`
	RouteName    string            `json:"route_name"`
	Timestamp    time.Time         `json:"timestamp"`
	Method       string            `json:"method"`
	Path         string            `json:"path"`
	RawQuery     string            `json:"raw_query,omitempty"`
	Protocol     Protocol          `json:"protocol"`
	ClientIP     string            `json:"client_ip"`
	StatusCode   int               `json:"status_code"`
	LatencyMs    int64             `json:"latency_ms"`
	RequestSize  int64             `json:"request_size"`
	ResponseSize int64             `json:"response_size"`
	RequestHeaders  map[string]string `json:"request_headers,omitempty"`
	ResponseHeaders map[string]string `json:"response_headers,omitempty"`
	RequestBody  []byte            `json:"request_body,omitempty"`
	ResponseBody []byte            `json:"response_body,omitempty"`
	Error        string            `json:"error,omitempty"`
}

// APIKey is used to authenticate requests to the management API.
type APIKey struct {
	ID        string    `json:"id"         db:"id"`
	Name      string    `json:"name"       db:"name"`
	KeyHash   string    `json:"-"          db:"key_hash"`    // bcrypt hash
	Scopes    []string  `json:"scopes"     db:"scopes"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	ExpiresAt time.Time `json:"expires_at" db:"expires_at"`
}

// RouteStats holds live aggregated metrics for a route.
type RouteStats struct {
	RouteID     string  `json:"route_id"`
	RPS         float64 `json:"rps"`
	P50LatencyMs float64 `json:"p50_latency_ms"`
	P99LatencyMs float64 `json:"p99_latency_ms"`
	ErrorRate   float64 `json:"error_rate"`
	TotalReqs   int64   `json:"total_reqs"`
}
