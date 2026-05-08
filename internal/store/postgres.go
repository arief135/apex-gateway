package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"apnv.id/apex/api-gateway/internal/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RouteStore persists and retrieves route configurations from PostgreSQL.
type RouteStore struct {
	pool *pgxpool.Pool
}

// NewRouteStore opens a PostgreSQL connection pool.
func NewRouteStore(ctx context.Context, dsn string) (*RouteStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres DSN: %w", err)
	}
	cfg.MaxConns = 25
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}
	return &RouteStore{pool: pool}, nil
}

// Close releases all pool connections.
func (s *RouteStore) Close() {
	s.pool.Close()
}

// Migrate runs the embedded schema migration.
func (s *RouteStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schema)
	return err
}

// ── CRUD ──────────────────────────────────────────────────────────────────

// ListRoutes returns all routes ordered by creation time.
func (s *RouteStore) ListRoutes(ctx context.Context) ([]*models.Route, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, path_pattern, methods, protocol, destination,
		       strip_prefix, headers, status, log_payload, rate_limit,
		       timeout_ms, created_at, updated_at
		FROM routes
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("listing routes: %w", err)
	}
	defer rows.Close()

	var routes []*models.Route
	for rows.Next() {
		r, err := scanRoute(rows)
		if err != nil {
			return nil, err
		}
		routes = append(routes, r)
	}
	return routes, rows.Err()
}

// GetRoute retrieves a single route by ID.
func (s *RouteStore) GetRoute(ctx context.Context, id string) (*models.Route, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, path_pattern, methods, protocol, destination,
		       strip_prefix, headers, status, log_payload, rate_limit,
		       timeout_ms, created_at, updated_at
		FROM routes WHERE id = $1
	`, id)
	if err != nil {
		return nil, fmt.Errorf("getting route %s: %w", id, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, ErrNotFound
	}
	return scanRoute(rows)
}

// CreateRoute inserts a new route and returns it with generated fields populated.
func (s *RouteStore) CreateRoute(ctx context.Context, r *models.Route) (*models.Route, error) {
	r.ID = uuid.New().String()
	r.CreatedAt = time.Now().UTC()
	r.UpdatedAt = r.CreatedAt
	if r.Status == "" {
		r.Status = models.RouteStatusActive
	}

	methodsJSON, err := json.Marshal(r.Methods)
	if err != nil {
		return nil, err
	}
	headersJSON, err := json.Marshal(r.Headers)
	if err != nil {
		return nil, err
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO routes (id, name, path_pattern, methods, protocol, destination,
		                    strip_prefix, headers, status, log_payload, rate_limit,
		                    timeout_ms, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	`,
		r.ID, r.Name, r.PathPattern, methodsJSON, string(r.Protocol), r.Destination,
		r.StripPrefix, headersJSON, string(r.Status), r.LogPayload, r.RateLimit,
		r.TimeoutMs, r.CreatedAt, r.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("inserting route: %w", err)
	}
	return r, nil
}

// UpdateRoute applies a full replacement update to a route.
func (s *RouteStore) UpdateRoute(ctx context.Context, r *models.Route) (*models.Route, error) {
	r.UpdatedAt = time.Now().UTC()

	methodsJSON, err := json.Marshal(r.Methods)
	if err != nil {
		return nil, err
	}
	headersJSON, err := json.Marshal(r.Headers)
	if err != nil {
		return nil, err
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE routes
		SET name=$2, path_pattern=$3, methods=$4, protocol=$5, destination=$6,
		    strip_prefix=$7, headers=$8, status=$9, log_payload=$10,
		    rate_limit=$11, timeout_ms=$12, updated_at=$13
		WHERE id=$1
	`,
		r.ID, r.Name, r.PathPattern, methodsJSON, string(r.Protocol), r.Destination,
		r.StripPrefix, headersJSON, string(r.Status), r.LogPayload, r.RateLimit,
		r.TimeoutMs, r.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("updating route: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrNotFound
	}
	return r, nil
}

// DeleteRoute removes a route by ID.
func (s *RouteStore) DeleteRoute(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM routes WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deleting route: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetRouteStatus toggles the status of a route.
func (s *RouteStore) SetRouteStatus(ctx context.Context, id string, status models.RouteStatus) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE routes SET status=$2, updated_at=$3 WHERE id=$1`,
		id, string(status), time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("setting route status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ── Traffic Log Persistence ────────────────────────────────────────────────

// InsertTrafficLog writes a captured traffic record to Postgres.
// This is a fallback path when Kafka is disabled.
func (s *RouteStore) InsertTrafficLog(ctx context.Context, l *models.TrafficLog) error {
	reqHeaders, _ := json.Marshal(l.RequestHeaders)
	respHeaders, _ := json.Marshal(l.ResponseHeaders)

	_, err := s.pool.Exec(ctx, `
		INSERT INTO traffic_logs (
			id, route_id, route_name, timestamp, method, path, raw_query,
			protocol, client_ip, status_code, latency_ms,
			request_size, response_size,
			request_headers, response_headers,
			request_body, response_body, error
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
	`,
		l.ID, l.RouteID, l.RouteName, l.Timestamp, l.Method, l.Path, l.RawQuery,
		string(l.Protocol), l.ClientIP, l.StatusCode, l.LatencyMs,
		l.RequestSize, l.ResponseSize,
		reqHeaders, respHeaders,
		l.RequestBody, l.ResponseBody, l.Error,
	)
	return err
}

// ListTrafficLogs queries recent logs with optional filters.
func (s *RouteStore) ListTrafficLogs(ctx context.Context, routeID string, limit int) ([]*models.TrafficLog, error) {
	args := []any{limit}
	where := ""
	if routeID != "" {
		args = append([]any{routeID}, args...)
		where = "WHERE route_id = $1"
		args[1] = limit
	}

	query := fmt.Sprintf(`
		SELECT id, route_id, route_name, timestamp, method, path, raw_query,
		       protocol, client_ip, status_code, latency_ms,
		       request_size, response_size, error
		FROM traffic_logs
		%s
		ORDER BY timestamp DESC
		LIMIT $%d
	`, where, len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []*models.TrafficLog
	for rows.Next() {
		l := &models.TrafficLog{}
		err := rows.Scan(
			&l.ID, &l.RouteID, &l.RouteName, &l.Timestamp, &l.Method, &l.Path, &l.RawQuery,
			&l.Protocol, &l.ClientIP, &l.StatusCode, &l.LatencyMs,
			&l.RequestSize, &l.ResponseSize, &l.Error,
		)
		if err != nil {
			return nil, err
		}
		logs = append(logs, l)
	}
	return logs, rows.Err()
}

// ── Helpers ────────────────────────────────────────────────────────────────

func scanRoute(rows pgx.Rows) (*models.Route, error) {
	r := &models.Route{}
	var methodsJSON, headersJSON []byte
	err := rows.Scan(
		&r.ID, &r.Name, &r.PathPattern, &methodsJSON, &r.Protocol, &r.Destination,
		&r.StripPrefix, &headersJSON, &r.Status, &r.LogPayload, &r.RateLimit,
		&r.TimeoutMs, &r.CreatedAt, &r.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scanning route: %w", err)
	}
	if err := json.Unmarshal(methodsJSON, &r.Methods); err != nil {
		return nil, fmt.Errorf("unmarshaling methods: %w", err)
	}
	if len(headersJSON) > 0 {
		if err := json.Unmarshal(headersJSON, &r.Headers); err != nil {
			return nil, fmt.Errorf("unmarshaling headers: %w", err)
		}
	}
	return r, nil
}

// ErrNotFound is returned when a requested resource does not exist.
var ErrNotFound = fmt.Errorf("not found")

// ── Schema ─────────────────────────────────────────────────────────────────

const schema = `
CREATE TABLE IF NOT EXISTS routes (
	id            TEXT PRIMARY KEY,
	name          TEXT NOT NULL,
	path_pattern  TEXT NOT NULL,
	methods       JSONB NOT NULL DEFAULT '["ANY"]',
	protocol      TEXT NOT NULL DEFAULT 'HTTP',
	destination   TEXT NOT NULL,
	strip_prefix  TEXT NOT NULL DEFAULT '',
	headers       JSONB NOT NULL DEFAULT '{}',
	status        TEXT NOT NULL DEFAULT 'active',
	log_payload   BOOLEAN NOT NULL DEFAULT true,
	rate_limit    INTEGER NOT NULL DEFAULT 0,
	timeout_ms    INTEGER NOT NULL DEFAULT 0,
	created_at    TIMESTAMPTZ NOT NULL,
	updated_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS traffic_logs (
	id               TEXT PRIMARY KEY,
	route_id         TEXT NOT NULL,
	route_name       TEXT NOT NULL,
	timestamp        TIMESTAMPTZ NOT NULL,
	method           TEXT NOT NULL,
	path             TEXT NOT NULL,
	raw_query        TEXT NOT NULL DEFAULT '',
	protocol         TEXT NOT NULL,
	client_ip        TEXT NOT NULL,
	status_code      INTEGER NOT NULL,
	latency_ms       BIGINT NOT NULL,
	request_size     BIGINT NOT NULL DEFAULT 0,
	response_size    BIGINT NOT NULL DEFAULT 0,
	request_headers  JSONB,
	response_headers JSONB,
	request_body     BYTEA,
	response_body    BYTEA,
	error            TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_traffic_logs_route_id  ON traffic_logs(route_id);
CREATE INDEX IF NOT EXISTS idx_traffic_logs_timestamp ON traffic_logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_routes_status ON routes(status);
`
