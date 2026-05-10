package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"apnv.id/apex/api-gateway/internal/models"
	"apnv.id/apex/api-gateway/internal/transform"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	goredis "github.com/redis/go-redis/v9"
)

// ── Postgres CRUD ──────────────────────────────────────────────────────────

const transformerSchema = `
CREATE TABLE IF NOT EXISTS transformers (
	id         TEXT PRIMARY KEY,
	route_id   TEXT NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
	name       TEXT NOT NULL,
	direction  TEXT NOT NULL CHECK (direction IN ('request','response')),
	script     TEXT NOT NULL,
	enabled    BOOLEAN NOT NULL DEFAULT true,
	"order"    INTEGER NOT NULL DEFAULT 0,
	created_at TIMESTAMPTZ NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transformers_route ON transformers(route_id, direction, "order");
`

// MigrateTransformers creates the transformers table if it does not exist.
func (s *RouteStore) MigrateTransformers(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, transformerSchema)
	return err
}

// ListTransformers returns all transformers for a route, ordered by direction then order.
func (s *RouteStore) ListTransformers(ctx context.Context, routeID string) ([]*models.Transformer, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, route_id, name, direction, script, enabled, "order", created_at, updated_at
		FROM transformers
		WHERE route_id = $1
		ORDER BY direction, "order", created_at
	`, routeID)
	if err != nil {
		return nil, fmt.Errorf("listing transformers: %w", err)
	}
	defer rows.Close()
	return scanTransformers(rows)
}

// ListTransformersByDirection returns transformers for a route filtered to one direction.
// This is the hot-path call made per request.
func (s *RouteStore) ListTransformersByDirection(ctx context.Context, routeID string, dir models.TransformDirection) ([]*models.Transformer, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, route_id, name, direction, script, enabled, "order", created_at, updated_at
		FROM transformers
		WHERE route_id = $1 AND direction = $2 AND enabled = true
		ORDER BY "order", created_at
	`, routeID, string(dir))
	if err != nil {
		return nil, fmt.Errorf("listing transformers by direction: %w", err)
	}
	defer rows.Close()
	return scanTransformers(rows)
}

// GetTransformer retrieves a transformer by ID.
func (s *RouteStore) GetTransformer(ctx context.Context, id string) (*models.Transformer, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, route_id, name, direction, script, enabled, "order", created_at, updated_at
		FROM transformers WHERE id = $1
	`, id)
	if err != nil {
		return nil, fmt.Errorf("getting transformer: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, ErrNotFound
	}
	return scanTransformer(rows)
}

// CreateTransformer inserts a new transformer.
func (s *RouteStore) CreateTransformer(ctx context.Context, t *models.Transformer) (*models.Transformer, error) {
	t.ID = uuid.New().String()
	t.CreatedAt = time.Now().UTC()
	t.UpdatedAt = t.CreatedAt

	_, err := s.pool.Exec(ctx, `
		INSERT INTO transformers (id, route_id, name, direction, script, enabled, "order", created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, t.ID, t.RouteID, t.Name, string(t.Direction), t.Script, t.Enabled, t.Order, t.CreatedAt, t.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("creating transformer: %w", err)
	}
	return t, nil
}

// UpdateTransformer replaces a transformer's mutable fields.
// It also calls transform.InvalidateCache so the compiled script is evicted.
func (s *RouteStore) UpdateTransformer(ctx context.Context, t *models.Transformer) (*models.Transformer, error) {
	t.UpdatedAt = time.Now().UTC()
	tag, err := s.pool.Exec(ctx, `
		UPDATE transformers
		SET name=$2, direction=$3, script=$4, enabled=$5, "order"=$6, updated_at=$7
		WHERE id=$1
	`, t.ID, t.Name, string(t.Direction), t.Script, t.Enabled, t.Order, t.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("updating transformer: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrNotFound
	}
	// Evict compiled script — the next request will recompile from the new source
	transform.InvalidateCache(t.ID)
	return t, nil
}

// DeleteTransformer removes a transformer and evicts its compiled program.
func (s *RouteStore) DeleteTransformer(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM transformers WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deleting transformer: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	transform.InvalidateCache(id)
	return nil
}

// ── Redis Transformer Cache ────────────────────────────────────────────────

const (
	transformerCacheTTL    = 30 * time.Second
	transformerCachePrefix = "gateway:transformers:"
)

func transformerCacheKey(routeID string, dir models.TransformDirection) string {
	return fmt.Sprintf("%s%s:%s", transformerCachePrefix, routeID, string(dir))
}

// SetTransformers serialises a transformer list into Redis.
func (c *Cache) SetTransformers(ctx context.Context, routeID string, dir models.TransformDirection, ts []*models.Transformer) error {
	data, err := json.Marshal(ts)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, transformerCacheKey(routeID, dir), data, transformerCacheTTL).Err()
}

// GetTransformers retrieves a cached transformer list. Returns nil on miss.
func (c *Cache) GetTransformers(ctx context.Context, routeID string, dir models.TransformDirection) ([]*models.Transformer, error) {
	data, err := c.client.Get(ctx, transformerCacheKey(routeID, dir)).Bytes()
	if err == goredis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ts []*models.Transformer
	if err := json.Unmarshal(data, &ts); err != nil {
		return nil, err
	}
	return ts, nil
}

// InvalidateTransformers evicts cached transformers for a route.
func (c *Cache) InvalidateTransformers(ctx context.Context, routeID string) error {
	keys := []string{
		transformerCacheKey(routeID, models.DirectionRequest),
		transformerCacheKey(routeID, models.DirectionResponse),
	}
	return c.client.Del(ctx, keys...).Err()
}

// ── Helpers ────────────────────────────────────────────────────────────────

func scanTransformers(rows pgx.Rows) ([]*models.Transformer, error) {
	var out []*models.Transformer
	for rows.Next() {
		t, err := scanTransformer(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func scanTransformer(rows pgx.Rows) (*models.Transformer, error) {
	t := &models.Transformer{}
	err := rows.Scan(&t.ID, &t.RouteID, &t.Name, &t.Direction, &t.Script,
		&t.Enabled, &t.Order, &t.CreatedAt, &t.UpdatedAt)
	return t, err
}
