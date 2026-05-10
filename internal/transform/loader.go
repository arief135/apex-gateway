package transform

import (
	"context"
	"time"

	"apnv.id/apex/api-gateway/internal/models"
	"github.com/rs/zerolog/log"
)

// TransformerSource is satisfied by *store.RouteStore.
// Defined here as an interface to avoid an import cycle.
type TransformerSource interface {
	ListTransformersByDirection(ctx context.Context, routeID string, dir models.TransformDirection) ([]*models.Transformer, error)
}

// TransformerCacheSource is satisfied by *store.Cache.
type TransformerCacheSource interface {
	GetTransformers(ctx context.Context, routeID string, dir models.TransformDirection) ([]*models.Transformer, error)
	SetTransformers(ctx context.Context, routeID string, dir models.TransformDirection, ts []*models.Transformer) error
}

// Loader fetches transformers for a route+direction using a cache-aside strategy:
//  1. Try Redis (fast, ~0.1ms)
//  2. On miss: query Postgres, backfill Redis
//
// This is instantiated once and shared across all request goroutines.
type Loader struct {
	db    TransformerSource
	cache TransformerCacheSource
}

// NewLoader creates a Loader. db and cache may be nil (safe no-op).
func NewLoader(db TransformerSource, cache TransformerCacheSource) *Loader {
	return &Loader{db: db, cache: cache}
}

// Load returns the ordered list of enabled transformers for a route+direction.
// Empty slice means no transforms are configured — the hot path checks len()
// and skips body buffering entirely when there's nothing to do.
func (l *Loader) Load(routeID string, dir models.TransformDirection) []*models.Transformer {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// 1. Redis cache
	if l.cache != nil {
		if ts, err := l.cache.GetTransformers(ctx, routeID, dir); err == nil && ts != nil {
			return enabled(ts)
		}
	}

	// 2. Postgres fallback
	if l.db == nil {
		return nil
	}
	ts, err := l.db.ListTransformersByDirection(ctx, routeID, dir)
	if err != nil {
		log.Warn().Err(err).Str("route_id", routeID).Msg("failed to load transformers from DB")
		return nil
	}

	// 3. Backfill cache (best effort)
	if l.cache != nil {
		all := ts // DB query already filters to enabled=true, but cache all for consistency
		if setErr := l.cache.SetTransformers(ctx, routeID, dir, all); setErr != nil {
			log.Warn().Err(setErr).Msg("failed to cache transformers")
		}
	}

	return enabled(ts)
}

func enabled(ts []*models.Transformer) []*models.Transformer {
	out := ts[:0]
	for _, t := range ts {
		if t.Enabled {
			out = append(out, t)
		}
	}
	return out
}
