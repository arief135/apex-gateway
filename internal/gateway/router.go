package gateway

import (
	"context"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"apnv.id/apex/api-gateway/internal/models"
	"apnv.id/apex/api-gateway/internal/store"
	"github.com/rs/zerolog/log"
)

// Router holds the active route table and matches incoming requests.
// It reloads routes from the cache/DB on a configurable interval.
type Router struct {
	mu         sync.RWMutex
	routes     []*models.Route
	routeStore *store.RouteStore
	cache      *store.Cache
	ttl        time.Duration
	stopCh     chan struct{}
}

// NewRouter initialises the router and performs the first route load.
func NewRouter(rs *store.RouteStore, cache *store.Cache, ttl time.Duration) (*Router, error) {
	r := &Router{
		routeStore: rs,
		cache:      cache,
		ttl:        ttl,
		stopCh:     make(chan struct{}),
	}
	if err := r.reload(context.Background()); err != nil {
		return nil, err
	}
	go r.watchRoutes()
	return r, nil
}

// Match finds the first active route that matches the request method and path.
// Returns nil if no route matches.
func (r *Router) Match(req *http.Request) *models.Route {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, route := range r.routes {
		if route.Status != models.RouteStatusActive {
			continue
		}
		if !methodMatches(route.Methods, req.Method) {
			continue
		}
		if pathMatches(route.PathPattern, req.URL.Path) {
			return route
		}
	}
	return nil
}

// Reload forces an immediate route table refresh. Called by admin handlers
// after a route mutation.
func (r *Router) Reload(ctx context.Context) error {
	return r.reload(ctx)
}

// Routes returns a snapshot of the current route table.
func (r *Router) Routes() []*models.Route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*models.Route, len(r.routes))
	copy(out, r.routes)
	return out
}

// Stop halts the background refresh goroutine.
func (r *Router) Stop() {
	close(r.stopCh)
}

// ── Internal ───────────────────────────────────────────────────────────────

// watchRoutes periodically reloads routes in the background.
func (r *Router) watchRoutes() {
	ticker := time.NewTicker(r.ttl)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := r.reload(ctx); err != nil {
				log.Error().Err(err).Msg("route reload failed")
			}
			cancel()
		case <-r.stopCh:
			return
		}
	}
}

// reload fetches routes from Redis cache, falling back to PostgreSQL.
func (r *Router) reload(ctx context.Context) error {
	routes, err := r.cache.GetRoutes(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("redis route cache miss, falling back to postgres")
	}
	if routes == nil {
		routes, err = r.routeStore.ListRoutes(ctx)
		if err != nil {
			return err
		}
		// Populate cache for next reads
		if cacheErr := r.cache.SetRoutes(ctx, routes); cacheErr != nil {
			log.Warn().Err(cacheErr).Msg("failed to set route cache")
		}
	}

	r.mu.Lock()
	r.routes = routes
	r.mu.Unlock()

	log.Debug().Int("count", len(routes)).Msg("route table reloaded")
	return nil
}

// methodMatches returns true if the request method is accepted by the route.
// A route with method "ANY" matches all HTTP methods.
func methodMatches(routeMethods []string, reqMethod string) bool {
	for _, m := range routeMethods {
		if m == "ANY" || strings.EqualFold(m, reqMethod) {
			return true
		}
	}
	return false
}

// pathMatches supports two styles:
//   - Exact:   /api/users          → matches /api/users only
//   - Wildcard: /api/users/**      → matches /api/users, /api/users/123, /api/users/123/profile
//   - Segment: /api/users/*        → matches /api/users/123 but not /api/users/123/profile
func pathMatches(pattern, reqPath string) bool {
	if pattern == reqPath {
		return true
	}
	if strings.HasSuffix(pattern, "/**") {
		prefix := strings.TrimSuffix(pattern, "/**")
		return reqPath == prefix || strings.HasPrefix(reqPath, prefix+"/")
	}
	if strings.HasSuffix(pattern, "/*") {
		prefix := strings.TrimSuffix(pattern, "/*")
		tail := strings.TrimPrefix(reqPath, prefix+"/")
		return strings.HasPrefix(reqPath, prefix+"/") && !strings.Contains(tail, "/")
	}
	// glob-style pattern matching using stdlib path package
	matched, _ := path.Match(pattern, reqPath)
	return matched
}
