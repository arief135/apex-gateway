package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"apnv.id/apex/api-gateway/internal/models"
	goredis "github.com/redis/go-redis/v9"
)

const (
	routesCacheKey  = "gateway:routes"
	routesCacheTTL  = 30 * time.Second
	rateLimitPrefix = "gateway:rl:"
	rateLimitWindow = time.Second
)

// Cache wraps Redis for hot-path operations: route caching and rate limiting.
type Cache struct {
	client *goredis.Client
}

// NewCache connects to Redis and returns a ready Cache.
func NewCache(addr, password string, db int) (*Cache, error) {
	client := goredis.NewClient(&goredis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     20,
		MinIdleConns: 5,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connecting to redis: %w", err)
	}
	return &Cache{client: client}, nil
}

// Close releases the Redis connection pool.
func (c *Cache) Close() error {
	return c.client.Close()
}

// ── Route Cache ────────────────────────────────────────────────────────────

// SetRoutes serialises the full route list into Redis with a short TTL.
// The gateway reads from this cache on every request to avoid DB hits.
func (c *Cache) SetRoutes(ctx context.Context, routes []*models.Route) error {
	data, err := json.Marshal(routes)
	if err != nil {
		return fmt.Errorf("marshaling routes: %w", err)
	}
	return c.client.Set(ctx, routesCacheKey, data, routesCacheTTL).Err()
}

// GetRoutes retrieves the cached route list. Returns nil, nil on cache miss.
func (c *Cache) GetRoutes(ctx context.Context) ([]*models.Route, error) {
	data, err := c.client.Get(ctx, routesCacheKey).Bytes()
	if err == goredis.Nil {
		return nil, nil // cache miss — caller should reload from DB
	}
	if err != nil {
		return nil, fmt.Errorf("reading routes from cache: %w", err)
	}
	var routes []*models.Route
	if err := json.Unmarshal(data, &routes); err != nil {
		return nil, fmt.Errorf("unmarshaling cached routes: %w", err)
	}
	return routes, nil
}

// InvalidateRoutes removes the cached route list, forcing the next request
// to reload from PostgreSQL. Call this after any route mutation.
func (c *Cache) InvalidateRoutes(ctx context.Context) error {
	return c.client.Del(ctx, routesCacheKey).Err()
}

// ── Rate Limiting ──────────────────────────────────────────────────────────

// AllowRequest implements a sliding-window token-bucket rate limiter.
// It returns (allowed, currentCount, error).
// limit is max requests per second per IP for the given routeID.
func (c *Cache) AllowRequest(ctx context.Context, routeID, clientIP string, limit int) (bool, int64, error) {
	if limit <= 0 {
		return true, 0, nil // unlimited
	}

	key := fmt.Sprintf("%s%s:%s", rateLimitPrefix, routeID, clientIP)
	now := time.Now().UnixMilli()
	windowStart := now - rateLimitWindow.Milliseconds()

	pipe := c.client.Pipeline()
	// Remove entries older than the sliding window
	pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%d", windowStart))
	// Count remaining entries in the window
	countCmd := pipe.ZCard(ctx, key)
	// Add current timestamp as a new entry
	pipe.ZAdd(ctx, key, goredis.Z{Score: float64(now), Member: fmt.Sprintf("%d", now)})
	// Reset TTL on each hit
	pipe.Expire(ctx, key, 2*rateLimitWindow)

	if _, err := pipe.Exec(ctx); err != nil {
		// On Redis error, fail open — don't block traffic
		return true, 0, fmt.Errorf("rate limit pipeline: %w", err)
	}

	count := countCmd.Val()
	if count >= int64(limit) {
		return false, count, nil
	}
	return true, count, nil
}

// ── Circuit Breaker State ─────────────────────────────────────────────────

// RecordFailure increments the failure counter for a route.
// Returns the current failure count within the window.
func (c *Cache) RecordFailure(ctx context.Context, routeID string) (int64, error) {
	key := fmt.Sprintf("gateway:cb:failures:%s", routeID)
	count, err := c.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	// Set expiry on first failure
	if count == 1 {
		c.client.Expire(ctx, key, 30*time.Second)
	}
	return count, nil
}

// ResetFailures clears the circuit breaker failure counter for a route.
func (c *Cache) ResetFailures(ctx context.Context, routeID string) error {
	return c.client.Del(ctx, fmt.Sprintf("gateway:cb:failures:%s", routeID)).Err()
}

// GetFailureCount returns the current failure count for a route.
func (c *Cache) GetFailureCount(ctx context.Context, routeID string) (int64, error) {
	count, err := c.client.Get(ctx, fmt.Sprintf("gateway:cb:failures:%s", routeID)).Int64()
	if err == goredis.Nil {
		return 0, nil
	}
	return count, err
}
