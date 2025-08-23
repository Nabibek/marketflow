package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"marketflow/internal/domain"

	"github.com/redis/go-redis/v9"
)

// RedisCache stores last tick and time-sorted ticks in Redis.
type RedisCache struct {
	rdb *redis.Client
	mem *MemoryCache
	ttl time.Duration
}

type redisMember struct {
	Price float64 `json:"price"`
	Ts    int64   `json:"ts"` // unix nano timestamp
}

// NewRedisCache creates a new RedisCache instance and pings Redis server.
func NewRedisCache(ctx context.Context, addr, password string, db int, ttl time.Duration) (*RedisCache, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	// Perform a ping to ensure Redis is reachable
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	rc := &RedisCache{
		rdb: rdb,
		mem: NewMemoryCache(ttl),
		ttl: ttl,
	}

	// Periodic cleanup to remove old ticks from Redis
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Trim old data periodically
				_ = rc.TrimOld(ctx, time.Now().Add(-rc.ttl))
			}
		}
	}()

	return rc, nil
}

// Close shuts down both Redis and memory cleaner.
func (r *RedisCache) Close() error {
	// Stop memory cleaner and Redis client
	r.mem.Close()
	return r.rdb.Close()
}

// Helper key generation functions
func lastKey(exchange, symbol string) string  { return fmt.Sprintf("last:%s:%s", exchange, symbol) }
func ticksKey(exchange, symbol string) string { return fmt.Sprintf("ticks:%s:%s", exchange, symbol) }

// PushTick stores a tick in Redis. If Redis is unavailable, it falls back to in-memory cache.
func (r *RedisCache) PushTick(ctx context.Context, t domain.PriceTick) error {
	m := redisMember{Price: t.Price, Ts: t.Ts.UnixNano()}
	b, _ := json.Marshal(m)

	// Store the latest price
	if err := r.rdb.Set(ctx, lastKey(t.Exchange, t.Symbol), b, r.ttl*2).Err(); err != nil {
		// Fallback to in-memory cache if Redis is unavailable
		_ = r.mem.PushTick(ctx, t)
		return fmt.Errorf("failed to set last price in Redis for %s:%s, using memory cache", t.Exchange, t.Symbol)
	}

	// Add the tick to a sorted set (using timestamp as score)
	score := float64(t.Ts.UnixNano()) / 1e6 // Convert nano to millis
	z := &redis.Z{Score: score, Member: string(b)}
	if err := r.rdb.ZAdd(ctx, ticksKey(t.Exchange, t.Symbol), *z).Err(); err != nil {
		// Fallback to memory cache if Redis fails
		_ = r.mem.PushTick(ctx, t)
		return fmt.Errorf("failed to add tick to Redis sorted set for %s:%s, using memory cache", t.Exchange, t.Symbol)
	}

	// Cleanup: remove old ticks beyond the TTL
	cut := float64(time.Now().Add(-r.ttl).UnixNano()) / 1e6
	if err := r.rdb.ZRemRangeByScore(ctx, ticksKey(t.Exchange, t.Symbol), "-inf", fmt.Sprintf("%f", cut)).Err(); err != nil {
		return fmt.Errorf("failed to trim old ticks in Redis for %s:%s", t.Exchange, t.Symbol)
	}

	return nil
}

// GetLatest fetches the latest price for a symbol from Redis (or in-memory cache if Redis fails).
func (r *RedisCache) GetLatest(ctx context.Context, symbol string) (map[string]float64, error) {
	pattern := fmt.Sprintf("last:*:%s", symbol)
	keys, err := r.rdb.Keys(ctx, pattern).Result()
	if err != nil || len(keys) == 0 {
		// Fallback to memory cache if Redis is unavailable or empty
		return r.mem.GetLatest(ctx, symbol)
	}

	// Collect results from Redis
	res := map[string]float64{}
	for _, k := range keys {
		b, err := r.rdb.Get(ctx, k).Bytes()
		if err != nil {
			continue
		}
		var m redisMember
		if err := json.Unmarshal(b, &m); err != nil {
			continue
		}
		// Extract exchange name from the Redis key
		parts := strings.Split(k, ":")
		if len(parts) >= 3 {
			ex := parts[1]
			res[ex] = m.Price
		}
	}

	if len(res) == 0 {
		return nil, fmt.Errorf("no data found for symbol %s", symbol)
	}
	return res, nil
}

// GetWindow fetches price ticks for a symbol within a given time window from Redis (or fallback to memory cache).
func (r *RedisCache) GetWindow(ctx context.Context, exchange, symbol string, d time.Duration) ([]domain.PriceTick, error) {
	nowMs := float64(time.Now().UnixNano()) / 1e6
	fromMs := float64(time.Now().Add(-d).UnixNano()) / 1e6
	members, err := r.rdb.ZRangeByScore(ctx, ticksKey(exchange, symbol), &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", fromMs),
		Max: fmt.Sprintf("%f", nowMs),
	}).Result()
	if err != nil || len(members) == 0 {
		// Fallback to memory cache if Redis fails
		return r.mem.GetWindow(ctx, exchange, symbol, d)
	}

	// Parse members into PriceTick objects
	var ticks []domain.PriceTick
	for _, mem := range members {
		var m redisMember
		if err := json.Unmarshal([]byte(mem), &m); err != nil {
			continue
		}
		ts := time.Unix(0, m.Ts)
		ticks = append(ticks, domain.PriceTick{
			Exchange: exchange,
			Symbol:   symbol,
			Price:    m.Price,
			Ts:       ts,
		})
	}
	return ticks, nil
}

// TrimOld removes old ticks from all tick keys (ticks:*).
func (r *RedisCache) TrimOld(ctx context.Context, olderThan time.Time) error {
	iter := r.rdb.Scan(ctx, 0, "ticks:*:*", 0).Iterator()
	cut := float64(olderThan.UnixNano()) / 1e6
	for iter.Next(ctx) {
		key := iter.Val()
		// Remove old entries from the sorted set
		_ = r.rdb.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%f", cut)).Err()
	}

	if err := iter.Err(); err != nil {
		// Fallback to memory cache trim
		return r.mem.TrimOld(ctx, olderThan)
	}
	return nil
}

// Health checks Redis connection health.
func (r *RedisCache) Health(ctx context.Context) error {
	_, err := r.rdb.Ping(ctx).Result()
	return err
}
