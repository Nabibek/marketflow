package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"marketflow/internal/domain"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache stores last tick and time-sorted ticks in Redis.
// Keys:
//
//	last:{exchange}:{symbol}   -> JSON {"price":..., "ts": <unixnano>}
//	ticks:{exchange}:{symbol}  -> sorted set: score = unix_millis, member = JSON {"price":..., "ts": <unixnano>}
type RedisCache struct {
	rdb *redis.Client
	// fallback
	mem *MemoryCache
	ttl time.Duration
}

// NewRedisCache creates and pings Redis.
func NewRedisCache(ctx context.Context, addr, password string, db int, ttl time.Duration) (*RedisCache, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	// ping
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	rc := &RedisCache{
		rdb: rdb,
		mem: NewMemoryCache(ttl),
		ttl: ttl,
	}

	// background cleanup: periodically trim old zset members
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = rc.TrimOld(ctx, time.Now().Add(-rc.ttl))
			}
		}
	}()

	return rc, nil
}

// Close both redis and memory cleaner
func (r *RedisCache) Close() error {
	// stop mem cleaner
	r.mem.Close()
	return r.rdb.Close()
}

type redisMember struct {
	Price float64 `json:"price"`
	Ts    int64   `json:"ts"` // unix nano
}

// helper keys
func lastKey(exchange, symbol string) string  { return fmt.Sprintf("last:%s:%s", exchange, symbol) }
func ticksKey(exchange, symbol string) string { return fmt.Sprintf("ticks:%s:%s", exchange, symbol) }

// PushTick: try Redis, on error fallback to memory cache (but still return nil to avoid panics upstream)
func (r *RedisCache) PushTick(ctx context.Context, t domain.PriceTick) error {
	m := redisMember{Price: t.Price, Ts: t.Ts.UnixNano()}
	b, _ := json.Marshal(m)

	// set last
	if err := r.rdb.Set(ctx, lastKey(t.Exchange, t.Symbol), b, r.ttl*2).Err(); err != nil {
		// fallback to memory, but do not return fatal err
		_ = r.mem.PushTick(ctx, t)
		return nil
	}

	// add to sorted set with score = unix milliseconds
	score := float64(t.Ts.UnixNano()) / 1e6
	z := &redis.Z{Score: score, Member: string(b)}
	if err := r.rdb.ZAdd(ctx, ticksKey(t.Exchange, t.Symbol), *z).Err(); err != nil {
		_ = r.mem.PushTick(ctx, t)
		return nil
	}
	// remove older-than-ttl members for the key (best-effort)
	cut := float64(time.Now().Add(-r.ttl).UnixNano()) / 1e6
	_ = r.rdb.ZRemRangeByScore(ctx, ticksKey(t.Exchange, t.Symbol), "-inf", fmt.Sprintf("%f", cut)).Err()
	return nil
}

// GetLatest: returns map[exchange]price for a symbol
func (r *RedisCache) GetLatest(ctx context.Context, symbol string) (map[string]float64, error) {
	// pattern last:*:symbol
	pattern := fmt.Sprintf("last:*:%s", symbol)
	keys, err := r.rdb.Keys(ctx, pattern).Result()
	if err != nil || len(keys) == 0 {
		// fallback to mem
		return r.mem.GetLatest(ctx, symbol)
	}

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
		// extract exchange from key "last:{exchange}:{symbol}"
		parts := strings.Split(k, ":")
		if len(parts) >= 3 {
			ex := parts[1]
			res[ex] = m.Price
		}
	}
	if len(res) == 0 {
		return nil, fmt.Errorf("no data")
	}
	return res, nil
}

// GetWindow: get ticks for given exchange+symbol within last d
func (r *RedisCache) GetWindow(ctx context.Context, exchange, symbol string, d time.Duration) ([]domain.PriceTick, error) {
	nowMs := float64(time.Now().UnixNano()) / 1e6
	fromMs := float64(time.Now().Add(-d).UnixNano()) / 1e6
	members, err := r.rdb.ZRangeByScore(ctx, ticksKey(exchange, symbol), &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", fromMs),
		Max: fmt.Sprintf("%f", nowMs),
	}).Result()
	if err != nil || len(members) == 0 {
		// fallback
		return r.mem.GetWindow(ctx, exchange, symbol, d)
	}

	out := make([]domain.PriceTick, 0, len(members))
	for _, mem := range members {
		var m redisMember
		if err := json.Unmarshal([]byte(mem), &m); err != nil {
			continue
		}
		ts := time.Unix(0, m.Ts)
		out = append(out, domain.PriceTick{
			Exchange: exchange,
			Symbol:   symbol,
			Price:    m.Price,
			Ts:       ts,
		})
	}
	return out, nil
}

// TrimOld: remove members olderThan from all ticks:* keys
func (r *RedisCache) TrimOld(ctx context.Context, olderThan time.Time) error {
	// find all ticks keys
	iter := r.rdb.Scan(ctx, 0, "ticks:*:*", 0).Iterator()
	cut := float64(olderThan.UnixNano()) / 1e6
	for iter.Next(ctx) {
		key := iter.Val()
		_ = r.rdb.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%f", cut)).Err()
	}
	if err := iter.Err(); err != nil {
		// fallback: try mem trim
		return r.mem.TrimOld(ctx, olderThan)
	}
	return nil
}
