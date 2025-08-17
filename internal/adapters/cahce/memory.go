package cahce

import (
	"context"
	"errors"
	"sync"
	"time"

	"marketflow/internal/domain"
)

var ErrnotFound = errors.New("Not found")

type MemoryCache struct {
	mu      sync.RWMutex
	last    map[string]map[string]domain.PriceTick
	window  map[string]map[string][]domain.PriceTick
	ttl     time.Duration
	cleaner *time.Ticker
	done    chan struct{}
}

func NewMemoryCache(ttl time.Duration) *MemoryCache {
	m := &MemoryCache{
		last:   make(map[string]map[string]domain.PriceTick),
		window: make(map[string]map[string][]domain.PriceTick),
		ttl:    ttl,
		done:   make(chan struct{}),
	}
	m.cleaner = time.NewTicker(time.Second * 10)
	go m.backgroundCleaner()
	return m
}

func (m *MemoryCache) backgroundCleaner() {
	for {
		select {
		case <-m.cleaner.C:
			cut := time.Now().Add(-m.ttl)
			m.TrimOld(context.Background(), cut)
		case <-m.done:
			m.cleaner.Stop()
			return
		}
	}
}

func (m *MemoryCache) Close() {
	close(m.done)
}

func (m *MemoryCache) PushTick(cxt context.Context, t domain.PriceTick) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// last
	if _, ok := m.last[t.Symbol]; !ok {
		m.last[t.Symbol] = make(map[string]domain.PriceTick)
	}
	m.last[t.Symbol][t.Exchange] = t

	// window
	if _, ok := m.window[t.Exchange]; !ok {
		m.window[t.Exchange][t.Symbol] = append(m.window[t.Exchange][t.Symbol], t)
	}
	return nil
}

func (m *MemoryCache) GetLastest(ctx context.Context, symbol string) (map[string]float64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	exMap, ok := m.last[symbol]
	if !ok {
		return nil, ErrnotFound
	}
	res := make(map[string]float64, len(exMap))
	for ex, t := range exMap {
		res[ex] = t.Price
	}
	return res, nil
}

func (m *MemoryCache) GetWindow(ctx context.Context, exchange, symbol string, d time.Duration) ([]domain.PriceTick, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	exMap, ok := m.window[exchange]
	if !ok {
		return nil, ErrNotFound
	}
	a
}
