package cache

import (
	"context"
	"errors"
	"sync"
	"time"

	"marketflow/internal/domain"
)

var ErrNotFound = errors.New("not found")

// MemoryCache — простой in-memory кэш: хранит последнюю цену и окно последних тиков.
type MemoryCache struct {
	mu      sync.RWMutex
	last    map[string]map[string]domain.PriceTick   // last[symbol][exchange]
	window  map[string]map[string][]domain.PriceTick // window[exchange][symbol]
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
	m.cleaner = time.NewTicker(10 * time.Second)
	go m.backgroundCleaner()
	return m
}

func (m *MemoryCache) backgroundCleaner() {
	for {
		select {
		case <-m.cleaner.C:
			cut := time.Now().Add(-m.ttl)
			_ = m.TrimOld(context.Background(), cut)
		case <-m.done:
			m.cleaner.Stop()
			return
		}
	}
}

func (m *MemoryCache) Close() {
	close(m.done)
}

func (m *MemoryCache) Health(ctx context.Context) error {
	return nil
}

func (m *MemoryCache) PushTick(ctx context.Context, t domain.PriceTick) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// last
	if _, ok := m.last[t.Symbol]; !ok {
		m.last[t.Symbol] = make(map[string]domain.PriceTick)
	}
	m.last[t.Symbol][t.Exchange] = t

	// window
	if _, ok := m.window[t.Exchange]; !ok {
		m.window[t.Exchange] = make(map[string][]domain.PriceTick)
	}
	m.window[t.Exchange][t.Symbol] = append(m.window[t.Exchange][t.Symbol], t)

	return nil
}

func (m *MemoryCache) GetLatest(ctx context.Context, symbol string) (map[string]float64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	exMap, ok := m.last[symbol]
	if !ok {
		return nil, ErrNotFound
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
	arr, ok := exMap[symbol]
	if !ok {
		return nil, ErrNotFound
	}

	cut := time.Now().Add(-d)
	i := 0
	for ; i < len(arr); i++ {
		if !arr[i].Ts.Before(cut) { // Ts >= cut
			break
		}
	}
	// вернём копию «хвоста», чтобы не уронить гонки за пределами локов
	return append([]domain.PriceTick(nil), arr[i:]...), nil
}

func (m *MemoryCache) TrimOld(ctx context.Context, olderThan time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for ex := range m.window {
		for sym := range m.window[ex] {
			arr := m.window[ex][sym]
			j := 0
			for ; j < len(arr); j++ {
				if !arr[j].Ts.Before(olderThan) { // Ts >= olderThan
					break
				}
			}
			if j == 0 {
				continue
			}
			if j >= len(arr) {
				m.window[ex][sym] = nil
			} else {
				m.window[ex][sym] = append([]domain.PriceTick(nil), arr[j:]...)
			}
		}
	}
	return nil
}
