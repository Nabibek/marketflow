package ports

import (
	"context"
	"marketflow/internal/domain"
	"time"
)

type Cache interface {
	PushTick(cxt context.Context, t domain.PriceTick) error
	GetLatest(cxt context.Context, symbol string) (map[string]float64, error)
	GetWindow(cxt context.Context, exchange, symbol string, d time.Duration) ([]domain.PriceTick, error)
	TrimOld(cxt context.Context, olderThan time.Time) error
}
