package ports

import (
	"context"
	"time"

	"marketflow/internal/domain"
)

type Cache interface {
	PushTick(cxt context.Context, t domain.PriceTick) error
	GetLastest(cxt context.Context, symbol string) (map[string]float64, error)
	GetWindow(cxt context.Context, exchange, symbol string, d time.Duration) ([]domain.PriceTick, error)
	TrimOld(cxt context.Context, olderThan time.Time) error
}
