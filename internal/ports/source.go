package ports

import (
	"context"
	"marketflow/internal/domain"
)

type TickerSource interface {
	Start(cxt context.Context) <-chan domain.PriceTick
}
