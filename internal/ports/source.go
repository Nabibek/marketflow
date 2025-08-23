package ports

import (
	"context"

	"marketflow/internal/domain"
)

type Source interface {
	// name of exchanges
	Name() string

	// Start запускает чтение данных и пишет тики
	Start(cxt context.Context, out chan<- domain.PriceTick) error
}
