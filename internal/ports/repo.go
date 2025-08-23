package ports

import (
	"context"

	"marketflow/internal/domain"
)

type Repository interface {
	InsertAggregates(cxt context.Context, rows []domain.MinAggRow) error
	Health(ctx context.Context) error
	Close() error
}
