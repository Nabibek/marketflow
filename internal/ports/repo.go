package ports

import (
	"context"

	"marketflow/internal/domain"
)

type Repository interface {
	InsertAggregates(cxt context.Context, items []domain.Aggregate) error
}
