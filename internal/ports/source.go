package ports

import (
	"context"

	"marketflow/internal/domain"
)

type TikerSource interface {
	Start(cxt context.Context) <-chan domain.PriceTick
}
