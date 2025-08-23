package exchange

import (
	"context"
	"math/rand"
	"time"

	"marketflow/internal/domain"
)

// GeneratorSource генерирует синтетические тики для набора символов.
type GeneratorSource struct {
	symbols  []string
	exchange string
	interval time.Duration
}

// NewGeneratorSource — конструктор.
func NewGeneratorSource(exchange string, symbols []string, interval time.Duration) *GeneratorSource {
	return &GeneratorSource{
		symbols:  symbols,
		exchange: exchange,
		interval: interval,
	}
}

// Name возвращает имя источника (exchange).
func (g *GeneratorSource) Name() string {
	return g.exchange
}

// Start запускает генератор: пишет тики в out до отмены ctx.
// Возвращает nil при штатной остановке (ctx canceled) или ошибку, если произошло что-то фатальное.
func (g *GeneratorSource) Start(ctx context.Context, out chan<- domain.PriceTick) error {
	// Инициализация случайных цен
	prices := make(map[string]float64, len(g.symbols))
	for _, s := range g.symbols {
		switch s {
		case "BTCUSDT":
			prices[s] = 50000 + rand.Float64()*1000
		case "ETFUSTD":
			prices[s] = 3000 + rand.Float64()*100
		default:
			prices[s] = 1 + rand.Float64()*10
		}
	}

	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// graceful stop
			return ctx.Err()
		case now := <-ticker.C:
			// генерируем тик для каждого символа и пишем в out
			for _, s := range g.symbols {
				// simple random walk
				delta := (rand.Float64() - 0.5) * 0.02 * prices[s]
				prices[s] += delta
				if prices[s] <= 0 {
					prices[s] = rand.Float64() * 10
				}
				select {
				case out <- domain.PriceTick{
					Exchange: g.exchange,
					Symbol:   s,
					Price:    prices[s],
					Ts:       now,
				}:
					// sent
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}
