package exchange

import (
	"context"
	"marketflow/internal/domain"
	"math/rand"
	"time"
)

type GeneratorSourse struct {
	symbols  []string
	exchange string
	interval time.Duration
}

func NewGeneratorSource(exchange string, symbols []string, interval time.Duration) *GeneratorSourse {
	return &GeneratorSourse{
		symbols:  symbols,
		exchange: exchange,
		interval: interval,
	}
}

func (g *GeneratorSourse) Start(ctx context.Context) <-chan domain.PriceTick {
	out := make(chan domain.PriceTick)
	go func() {
		defer close(out)
		prices := make(map[string]float64)
		for _, s := range g.symbols {
			switch {
			case s == "BTCUSDT":
				prices[s] = 50000 + rand.Float64()*1000
			case s == "ETFUSTD":
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
				return
			case now := <-ticker.C:

				for _, s := range g.symbols {
					// random walk
					delta := (rand.Float64() - 0.5) * 0.02 * prices[s]
					prices[s] += delta
					if prices[s] <= 0 {
						prices[s] = rand.Float64() * 10
					}
					out <- domain.PriceTick{
						Exchange: g.exchange,
						Symbol:   s,
						Price:    prices[s],
						Ts:       now,
					}
				}
			}
		}
	}()
	return out
}
