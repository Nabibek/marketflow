package app

import (
	"context"
	"errors"
	"log/slog"
	"marketflow/internal/domain"
	"marketflow/internal/ports"
	"math"
	"sync"
	"time"
)

const workersPerSource = 5

type Service struct {
	logger  *slog.Logger
	cache   ports.Cache
	sources []ports.TickerSource

	wg       sync.WaitGroup
	cancelMu sync.Mutex
	cancel   context.CancelFunc

	// seen pairs (exchange -> set(symbol))
	seenMu sync.RWMutex
	seen   map[string]map[string]struct{}
}

func NewService(logger *slog.Logger, cache ports.Cache) *Service {
	return &Service{
		logger: logger,
		cache:  cache,
		seen:   make(map[string]map[string]struct{}),
	}
}

func (s *Service) AttachSource(src ports.TickerSource) {
	s.sources = append(s.sources, src)
}

// recordSeen marks that we have seen ticks for exchange/symbol
func (s *Service) recordSeen(exchange, symbol string) {
	s.seenMu.Lock()
	defer s.seenMu.Unlock()
	if _, ok := s.seen[exchange]; !ok {
		s.seen[exchange] = make(map[string]struct{})
	}
	s.seen[exchange][symbol] = struct{}{}
}

// snapshotSeen returns a copy of seen map as exchange -> []symbol
func (s *Service) snapshotSeen() map[string][]string {
	s.seenMu.RLock()
	defer s.seenMu.RUnlock()
	out := make(map[string][]string, len(s.seen))
	for ex, m := range s.seen {
		arr := make([]string, 0, len(m))
		for sym := range m {
			arr = append(arr, sym)
		}
		out[ex] = arr
	}
	return out
}

// StartSources: на каждый источник — диспетчер + N воркеров; общий fan-in → кэш.
func (s *Service) StartSources(ctx context.Context) {
	s.cancelMu.Lock()
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.cancelMu.Unlock()

	processedCh := make(chan domain.PriceTick, 4096) // fan-in

	// общий писатель в cache.PushTick
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case t, ok := <-processedCh:
				if !ok {
					return
				}
				if err := s.cache.PushTick(ctx, t); err != nil {
					s.logger.Warn("cache push error", "err", err, "exchange", t.Exchange, "symbol", t.Symbol)
				}
			}
		}
	}()

	// по каждому источнику создаём воркеры и диспетчер
	for _, src := range s.sources {
		inCh := src.Start(ctx)

		// входные каналы для воркеров (fan-out)
		workerIns := make([]chan domain.PriceTick, workersPerSource)
		for i := 0; i < workersPerSource; i++ {
			workerIns[i] = make(chan domain.PriceTick, 1024)

			// каждый воркер валидирует тик, записывает seen и шлёт в processedCh
			s.wg.Add(1)
			go func(in <-chan domain.PriceTick) {
				defer s.wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case t, ok := <-in:
						if !ok {
							return
						}
						// валидация
						if t.Price <= 0 || t.Exchange == "" || t.Symbol == "" || t.Ts.IsZero() {
							continue
						}
						// отметим, что видели эту пару
						s.recordSeen(t.Exchange, t.Symbol)

						// отправим в общий канал на запись в кэш
						select {
						case processedCh <- t:
						case <-ctx.Done():
							return
						}
					}
				}
			}(workerIns[i])
		}

		// диспетчер (round-robin) раскидывает входные тики по воркерам
		s.wg.Add(1)
		go func(in <-chan domain.PriceTick, outs []chan domain.PriceTick) {
			defer s.wg.Done()
			var idx int
			for {
				select {
				case <-ctx.Done():
					// закрыть воркерам входы
					for _, c := range outs {
						close(c)
					}
					return
				case t, ok := <-in:
					if !ok {
						for _, c := range outs {
							close(c)
						}
						return
					}
					// best-effort non-blocking: if worker channel full, drop to next
					for i := 0; i < len(outs); i++ {
						pos := (idx + i) % len(outs)
						select {
						case outs[pos] <- t:
							idx = (pos + 1) % len(outs)
							goto dispatched
						default:
						}
					}
					// если все заполнены — блокируемся на текущем (чтобы не потерять данные)
					outs[idx] <- t
					idx = (idx + 1) % len(outs)
				dispatched:
				}
			}
		}(inCh, workerIns)
	}

	// закрыть processedCh, когда все горутины закончатся
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-ctx.Done()
		// подождём кратко, чтобы дать воркерам слить буферы
		time.Sleep(50 * time.Millisecond)
		close(processedCh)
	}()
}

func (s *Service) Stop() {
	s.cancelMu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.cancelMu.Unlock()
	s.wg.Wait()
}

func (s *Service) GetLatest(ctx context.Context, symbol string) (map[string]float64, error) {
	return s.cache.GetLatest(ctx, symbol)
}

// -------- Статистика за период (без БД): считаем по данным окна из Cache --------

type Stats struct {
	Count int
	Avg   float64
	Min   float64
	Max   float64
}

var ErrNoData = errors.New("no data for period")

// GetStats символ+опционально exchange ("" => все биржи), за период d.
func (s *Service) GetStats(ctx context.Context, exchange, symbol string, d time.Duration) (Stats, error) {
	var ticks []domain.PriceTick

	if exchange != "" {
		// только одна биржа
		arr, err := s.cache.GetWindow(ctx, exchange, symbol, d)
		if err != nil {
			return Stats{}, ErrNoData
		}
		ticks = append(ticks, arr...)
	} else {
		// все биржи: соберём список по GetLatest (быстро), а данные — из окна
		latest, err := s.cache.GetLatest(ctx, symbol)
		if err != nil {
			return Stats{}, ErrNoData
		}
		for ex := range latest {
			arr, err := s.cache.GetWindow(ctx, ex, symbol, d)
			if err == nil && len(arr) > 0 {
				ticks = append(ticks, arr...)
			}
		}
	}

	if len(ticks) == 0 {
		return Stats{}, ErrNoData
	}

	// посчитаем
	minV := math.Inf(1)
	maxV := math.Inf(-1)
	var sum float64
	for _, t := range ticks {
		p := t.Price
		if p < minV {
			minV = p
		}
		if p > maxV {
			maxV = p
		}
		sum += p
	}
	return Stats{
		Count: len(ticks),
		Avg:   sum / float64(len(ticks)),
		Min:   minV,
		Max:   maxV,
	}, nil
}

// StartAggregator запускает scheduler, который каждую minute агрегирует данные и пишет в repo
func (s *Service) StartAggregator(ctx context.Context, repo ports.Repository, interval time.Duration) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// align first tick to next interval boundary to avoid partial-minute aggregates if desired
		// but for simplicity we just wait interval then run repeatedly
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				// snapshot seen pairs
				pairs := s.snapshotSeen()
				if len(pairs) == 0 {
					continue
				}

				aggregates := make([]domain.Aggregate, 0, 256)
				// use end time as truncated minute
				ts := now.Truncate(time.Minute)

				for ex, syms := range pairs {
					for _, sym := range syms {
						// get last 60s from cache
						ticks, err := s.cache.GetWindow(ctx, ex, sym, 60*time.Second)
						if err != nil || len(ticks) == 0 {
							continue
						}
						var sum, minV, maxV float64
						minV = math.Inf(1)
						maxV = math.Inf(-1)
						for _, t := range ticks {
							p := t.Price
							sum += p
							if p < minV {
								minV = p
							}
							if p > maxV {
								maxV = p
							}
						}
						avg := sum / float64(len(ticks))
						aggregates = append(aggregates, domain.Aggregate{
							Exchange: ex,
							Symbol:   sym,
							Ts:       ts,
							Avg:      avg,
							Min:      minV,
							Max:      maxV,
						})
					}
				}

				if len(aggregates) == 0 {
					continue
				}

				// try insert; on error, log and continue (could implement retry/backoff)
				if err := repo.InsertAggregates(ctx, aggregates); err != nil {
					s.logger.Warn("failed to insert aggregates", "err", err, "count", len(aggregates))
				} else {
					s.logger.Info("aggregates written", "count", len(aggregates), "ts", ts)
				}
			}
		}
	}()
}
