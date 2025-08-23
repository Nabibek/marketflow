package app

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"marketflow/internal/domain"
	"marketflow/internal/ports"
)

type Mode string

const (
	ModeTest Mode = "test"
	ModeLive Mode = "live"
)

type sourceRunner struct {
	src    ports.Source
	cancel context.CancelFunc
}

type Service struct {
	log *slog.Logger

	cache ports.Cache

	// агрегатор
	aggMu       sync.Mutex
	aggRunning  bool
	aggStopChan chan struct{}

	// источники
	mu          sync.Mutex
	mode        Mode
	startedAt   time.Time
	testSources []ports.Source
	liveSources []ports.Source
	runners     []*sourceRunner // активные раннеры текущего режима

	// общая очередь тиков для fan-in
	inCh chan domain.PriceTick
	wg   sync.WaitGroup
}

func NewService(log *slog.Logger, cache ports.Cache) *Service {
	return &Service{
		log:       log,
		cache:     cache,
		mode:      ModeTest,
		inCh:      make(chan domain.PriceTick, 1024),
		startedAt: time.Now(),
	}
}

func (s *Service) AttachTestSource(src ports.Source) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.testSources = append(s.testSources, src)
}

func (s *Service) AttachLiveSource(src ports.Source) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.liveSources = append(s.liveSources, src)
}

// запуск текущего режима
func (s *Service) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.log.Info("service starting", "mode", s.mode)
	s.startedAt = time.Now()

	// общий consumer, который пишет в cache
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case t, ok := <-s.inCh:
				if !ok {
					return
				}
				_ = s.cache.PushTick(ctx, t)
			case <-ctx.Done():
				return
			}
		}
	}()

	var srcs []ports.Source
	switch s.mode {
	case ModeTest:
		srcs = s.testSources
	case ModeLive:
		srcs = s.liveSources
	}

	for _, src := range srcs {
		cctx, cancel := context.WithCancel(ctx)
		r := &sourceRunner{src: src, cancel: cancel}
		s.runners = append(s.runners, r)
		s.wg.Add(1)
		go func(sr *sourceRunner) {
			defer s.wg.Done()
			if err := sr.src.Start(cctx, s.inCh); err != nil {
				s.log.Warn("source stopped with error", "exchange", sr.src.Name(), "err", err)
			} else {
				s.log.Info("source stopped", "exchange", sr.src.Name())
			}
		}(r)
	}
}

// остановка всех источников и consumer
func (s *Service) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range s.runners {
		r.cancel()
	}
	s.runners = nil

	// закрывать inCh нельзя, т.к. сервис может быть перезапущен.
}

// переключение режима: аккуратно останавливаем текущие источники и стартуем новые
func (s *Service) SwitchMode(ctx context.Context, m Mode) {
	s.mu.Lock()
	if s.mode == m {
		s.mu.Unlock()
		return
	}
	s.log.Info("switching mode", "from", s.mode, "to", m)

	// стоп старых
	for _, r := range s.runners {
		r.cancel()
	}
	s.runners = nil
	s.mode = m
	s.mu.Unlock()

	// запуск новых
	s.Start(ctx)
}

// агрегатор — как был, только имя метода то же, чтобы твой main не ломать
func (s *Service) StartAggregator(ctx context.Context, repo ports.Repository, every time.Duration) {
	s.aggMu.Lock()
	if s.aggRunning {
		s.aggMu.Unlock()
		return
	}
	s.aggRunning = true
	s.aggStopChan = make(chan struct{})
	s.aggMu.Unlock()

	s.log.Info("aggregator starting", "period", every.String())

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(every)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// соберем за 60s из cache
				cut := 60 * time.Second
				rows := s.buildMinuteAggregates(ctx, cut)
				if len(rows) == 0 {
					continue
				}
				if err := repo.InsertAggregates(ctx, rows); err != nil {
					s.log.Error("insert aggregates failed", "err", err, "rows", len(rows))
				}
			case <-s.aggStopChan:
				s.log.Info("aggregator stop requested")
				return
			case <-ctx.Done():
				s.log.Info("aggregator exit due to context")
				return
			}
		}
	}()
}

func (s *Service) buildMinuteAggregates(ctx context.Context, d time.Duration) []domain.MinAggRow {
	// минимальная реализация: мы не знаем всех источников и пар из cache абстракции
	// Поэтому соберем по тем, что приходят в течение d из источников текущего режима
	// (в твоей реализации у cache есть GetWindow(exchange, symbol, d))
	pairs := []string{"BTCUSDT", "DOGEUSDT", "TONUSDT", "SOLUSDT", "ETHUSDT"}
	exchanges := s.listExchanges()

	var rows []domain.MinAggRow
	now := time.Now()

	for _, ex := range exchanges {
		for _, sym := range pairs {
			w, err := s.cache.GetWindow(ctx, ex, sym, d)
			if err != nil || len(w) == 0 {
				continue
			}
			var sum float64
			min := w[0].Price
			max := w[0].Price
			for _, t := range w {
				p := t.Price
				sum += p
				if p < min {
					min = p
				}
				if p > max {
					max = p
				}
			}
			avg := sum / float64(len(w))
			rows = append(rows, domain.MinAggRow{
				Pair:     sym,
				Exchange: ex,
				Ts:       now,
				Avg:      avg,
				Min:      min,
				Max:      max,
			})
		}
	}
	return rows
}

func (s *Service) listExchanges() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := make(map[string]struct{})
	for _, r := range s.runners {
		m[r.src.Name()] = struct{}{}
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Health info DTO
type HealthInfo struct {
	Status     string        `json:"status"`
	Mode       Mode          `json:"mode"`
	Uptime     time.Duration `json:"uptime"`
	Sources    []string      `json:"sources"`
	RedisOK    bool          `json:"redis_ok"`
	PostgresOK *bool         `json:"postgres_ok,omitempty"`
}

func (s *Service) Health(ctx context.Context, repo ports.Repository) HealthInfo {
	h := HealthInfo{
		Status:  "ok",
		Mode:    s.mode,
		Uptime:  time.Since(s.startedAt),
		Sources: s.listExchanges(),
	}
	if err := s.cache.Health(ctx); err != nil {
		h.RedisOK = false
		h.Status = "degraded"
	} else {
		h.RedisOK = true
	}
	if repo != nil {
		ok := repo.Health(ctx) == nil
		h.PostgresOK = &ok
		if !ok {
			h.Status = "degraded"
		}
	}
	return h
}

// Получение последних цен для символа
func (s *Service) GetLatest(ctx context.Context, symbol string) (map[string]float64, error) {
	// Получаем последние цены из кэша
	data, err := s.cache.GetLatest(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest prices: %w", err)
	}
	return data, nil
}

// GetStats возвращает статистику по наибольшей, наименьшей, средней цене за период
func (s *Service) GetStats(ctx context.Context, exchange, symbol string, d time.Duration) (*domain.PriceStats, error) {
	// Получаем все тики для заданной валютной пары за период
	ticks, err := s.cache.GetWindow(ctx, exchange, symbol, d)
	if err != nil {
		return nil, fmt.Errorf("failed to get ticks: %w", err)
	}

	if len(ticks) == 0 {
		return nil, fmt.Errorf("no ticks available for %s on %s", symbol, exchange)
	}

	var min, max, sum float64
	count := float64(len(ticks))

	// Инициализируем min и max первым тиком
	min = ticks[0].Price
	max = ticks[0].Price

	// Подсчитываем минимальное, максимальное и среднее значение
	for _, tick := range ticks {
		price := tick.Price
		if price < min {
			min = price
		}
		if price > max {
			max = price
		}
		sum += price
	}

	// Средняя цена
	avg := sum / count

	// Возвращаем статистику
	stats := &domain.PriceStats{
		Min:   min,
		Max:   max,
		Avg:   avg,
		Count: count,
	}

	return stats, nil
}

func (s *Service) GetWindow(cxt context.Context, exchange, symbol string, d time.Duration) ([]domain.PriceTick, error) {
	// если явно указан exchange — делегируем к кэшу
	if exchange != "" {
		return s.cache.GetWindow(cxt, exchange, symbol, d)
	}

	// иначе — собираем со всех активных источников
	exchanges := s.listExchanges()
	if len(exchanges) == 0 {
		return nil, fmt.Errorf("no active exchanges")
	}

	var all []domain.PriceTick
	for _, ex := range exchanges {
		// Safe to call cache.GetWindow — он вернёт fallback на память при отсутствии Redis
		w, err := s.cache.GetWindow(cxt, ex, symbol, d)
		if err != nil {
			// Если для конкретного exchange нет данных — просто пропускаем
			continue
		}
		if len(w) > 0 {
			all = append(all, w...)
		}
	}

	if len(all) == 0 {
		return nil, fmt.Errorf("no ticks for %s", symbol)
	}

	// Сортируем по временам (по возрастанию)
	sort.Slice(all, func(i, j int) bool {
		return all[i].Ts.Before(all[j].Ts)
	})

	return all, nil
}
