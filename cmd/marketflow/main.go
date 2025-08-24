package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"marketflow/internal/adapters/cache"
	"marketflow/internal/adapters/exchange"
	"marketflow/internal/adapters/storage"
	"marketflow/internal/app"
	"marketflow/internal/domain"
	"marketflow/internal/http"
	"marketflow/internal/logging"
	"marketflow/internal/ports"
)

const usageText = `Usage:
  marketflow [--port <N>]
  marketflow --help

Options:
  --port N     Port number (overrides env SERVER_PORT)
`

func main() {
	portFlag := flag.Int("port", 0, "Port number (overrides env SERVER_PORT)")
	help := flag.Bool("help", false, "Show help")
	flag.Parse()

	if *help {
		fmt.Print(usageText)
		return
	}

	logger := logging.NewLogger(slog.LevelInfo)

	// Graceful context
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("marketflow starting")

	// --- Чтение ENV ---
	httpPort := getEnvAsInt("SERVER_PORT", 8080)
	if *portFlag != 0 {
		httpPort = *portFlag
	}

	redisAddr := getEnv("REDIS_ADDR", "127.0.0.1:6379")
	redisPass := getEnv("REDIS_PASSWORD", "")
	redisDB := getEnvAsInt("REDIS_DB", 0)
	redisTTL := getEnvAsDuration("REDIS_TTL", 60*time.Second)

	pgDSN := getEnv("POSTGRES_DSN", "")

	// --- Redis cache ---
	var c ports.Cache
	var redisCache *cache.RedisCache
	var err error
	redisCache, err = cache.NewRedisCache(ctx, redisAddr, redisPass, redisDB, redisTTL)
	if err != nil {
		logger.Warn("redis not available, using in-memory cache", "err", err)
		c = cache.NewMemoryCache(redisTTL)
	} else {
		logger.Info("redis cache connected", "addr", redisAddr)
		c = redisCache
		defer func() {
			if err := redisCache.Close(); err != nil {
				logger.Warn("error closing redis cache", "err", err)
			}
		}()
	}

	// --- Сервис ---
	svc := app.NewService(logger, c)

	// генератор котировок
	symbols := []string{"BTCUSDT", "DOGEUSDT", "TONUSDT", "SOLUSDT", "ETHUSDT"}
	gen := exchange.NewGeneratorSource("GENERATOR", symbols, 1*time.Second)
	svc.AttachTestSource(gen)

	// live

	priceChan := make(chan domain.PriceTick, 100)
	svc.AttachLiveSource(exchange.NewTCPSource("EX1", "127.0.0.1:40101", priceChan))
	svc.AttachLiveSource(exchange.NewTCPSource("EX2", "127.0.0.1:40102", priceChan))
	svc.AttachLiveSource(exchange.NewTCPSource("EX3", "127.0.0.1:40103", priceChan))

	// Обработка тиков из канала и сохранение в Redis/Postgres.
	go func() {
		for tick := range priceChan {
			log.Printf("Received price tick: %v", tick)

			// Отправка тика в кэш (Redis или in-memory)
			if err := redisCache.PushTick(ctx, tick); err != nil {
				log.Printf("Error saving tick: %v", err)
			}
		}
	}()

	// --- Postgres ---
	var repo ports.Repository
	if pgDSN != "" {
		pgRepo, err := storage.NewPostgresRepo(ctx, pgDSN)
		if err != nil {
			logger.Warn("postgres not available, continuing without persistent storage", "err", err)
			repo = nil
		} else {
			logger.Info("postgres connected")
			repo = pgRepo
			defer func() {
				if err := pgRepo.Close(); err != nil {
					logger.Warn("error closing pg repo", "err", err)
				}
			}()
		}
	}

	// агрегатор
	if repo != nil {
		svc.StartAggregator(ctx, repo, 1*time.Minute)
	} else {
		logger.Warn("aggregator disabled (no postgres)")
	}

	// --- HTTP сервер ---
	addr := fmt.Sprintf(":%d", httpPort)
	httpSrv := http.NewServer(addr, svc, logger).WithRepo(repo)

	// запускаем сервер в том же потоке, убираем горутину
	if err := httpSrv.Start(ctx); err != nil && err != context.Canceled {
		logger.Error("http server failed", "err", err)
		stop()
	}

	// Завершаем работу, когда получаем сигнал остановки.
	select {
	case <-ctx.Done():
		logger.Info("shutdown initiated")
	}

	svc.Stop()
	time.Sleep(100 * time.Millisecond)

	logger.Info("marketflow stopped")
}

// --- helpers ---

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvAsInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var val int
		fmt.Sscanf(v, "%d", &val)
		return val
	}
	return def
}

func getEnvAsDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}
