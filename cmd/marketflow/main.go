package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"marketflow/internal/logging"
)

const usageText = `Usage:
  marketflow [--port <N>]
  marketflow --help

Options:
  --port N     Port number
`

func main() {
	// флаги CLI
	port := flag.Int("port", 8080, "Port number")
	help := flag.Bool("help", false, "Show help")
	flag.Parse()

	if *help {
		fmt.Print(usageText)
		return
	}

	// базовый логер (slog) с уровнем Info
	logger := logging.NewLogger(slog.LevelInfo)

	// валидация входных параметров
	if *port <= 0 || *port > 65535 {
		logger.Error("invalid --port value", "port", *port)
		os.Exit(2)
	}

	// контекст и graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("marketflow starting...", "port", *port)

	// Здесь позже будет wiring конфигурации, источников, HTTP-сервера и т.п.
	// Пока просто «держим» процесс живым до сигнала, чтобы проверить shutdown.
	select {
	case <-ctx.Done():
	}

	// Финальная уборка (на будущее: закрыть соединения, дождаться воркеров)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = shutdownCtx // placeholder

	logger.Info("marketflow stopped")
}
