package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"marketflow/internal/app"
	"marketflow/internal/ports"
)

type Server struct {
	addr   string
	svc    *app.Service
	logger *slog.Logger
	srv    *http.Server
	repo   ports.Repository
}

func NewServer(addr string, svc *app.Service, logger *slog.Logger) *Server {
	return &Server{
		addr:   addr,
		svc:    svc,
		logger: logger,
	}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// Маршруты для цен
	mux.HandleFunc("/prices/latest/", s.handleLatest)
	mux.HandleFunc("/prices/highest/", s.handleHighest)
	mux.HandleFunc("/prices/lowest/", s.handleLowest)
	mux.HandleFunc("/prices/average/", s.handleAverage)

	// Обработка здоровья сервера
	mux.HandleFunc("/health", s.handleHealth)

	// Обработка переключения режимов
	mux.HandleFunc("/mode/test", s.handleModeTest)
	mux.HandleFunc("/mode/live", s.handleModeLive)

	s.srv = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		// graceful shutdown
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.srv.Shutdown(shutdownCtx)
	}()

	s.logger.Info("http server starting", "addr", s.addr)
	return s.srv.Serve(ln)
}

// handleLatest — обработка запроса на последние цены.
func (s *Server) handleLatest(w http.ResponseWriter, r *http.Request) {
	trim := strings.TrimPrefix(r.URL.Path, "/prices/latest/")
	parts := strings.Split(trim, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "symbol required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	if len(parts) == 1 {
		symbol := parts[0]
		data, err := s.svc.GetLatest(ctx, symbol)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, map[string]interface{}{"symbol": symbol, "data": data})
		return
	}

	exchange := parts[0]
	symbol := parts[1]
	data, err := s.svc.GetLatest(ctx, symbol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	p, ok := data[exchange]
	if !ok {
		http.Error(w, fmt.Sprintf("no data for %s on %s", symbol, exchange), http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]interface{}{"exchange": exchange, "symbol": symbol, "price": p})
}

// handleHighest — обработка запроса на наибольшую цену.
func (s *Server) handleHighest(w http.ResponseWriter, r *http.Request) {
	ex, sym, d, err := parsePathAndPeriod(r.URL.Path, "/prices/highest/", r.URL.Query().Get("period"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stats, e := s.svc.GetStats(r.Context(), ex, sym, d)
	if e != nil {
		http.Error(w, e.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]interface{}{
		"exchange": ex, "symbol": sym, "period": d.String(),
		"highest": stats.Max, "count": stats.Count,
	})
}

// handleLowest — обработка запроса на наименьшую цену.
func (s *Server) handleLowest(w http.ResponseWriter, r *http.Request) {
	ex, sym, d, err := parsePathAndPeriod(r.URL.Path, "/prices/lowest/", r.URL.Query().Get("period"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stats, e := s.svc.GetStats(r.Context(), ex, sym, d)
	if e != nil {
		http.Error(w, e.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]interface{}{
		"exchange": ex, "symbol": sym, "period": d.String(),
		"lowest": stats.Min, "count": stats.Count,
	})
}

// handleAverage — обработка запроса на среднюю цену.
func (s *Server) handleAverage(w http.ResponseWriter, r *http.Request) {
	ex, sym, d, err := parsePathAndPeriod(r.URL.Path, "/prices/average/", r.URL.Query().Get("period"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stats, e := s.svc.GetStats(r.Context(), ex, sym, d)
	if e != nil {
		http.Error(w, e.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]interface{}{
		"exchange": ex, "symbol": sym, "period": d.String(),
		"average": stats.Avg, "count": stats.Count,
	})
}

// writeJSON — вспомогательная функция для записи JSON-ответа.
func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// parsePathAndPeriod — помогает разбирать URL для обработки путей и периода.
func parsePathAndPeriod(path, prefix, period string) (exchange, symbol string, d time.Duration, err error) {
	trim := strings.TrimPrefix(path, prefix)
	parts := strings.Split(trim, "/")

	if len(parts) == 0 || parts[0] == "" {
		return "", "", 0, fmt.Errorf("symbol required")
	}
	if len(parts) == 1 {
		symbol = parts[0]
	} else {
		exchange = parts[0]
		symbol = parts[1]
	}

	// период
	if period == "" {
		return exchange, symbol, time.Minute, nil
	}
	// поддержим стандартный формат time.ParseDuration
	if dur, e := time.ParseDuration(period); e == nil {
		return exchange, symbol, dur, nil
	}
	// простая форма: число => секунды
	if n, e := strconv.Atoi(period); e == nil {
		return exchange, symbol, time.Duration(n) * time.Second, nil
	}
	return "", "", 0, fmt.Errorf("invalid period: %q", period)
}

func (s *Server) WithRepo(repo ports.Repository) *Server {
	s.repo = repo
	return s
}

// handleHealth — обработка запроса для проверки состояния.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	info := s.svc.Health(ctx, s.repo)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

// handleModeTest — переключение режима на "test".
func (s *Server) handleModeTest(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("HTTP: switch mode -> test")
	go s.svc.SwitchMode(r.Context(), app.ModeTest)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"mode":"test"}`))
}

// handleModeLive — переключение режима на "live".
func (s *Server) handleModeLive(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("HTTP: switch mode -> live")
	go s.svc.SwitchMode(r.Context(), app.ModeLive)
	// пока live-источники не подключены — режим включится, но источников нет
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"mode":"live"}`))
}
