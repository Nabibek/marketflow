package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"marketflow/internal/app"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	addr   string
	svc    *app.Service
	logger *slog.Logger
	srv    *http.Server
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
	mux.HandleFunc("/prices/latest/", s.handleLatest)
	mux.HandleFunc("/prices/highest/", s.handleHighest)
	mux.HandleFunc("/prices/lowest/", s.handleLowest)
	mux.HandleFunc("/prices/average/", s.handleAverage)

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

func (s *Server) handleLatest(w http.ResponseWriter, r *http.Request) {
	// /prices/latest/{symbol}  OR  /prices/latest/{exchange}/{symbol}
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

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// parsePathAndPeriod поддерживает:
//
//	/prices/.../{symbol}
//	/prices/.../{exchange}/{symbol}
//
// period по умолчанию 1m; допускаем s/m (и числа, трактуем как секунды).
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
