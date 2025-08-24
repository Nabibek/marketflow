package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"marketflow/internal/domain"
)

type TCPSource struct {
	name string
	addr string
}

func NewTCPSource(name, addr string, priceChan chan<- domain.PriceTick) *TCPSource {
	return &TCPSource{name: name, addr: addr}
}

func (s *TCPSource) Name() string {
	return s.name
}

func (s *TCPSource) Start(ctx context.Context, out chan<- domain.PriceTick) error {
	conn, err := net.Dial("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", s.addr, err)
	}
	defer conn.Close()

	log.Printf("Connected to %s", s.addr)

	// Пример парсинга данных из строки или JSON
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// Чтение данных
			var tick domain.PriceTick
			decoder := json.NewDecoder(conn)
			if err := decoder.Decode(&tick); err != nil {
				log.Printf("Error reading from %s: %v", s.addr, err)
				return err
			}

			// Отправка данных в канал
			out <- tick
		}
	}
}
