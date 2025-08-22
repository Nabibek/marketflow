package config

import (
	"encoding/json"
	"os"
	"time"
)

type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
	TTL      string `json:"ttl"` // e.g. "60s"
}

type PostgresConfig struct {
	DSN string `json:"dsn"` // e.g. "postgres://user:pass@host:5432/dbname?sslmode=disable"
}

type AppConfig struct {
	HTTPPort int            `json:"http_port"`
	Redis    RedisConfig    `json:"redis"`
	Postgres PostgresConfig `json:"postgres"`
}

func Load(path string) (AppConfig, error) {
	var cfg AppConfig
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// ParseDuration parses duration string like "60s" and returns default 60s on error or empty.
func ParseDuration(s string) time.Duration {
	if s == "" {
		return 60 * time.Second
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 60 * time.Second
	}
	return d
}
