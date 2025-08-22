package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"marketflow/internal/domain"

	_ "github.com/lib/pq"
)

type PostgresRepo struct {
	db *sql.DB
}

func NewPostgresRepo(ctx context.Context, dsn string) (*PostgresRepo, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &PostgresRepo{db: db}, nil
}

func (r *PostgresRepo) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

func (r *PostgresRepo) InsertAggregates(ctx context.Context, items []domain.Aggregate) error {
	if len(items) == 0 {
		return nil
	}
	cols := []string{"pair_name", "exchange", "timestamp", "average_price", "min_price", "max_price"}

	var sb strings.Builder
	sb.WriteString("INSERT INTO market_aggregates (")
	sb.WriteString(strings.Join(cols, ","))
	sb.WriteString(") VALUES ")
	args := make([]interface{}, 0, len(items)*len(cols))
	for i, it := range items {
		if i > 0 {
			sb.WriteString(",")
		}
		base := i*len(cols) + 1
		placeholders := make([]string, len(cols))
		for j := range cols {
			placeholders[j] = fmt.Sprintf("$%d", base+j)
		}
		sb.WriteString("(" + strings.Join(placeholders, ",") + ")")

		args = append(args, it.Symbol, it.Exchange, it.Ts, it.Avg, it.Min, it.Max)
	}
	query := sb.String()

	_, err := r.db.ExecContext(ctx, query, args...)
	return err
}
