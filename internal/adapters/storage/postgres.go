package storage

import (
	"context"
	"database/sql"
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

func (p *PostgresRepo) InsertAggregates(ctx context.Context, rows []domain.MinAggRow) error {
	if len(rows) == 0 {
		return nil
	}
	const q = `
		INSERT INTO market_aggregates (pair_name, exchange, timestamp, average_price, min_price, max_price)
		VALUES ($1,$2,$3,$4,$5,$6)
	`
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, r := range rows {
		if _, err := stmt.ExecContext(ctx, r.Pair, r.Exchange, r.Ts, r.Avg, r.Min, r.Max); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (p *PostgresRepo) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return p.db.PingContext(ctx)
}
