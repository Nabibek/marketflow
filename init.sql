CREATE TABLE IF NOT EXISTS market_aggregates (
  id BIGSERIAL PRIMARY KEY,
  pair_name TEXT NOT NULL,
  exchange  TEXT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  average_price DOUBLE PRECISION NOT NULL,
  min_price     DOUBLE PRECISION NOT NULL,
  max_price     DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_market_aggregates_pair_exchange_ts
  ON market_aggregates (pair_name, exchange, timestamp DESC);
