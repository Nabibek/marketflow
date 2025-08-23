package domain

import "time"

type PriceTick struct {
	Exchange string
	Symbol   string
	Price    float64
	Ts       time.Time
}

type MinAggRow struct {
	Pair     string    // pair_name
	Exchange string    // exchange
	Ts       time.Time // timestamp
	Avg      float64   // average_price
	Min      float64   // min_price
	Max      float64   // max_price
}

type PriceStats struct {
	Avg   float64 `json:"avg"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Count float64 `json:"count"`
}
