package domain

import "time"

type PriceTick struct {
	Exchange string
	Symbol   string
	Price    float64
	Ts       time.Time
}

type Aggregate struct {
	Exchange      string
	Symbol        string
	Ts            time.Time
	Avg, Min, Max float64
}
