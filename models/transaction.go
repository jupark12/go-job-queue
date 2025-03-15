package models

import "time"

type Transaction struct {
	Date        time.Time
	Description string
	Amount      float64
	Type        string // "debit" or "credit"
}
