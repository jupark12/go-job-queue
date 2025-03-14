package models

type Transaction struct {
	Date        string
	Description string
	Amount      float64
	Type        string // "debit" or "credit"
}
