package internal

import "time"

const TopicOrderCreated = "order.created"

type OrderCreated struct {
	EventID   string    `json:"event_id"`
	OrderID   int64     `json:"order_id"`
	UserID    int64     `json:"user_id"`
	Amount    int64     `json:"amount"`
	CreatedAt time.Time `json:"created_at"`
}
