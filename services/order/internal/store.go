package internal

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) Create(ctx context.Context, userID int64, item string, amount int64) (Order, error) {
	o := Order{UserID: userID, Item: item, Amount: amount}

	err := s.db.QueryRow(ctx,
		`INSERT INTO orders (user_id, item, amount) VALUES ($1, $2, $3) RETURNING id, status, created_at`,
		userID, item, amount).Scan(&o.ID, &o.Status, &o.CreatedAt)

	return o, err
}

func (s *Store) Get(ctx context.Context, id int64) (Order, error) {
	var o Order

	err := s.db.QueryRow(ctx,
		`SELECT id, user_id, item, amount, status, created_at FROM orders WHERE id = $1`, id).
		Scan(&o.ID, &o.UserID, &o.Item, &o.Amount, &o.Status, &o.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return o, ErrNotFound
	}

	return o, err
}
