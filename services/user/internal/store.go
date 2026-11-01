package internal

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")
var ErrEmailTaken = errors.New("email already registered")

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) Create(ctx context.Context, name, email string) (User, error) {
	u := User{Name: name, Email: email}

	err := s.db.QueryRow(ctx,
		`INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, created_at`,
		name, email).Scan(&u.ID, &u.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return u, ErrNotFound
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return u, ErrEmailTaken
	}

	return u, err
}

func (s *Store) Get(ctx context.Context, id int64) (User, error) {
	var u User

	err := s.db.QueryRow(ctx, `SELECT id, name, email, created_at FROM users WHERE id = $1`, id).
		Scan(&u.ID, &u.Name, &u.Email, &u.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return u, ErrNotFound
	}

	return u, err
}
