package internal

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

var ErrUserNotFound = errors.New("user not found")

type UserClient struct {
	baseURL string
	http    *http.Client
}

func NewUserClient(baseURL string) *UserClient {
	return &UserClient{baseURL: baseURL, http: &http.Client{Timeout: 3 * time.Second}}
}

func (c *UserClient) Exists(ctx context.Context, id int64) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/users/%d", c.baseURL, id), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("call user service: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusNotFound:
		return ErrUserNotFound
	default:
		return fmt.Errorf("user service returned %d", resp.StatusCode)
	}
}
