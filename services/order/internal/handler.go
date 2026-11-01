package internal

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
)

type Handler struct {
	store *Store
	users *UserClient
	pub   *Publisher
	log   *slog.Logger
}

func NewHandler(s *Store, users *UserClient, pub *Publisher, log *slog.Logger) *Handler {
	return &Handler{
		store: s,
		users: users,
		pub:   pub,
		log:   log,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /orders", h.create)
	mux.HandleFunc("GET /orders/{id}", h.get)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var in struct {
		UserID int64  `json:"user_id"`
		Item   string `json:"item"`
		Amount int64  `json:"amount"`
	}

	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.UserID <= 0 || in.Item == "" || in.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "user_id, item and positive amount required")
		return
	}

	err := h.users.Exists(r.Context(), in.UserID)
	switch {
	case errors.Is(err, ErrUserNotFound):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	case err != nil:
		h.log.Error("check user", "err", err)
		writeError(w, http.StatusServiceUnavailable, "user service unavailable")
		return
	}

	o, err := h.store.Create(r.Context(), in.UserID, in.Item, in.Amount)
	if err != nil {
		h.log.Error("create order", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	
	err = h.pub.PublishOrderCreated(r.Context(), OrderCreated{
		EventID:   rand.Text(),
		OrderID:   o.ID,
		UserID:    o.UserID,
		Amount:    o.Amount,
		CreatedAt: o.CreatedAt,
	})
	if err != nil {
		h.log.Error("publish order created", "order_id", o.ID, "err", err)
	}
	writeJSON(w, http.StatusCreated, o)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	o, err := h.store.Get(r.Context(), id)
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		h.log.Error("get order", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	default:
		writeJSON(w, http.StatusOK, o)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
