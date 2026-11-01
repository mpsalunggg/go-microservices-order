# Roadmap: Belajar & Membangun Go Microservices dari Nol

Panduan ini berisi langkah berurutan untuk membangun boilerplate microservices dengan Go sambil memahami konsepnya. Setiap fase punya **tujuan**, **konsep yang dipelajari**, **langkah**, dan **kriteria selesai**. Kerjakan secara urut dan jangan lompat fase. Setiap fase menyelesaikan masalah yang muncul di fase sebelumnya.

> Prinsip utama: **mulai minimal, tambah komponen hanya saat masalahnya benar-benar muncul.**

---

## Daftar Isi

0. [Pendahuluan: Apa itu Microservices](#0-pendahuluan-apa-itu-microservices)
1. [Persiapan](#1-persiapan)
2. [Desain Domain](#2-desain-domain)
3. [Fase 1: Service Pertama (`user`)](#3-fase-1-service-pertama-user)
4. [Fase 2: Service Kedua (`order`) + Komunikasi Sync](#4-fase-2-service-kedua-order--komunikasi-sync)
5. [Fase 3: Komunikasi Async (Event) dengan Kafka](#5-fase-3-komunikasi-async-event-dengan-kafka)
6. [Fase 4: API Gateway + Auth](#6-fase-4-api-gateway--auth)
7. [Fase 5: Observability](#7-fase-5-observability)
8. [Fase 6: Resilience & Konsistensi Data](#8-fase-6-resilience--konsistensi-data)
9. [Fase 7: Testing](#9-fase-7-testing)
10. [Fase 8: gRPC (Opsional)](#10-fase-8-grpc-opsional)
11. [Fase 9: CI/CD & Deployment](#11-fase-9-cicd--deployment)
12. [Anti-pattern yang Harus Dihindari](#12-anti-pattern-yang-harus-dihindari)
13. [Checklist Akhir & Referensi](#13-checklist-akhir--referensi)

---

## 0. Pendahuluan: Apa itu Microservices

**Microservices** adalah gaya arsitektur yang memecah satu aplikasi menjadi beberapa service kecil yang berdiri sendiri. Tiap service:

- Mengurus **satu domain bisnis** (user, order, payment).
- Punya **database sendiri**.
- **Di-deploy terpisah**.
- Berkomunikasi lewat **network** (HTTP, gRPC, message broker).

### Monolith vs Microservices

```
MONOLITH                          MICROSERVICES

┌─────────────────────┐                ┌─────────────┐
│   Aplikasi Toko     │       client ─▶│ API Gateway │
│ user|order|payment  │                └──┬───┬───┬──┘
└─────────┬───────────┘                   ▼   ▼   ▼
          │                            user order payment
     ┌────┴────┐                        │     │     │
     │   DB    │                       DB1   DB2   DB3
     └─────────┘
```

### Trade-off

| Kelebihan | Kekurangan |
|---|---|
| Deploy independen per tim | Network bisa lambat atau gagal |
| Scale hanya bagian yang ramai | Tidak ada `JOIN` atau transaksi lintas service |
| Satu service mati, yang lain bisa tetap jalan | Debugging butuh tracing |
| Bebas pilih teknologi per service | Ops lebih berat (banyak deploy, monitoring) |

### Kapan dipakai?

- **Monolith dulu**: tim kecil, produk baru, domain belum jelas.
- **Microservices**: banyak tim yang saling menghambat, kebutuhan scale berbeda per bagian, deploy monolith sudah berisiko.

> Microservices menukar kerumitan di dalam kode dengan kerumitan di network dan operasional. Di proyek ini kita membangunnya untuk **belajar**, supaya merasakan sendiri masalah-masalah tersebut.

---

## 1. Persiapan

### Tools

| Tool | Kegunaan | Cek |
|---|---|---|
| Go 1.22+ | Bahasa (butuh routing baru `GET /users/{id}`) | `go version` |
| Docker + Docker Compose | Menjalankan semua service dan infra secara lokal | `docker compose version` |
| make | Shortcut command | `make --version` |
| goose | Menjalankan migration database | `goose --version` |
| curl / HTTPie / Bruno | Mencoba API | `curl --version` |
| psql / DBeaver (opsional) | Melihat isi database | |

### Setup sekali di awal

```bash
# 1. Pastikan binary hasil `go install` bisa dipanggil dari terminal
echo 'export PATH="$PATH:$(go env GOPATH)/bin"' >> ~/.zshrc
source ~/.zshrc

# 2. Install goose (CLI migration)
go install github.com/pressly/goose/v3/cmd/goose@latest

# 3. Catat versi Go kamu. Versi ini dipakai lagi di Dockerfile.
go version   # contoh: go1.26.2 → nanti pakai image golang:1.26-alpine
```

### Konsep Go yang wajib dikuasai dulu

1. **`net/http`**: `http.Handler`, `http.ServeMux`, `http.Server`.
2. **`context.Context`**: membawa deadline atau cancel ke seluruh pemanggilan. Ini kunci timeout antar service.
3. **Goroutine & channel** dasar.
4. **Error handling**: `errors.Is`, `errors.As`, wrapping dengan `fmt.Errorf("...: %w", err)`.
5. **Interface**: cukup paham konsepnya. Jangan buat interface sebelum ada dua implementasi atau kebutuhan mock.
6. **`encoding/json`**: encode/decode request dan response.

Selesai jika: bisa membuat HTTP server sederhana yang mengembalikan JSON tanpa melihat contoh.

---

## 2. Desain Domain

Contoh sistem: **toko online sederhana**.

| Service | Tanggung jawab | Data miliknya |
|---|---|---|
| `user` | Registrasi dan data user | tabel `users` |
| `order` | Membuat dan melihat order | tabel `orders` |
| `notification` | Mengirim notifikasi (dummy: log saja) | tidak ada / minimal |
| `gateway` | Pintu masuk, routing, dan auth | tidak ada |

### Aturan emas

1. **Database per service.** Service `order` **tidak boleh** query tabel `users`. Kalau butuh data user, panggil API `user` atau simpan salinan seperlunya dari event.
2. **Service berkomunikasi lewat kontrak** (API/event), bukan lewat kode yang dibagi bersama.
3. **Satu service, satu alasan untuk berubah.**

### Struktur repo (monorepo, satu `go.mod`)

```
go-microservices-starter/
├── go.mod
├── Dockerfile               # satu Dockerfile, service dipilih via build arg
├── docker-compose.yml
├── Makefile
├── services/
│   ├── user/
│   │   ├── cmd/main.go
│   │   ├── internal/
│   │   │   ├── handler.go   # HTTP layer
│   │   │   ├── store.go     # akses DB
│   │   │   └── user.go      # model + logic
│   │   └── migrations/
│   ├── order/
│   ├── notification/
│   └── gateway/
└── ROADMAP.md
```

> Kenapa satu `go.mod`? Lebih sederhana untuk belajar. Pindah ke `go.mod` per service (atau `go work`) kalau nanti tiap service benar-benar dipegang tim berbeda.
>
> Folder `internal/` mencegah service lain meng-import kode service ini. Compiler Go yang menegakkan batas ini.

---

## 3. Fase 1: Service Pertama (`user`)

**Tujuan:** satu service production-ready yang jalan di Docker.
**Konsep:** struktur service, config via env, structured logging, migration, health check, graceful shutdown, container.

> 💡 **Penjelasan singkat**
> - **Service** adalah program kecil yang berjalan sendiri dan punya API sendiri. Di fase ini kita membuat satu service untuk data user.
> - **Config via env**: alamat DB, port, dan secret dibaca dari environment variable, bukan ditulis di kode. Jadi kode yang sama bisa jalan di laptop maupun server.
> - **Structured logging**: log ditulis dalam format JSON (`{"level":"INFO","msg":"..."}`) supaya mudah dicari dan difilter oleh mesin.
> - **Migration**: file SQL bernomor yang mencatat setiap perubahan struktur tabel. Mirip git, tapi untuk schema database.
> - **Health check**: endpoint `/healthz` yang menjawab "saya sehat atau tidak". Docker/Kubernetes memakainya untuk memutuskan apakah service perlu di-restart.
> - **Graceful shutdown**: saat diminta berhenti, service menyelesaikan request yang sedang diproses dulu, baru mati.
> - **Container (Docker)**: membungkus aplikasi beserta semua kebutuhannya agar jalan sama persis di mesin mana pun.

### Gambaran urutan kerja

Ikuti urutan ini persis. Sebagian besar error muncul karena ada langkah yang terlewat atau urutannya tertukar.

```
1. Init project + buat semua folder
2. Buat migration (struktur tabel)
3. Tulis kode: model → store → handler → main
4. go mod tidy + go build ./...      ← checkpoint: harus lolos sebelum lanjut
5. Dockerfile + docker-compose + Makefile
6. make up → make migrate-user → curl
```

Semua perintah dijalankan dari **root project** (`go-microservices-starter/`).

---

### Langkah 1: Init project & struktur folder

```bash
go mod init github.com/<username>/go-microservices-starter
mkdir -p services/user/cmd services/user/internal services/user/migrations
```

- `<username>` diganti username GitHub kamu. Path module ini menjadi **awalan semua import** di project, misalnya `github.com/<username>/go-microservices-starter/services/user/internal`.
- Folder `migrations` dibuat sekarang, karena goose tidak membuat folder sendiri.
- Belum perlu `go get` apa pun. Dependency ditambahkan otomatis di Langkah 7 lewat `go mod tidy`.

---

### Langkah 2: Migration (struktur tabel)

```bash
goose -dir services/user/migrations -s create create_users sql
```

Perintah ini membuat file `services/user/migrations/00001_create_users.sql`. Ganti seluruh isinya:

```sql
-- +goose Up
CREATE TABLE users (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE users;
```

- **Up**: dijalankan saat migrate (membuat atau mengubah struktur).
- **Down**: dijalankan saat rollback, dan harus kebalikan dari Up.
- `-s` membuat nomor berurutan (`00001_`, `00002_`). Tanpa flag ini, nama file memakai timestamp.

> ⚠️ Membuat file migration **belum** membuat tabel. Tabel baru ada setelah `make migrate-user` dijalankan di Langkah 11. Kalau langkah itu terlewat, akan muncul error `relation "users" does not exist`.
>
> Schema selalu diubah lewat **file migration baru**, bukan dengan mengedit migration lama atau mengubah DB secara manual.

---

### Langkah 3: Model

Kode service dibagi menjadi 3 file dalam **satu package** `internal`:

| File | Tugas |
|---|---|
| `user.go` | Bentuk data (struct `User`) |
| `store.go` | Satu-satunya bagian yang berbicara dengan database |
| `handler.go` | Menerima HTTP request, validasi input, memanggil store, lalu mengirim JSON |

> Karena ketiganya berada di package yang sama, mereka saling memanggil **tanpa import dan tanpa prefix** (tulis `ErrNotFound`, bukan `internal.ErrNotFound`). Kalau sebuah file meng-import package-nya sendiri, akan muncul error `import cycle not allowed`.

`services/user/internal/user.go`

```go
package internal

import "time"

type User struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}
```

---

### Langkah 4: Store (akses database)

Store menerjemahkan error database menjadi **error domain** (`ErrNotFound`, `ErrEmailTaken`). Dengan begitu handler tidak perlu tahu detail Postgres.

`services/user/internal/store.go`

```go
package internal

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound   = errors.New("user not found")
	ErrEmailTaken = errors.New("email already registered")
)

// Kode error Postgres untuk pelanggaran constraint UNIQUE.
const pgUniqueViolation = "23505"

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) Create(ctx context.Context, name, email string) (User, error) {
	u := User{Name: name, Email: email}

	// Name dan Email sudah diketahui, jadi hanya kolom yang dibuat DB
	// (id, created_at) yang di-scan. Jumlah kolom RETURNING = jumlah argumen Scan.
	err := s.db.QueryRow(ctx,
		`INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, created_at`,
		name, email,
	).Scan(&u.ID, &u.CreatedAt)

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
		return User{}, ErrEmailTaken
	}
	return u, err
}

func (s *Store) Get(ctx context.Context, id int64) (User, error) {
	var u User
	err := s.db.QueryRow(ctx,
		`SELECT id, name, email, created_at FROM users WHERE id = $1`,
		id,
	).Scan(&u.ID, &u.Name, &u.Email, &u.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}
```

> **Aturan `Scan`:** jumlah dan urutan variabel di `Scan(...)` harus sama persis dengan kolom di `SELECT`/`RETURNING`. Kalau tidak, muncul error `number of field descriptions must equal number of destinations`.

---

### Langkah 5: Handler (HTTP)

`services/user/internal/handler.go`

```go
package internal

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
)

type Handler struct {
	store *Store
	log   *slog.Logger
}

func NewHandler(store *Store, log *slog.Logger) *Handler {
	return &Handler{store: store, log: log}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /users", h.create)
	mux.HandleFunc("GET /users/{id}", h.get)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if in.Name == "" || in.Email == "" {
		writeError(w, http.StatusBadRequest, "name and email are required")
		return
	}

	u, err := h.store.Create(r.Context(), in.Name, in.Email)
	switch {
	case errors.Is(err, ErrEmailTaken):
		writeError(w, http.StatusConflict, err.Error())
	case err != nil:
		h.log.Error("create user", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	default:
		writeJSON(w, http.StatusCreated, u)
	}
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	u, err := h.store.Get(r.Context(), id)
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		h.log.Error("get user", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	default:
		writeJSON(w, http.StatusOK, u)
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
```

**Pola penanganan error** (dipakai di semua handler):

1. Error yang **sudah dikenal** ditangani lebih dulu dengan status yang tepat: 400 (input salah), 404 (tidak ada), 409 (bentrok).
2. **Sisanya** menjadi 500. Hanya error jenis ini yang dicatat sebagai `ERROR` di log, karena memang tidak terduga.
3. `switch` dengan urutan dari atas ke bawah memastikan error spesifik tidak "tertelan" oleh `err != nil`.
4. Pesan 500 ke client sengaja dibuat umum (`internal error`). Detailnya hanya ada di log, supaya informasi internal tidak bocor.

---

### Langkah 6: `main.go` (config, logging, health check, graceful shutdown)

`services/user/cmd/main.go`

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/<username>/go-microservices-starter/services/user/internal"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "user")

	if err := run(log); err != nil {
		log.Error("service stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	// ctx dibatalkan saat menerima SIGINT/SIGTERM (Ctrl+C atau docker stop).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return errors.New("DATABASE_URL is required")
	}

	db, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("create db pool: %w", err)
	}
	defer db.Close()

	// pgxpool.New belum benar-benar konek. Ping supaya salah config langsung ketahuan.
	if err := db.Ping(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			http.Error(w, "db unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	})
	internal.NewHandler(internal.NewStore(db), log).Register(mux)

	srv := &http.Server{
		Addr:              ":" + envOr("PORT", "8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", srv.Addr)
		serverErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		return err // gagal start, misalnya port sudah dipakai
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

Kenapa bentuknya seperti ini:

- **`main` hanya memanggil `run`.** Semua error dikembalikan ke satu tempat, lalu dicatat sekali. `defer` (menutup DB, dll.) tetap berjalan, karena `os.Exit` hanya dipanggil di `main`.
- **Fail fast:** kalau `DATABASE_URL` kosong atau DB tidak bisa dihubungi, service langsung berhenti dengan pesan yang jelas, bukan error membingungkan saat request pertama datang.
- **`/healthz`** mengembalikan 503 kalau DB mati.
- **Graceful shutdown:** saat menerima sinyal stop, server berhenti menerima request baru dan menunggu request yang sedang berjalan selesai (maksimal 10 detik).

---

### Langkah 7: Checkpoint: rapikan dependency & compile

```bash
go mod tidy
go build ./...
go vet ./...
```

- `go mod tidy` memindai semua import di kode, lalu mengunduh dan mencatat dependency yang dibutuhkan ke `go.mod`/`go.sum`. **Jalankan setiap kali menambah import baru.**
- `go build ./...` harus selesai **tanpa output**. Jangan lanjut ke Docker sebelum langkah ini lolos, karena error compile jauh lebih mudah dibaca di sini daripada di log build Docker.
- Kalau editor menampilkan error aneh seperti `missing metadata` atau `BrokenImport`, percayai hasil `go build ./...` di terminal. Restart editor dengan Cmd+Shift+P → **Go: Restart Language Server**.

---

### Langkah 8: Dockerfile

`Dockerfile` (di root, dipakai semua service)

```dockerfile
# Versi image harus >= versi `go` di go.mod (lihat baris ke-3 go.mod)
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
RUN CGO_ENABLED=0 go build -o /app ./services/${SERVICE}/cmd

FROM gcr.io/distroless/static-debian12
COPY --from=build /app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
```

- **Multi-stage:** stage `build` berisi compiler. Image final hanya berisi binary (sekitar 10–20MB) dan berjalan sebagai non-root.
- **Versi Go:** `go mod init` menulis versi Go laptop ke `go.mod` (misalnya `go 1.26.2`). Image `golang:1.26-alpine` harus sama atau lebih baru. Kalau lebih lama, akan muncul error `go.mod requires go >= ...`.
- `COPY go.mod go.sum` dilakukan sebelum `COPY . .` supaya download dependency di-cache dan build berikutnya lebih cepat.

---

### Langkah 9: docker-compose

`docker-compose.yml`

```yaml
services:
  user-db:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: user
    ports: ["5433:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user"]
      interval: 3s
      retries: 10

  user:
    build: { context: ., args: { SERVICE: user } }
    environment:
      DATABASE_URL: postgres://user:secret@user-db:5432/user?sslmode=disable
    ports: ["8081:8080"]
    depends_on:
      user-db: { condition: service_healthy }
```

**Perhatikan alamat DB yang berbeda:**

| Dari mana | Host:port | Dipakai oleh |
|---|---|---|
| Di dalam Docker network | `user-db:5432` | Service `user` (container) |
| Dari laptop | `localhost:5433` | goose, psql, DBeaver |

`5433:5432` artinya port 5433 di laptop diteruskan ke port 5432 di container. Port 5433 dipakai supaya tidak bentrok dengan Postgres lokal yang mungkin sudah berjalan di 5432.

---

### Langkah 10: Makefile

`Makefile` (indentasi **wajib pakai Tab**, bukan spasi)

```makefile
USER_DB_URL := postgres://user:secret@localhost:5433/user?sslmode=disable

.PHONY: build up down logs migrate-user migrate-user-status db-user test

build:
	go build ./...

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

migrate-user:
	goose -dir services/user/migrations postgres "$(USER_DB_URL)" up

migrate-user-status:
	goose -dir services/user/migrations postgres "$(USER_DB_URL)" status

db-user:
	docker compose exec user-db psql -U user -d user

test:
	go test ./...
```

---

### Langkah 11: Jalankan & coba

```bash
make up              # build image + jalankan Postgres & service user
make migrate-user    # buat tabel users (WAJIB sebelum curl pertama)
```

Coba semua skenario, lalu bandingkan hasilnya dengan **status yang diharapkan**:

```bash
# 201 Created: user baru
curl -i -X POST localhost:8081/users \
  -H 'Content-Type: application/json' \
  -d '{"name":"Budi","email":"budi@mail.com"}'

# 409 Conflict: email yang sama dikirim lagi
curl -i -X POST localhost:8081/users \
  -H 'Content-Type: application/json' \
  -d '{"name":"Budi","email":"budi@mail.com"}'

# 400 Bad Request: field kosong
curl -i -X POST localhost:8081/users -d '{"name":""}'

# 200 OK: ambil user (pakai id dari response 201)
curl -i localhost:8081/users/1

# 404 Not Found: id tidak ada
curl -i localhost:8081/users/999

# 400 Bad Request: id bukan angka
curl -i localhost:8081/users/abc

# 200 ok: health check
curl -i localhost:8081/healthz
```

Setiap kali kode Go berubah, jalankan `make up` lagi supaya image di-build ulang. Saat ada response 500, langkah pertama selalu `docker compose logs user`.

> **ID tidak berurutan?** Itu normal. `BIGSERIAL` mengambil nomor dari sequence saat INSERT **dicoba**. Kalau INSERT gagal (misalnya email duplikat), nomor itu tetap hangus. Jangan mengandalkan ID yang berurutan tanpa celah.

### ✅ Selesai jika

- [ ] `go build ./...` lolos tanpa error.
- [ ] `make up` menjalankan Postgres + service `user`.
- [ ] Ketujuh skenario curl di atas mengembalikan status yang diharapkan.
- [ ] Log keluar dalam format JSON, dan hanya error 500 yang tercatat sebagai `ERROR`.
- [ ] `docker compose stop user` memunculkan log `shutting down`, bukan proses yang langsung mati.
- [ ] `/healthz` mengembalikan 503 saat `user-db` dimatikan (`docker compose stop user-db`).

---

### 🛠️ Troubleshooting Fase 1

| Error | Penyebab | Solusi |
|---|---|---|
| `goose: command not found` | Folder binary Go belum masuk PATH | Jalankan setup PATH di bagian [Persiapan](#1-persiapan) |
| `goose run: ... directory does not exist` | Folder migrations belum dibuat | `mkdir -p services/user/migrations` |
| `missing go.sum entry for module ...` | Dependency belum dicatat di `go.sum` | `go mod tidy` |
| `go.mod requires go >= X (running go Y)` | Versi image Docker lebih lama dari versi di `go.mod` | Samakan tag image di Dockerfile (misalnya `golang:1.26-alpine`) |
| `import cycle not allowed` / `missing metadata` | File meng-import package-nya sendiri | Hapus import tersebut, panggil langsung tanpa prefix |
| `relation "users" does not exist` | Migration belum dijalankan | `make migrate-user` |
| `number of field descriptions must equal number of destinations` | Jumlah argumen `Scan` ≠ jumlah kolom `SELECT`/`RETURNING` | Samakan jumlah dan urutannya |
| `duplicate key value violates unique constraint` | Email sudah terdaftar (seharusnya sudah menjadi 409 di kode ini) | Pakai email lain, atau cek penanganan `ErrEmailTaken` |
| `connection refused` ke DB dari laptop | DB belum jalan atau port salah | `docker compose ps`, gunakan `localhost:5433` |
| `ping db: ... dial tcp ...` di log service | Host DB salah di dalam container | Di compose, gunakan `user-db:5432`, bukan `localhost` |
| `port is already allocated` | Port 8081/5433 dipakai program lain | Matikan program itu atau ganti port kiri di `ports:` |
| Response 500 `internal error` | Error tak terduga di server | `docker compose logs user` untuk melihat detailnya |

---

## 4. Fase 2: Service Kedua (`order`) + Komunikasi Sync

**Tujuan:** `order` memvalidasi user dengan memanggil service `user` via HTTP.
**Konsep:** service-to-service call, timeout, propagasi `context`, retry, dan **merasakan bahwa network bisa gagal**.

> 💡 **Penjelasan singkat**
> - **Komunikasi sync**: service A memanggil service B lalu **menunggu** jawabannya, seperti menelepon dan menunggu diangkat.
> - Di monolith, memanggil fungsi lain hampir pasti berhasil dalam hitungan nanodetik. Di microservices, panggilan itu lewat network dan bisa **lambat, gagal, atau tidak pernah dijawab**.
> - **Timeout**: batas waktu menunggu. Lewat dari batas itu, panggilan dibatalkan dan error dikembalikan. Tanpa timeout, satu service yang lambat membuat semua pemanggilnya ikut menggantung.
> - **`context`**: "amplop" yang dibawa dari request masuk ke setiap panggilan berikutnya. Isinya batas waktu dan sinyal batal. Kalau client membatalkan request, semua panggilan turunannya ikut berhenti.
> - **Retry**: mencoba lagi panggilan yang gagal karena gangguan sementara. Harus dibatasi dan diberi jeda supaya tidak memperparah beban.

### Langkah 1: Buat service `order`

Copy pola dari `user`: DB sendiri (`order-db`), tabel `orders (id, user_id, item, amount, status, created_at)`, dan endpoint `POST /orders` serta `GET /orders/{id}`.

> Perhatikan bahwa `orders.user_id` **bukan foreign key** ke tabel `users`, karena tabelnya ada di DB lain. Integritas datanya dijaga lewat logic aplikasi.

### Langkah 2: HTTP client ke service `user` dengan timeout

`services/order/internal/userclient.go`

```go
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
```

Di handler `POST /orders`: panggil `Exists(r.Context(), userID)` sebelum insert. Kalau hasilnya `ErrUserNotFound`, kembalikan 422. Kalau error lain, kembalikan **503** (dependency sedang bermasalah, jangan pakai 500).

### Langkah 3: Tambahkan ke compose

```yaml
  order:
    build: { context: ., args: { SERVICE: order } }
    environment:
      DATABASE_URL: postgres://order:secret@order-db:5432/order?sslmode=disable
      USER_SERVICE_URL: http://user:8080   # nama service compose = hostname
    ports: ["8082:8080"]
```

### Langkah 4: Eksperimen (bagian terpenting)

1. `docker compose stop user`, lalu buat order. Apa yang terjadi? (Seharusnya 503 cepat, bukan hang.)
2. Tambahkan `time.Sleep(5 * time.Second)` di handler `user`. Apakah timeout 2 detik bekerja?
3. Hapus timeout, lalu ulangi. Lihat bagaimana request `order` ikut menggantung. Inilah **cascading failure**.

### Langkah 5: Retry (hati-hati)

- Retry hanya untuk **error sementara** (network error, 502/503/504), maksimal 2–3 kali, dengan **backoff** (misal 100ms, 200ms, 400ms + jitter).
- Retry hanya untuk operasi yang **idempotent** (GET aman, POST belum tentu).
- Retry yang salah memperparah beban saat service sedang sakit (retry storm).

### ✅ Selesai jika

- [ ] Order hanya bisa dibuat untuk user yang ada.
- [ ] Saat `user` mati, `order` membalas 503 dalam waktu ≤ 2–3 detik.
- [ ] Kamu bisa menjelaskan kenapa setiap panggilan network **wajib** punya timeout.

---

## 5. Fase 3: Komunikasi Async (Event) dengan Kafka

**Tujuan:** setelah order dibuat, `order` menerbitkan event `OrderCreated` ke Kafka. Service `notification` membacanya tanpa `order` perlu tahu siapa saja penerimanya.
**Konsep:** message broker, topic, partition, offset, consumer group, at-least-once, idempotency, dead letter queue, outbox pattern.

> 💡 **Penjelasan singkat**
> - **Komunikasi async**: service A mengirim pesan lalu langsung lanjut bekerja **tanpa menunggu**, seperti mengirim chat, bukan menelepon.
> - **Event**: pesan berisi fakta yang sudah terjadi, misalnya "order #12 sudah dibuat".
> - **Kafka**: message broker berbentuk **log yang hanya bisa ditambah** (append-only). Pesan tidak langsung dihapus setelah dibaca. Pesan disimpan sesuai *retention* (default 7 hari), jadi bisa dibaca ulang (*replay*).
> - **Topic**: "kategori" pesan, misalnya `order.created`. Producer menulis ke topic, consumer membaca dari topic.
> - **Partition**: satu topic dipecah menjadi beberapa partition supaya bisa dibaca paralel. Urutan pesan **hanya dijamin di dalam satu partition**.
> - **Key**: menentukan partition. Pesan dengan key sama (misalnya `order_id` yang sama) selalu masuk partition yang sama, jadi urutannya terjaga.
> - **Offset**: nomor urut pesan di dalam partition. Consumer mencatat (*commit*) offset terakhir yang sudah diproses, seperti pembatas buku.
> - **Consumer group**: sekelompok instance dari service yang sama. Kafka membagi partition ke anggota group, sehingga setiap pesan diproses **satu** anggota saja. Group yang berbeda (misalnya `notification` dan `analytics`) masing-masing mendapat salinan semua pesan.
> - **At-least-once**: offset di-commit **setelah** pesan diproses. Kalau service mati di tengah jalan, pesan dibaca ulang. Tidak ada pesan yang hilang, tapi bisa terproses **dua kali**.
> - **Idempotency**: memproses pesan yang sama dua kali hasilnya sama dengan sekali.
> - **DLQ (Dead Letter Queue)**: topic "tempat sampah" untuk pesan yang gagal diproses (misalnya JSON rusak), supaya tidak memblokir pesan lain dan bisa diperiksa nanti.
> - **Outbox pattern**: cara memastikan "simpan data" dan "kirim event" tidak terjadi hanya salah satunya.

### Sync vs Async

| | Sync (HTTP) | Async (Kafka) |
|---|---|---|
| Kapan | Butuh jawaban sekarang ("user ada?") | Memberi tahu bahwa sesuatu terjadi ("order dibuat") |
| Coupling | Pemanggil harus tahu dan menunggu penerima | Producer tidak tahu siapa consumer-nya |
| Kalau penerima mati | Request gagal | Pesan tetap tersimpan di Kafka, diproses saat penerima hidup lagi |

### Kenapa Kafka? (dan alternatifnya)

| Broker | Kelebihan | Kekurangan |
|---|---|---|
| **Kafka** | Standar industri untuk event streaming, throughput sangat tinggi, pesan bisa di-replay | Berat (butuh ~1GB RAM), konsepnya lebih banyak |
| NATS JetStream | Sangat ringan dan simpel | Ekosistem lebih kecil |
| RabbitMQ | Routing pesan fleksibel | Bukan log, sehingga pesan hilang setelah di-ack (tidak bisa replay) |

Di roadmap ini kita memakai Kafka karena paling banyak dipakai di perusahaan dan konsepnya (partition, offset, consumer group) penting untuk dipahami.

### Gambaran urutan kerja

```
1. Tambahkan Kafka + kafka-init (buat topic) ke compose
2. Pahami alamat listener (dalam Docker vs dari laptop)
3. Definisikan kontrak event
4. Producer di service order
5. Consumer di service notification (baru)
6. Idempotency
7. Outbox pattern (perbaikan Langkah 4)
8. Cara mengintip isi Kafka
```

Client Go yang dipakai: [franz-go](https://github.com/twmb/franz-go). Library ini pure Go, sehingga tetap jalan dengan `CGO_ENABLED=0` dan image distroless dari Fase 1. Client resmi Confluent butuh cgo, jadi tidak dipakai.

---

### Langkah 1: Kafka di docker-compose

Kafka dijalankan dalam mode **KRaft** (tanpa Zookeeper), single node, cukup untuk belajar. Tambahkan ke `docker-compose.yml`:

```yaml
  kafka:
    image: apache/kafka:4.0.0
    ports: ["9092:9092"]
    environment:
      CLUSTER_ID: 4L6g3nShT-eMCtK--X86sw
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:29092 > /dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 20

  # Jalan sekali untuk membuat topic, lalu selesai.
  kafka-init:
    image: apache/kafka:4.0.0
    depends_on:
      kafka: { condition: service_healthy }
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic order.created --partitions 3 --replication-factor 1
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic order.created.dlq --partitions 1 --replication-factor 1

  # Opsional: UI di http://localhost:8090 untuk melihat topic, pesan, dan consumer group
  kafka-ui:
    image: ghcr.io/kafbat/kafka-ui:latest
    ports: ["8090:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    depends_on:
      kafka: { condition: service_healthy }
```

Penjelasan:
- `CLUSTER_ID` adalah ID unik cluster. Untuk lokal, nilai di atas boleh dipakai apa adanya.
- `PROCESS_ROLES: broker,controller`: satu node berperan sebagai penyimpan pesan sekaligus pengatur cluster.
- `REPLICATION_FACTOR: 1`: hanya ada 1 broker, jadi setiap data cukup 1 salinan. Di production minimal 3.
- `AUTO_CREATE_TOPICS_ENABLE: "false"`: topic **wajib** dibuat eksplisit lewat `kafka-init`. Tanpa ini, salah ketik nama topic akan diam-diam membuat topic baru dan pesan "hilang" ke sana.
- Peringatan Kafka soal nama topic yang mengandung `.` atau `_` aman diabaikan, asalkan kamu konsisten memakai salah satunya saja (di sini `.`).

Service yang memakai Kafka harus menunggu topic selesai dibuat:

```yaml
    depends_on:
      kafka-init: { condition: service_completed_successfully }
```

---

### Langkah 2: Alamat Kafka (sumber error paling sering)

Kafka punya dua "pintu" (listener). Klien terhubung ke salah satunya, lalu Kafka **membalas dengan alamat yang harus dipakai selanjutnya** (`ADVERTISED_LISTENERS`). Kalau pintunya salah, koneksi pertama berhasil, tapi koneksi berikutnya gagal.

| Dari mana | Alamat | Dipakai oleh |
|---|---|---|
| Di dalam Docker network | `kafka:29092` | Service `order`, `notification`, `kafka-ui` |
| Dari laptop | `localhost:9092` | `go run` lokal, tools di laptop |

Di compose, service memakai env:

```yaml
      KAFKA_BROKERS: kafka:29092
```

> ⚠️ Jangan pakai `localhost:9092` di dalam container. Bagi container, `localhost` adalah dirinya sendiri, bukan Kafka.

---

### Langkah 3: Kontrak event

`services/order/internal/event.go`

```go
package internal

import "time"

const TopicOrderCreated = "order.created"

// OrderCreated adalah kontrak event. Field boleh ditambah, tapi jangan
// dihapus atau diganti namanya, karena consumer lain bergantung padanya.
type OrderCreated struct {
	EventID   string    `json:"event_id"`
	OrderID   int64     `json:"order_id"`
	UserID    int64     `json:"user_id"`
	Amount    int64     `json:"amount"`
	CreatedAt time.Time `json:"created_at"`
}
```

- Nama event memakai **past tense** (`Created`), karena event adalah fakta yang sudah terjadi.
- `EventID` unik untuk setiap event dan dipakai untuk idempotency di Langkah 6.
- Event adalah **kontrak publik**. Mengubahnya sembarangan sama berbahayanya dengan mengubah API.

---

### Langkah 4: Producer di service `order`

`services/order/internal/publisher.go`

```go
package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Publisher struct {
	client *kgo.Client
}

func NewPublisher(brokers []string) (*Publisher, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &Publisher{client: client}, nil
}

func (p *Publisher) Close() {
	p.client.Close()
}

func (p *Publisher) PublishOrderCreated(ctx context.Context, evt OrderCreated) error {
	value, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	record := &kgo.Record{
		Topic: TopicOrderCreated,
		// Key menentukan partition. Event dengan order_id sama selalu
		// masuk partition yang sama, jadi urutannya terjaga.
		Key:   []byte(strconv.FormatInt(evt.OrderID, 10)),
		Value: value,
	}
	return p.client.ProduceSync(ctx, record).FirstErr()
}
```

Di `main.go` service `order`:

```go
pub, err := internal.NewPublisher(strings.Split(os.Getenv("KAFKA_BROKERS"), ","))
if err != nil {
	return err
}
defer pub.Close()
```

Panggil `pub.PublishOrderCreated(...)` di handler setelah order berhasil disimpan. `EventID` bisa diisi `rand.Text()` dari `crypto/rand` (Go 1.24+). Setelah menambah import, jalankan `go mod tidy`.

> ⚠️ Versi ini punya celah: kalau insert order berhasil tapi publish gagal (atau sebaliknya), data dan event tidak sinkron. Celah ini ditutup di Langkah 7 (outbox). Mulai dari versi sederhana ini dulu supaya alurnya terlihat jelas.

---

### Langkah 5: Consumer di service `notification`

Buat service baru: `mkdir -p services/notification/cmd`. Service ini tidak butuh HTTP server maupun database (untuk sekarang).

`services/notification/cmd/main.go`

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topicOrderCreated = "order.created"
	topicDLQ          = "order.created.dlq"
	consumerGroup     = "notification"
)

// Consumer cukup mendefinisikan field yang ia butuhkan dari event.
// Tidak perlu import kode dari service order.
type orderCreated struct {
	EventID string `json:"event_id"`
	OrderID int64  `json:"order_id"`
	UserID  int64  `json:"user_id"`
	Amount  int64  `json:"amount"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "notification")

	if err := run(log); err != nil {
		log.Error("service stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		return errors.New("KAFKA_BROKERS is required")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topicOrderCreated),
		kgo.DisableAutoCommit(), // commit manual SETELAH pesan diproses
	)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()

	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}

	log.Info("consuming", "topic", topicOrderCreated, "group", consumerGroup)
	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			log.Info("shutting down")
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.Error("fetch", "topic", topic, "partition", partition, "err", err)
		})

		for _, record := range fetches.Records() {
			if err := handle(log, record); err != nil {
				log.Warn("message failed, sending to DLQ",
					"partition", record.Partition, "offset", record.Offset, "err", err)

				dlq := &kgo.Record{Topic: topicDLQ, Key: record.Key, Value: record.Value}
				if err := client.ProduceSync(ctx, dlq).FirstErr(); err != nil {
					// Berhenti tanpa commit: pesan akan dibaca ulang setelah restart, tidak hilang.
					return fmt.Errorf("produce to DLQ: %w", err)
				}
			}
		}

		// Commit = "sudah selesai sampai offset ini". Kalau service mati sebelum
		// commit, pesan dibaca ulang (at-least-once), jadi handle harus idempotent.
		if err := client.CommitUncommittedOffsets(ctx); err != nil {
			log.Error("commit offsets", "err", err)
		}
	}
}

func handle(log *slog.Logger, record *kgo.Record) error {
	var evt orderCreated
	if err := json.Unmarshal(record.Value, &evt); err != nil {
		return fmt.Errorf("decode event: %w", err)
	}
	if evt.EventID == "" || evt.OrderID == 0 {
		return errors.New("invalid event: missing event_id or order_id")
	}

	// Di dunia nyata: kirim email/push notification di sini.
	log.Info("send notification",
		"event_id", evt.EventID, "order_id", evt.OrderID, "user_id", evt.UserID, "amount", evt.Amount)
	return nil
}
```

Alur loop consumer:

```
PollFetches (ambil batch pesan)
   └─▶ untuk setiap pesan: handle()
          ├─ sukses → lanjut
          └─ gagal  → kirim ke DLQ, lanjut (pesan rusak tidak memblokir antrean)
   └─▶ CommitUncommittedOffsets (tandai batch sudah selesai)
   └─▶ ulangi
```

Tambahkan ke `docker-compose.yml`:

```yaml
  notification:
    build: { context: ., args: { SERVICE: notification } }
    environment:
      KAFKA_BROKERS: kafka:29092
    depends_on:
      kafka-init: { condition: service_completed_successfully }
```

Lalu `go mod tidy && go build ./...` dan `make up`.

> **Coba scale:** `docker compose up -d --scale notification=3`. Topic punya 3 partition, jadi masing-masing instance mendapat 1 partition. Coba `--scale notification=4`: instance ke-4 akan menganggur, karena **jumlah consumer aktif dalam satu group maksimal sama dengan jumlah partition**.

---

### Langkah 6: Idempotency

Karena at-least-once, pesan yang sama **bisa datang dua kali**, misalnya saat service restart sebelum commit atau saat rebalance. Consumer harus aman menerima duplikat.

Caranya: catat `event_id` yang sudah diproses. Langkah ini butuh database untuk `notification` (tambahkan Postgres sendiri, `notification-db`, dengan pola yang sama seperti Fase 1):

```sql
-- +goose Up
CREATE TABLE processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE processed_events;
```

Di `handle`, sebelum mengirim notifikasi:

```go
tag, err := db.Exec(ctx,
	`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
	evt.EventID)
if err != nil {
	return err
}
if tag.RowsAffected() == 0 {
	return nil // sudah pernah diproses, lewati
}
// ... kirim notifikasi
```

> Idealnya `INSERT processed_events` dan efek samping (misalnya simpan notifikasi ke DB) berada dalam **satu transaksi**. Untuk efek ke luar seperti email, duplikat kecil biasanya masih bisa diterima.

---

### Langkah 7: Outbox pattern (menutup celah dual write)

**Masalahnya (dual write):** handler melakukan dua penulisan ke dua sistem berbeda (Postgres dan Kafka). Tidak ada transaksi yang mencakup keduanya:

```
INSERT order ✅  →  Publish Kafka ❌  = order ada, event hilang
Publish Kafka ✅ →  INSERT order ❌   = event ada, order tidak ada
```

**Solusinya:** tulis event ke tabel `outbox` di database yang sama, **dalam satu transaksi** dengan insert order. Worker terpisah membaca outbox lalu mengirimnya ke Kafka.

```
┌──────── 1 transaksi ────────┐
│ INSERT INTO orders ...      │
│ INSERT INTO outbox ...      │
└─────────────────────────────┘
           │
   Outbox worker (tiap 1 detik)
           │  baca outbox yang belum terkirim
           ▼
         Kafka  →  tandai published_at
```

Migration di service `order`:

```sql
-- +goose Up
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    key          TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox;
```

`services/order/internal/store.go` (bagian `Create`, butuh import `crypto/rand`, `encoding/json`, dan `strconv`):

```go
// Create menyimpan order DAN event-nya dalam satu transaksi.
// Keduanya tersimpan, atau tidak sama sekali.
func (s *Store) Create(ctx context.Context, userID int64, item string, amount int64) (Order, error) {
	o := Order{UserID: userID, Item: item, Amount: amount}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Order{}, err
	}
	defer tx.Rollback(ctx) // tidak berefek jika Commit sudah berhasil

	err = tx.QueryRow(ctx,
		`INSERT INTO orders (user_id, item, amount) VALUES ($1, $2, $3) RETURNING id, created_at`,
		userID, item, amount,
	).Scan(&o.ID, &o.CreatedAt)
	if err != nil {
		return Order{}, err
	}

	payload, err := json.Marshal(OrderCreated{
		EventID:   rand.Text(),
		OrderID:   o.ID,
		UserID:    o.UserID,
		Amount:    o.Amount,
		CreatedAt: o.CreatedAt,
	})
	if err != nil {
		return Order{}, err
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`,
		TopicOrderCreated, strconv.FormatInt(o.ID, 10), payload,
	)
	if err != nil {
		return Order{}, err
	}

	return o, tx.Commit(ctx)
}
```

`services/order/internal/outbox.go`

```go
package internal

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type OutboxWorker struct {
	db     *pgxpool.Pool
	client *kgo.Client
	log    *slog.Logger
}

func NewOutboxWorker(db *pgxpool.Pool, client *kgo.Client, log *slog.Logger) *OutboxWorker {
	return &OutboxWorker{db: db, client: client, log: log}
}

// Run mengirim isi outbox ke Kafka setiap detik sampai ctx dibatalkan.
func (w *OutboxWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.publishBatch(ctx); err != nil {
				w.log.Error("publish outbox", "err", err)
			}
		}
	}
}

func (w *OutboxWorker) publishBatch(ctx context.Context) error {
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// SKIP LOCKED: kalau ada beberapa replica worker, baris yang sedang
	// diproses replica lain dilewati, jadi tidak dikirim dobel bersamaan.
	rows, err := tx.Query(ctx, `
		SELECT id, topic, key, payload FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT 100
		FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return err
	}

	var ids []int64
	var records []*kgo.Record
	for rows.Next() {
		var id int64
		var r kgo.Record
		if err := rows.Scan(&id, &r.Topic, &r.Key, &r.Value); err != nil {
			return err
		}
		ids = append(ids, id)
		records = append(records, &r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}

	if err := w.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
```

Jalankan worker di `main.go` service `order`, di goroutine terpisah dengan `ctx` yang sama dengan server, supaya ikut berhenti saat shutdown:

```go
go internal.NewOutboxWorker(db, kafkaClient, log).Run(ctx)
```

Setelah outbox dipakai, `Publisher` dari Langkah 4 tidak dibutuhkan lagi oleh handler. Handler cukup memanggil `store.Create`.

> Outbox juga at-least-once: kalau Kafka sudah menerima pesan tapi `UPDATE published_at` gagal, pesan dikirim ulang di putaran berikutnya. Inilah alasan consumer **wajib idempotent** (Langkah 6).

---

### Langkah 8: Mengintip isi Kafka

Pakai Kafka UI di `http://localhost:8090`, atau CLI berikut:

```bash
# Daftar topic
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list

# Baca semua pesan di topic (Ctrl+C untuk berhenti)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic order.created --from-beginning --property print.key=true

# Lihat posisi consumer group dan LAG (jumlah pesan yang belum diproses)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group notification

# Kirim pesan manual (format key:value), berguna untuk menguji consumer
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:29092 --topic order.created \
  --property parse.key=true --property key.separator=: <<< '42:{"event_id":"evt-1","order_id":42,"user_id":7,"amount":150000}'
```

Tambahkan target berikut ke Makefile supaya tidak perlu mengetik panjang:

```makefile
kafka-topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list

kafka-lag:
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group notification
```

### ✅ Selesai jika

- [ ] `make up` menjalankan Kafka, `kafka-init` selesai dengan exit 0, dan topic `order.created` punya 3 partition.
- [ ] Membuat order memunculkan log `send notification` di service `notification`.
- [ ] Pesan rusak (kirim `43:not-json` lewat console producer) masuk ke `order.created.dlq`, dan consumer tetap jalan.
- [ ] Matikan `notification`, buat 3 order, lalu nyalakan lagi. Ketiga notifikasi tetap terkirim, dan `make kafka-lag` menunjukkan LAG kembali 0.
- [ ] `--scale notification=3`: setiap instance memproses partition yang berbeda.
- [ ] Event duplikat (kirim pesan dengan `event_id` sama dua kali) hanya diproses sekali.
- [ ] Kamu bisa menjelaskan masalah dual write dan bagaimana outbox menyelesaikannya.

**Eksperimen replay** (fitur khas Kafka): hentikan `notification`, lalu reset offset group ke awal:

```bash
docker compose stop notification
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 \
  --group notification --topic order.created --reset-offsets --to-earliest --execute
docker compose start notification
```

Semua event lama diproses ulang. Dengan idempotency, tidak ada notifikasi ganda.

---

### 🛠️ Troubleshooting Fase 3

| Gejala / Error | Penyebab | Solusi |
|---|---|---|
| `dial tcp [::1]:9092: connect: connection refused` di log container | Container memakai `localhost:9092` | Di compose, gunakan `KAFKA_BROKERS: kafka:29092` |
| Dari laptop terhubung, tapi lalu timeout ke `kafka:29092` | Klien di laptop memakai port internal | Dari laptop, gunakan `localhost:9092` |
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic belum dibuat (auto-create dimatikan) | Cek log `docker compose logs kafka-init`, pastikan service bergantung pada `kafka-init` |
| `ping kafka: ...` saat service start | Kafka belum siap | Pakai `depends_on` dengan `service_healthy` / `service_completed_successfully` |
| Consumer tidak membaca ulang pesan lama | Group yang sudah punya offset melanjutkan dari commit terakhir (group baru di franz-go mulai dari awal topic) | Untuk membaca dari awal, reset offset (lihat eksperimen replay) |
| Instance consumer ada yang menganggur | Jumlah instance > jumlah partition | Normal. Tambah partition jika butuh paralelisme lebih |
| Pesan diproses dua kali | At-least-once (restart/rebalance sebelum commit) | Normal. Pastikan consumer idempotent (Langkah 6) |
| Kafka container mati / `exit code 137` | Kehabisan memori | Naikkan memory Docker Desktop (minimal 4GB) |
| Error `missing go.sum entry` untuk franz-go | Import baru belum dicatat | `go mod tidy` |
| Peringatan "topics with a period ('.') or underscore ('_') could collide" | Info dari Kafka soal penamaan | Aman diabaikan, asal konsisten memakai satu jenis pemisah |

---

## 6. Fase 4: API Gateway + Auth

**Tujuan:** client hanya mengenal satu alamat. Gateway meneruskan request ke service yang tepat dan memvalidasi auth.
**Konsep:** reverse proxy, edge authentication, request ID.

> 💡 **Penjelasan singkat**
> - **API Gateway**: resepsionis gedung. Tamu (client) cukup datang ke satu pintu, lalu resepsionis mengarahkan ke ruangan (service) yang tepat. Client tidak perlu tahu ada berapa service atau alamatnya di mana.
> - **Reverse proxy**: program yang menerima request lalu meneruskannya ke server lain di belakangnya. Gateway adalah reverse proxy yang ditambah fitur lain.
> - **Edge authentication**: pengecekan login dilakukan sekali di gateway (pintu depan), sehingga tiap service tidak perlu mengulang validasi token.
> - **JWT** (JSON Web Token): "kartu identitas digital" bertanda tangan yang diberikan saat login. Gateway cukup memeriksa tanda tangannya tanpa perlu bertanya ke database.
> - **Request ID**: nomor unik per request yang ikut dibawa ke semua service, supaya log dari banyak service bisa dihubungkan menjadi satu cerita.

### Langkah 1: Gateway sederhana dengan stdlib

`services/gateway/cmd/main.go` (inti)

```go
func proxy(target string) http.Handler {
	u, err := url.Parse(target)
	if err != nil {
		panic(err)
	}
	return httputil.NewSingleHostReverseProxy(u)
}

mux := http.NewServeMux()
mux.Handle("/users/", proxy(os.Getenv("USER_SERVICE_URL")))
mux.Handle("/orders/", proxy(os.Getenv("ORDER_SERVICE_URL")))
mux.Handle("/orders", proxy(os.Getenv("ORDER_SERVICE_URL")))

handler := requestID(auth(mux))
```

> Alternatif tanpa menulis kode: **Traefik**, **Nginx**, **Kong**, atau **KrakenD**. Menulis sendiri lebih bagus untuk belajar cara kerjanya.

Setelah gateway ada, **hapus `ports:`** dari service internal di compose. Hanya gateway yang terekspos keluar.

### Langkah 2: Request ID middleware

```go
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = rand.Text() // crypto/rand, Go 1.24+
			r.Header.Set("X-Request-ID", id)
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}
```

Setiap service meneruskan `X-Request-ID` ke panggilan berikutnya dan menulisnya di setiap log. Dengan begitu satu request bisa dilacak di semua service.

### Langkah 3: Auth dengan JWT di gateway

- Buat endpoint login (bisa di service `user` atau service `auth` terpisah) yang menerbitkan JWT.
- Gateway memvalidasi JWT (signature, `exp`), lalu meneruskan `X-User-ID` ke service di belakangnya.
- Library: `github.com/golang-jwt/jwt/v5`.

> **Keamanan:** service internal hanya boleh memercayai header `X-User-ID` kalau mereka **tidak bisa diakses langsung** dari luar, yaitu hanya lewat gateway di network internal. Gateway juga wajib **menghapus** header `X-User-ID` yang dikirim client sebelum mengisinya sendiri. Simpan secret JWT di env atau secret manager, jangan di-commit ke git.

### ✅ Selesai jika

- [ ] Semua request lewat `localhost:8080` (gateway).
- [ ] Service internal tidak bisa diakses langsung dari host.
- [ ] Request tanpa token valid ditolak dengan 401.
- [ ] Satu `X-Request-ID` muncul di log gateway, `order`, dan `user`.

---

## 7. Fase 5: Observability

**Tujuan:** bisa menjawab "request ini lambat atau gagal di mana?" dan "bagaimana kesehatan sistem sekarang?".
**Konsep:** tiga pilar observability: **logs, metrics, traces**.

> 💡 **Penjelasan singkat**
> - **Observability**: kemampuan memahami apa yang terjadi di dalam sistem hanya dari data yang dikeluarkannya. Di microservices ini wajib, karena satu request bisa melewati banyak service.
> - **Logs**: catatan kejadian, seperti buku harian ("jam 10:01 user 5 gagal login").
> - **Metrics**: angka yang diukur terus-menerus, seperti speedometer (request per detik, persentase error, kecepatan respons).
> - **Traces**: rekaman perjalanan satu request dari service ke service beserta durasi tiap langkahnya, seperti fitur lacak paket kurir.
> - **OpenTelemetry**: standar terbuka untuk mengumpulkan ketiga data di atas. **Jaeger** menampilkan trace, **Prometheus** menyimpan metrics, dan **Grafana** membuat dashboard-nya.

| Pilar | Menjawab | Tools |
|---|---|---|
| Logs | Apa yang terjadi di request tertentu? | `slog` → Loki / ELK |
| Metrics | Berapa RPS, error rate, latency p99? | Prometheus + Grafana |
| Traces | Request ini menghabiskan waktu di service mana? | OpenTelemetry → Jaeger / Tempo |

### Langkah 1: Tracing dengan OpenTelemetry

```bash
go get go.opentelemetry.io/otel \
       go.opentelemetry.io/otel/sdk \
       go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc \
       go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp
```

- Bungkus server: `otelhttp.NewHandler(mux, "user")`.
- Bungkus client: `&http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}`.
- Trace context otomatis diteruskan lewat header `traceparent` (standar W3C).
- Untuk Kafka: trace context dibawa lewat **record headers**. Pakai plugin `github.com/twmb/franz-go/plugin/kotel` supaya producer dan consumer otomatis menyisipkan dan membaca `traceparent`. Hasilnya, trace di Jaeger tersambung dari `order` lewat Kafka sampai ke `notification`.

Tambahkan Jaeger ke compose:

```yaml
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports: ["16686:16686", "4317:4317"]
```

Buka `http://localhost:16686`. Satu request `POST /orders` akan tampil sebagai satu trace: gateway → order → user.

### Langkah 2: Metrics dengan Prometheus

- Expose `GET /metrics` (`github.com/prometheus/client_golang`).
- Metrik minimum (metode **RED**): **R**ate (request/detik), **E**rrors (error rate), **D**uration (histogram latency).
- Tambahkan Prometheus + Grafana di compose, lalu buat dashboard sederhana.

### Langkah 3: Ganti request ID dengan trace ID

Ambil `trace_id` dari span dan masukkan ke setiap log. Hasilnya, dari log bisa langsung loncat ke trace di Jaeger.

### ✅ Selesai jika

- [ ] Satu request terlihat utuh lintas service di Jaeger.
- [ ] Ada dashboard Grafana berisi RPS, error rate, dan latency p95/p99 per service.
- [ ] Log berisi `trace_id`.

---

## 8. Fase 6: Resilience & Konsistensi Data

**Tujuan:** sistem tetap berfungsi (terdegradasi dengan baik) saat sebagian komponen bermasalah, dan data tetap konsisten tanpa transaksi lintas service.

> 💡 **Penjelasan singkat**
> - **Resilience**: daya tahan sistem. Kalau satu bagian rusak, sistem tidak ikut runtuh semuanya, cukup sebagian fitur yang terganggu.
> - **Circuit breaker**: seperti sekring listrik. Kalau service tujuan terus gagal, "sekring putus" dan panggilan langsung ditolak tanpa menunggu, sampai service itu pulih.
> - **Rate limiting**: membatasi jumlah request per waktu, misalnya maksimal 100 request per detik per user, supaya sistem tidak kebanjiran.
> - **Saga**: cara menjaga konsistensi data lintas service. Karena tidak bisa satu transaksi untuk semua DB, prosesnya dipecah menjadi langkah-langkah. Kalau satu langkah gagal, langkah sebelumnya dibatalkan dengan aksi "kebalikan" (kompensasi), misalnya refund atau mengembalikan stok.
> - **Eventual consistency**: data di semua service **akhirnya** akan sama, tapi tidak harus di detik yang sama.

### Pola resilience

| Pola | Masalah yang diselesaikan | Implementasi |
|---|---|---|
| **Timeout** | Request menggantung | Sudah dibuat di Fase 2 |
| **Retry + backoff** | Error sementara | Sudah dibuat di Fase 2 |
| **Circuit breaker** | Terus memanggil service yang sudah pasti mati | `github.com/sony/gobreaker` |
| **Rate limiting** | Lonjakan traffic atau abuse | `golang.org/x/time/rate` di gateway |
| **Bulkhead** | Satu dependency lambat menghabiskan semua resource | Batasi jumlah request bersamaan per dependency (semaphore/channel) |
| **Fallback** | Dependency mati | Kembalikan data cache atau default |

Circuit breaker secara singkat: setelah N kegagalan berturut-turut, breaker **open** dan langsung menolak panggilan tanpa memanggil service. Setelah jeda tertentu, breaker masuk **half-open** dan mencoba sedikit request. Kalau berhasil, breaker kembali **closed**.

### Konsistensi data: Saga

Kasus: order butuh **reserve stok** (service `inventory`) dan **charge payment** (service `payment`). Tidak ada `BEGIN/COMMIT` lintas service.

**Saga** adalah rangkaian transaksi lokal. Kalau satu langkah gagal, jalankan **compensating action** untuk membatalkan langkah-langkah sebelumnya.

```
OrderCreated ─▶ inventory: reserve stok ─▶ StockReserved
             ─▶ payment: charge         ─▶ PaymentFailed
             ─▶ inventory: release stok (kompensasi)
             ─▶ order: status = CANCELLED
```

| Gaya | Cara | Cocok untuk |
|---|---|---|
| **Choreography** | Tiap service bereaksi terhadap event service lain | Alur pendek (2–4 langkah) |
| **Orchestration** | Satu orchestrator memerintah tiap langkah | Alur panjang dan kompleks (Temporal bisa membantu) |

Konsekuensinya adalah **eventual consistency**: untuk sesaat, status order bisa `PENDING` sebelum menjadi `CONFIRMED` atau `CANCELLED`. Desain UI dan API harus menerima hal ini.

### ✅ Selesai jika

- [ ] Circuit breaker terbukti open saat `user` dimatikan, sehingga `order` langsung gagal cepat.
- [ ] Gateway membalas 429 saat traffic melewati batas.
- [ ] (Opsional) Ada saga sederhana dengan service `inventory` atau `payment` dummy beserta kompensasinya.

---

## 9. Fase 7: Testing

**Tujuan:** perubahan bisa di-deploy dengan percaya diri.

> 💡 **Penjelasan singkat**
> - **Unit test**: menguji satu fungsi kecil secara terisolasi. Cepat dan banyak jumlahnya.
> - **Integration test**: menguji kode bersama komponen sungguhan, misalnya query SQL terhadap Postgres asli di container.
> - **Contract test**: memastikan service A dan B masih "sepakat" soal format API/event, supaya perubahan di satu service tidak diam-diam merusak service lain.
> - **End-to-end (e2e) test**: menguji alur lengkap seperti yang dilakukan user sungguhan, dari gateway sampai notifikasi. Paling lambat, jadi cukup beberapa saja.

| Level | Yang dites | Tools |
|---|---|---|
| Unit | Logic murni (validasi, kalkulasi) | `testing` (stdlib) |
| Handler | HTTP handler | `net/http/httptest` |
| Integration | Store dengan DB sungguhan | `testcontainers-go` (Postgres di Docker) |
| Contract (opsional) | Kontrak API/event antar service tidak rusak | Pact, atau schema test sederhana |
| End-to-end | Alur penuh lewat gateway | Script `curl`/Go test terhadap compose |

Contoh test handler:

```go
func TestGetUser_InvalidID(t *testing.T) {
	h := NewHandler(nil, slog.Default())
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/users/abc", nil))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}
```

> Fokuskan integration test ke store (query SQL sungguhan). Mock DB sering lolos test padahal query-nya salah.

### ✅ Selesai jika

- [ ] `make test` menjalankan unit + integration test.
- [ ] Ada minimal satu test e2e untuk alur `create user → create order → notifikasi`.

---

## 10. Fase 8: gRPC (Opsional)

> 💡 **Penjelasan singkat**
> - **gRPC**: cara komunikasi antar service buatan Google. Data dikirim dalam format biner (lebih kecil dan cepat dari JSON) lewat HTTP/2.
> - **Protobuf** (`.proto`): file kontrak yang mendefinisikan fungsi dan bentuk data. Dari file ini, kode client dan server di-generate otomatis, jadi kedua sisi pasti cocok.
> - Gunakan gRPC untuk komunikasi **internal** antar service. API untuk browser atau mobile biasanya tetap HTTP/JSON.

**Kapan pindah dari HTTP/JSON ke gRPC untuk komunikasi internal?**

- Panggilan antar service sudah banyak dan performa mulai penting.
- Butuh kontrak yang ketat dan ter-generate (tidak ada lagi typo field JSON).
- Butuh streaming.

Tetap gunakan HTTP/JSON untuk API publik di gateway.

### Langkah

1. Install `buf` + `protoc-gen-go` + `protoc-gen-go-grpc`.
2. Tulis kontrak:

```protobuf
syntax = "proto3";
package user.v1;
option go_package = "github.com/<username>/go-microservices-starter/gen/user/v1;userv1";

service UserService {
  rpc GetUser(GetUserRequest) returns (GetUserResponse);
}

message GetUserRequest { int64 id = 1; }
message GetUserResponse { int64 id = 1; string name = 2; string email = 3; }
```

3. `buf generate`, implementasi server di `user`, lalu client di `order`.
4. Aturan versioning: **jangan pernah mengubah nomor field**. Tambah field baru boleh, hapus field harus ditandai `reserved`.

> Alternatif yang lebih ringan: **Connect RPC** (`connectrpc.com`). Kompatibel dengan gRPC dan juga bisa dipanggil lewat HTTP/JSON biasa.

---

## 11. Fase 9: CI/CD & Deployment

> 💡 **Penjelasan singkat**
> - **CI** (Continuous Integration): setiap kali kode di-push, mesin otomatis menjalankan lint, test, dan build. Kalau ada yang rusak, ketahuan sebelum masuk `main`.
> - **CD** (Continuous Delivery/Deployment): setelah CI lulus, aplikasi otomatis dikemas (image Docker) dan di-deploy ke server.
> - **Container registry** (GHCR/Docker Hub): "gudang" tempat menyimpan image Docker yang siap di-deploy.
> - **Kubernetes (K8s)**: "manajer" untuk banyak container. Ia menjalankan container di banyak server, me-restart yang mati, menambah jumlahnya saat ramai, dan mengganti versi tanpa downtime.
> - **Deployment** (di K8s) mengatur berapa salinan service yang berjalan. **Service** (di K8s) memberi alamat tetap agar service lain bisa menemukannya.

### Langkah 1: CI dengan GitHub Actions

`.github/workflows/ci.yml`

```yaml
name: ci
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version-file: go.mod }
      - run: go vet ./...
      - uses: golangci/golangci-lint-action@v6
      - run: go test -race ./...
  build:
    needs: test
    runs-on: ubuntu-latest
    strategy:
      matrix: { service: [user, order, notification, gateway] }
    steps:
      - uses: actions/checkout@v4
      - run: docker build --build-arg SERVICE=${{ matrix.service }} -t ${{ matrix.service }}:${{ github.sha }} .
```

Tambahkan push ke registry (GHCR / Docker Hub) saat merge ke `main`. Tag image dengan **git SHA**, bukan `latest`.

### Langkah 2: Deploy ke Kubernetes (dasar)

Coba lokal dulu dengan **kind** atau **minikube**.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: user }
spec:
  replicas: 2
  selector: { matchLabels: { app: user } }
  template:
    metadata: { labels: { app: user } }
    spec:
      containers:
        - name: user
          image: ghcr.io/<username>/user:<git-sha>
          ports: [{ containerPort: 8080 }]
          env:
            - name: DATABASE_URL
              valueFrom: { secretKeyRef: { name: user-db, key: url } }
          readinessProbe: { httpGet: { path: /healthz, port: 8080 } }
          livenessProbe:  { httpGet: { path: /healthz, port: 8080 } }
          resources:
            requests: { cpu: 50m, memory: 64Mi }
            limits:   { memory: 128Mi }
---
apiVersion: v1
kind: Service
metadata: { name: user }
spec:
  selector: { app: user }
  ports: [{ port: 80, targetPort: 8080 }]
```

Hal yang perlu dipelajari di tahap ini: Deployment, Service, ConfigMap, Secret, Ingress, probes, rolling update, dan HPA (autoscale). Untuk banyak service, pakai **Helm** atau **Kustomize**.

> Graceful shutdown dari Fase 1 berperan di sini: saat rolling update, Kubernetes mengirim SIGTERM, lalu service menyelesaikan request yang sedang berjalan sebelum mati.

### ✅ Selesai jika

- [ ] Setiap push menjalankan lint + test + build image.
- [ ] Semua service jalan di cluster lokal (kind).
- [ ] Rolling update berjalan tanpa request gagal.

---

## 12. Anti-pattern yang Harus Dihindari

| Anti-pattern | Gejala | Solusi |
|---|---|---|
| **Distributed monolith** | Ubah satu fitur harus deploy 4 service sekaligus | Perbaiki batas domain, kurangi panggilan sync berantai |
| **Shared database** | Dua service membaca/menulis tabel yang sama | Satu pemilik data, yang lain lewat API/event |
| **Shared library berisi logic bisnis** | Update lib memaksa semua service ikut deploy | Share hanya utilitas teknis kecil, bukan domain |
| **Chatty services** | Satu request memicu 20 panggilan antar service | Gabungkan service, atau simpan salinan data lewat event |
| **Tanpa timeout** | Satu service lambat menyebabkan semua ikut hang | Timeout di setiap panggilan network |
| **Over-engineering di awal** | K8s + Istio + Kafka + CQRS untuk 2 service | Tambah komponen hanya saat masalahnya nyata |
| **Terlalu kecil (nanoservices)** | Satu service per tabel atau per fungsi | Potong per kapabilitas bisnis, bukan per entitas |

---

## 13. Checklist Akhir & Referensi

### Checklist boilerplate

**Per service**
- [ ] Struktur `cmd/` + `internal/`
- [ ] Config dari env var
- [ ] Structured logging (`slog`) + `trace_id`
- [ ] `/healthz` (dan `/readyz` jika perlu)
- [ ] Graceful shutdown
- [ ] Migration ter-version
- [ ] Timeout di setiap panggilan keluar
- [ ] Unit + integration test

**Antar service**
- [ ] Database per service
- [ ] Sync via HTTP/gRPC dengan timeout, retry, dan circuit breaker
- [ ] Async via Kafka dengan consumer yang idempotent, manual commit, dan DLQ
- [ ] Outbox untuk event penting

**Platform**
- [ ] API gateway (routing, auth, rate limit)
- [ ] Tracing (OpenTelemetry + Jaeger)
- [ ] Metrics (Prometheus + Grafana)
- [ ] docker-compose untuk dev lokal
- [ ] CI: lint, test, build image
- [ ] Manifest deployment (K8s/Helm)

### Urutan ringkas

```
Fase 1  user service        → struktur, DB, Docker, shutdown
Fase 2  order + HTTP call   → timeout, failure
Fase 3  Kafka + notification → event, partition, consumer group, idempotency, outbox
Fase 4  gateway             → single entry, JWT, request ID
Fase 5  observability       → tracing, metrics
Fase 6  resilience          → circuit breaker, rate limit, saga
Fase 7  testing             → unit, integration, e2e
Fase 8  gRPC (opsional)     → kontrak ketat
Fase 9  CI/CD + K8s         → otomatisasi & deploy
```

### Glosarium singkat

| Istilah | Arti singkat |
|---|---|
| Service | Program kecil mandiri dengan satu tanggung jawab bisnis |
| Monolith | Satu aplikasi besar berisi semua fitur |
| Bounded context | Batas domain bisnis yang jelas untuk satu service |
| API | "Pintu" untuk berkomunikasi dengan sebuah service |
| Sync / Async | Menunggu jawaban / kirim lalu lanjut tanpa menunggu |
| Message broker | Perantara yang menyimpan dan mengantarkan pesan antar service |
| Event | Pesan berisi fakta yang sudah terjadi |
| Topic | Kategori pesan di Kafka, misalnya `order.created` |
| Partition | Pecahan topic. Urutan pesan dijamin per partition |
| Offset | Nomor urut pesan di partition, dipakai sebagai "pembatas buku" consumer |
| Consumer group | Kumpulan instance yang berbagi partition, sehingga tiap pesan diproses sekali per group |
| DLQ | Topic untuk pesan yang gagal diproses |
| Idempotent | Dijalankan berkali-kali, hasilnya sama dengan sekali |
| Timeout | Batas waktu maksimal menunggu jawaban |
| Circuit breaker | "Sekring" yang memutus panggilan ke service yang terus gagal |
| Saga | Rangkaian transaksi lokal + aksi pembatalan jika ada yang gagal |
| Eventual consistency | Data akan sinkron, tapi tidak seketika |
| API Gateway | Pintu masuk tunggal untuk semua request dari luar |
| JWT | Token login bertanda tangan digital |
| Trace | Rekaman perjalanan satu request melewati banyak service |
| Container | Paket aplikasi + dependensinya yang jalan sama di mana pun |
| Kubernetes | Sistem untuk menjalankan dan mengelola banyak container |
| CI/CD | Otomatisasi test, build, dan deploy |

### Referensi belajar

- **Buku**
  - *Building Microservices* (2nd ed.), Sam Newman: buku utama.
  - *Monolith to Microservices*, Sam Newman: cara memecah monolith.
  - *Designing Data-Intensive Applications*, Martin Kleppmann: fondasi data dan sistem terdistribusi.
- **Web**
  - [microservices.io](https://microservices.io): katalog pattern (Saga, Outbox, API Gateway, dll.).
  - [Martin Fowler: Microservices](https://martinfowler.com/articles/microservices.html)
  - [The Twelve-Factor App](https://12factor.net)
  - [Go docs: net/http](https://pkg.go.dev/net/http), [log/slog](https://pkg.go.dev/log/slog)
  - [OpenTelemetry Go](https://opentelemetry.io/docs/languages/go/)
  - [Apache Kafka docs](https://kafka.apache.org/documentation/)
  - [franz-go](https://github.com/twmb/franz-go): Kafka client Go yang dipakai di roadmap ini

---

> Tips: setelah menyelesaikan tiap fase, tulis catatan singkat berisi **masalah apa yang muncul dan kenapa solusinya dibutuhkan**. Itulah inti belajar microservices: setiap komponen ada karena ada masalah yang ingin diselesaikan.
