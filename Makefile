USER_DB_URL := postgres://user:secret@localhost:5433/user?sslmode=disable
ORDER_DB_URL := postgres://order:secret@localhost:5434/order?sslmode=disable
KAFKA_LOCAL := localhost:9092

.PHONY: build up infra run-user run-order run-notification down logs migrate migrate-user migrate-user-status migrate-order migrate-order-status db-user db-order kafka-topics kafka-watch kafka-lag test

build:
	go build ./...

up:
	docker compose up -d --build

infra:
	docker compose up -d --wait user-db order-db kafka kafka-ui
	docker compose up kafka-init

run-user:
	DATABASE_URL="$(USER_DB_URL)" PORT=8081 go run ./services/user/cmd

run-order:
	DATABASE_URL="$(ORDER_DB_URL)" USER_SERVICE_URL=http://localhost:8081 KAFKA_BROKERS=$(KAFKA_LOCAL) PORT=8082 go run ./services/order/cmd

run-notification:
	KAFKA_BROKERS=$(KAFKA_LOCAL) go run ./services/notification/cmd

down:
	docker compose down

logs:
	docker compose logs -f

migrate: migrate-user migrate-order

migrate-user:
	goose -dir services/user/migrations postgres "$(USER_DB_URL)" up

migrate-user-status:
	goose -dir services/user/migrations postgres "$(USER_DB_URL)" status

migrate-order:
	goose -dir services/order/migrations postgres "$(ORDER_DB_URL)" up

migrate-order-status:
	goose -dir services/order/migrations postgres "$(ORDER_DB_URL)" status

db-user:
	docker compose exec user-db psql -U user -d user

db-order:
	docker compose exec order-db psql -U order -d order

kafka-topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list

# Lihat pesan masuk secara live. Topic lain: make kafka-watch TOPIC=order.created.dlq
TOPIC ?= order.created
kafka-watch:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic $(TOPIC) --from-beginning --property print.key=true --property print.partition=true --property print.offset=true

kafka-lag:
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group notification

test:
	go test ./...
