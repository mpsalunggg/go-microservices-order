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
		kgo.DisableAutoCommit(),
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
					return fmt.Errorf("produce to DLQ: %w", err)
				}
			}
		}

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

	log.Info("send notification",
		"event_id", evt.EventID, "order_id", evt.OrderID, "user_id", evt.UserID, "amount", evt.Amount)
	return nil
}
