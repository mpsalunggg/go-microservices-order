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
		Key:   []byte(strconv.FormatInt(evt.OrderID, 10)),
		Value: value,
	}
	return p.client.ProduceSync(ctx, record).FirstErr()
}
