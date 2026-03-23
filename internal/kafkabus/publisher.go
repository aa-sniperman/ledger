package kafkabus

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/sniperman/ledger/internal/command"
)

type kafkaMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Publisher struct {
	topic  string
	writer kafkaMessageWriter
	now    func() time.Time
}

func NewPublisher(brokers []string, topic string) (*Publisher, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one kafka broker is required")
	}
	if strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("kafka topic is required")
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
		Balancer:     &kafka.Hash{},
	}

	return NewPublisherWithWriter(topic, writer)
}

func NewPublisherWithWriter(topic string, writer kafkaMessageWriter) (*Publisher, error) {
	if strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("kafka topic is required")
	}
	if writer == nil {
		return nil, fmt.Errorf("kafka writer is required")
	}

	return &Publisher{
		topic:  topic,
		writer: writer,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

func (p *Publisher) PublishAccepted(ctx context.Context, envelope command.Envelope) error {
	message, err := MessageFromEnvelope(envelope, p.now())
	if err != nil {
		return err
	}

	payload, err := EncodeMessage(message)
	if err != nil {
		return err
	}

	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(message.ShardID),
		Value: payload,
		Time:  message.PublishedAt,
	}); err != nil {
		return fmt.Errorf("publish kafka command %s: %w", envelope.CommandID, err)
	}

	return nil
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}
