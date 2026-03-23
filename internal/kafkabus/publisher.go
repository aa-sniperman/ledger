package kafkabus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
	start := time.Now()
	message, err := MessageFromEnvelope(envelope, p.now())
	if err != nil {
		return err
	}

	payload, err := EncodeMessage(message)
	if err != nil {
		return err
	}

	key, err := partitionKeyForEnvelope(envelope)
	if err != nil {
		return err
	}

	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: payload,
		Time:  message.PublishedAt,
	}); err != nil {
		return fmt.Errorf("publish kafka command %s: %w", envelope.CommandID, err)
	}

	slog.Info("published kafka command", "command_id", envelope.CommandID, "type", envelope.Type, "shard_id", envelope.ShardID, "partition_key", key, "topic", p.topic, "duration", time.Since(start))

	return nil
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

type kafkaCreateCommandPayload struct {
	Input struct {
		TransactionID string `json:"TransactionID"`
	} `json:"input"`
}

type kafkaTransitionCommandPayload struct {
	TransactionID string `json:"transaction_id"`
}

func partitionKeyForEnvelope(envelope command.Envelope) (string, error) {
	transactionID, err := transactionIDForEnvelope(envelope)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(transactionID) == "" {
		return string(envelope.ShardID), nil
	}

	return fmt.Sprintf("%s:tx:%s", envelope.ShardID, transactionID), nil
}

func transactionIDForEnvelope(envelope command.Envelope) (string, error) {
	switch envelope.Type {
	case command.TypeTransactionCreate, command.TypeWithdrawalCreate, command.TypeDepositRecord:
		var payload kafkaCreateCommandPayload
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return "", fmt.Errorf("decode create command payload for kafka partition key: %w", err)
		}
		return strings.TrimSpace(payload.Input.TransactionID), nil
	case command.TypeTransactionPost, command.TypeTransactionArchive, command.TypeWithdrawalPost, command.TypeWithdrawalArchive:
		var payload kafkaTransitionCommandPayload
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return "", fmt.Errorf("decode transition command payload for kafka partition key: %w", err)
		}
		return strings.TrimSpace(payload.TransactionID), nil
	default:
		return "", nil
	}
}
