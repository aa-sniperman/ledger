package kafkabus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/sharding"
)

type CommandProcessor interface {
	ProcessByID(ctx context.Context, commandID string, shardID sharding.ShardID, now time.Time) (command.Envelope, bool, error)
}

type kafkaMessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Consumer struct {
	reader    kafkaMessageReader
	processor CommandProcessor
	logger    *slog.Logger
	now       func() time.Time
}

func NewConsumer(brokers []string, topic, groupID string, processor CommandProcessor, logger *slog.Logger) (*Consumer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one kafka broker is required")
	}
	if strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("kafka topic is required")
	}
	if strings.TrimSpace(groupID) == "" {
		return nil, fmt.Errorf("kafka consumer group is required")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return NewConsumerWithReader(reader, processor, logger)
}

func NewConsumerWithReader(reader kafkaMessageReader, processor CommandProcessor, logger *slog.Logger) (*Consumer, error) {
	if reader == nil {
		return nil, fmt.Errorf("kafka reader is required")
	}
	if processor == nil {
		return nil, fmt.Errorf("command processor is required")
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Consumer{
		reader:    reader,
		processor: processor,
		logger:    logger,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.reader.Close()

	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("fetch kafka message: %w", err)
		}

		accepted, err := DecodeMessage(message.Value)
		if err != nil {
			return fmt.Errorf("decode kafka message: %w", err)
		}

		envelope, ok, err := c.processor.ProcessByID(ctx, accepted.CommandID, accepted.ShardID, c.now())
		if err != nil {
			return fmt.Errorf("process kafka command %s: %w", accepted.CommandID, err)
		}

		if commitErr := c.reader.CommitMessages(ctx, message); commitErr != nil {
			return fmt.Errorf("commit kafka message %s: %w", accepted.CommandID, commitErr)
		}

		if ok {
			c.logger.Info("processed kafka command", "shard_id", accepted.ShardID, "command_id", envelope.CommandID, "type", envelope.Type, "status", envelope.Status)
			continue
		}

		c.logger.Info("skipped kafka command", "shard_id", accepted.ShardID, "command_id", accepted.CommandID)
	}
}
