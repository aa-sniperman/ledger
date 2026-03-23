package kafkabus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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

type partitionMessage struct {
	message  kafka.Message
	accepted AcceptedCommandMessage
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

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg             sync.WaitGroup
		mu             sync.Mutex
		partitionChans = make(map[int]chan partitionMessage)
		errCh          = make(chan error, 1)
	)

	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
		cancel()
	}

	getPartitionChan := func(partition int) chan partitionMessage {
		mu.Lock()
		defer mu.Unlock()

		if ch, ok := partitionChans[partition]; ok {
			return ch
		}

		ch := make(chan partitionMessage, 64)
		partitionChans[partition] = ch
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.runPartitionWorker(runCtx, partition, ch, reportErr)
		}()
		return ch
	}

	for {
		select {
		case err := <-errCh:
			wg.Wait()
			return err
		default:
		}

		message, err := c.reader.FetchMessage(runCtx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				cancel()
				closePartitionChans(partitionChans, &mu)
				wg.Wait()
				select {
				case workerErr := <-errCh:
					return workerErr
				default:
				}
				return nil
			}
			cancel()
			closePartitionChans(partitionChans, &mu)
			wg.Wait()
			return fmt.Errorf("fetch kafka message: %w", err)
		}

		accepted, err := DecodeMessage(message.Value)
		if err != nil {
			cancel()
			closePartitionChans(partitionChans, &mu)
			wg.Wait()
			return fmt.Errorf("decode kafka message: %w", err)
		}

		partitionCh := getPartitionChan(message.Partition)

		select {
		case partitionCh <- partitionMessage{message: message, accepted: accepted}:
		case <-runCtx.Done():
			closePartitionChans(partitionChans, &mu)
			wg.Wait()
			select {
			case workerErr := <-errCh:
				return workerErr
			default:
			}
			return nil
		}
	}
}

func (c *Consumer) runPartitionWorker(ctx context.Context, partition int, messages <-chan partitionMessage, reportErr func(error)) {
	for {
		select {
		case <-ctx.Done():
			return
		case partitionMessage, ok := <-messages:
			if !ok {
				return
			}

			envelope, processed, err := c.processor.ProcessByID(ctx, partitionMessage.accepted.CommandID, partitionMessage.accepted.ShardID, c.now())
			if err != nil {
				reportErr(fmt.Errorf("process kafka command %s on partition %d: %w", partitionMessage.accepted.CommandID, partition, err))
				return
			}

			if commitErr := c.reader.CommitMessages(ctx, partitionMessage.message); commitErr != nil {
				reportErr(fmt.Errorf("commit kafka message %s on partition %d: %w", partitionMessage.accepted.CommandID, partition, commitErr))
				return
			}

			if processed {
				c.logger.Info("processed kafka command", "partition", partition, "shard_id", partitionMessage.accepted.ShardID, "command_id", envelope.CommandID, "type", envelope.Type, "status", envelope.Status)
				continue
			}

			c.logger.Info("skipped kafka command", "partition", partition, "shard_id", partitionMessage.accepted.ShardID, "command_id", partitionMessage.accepted.CommandID)
		}
	}
}

func closePartitionChans(partitionChans map[int]chan partitionMessage, mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()

	for partition, ch := range partitionChans {
		close(ch)
		delete(partitionChans, partition)
	}
}
