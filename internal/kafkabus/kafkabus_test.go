package kafkabus

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/sharding"
)

func TestPublisherUsesShardIDAsKafkaKey(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{}
	publisher, err := NewPublisherWithWriter("ledger.commands", writer)
	if err != nil {
		t.Fatalf("build publisher: %v", err)
	}
	publisher.now = func() time.Time { return time.Date(2026, 3, 23, 15, 0, 0, 0, time.UTC) }

	envelope := command.Envelope{
		CommandID:      "cmd_123",
		IdempotencyKey: "idem_123",
		ShardID:        "shard-a",
		Type:           command.TypeWithdrawalCreate,
		Payload:        json.RawMessage(`{"ok":true}`),
		Status:         command.StatusAccepted,
		CreatedAt:      time.Date(2026, 3, 23, 14, 0, 0, 0, time.UTC),
		UpdatedAt:      time.Date(2026, 3, 23, 14, 0, 0, 0, time.UTC),
	}

	if err := publisher.PublishAccepted(context.Background(), envelope); err != nil {
		t.Fatalf("publish accepted: %v", err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("expected 1 kafka message, got %d", len(writer.messages))
	}
	if got := string(writer.messages[0].Key); got != "shard-a" {
		t.Fatalf("expected shard key shard-a, got %q", got)
	}

	message, err := DecodeMessage(writer.messages[0].Value)
	if err != nil {
		t.Fatalf("decode published message: %v", err)
	}
	if message.CommandID != "cmd_123" || message.ShardID != "shard-a" {
		t.Fatalf("unexpected accepted command message: %+v", message)
	}
}

func TestConsumerProcessesAndCommitsMessage(t *testing.T) {
	t.Parallel()

	payload, err := EncodeMessage(AcceptedCommandMessage{
		CommandID:   "cmd_123",
		ShardID:     "shard-a",
		CommandType: command.TypeWithdrawalCreate,
		PublishedAt: time.Date(2026, 3, 23, 15, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}

	reader := &fakeReader{
		messages: []kafka.Message{{Value: payload}},
	}
	processor := &fakeProcessor{
		envelope: command.Envelope{
			CommandID: "cmd_123",
			ShardID:   "shard-a",
			Type:      command.TypeWithdrawalCreate,
			Status:    command.StatusSucceeded,
		},
		ok: true,
	}

	consumer, err := NewConsumerWithReader(reader, processor, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("build consumer: %v", err)
	}
	consumer.now = func() time.Time { return time.Date(2026, 3, 23, 15, 1, 0, 0, time.UTC) }

	ctx, cancel := context.WithCancel(context.Background())
	reader.afterCommit = cancel

	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}

	if processor.commandID != "cmd_123" || processor.shardID != "shard-a" {
		t.Fatalf("unexpected processor invocation: %+v", processor)
	}
	if reader.commitCount != 1 {
		t.Fatalf("expected 1 commit, got %d", reader.commitCount)
	}
}

type fakeWriter struct {
	messages []kafka.Message
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.messages = append(w.messages, msgs...)
	return nil
}

func (w *fakeWriter) Close() error {
	return nil
}

type fakeReader struct {
	messages    []kafka.Message
	index       int
	commitCount int
	afterCommit func()
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if r.index >= len(r.messages) {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	msg := r.messages[r.index]
	r.index++
	return msg, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, _ ...kafka.Message) error {
	r.commitCount++
	if r.afterCommit != nil {
		r.afterCommit()
	}
	return nil
}

func (r *fakeReader) Close() error {
	return nil
}

type fakeProcessor struct {
	commandID string
	shardID   sharding.ShardID
	envelope  command.Envelope
	ok        bool
	err       error
}

func (p *fakeProcessor) ProcessByID(_ context.Context, commandID string, shardID sharding.ShardID, _ time.Time) (command.Envelope, bool, error) {
	p.commandID = commandID
	p.shardID = shardID
	return p.envelope, p.ok, p.err
}

func TestDecodeMessageRejectsInvalidPayload(t *testing.T) {
	t.Parallel()

	_, err := DecodeMessage([]byte(`{"command_id":"","shard_id":"shard-a","command_type":"payments.withdrawals.create","published_at":"2026-03-23T15:00:00Z"}`))
	if err == nil {
		t.Fatal("expected decode validation error")
	}
}

func TestConsumerReturnsProcessorError(t *testing.T) {
	t.Parallel()

	payload, err := EncodeMessage(AcceptedCommandMessage{
		CommandID:   "cmd_123",
		ShardID:     "shard-a",
		CommandType: command.TypeWithdrawalCreate,
		PublishedAt: time.Date(2026, 3, 23, 15, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}

	reader := &fakeReader{messages: []kafka.Message{{Value: payload}}}
	consumer, err := NewConsumerWithReader(reader, &fakeProcessor{err: errors.New("boom")}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("build consumer: %v", err)
	}

	err = consumer.Run(context.Background())
	if err == nil {
		t.Fatal("expected processor error")
	}
}
