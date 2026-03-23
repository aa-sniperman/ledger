package kafkabus

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/sharding"
)

func TestPublisherUsesTransactionScopedKafkaKeyForCreate(t *testing.T) {
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
		Payload:        json.RawMessage(`{"user_id":"user_123","input":{"TransactionID":"tx_123"}}`),
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
	if got := string(writer.messages[0].Key); got != "shard-a:tx:tx_123" {
		t.Fatalf("expected transaction-scoped key shard-a:tx:tx_123, got %q", got)
	}

	message, err := DecodeMessage(writer.messages[0].Value)
	if err != nil {
		t.Fatalf("decode published message: %v", err)
	}
	if message.CommandID != "cmd_123" || message.ShardID != "shard-a" {
		t.Fatalf("unexpected accepted command message: %+v", message)
	}
}

func TestPartitionKeyForEnvelopeUsesSameTransactionKeyForTransitions(t *testing.T) {
	t.Parallel()

	createKey, err := partitionKeyForEnvelope(command.Envelope{
		ShardID: "shard-b",
		Type:    command.TypeDepositRecord,
		Payload: json.RawMessage(`{"user_id":"user_1","input":{"TransactionID":"tx_789"}}`),
	})
	if err != nil {
		t.Fatalf("create key: %v", err)
	}

	postKey, err := partitionKeyForEnvelope(command.Envelope{
		ShardID: "shard-b",
		Type:    command.TypeWithdrawalPost,
		Payload: json.RawMessage(`{"user_id":"user_1","idempotency_key":"idem","transaction_id":"tx_789"}`),
	})
	if err != nil {
		t.Fatalf("post key: %v", err)
	}

	if createKey != postKey {
		t.Fatalf("expected same key for same transaction lifecycle, got create=%q post=%q", createKey, postKey)
	}
}

func TestPartitionKeyForEnvelopeFallsBackToShardWhenTransactionIDMissing(t *testing.T) {
	t.Parallel()

	key, err := partitionKeyForEnvelope(command.Envelope{
		ShardID: "shard-a",
		Type:    command.TypeWithdrawalCreate,
		Payload: json.RawMessage(`{"user_id":"user_1","input":{}}`),
	})
	if err != nil {
		t.Fatalf("partition key: %v", err)
	}
	if key != "shard-a" {
		t.Fatalf("expected shard fallback key, got %q", key)
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
	mu          sync.Mutex
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.index >= len(r.messages) {
		r.mu.Unlock()
		<-ctx.Done()
		r.mu.Lock()
		return kafka.Message{}, ctx.Err()
	}
	msg := r.messages[r.index]
	r.index++
	return msg, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, _ ...kafka.Message) error {
	r.mu.Lock()
	r.commitCount++
	afterCommit := r.afterCommit
	r.mu.Unlock()
	if afterCommit != nil {
		afterCommit()
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

type recordingProcessor struct {
	mu             sync.Mutex
	started        map[string]chan struct{}
	release        map[string]chan struct{}
	order          []string
	concurrency    int
	maxConcurrency int
}

func newRecordingProcessor() *recordingProcessor {
	return &recordingProcessor{
		started: make(map[string]chan struct{}),
		release: make(map[string]chan struct{}),
	}
}

func (p *recordingProcessor) ProcessByID(ctx context.Context, commandID string, shardID sharding.ShardID, _ time.Time) (command.Envelope, bool, error) {
	p.mu.Lock()
	p.order = append(p.order, commandID)
	p.concurrency++
	if p.concurrency > p.maxConcurrency {
		p.maxConcurrency = p.concurrency
	}
	started := make(chan struct{})
	close(started)
	p.started[commandID] = started
	release, ok := p.release[commandID]
	if !ok {
		release = make(chan struct{})
		p.release[commandID] = release
	}
	p.mu.Unlock()

	select {
	case <-release:
	case <-ctx.Done():
		return command.Envelope{}, false, ctx.Err()
	}

	p.mu.Lock()
	p.concurrency--
	p.mu.Unlock()

	return command.Envelope{
		CommandID: commandID,
		ShardID:   shardID,
		Type:      command.TypeWithdrawalCreate,
		Status:    command.StatusSucceeded,
	}, true, nil
}

func (p *recordingProcessor) waitStarted(t *testing.T, commandID string) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	for {
		p.mu.Lock()
		started, ok := p.started[commandID]
		p.mu.Unlock()
		if ok {
			<-started
			return
		}

		select {
		case <-deadline:
			t.Fatalf("command %s did not start", commandID)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (p *recordingProcessor) allow(commandID string) {
	p.mu.Lock()
	release, ok := p.release[commandID]
	if !ok {
		release = make(chan struct{})
		p.release[commandID] = release
	}
	p.mu.Unlock()
	close(release)
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

func TestConsumerProcessesPartitionsInParallel(t *testing.T) {
	t.Parallel()

	payloadA, err := EncodeMessage(AcceptedCommandMessage{
		CommandID:   "cmd_a",
		ShardID:     "shard-a",
		CommandType: command.TypeWithdrawalCreate,
		PublishedAt: time.Date(2026, 3, 23, 16, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("encode message A: %v", err)
	}
	payloadB, err := EncodeMessage(AcceptedCommandMessage{
		CommandID:   "cmd_b",
		ShardID:     "shard-b",
		CommandType: command.TypeWithdrawalCreate,
		PublishedAt: time.Date(2026, 3, 23, 16, 0, 1, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("encode message B: %v", err)
	}

	reader := &fakeReader{
		messages: []kafka.Message{
			{Partition: 0, Value: payloadA},
			{Partition: 1, Value: payloadB},
		},
	}
	processor := newRecordingProcessor()

	consumer, err := NewConsumerWithReader(reader, processor, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("build consumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	reader.afterCommit = func() {
		reader.mu.Lock()
		commits := reader.commitCount
		reader.mu.Unlock()
		if commits == 2 {
			cancel()
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	processor.waitStarted(t, "cmd_a")
	processor.waitStarted(t, "cmd_b")

	processor.mu.Lock()
	maxConcurrency := processor.maxConcurrency
	processor.mu.Unlock()
	if maxConcurrency < 2 {
		t.Fatalf("expected parallel processing across partitions, max concurrency=%d", maxConcurrency)
	}

	processor.allow("cmd_a")
	processor.allow("cmd_b")

	if err := <-done; err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	if reader.commitCount != 2 {
		t.Fatalf("expected 2 commits, got %d", reader.commitCount)
	}
}

func TestConsumerPreservesSequentialOrderWithinPartition(t *testing.T) {
	t.Parallel()

	payloadA, err := EncodeMessage(AcceptedCommandMessage{
		CommandID:   "cmd_a",
		ShardID:     "shard-a",
		CommandType: command.TypeWithdrawalCreate,
		PublishedAt: time.Date(2026, 3, 23, 16, 5, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("encode message A: %v", err)
	}
	payloadB, err := EncodeMessage(AcceptedCommandMessage{
		CommandID:   "cmd_b",
		ShardID:     "shard-a",
		CommandType: command.TypeWithdrawalCreate,
		PublishedAt: time.Date(2026, 3, 23, 16, 5, 1, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("encode message B: %v", err)
	}

	reader := &fakeReader{
		messages: []kafka.Message{
			{Partition: 0, Value: payloadA},
			{Partition: 0, Value: payloadB},
		},
	}
	processor := newRecordingProcessor()

	consumer, err := NewConsumerWithReader(reader, processor, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("build consumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	reader.afterCommit = func() {
		reader.mu.Lock()
		commits := reader.commitCount
		reader.mu.Unlock()
		if commits == 2 {
			cancel()
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	processor.waitStarted(t, "cmd_a")

	processor.mu.Lock()
	_, secondStarted := processor.started["cmd_b"]
	processor.mu.Unlock()
	if secondStarted {
		t.Fatal("expected second command on same partition to wait for the first")
	}

	processor.allow("cmd_a")
	processor.waitStarted(t, "cmd_b")
	processor.allow("cmd_b")

	if err := <-done; err != nil {
		t.Fatalf("run consumer: %v", err)
	}

	processor.mu.Lock()
	defer processor.mu.Unlock()
	if len(processor.order) != 2 || processor.order[0] != "cmd_a" || processor.order[1] != "cmd_b" {
		t.Fatalf("expected partition order [cmd_a cmd_b], got %v", processor.order)
	}
}
