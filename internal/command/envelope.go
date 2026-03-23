package command

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sniperman/ledger/internal/sharding"
)

type Type string

const (
	TypeTransactionCreate  Type = "transactions.create"
	TypeTransactionPost    Type = "transactions.post"
	TypeTransactionArchive Type = "transactions.archive"
	TypeWithdrawalCreate   Type = "payments.withdrawals.create"
	TypeWithdrawalPost     Type = "payments.withdrawals.post"
	TypeWithdrawalArchive  Type = "payments.withdrawals.archive"
	TypeDepositRecord      Type = "payments.deposits.record"
)

type Status string

const (
	StatusAccepted        Status = "accepted"
	StatusProcessing      Status = "processing"
	StatusSucceeded       Status = "succeeded"
	StatusFailedRetryable Status = "failed_retryable"
	StatusFailedTerminal  Status = "failed_terminal"
)

type Envelope struct {
	CommandID      string           `json:"command_id"`
	IdempotencyKey string           `json:"idempotency_key"`
	ShardID        sharding.ShardID `json:"shard_id"`
	Type           Type             `json:"type"`
	Payload        json.RawMessage  `json:"payload"`
	Status         Status           `json:"status"`
	AttemptCount   int              `json:"attempt_count"`
	NextAttemptAt  *time.Time       `json:"next_attempt_at,omitempty"`
	Result         json.RawMessage  `json:"result,omitempty"`
	ErrorCode      string           `json:"error_code,omitempty"`
	ErrorMessage   string           `json:"error_message,omitempty"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
}

func (e Envelope) Validate() error {
	if strings.TrimSpace(e.CommandID) == "" {
		return fmt.Errorf("command id is required")
	}
	if strings.TrimSpace(e.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency key is required")
	}
	if err := e.ShardID.Validate(); err != nil {
		return err
	}
	if err := e.Type.Validate(); err != nil {
		return err
	}
	if len(e.Payload) == 0 {
		return fmt.Errorf("payload is required")
	}
	if err := e.Status.Validate(); err != nil {
		return err
	}
	if e.AttemptCount < 0 {
		return fmt.Errorf("attempt_count must not be negative")
	}
	if e.CreatedAt.IsZero() {
		return fmt.Errorf("created_at is required")
	}
	if e.UpdatedAt.IsZero() {
		return fmt.Errorf("updated_at is required")
	}
	return nil
}

func (t Type) Validate() error {
	switch t {
	case TypeTransactionCreate, TypeTransactionPost, TypeTransactionArchive, TypeWithdrawalCreate, TypeWithdrawalPost, TypeWithdrawalArchive, TypeDepositRecord:
		return nil
	default:
		return fmt.Errorf("invalid command type %q", t)
	}
}

func (s Status) Validate() error {
	switch s {
	case StatusAccepted, StatusProcessing, StatusSucceeded, StatusFailedRetryable, StatusFailedTerminal:
		return nil
	default:
		return fmt.Errorf("invalid command status %q", s)
	}
}
