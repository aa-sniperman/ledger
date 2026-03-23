package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/sharding"
)

type CommandRepository struct {
	db DBTX
}

func NewCommandRepository(db DBTX) *CommandRepository {
	return &CommandRepository{db: db}
}

func (r *CommandRepository) Create(ctx context.Context, envelope command.Envelope) error {
	if err := envelope.Validate(); err != nil {
		return err
	}

	const query = `
INSERT INTO ledger.ledger_commands (
	command_id,
	idempotency_key,
	shard_id,
	command_type,
	payload_json,
	status,
	attempt_count,
	next_attempt_at,
	result_json,
	error_code,
	error_message,
	created_at,
	updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
`

	if _, err := r.db.ExecContext(
		ctx,
		query,
		envelope.CommandID,
		envelope.IdempotencyKey,
		envelope.ShardID,
		envelope.Type,
		envelope.Payload,
		envelope.Status,
		0,
		nil,
		nil,
		nil,
		nil,
		envelope.CreatedAt,
		envelope.UpdatedAt,
	); err != nil {
		return fmt.Errorf("insert command %s: %w", envelope.CommandID, err)
	}

	return nil
}

func (r *CommandRepository) GetByID(ctx context.Context, commandID string) (command.Envelope, error) {
	const query = `
SELECT
	command_id,
	idempotency_key,
	shard_id,
	command_type,
	payload_json,
	status,
	attempt_count,
	next_attempt_at,
	result_json,
	error_code,
	error_message,
	created_at,
	updated_at
FROM ledger.ledger_commands
WHERE command_id = $1
`

	return r.getOne(ctx, query, commandID)
}

func (r *CommandRepository) GetByTypeAndIdempotencyKey(ctx context.Context, commandType command.Type, idempotencyKey string) (command.Envelope, error) {
	const query = `
SELECT
	command_id,
	idempotency_key,
	shard_id,
	command_type,
	payload_json,
	status,
	attempt_count,
	next_attempt_at,
	result_json,
	error_code,
	error_message,
	created_at,
	updated_at
FROM ledger.ledger_commands
WHERE command_type = $1
  AND idempotency_key = $2
`

	return r.getOne(ctx, query, commandType, idempotencyKey)
}

func (r *CommandRepository) ClaimNext(ctx context.Context, shardID sharding.ShardID, now time.Time) (command.Envelope, bool, error) {
	const query = `
WITH candidate AS (
	SELECT command_id
	FROM ledger.ledger_commands
	WHERE shard_id = $1
	  AND status IN ('accepted', 'failed_retryable')
	  AND (next_attempt_at IS NULL OR next_attempt_at <= $2)
	ORDER BY created_at, command_id
	LIMIT 1
	FOR UPDATE SKIP LOCKED
)
UPDATE ledger.ledger_commands c
SET
	status = $3,
	attempt_count = c.attempt_count + 1,
	updated_at = $2
FROM candidate
WHERE c.command_id = candidate.command_id
RETURNING
	c.command_id,
	c.idempotency_key,
	c.shard_id,
	c.command_type,
	c.payload_json,
	c.status,
	c.attempt_count,
	c.next_attempt_at,
	c.result_json,
	c.error_code,
	c.error_message,
	c.created_at,
	c.updated_at
`

	envelope, err := r.scanOne(ctx, query, shardID, now, command.StatusProcessing)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return command.Envelope{}, false, nil
		}
		return command.Envelope{}, false, err
	}

	return envelope, true, nil
}

func (r *CommandRepository) MarkSucceeded(ctx context.Context, commandID string, resultJSON []byte, updatedAt time.Time) error {
	const query = `
UPDATE ledger.ledger_commands
SET
	status = $1,
	result_json = $2,
	error_code = NULL,
	error_message = NULL,
	next_attempt_at = NULL,
	updated_at = $3
WHERE command_id = $4
`

	return r.updateCommandStatus(ctx, query, command.StatusSucceeded, resultJSON, updatedAt, commandID)
}

func (r *CommandRepository) MarkFailed(ctx context.Context, commandID string, status command.Status, errorCode, errorMessage string, nextAttemptAt *time.Time, updatedAt time.Time) error {
	const query = `
UPDATE ledger.ledger_commands
SET
	status = $1,
	error_code = $2,
	error_message = $3,
	next_attempt_at = $4,
	updated_at = $5
WHERE command_id = $6
`

	result, err := r.db.ExecContext(ctx, query, status, errorCode, errorMessage, nextAttemptAt, updatedAt, commandID)
	if err != nil {
		return fmt.Errorf("update failed command %s: %w", commandID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for failed command %s: %w", commandID, err)
	}
	if rowsAffected != 1 {
		return fmt.Errorf("%w: command %s", ErrNotFound, commandID)
	}

	return nil
}

func (r *CommandRepository) getOne(ctx context.Context, query string, args ...any) (command.Envelope, error) {
	return r.scanOne(ctx, query, args...)
}

func (r *CommandRepository) updateCommandStatus(ctx context.Context, query string, status command.Status, resultJSON []byte, updatedAt time.Time, commandID string) error {
	result, err := r.db.ExecContext(ctx, query, status, resultJSON, updatedAt, commandID)
	if err != nil {
		return fmt.Errorf("update succeeded command %s: %w", commandID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for succeeded command %s: %w", commandID, err)
	}
	if rowsAffected != 1 {
		return fmt.Errorf("%w: command %s", ErrNotFound, commandID)
	}

	return nil
}

func (r *CommandRepository) scanOne(ctx context.Context, query string, args ...any) (command.Envelope, error) {
	row := r.db.QueryRowContext(ctx, query, args...)

	var envelope command.Envelope
	var shardID string
	var commandType string
	var status string
	var payload []byte
	var result []byte
	var errorCode sql.NullString
	var errorMessage sql.NullString

	err := row.Scan(
		&envelope.CommandID,
		&envelope.IdempotencyKey,
		&shardID,
		&commandType,
		&payload,
		&status,
		&envelope.AttemptCount,
		&envelope.NextAttemptAt,
		&result,
		&errorCode,
		&errorMessage,
		&envelope.CreatedAt,
		&envelope.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return command.Envelope{}, ErrNotFound
		}
		return command.Envelope{}, fmt.Errorf("scan command row: %w", err)
	}

	envelope.ShardID = sharding.ShardID(shardID)
	envelope.Type = command.Type(commandType)
	envelope.Payload = json.RawMessage(payload)
	envelope.Result = json.RawMessage(result)
	envelope.Status = command.Status(status)
	if errorCode.Valid {
		envelope.ErrorCode = errorCode.String
	}
	if errorMessage.Valid {
		envelope.ErrorMessage = errorMessage.String
	}

	return envelope, nil
}
