package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/service"
)

type enqueueCreateTransactionCommandRequest struct {
	UserID         string                          `json:"user_id"`
	IdempotencyKey string                          `json:"idempotency_key"`
	Flow           string                          `json:"flow"`
	TransactionID  string                          `json:"transaction_id"`
	PostingKey     string                          `json:"posting_key"`
	Type           string                          `json:"type"`
	Status         string                          `json:"status"`
	EffectiveAt    time.Time                       `json:"effective_at"`
	CreatedAt      time.Time                       `json:"created_at"`
	Entries        []createTransactionEntryRequest `json:"entries"`
}

type enqueueTransitionCommandRequest struct {
	UserID         string `json:"user_id"`
	IdempotencyKey string `json:"idempotency_key"`
	TransactionID  string `json:"transaction_id"`
}

type enqueueWithdrawalCreateCommandRequest struct {
	UserID         string    `json:"user_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	TransactionID  string    `json:"transaction_id"`
	PostingKey     string    `json:"posting_key"`
	Amount         int64     `json:"amount"`
	Currency       string    `json:"currency"`
	EffectiveAt    time.Time `json:"effective_at"`
	CreatedAt      time.Time `json:"created_at"`
}

type enqueueDepositRecordCommandRequest struct {
	UserID         string    `json:"user_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	TransactionID  string    `json:"transaction_id"`
	PostingKey     string    `json:"posting_key"`
	Amount         int64     `json:"amount"`
	Currency       string    `json:"currency"`
	EffectiveAt    time.Time `json:"effective_at"`
	CreatedAt      time.Time `json:"created_at"`
}

type commandResponse struct {
	CommandID      string          `json:"command_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	ShardID        string          `json:"shard_id"`
	Type           string          `json:"type"`
	Status         string          `json:"status"`
	AttemptCount   int             `json:"attempt_count"`
	NextAttemptAt  *time.Time      `json:"next_attempt_at,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	ErrorCode      string          `json:"error_code,omitempty"`
	ErrorMessage   string          `json:"error_message,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

func (s *Server) handleEnqueueTransactionCreate(w http.ResponseWriter, r *http.Request) {
	var request enqueueCreateTransactionCommandRequest
	if !decodeJSONBody(w, r, &request) {
		return
	}

	createInput, err := toCreateTransactionInput(createTransactionRequest{
		Flow:          request.Flow,
		TransactionID: request.TransactionID,
		PostingKey:    request.PostingKey,
		Type:          request.Type,
		Status:        request.Status,
		EffectiveAt:   request.EffectiveAt,
		CreatedAt:     request.CreatedAt,
		Entries:       request.Entries,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	envelope, idempotent, err := s.commandService.EnqueueTransactionCreate(r.Context(), service.EnqueueCreateTransactionCommandInput{
		UserID:         request.UserID,
		IdempotencyKey: request.IdempotencyKey,
		CreateInput:    createInput,
	})
	if err != nil {
		if isClientError(err) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		slog.Error("enqueue transaction create command", "user_id", request.UserID, "idempotency_key", request.IdempotencyKey, "transaction_id", request.TransactionID, "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeCommandResponse(w, envelope, idempotent)
}

func (s *Server) handleEnqueueWithdrawalCreate(w http.ResponseWriter, r *http.Request) {
	var request enqueueWithdrawalCreateCommandRequest
	if !decodeJSONBody(w, r, &request) {
		return
	}

	if envelope, ok := s.idempotencyCache.Get(command.TypeWithdrawalCreate, request.IdempotencyKey); ok {
		writeCommandResponse(w, envelope, true)
		return
	}

	envelope, idempotent, err := s.commandService.EnqueueWithdrawalCreate(r.Context(), service.EnqueuePaymentCreateCommandInput{
		UserID:         request.UserID,
		IdempotencyKey: request.IdempotencyKey,
		TransactionID:  request.TransactionID,
		PostingKey:     request.PostingKey,
		Amount:         request.Amount,
		Currency:       request.Currency,
		EffectiveAt:    request.EffectiveAt,
		CreatedAt:      request.CreatedAt,
	})
	if err != nil {
		if isClientError(err) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		slog.Error("enqueue withdrawal create command", "user_id", request.UserID, "idempotency_key", request.IdempotencyKey, "transaction_id", request.TransactionID, "currency", request.Currency, "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	s.idempotencyCache.Put(command.TypeWithdrawalCreate, request.IdempotencyKey, envelope)
	writeCommandResponse(w, envelope, idempotent)
}

func (s *Server) handleEnqueueWithdrawalPost(w http.ResponseWriter, r *http.Request) {
	s.handleEnqueueTransition(w, r, command.TypeWithdrawalPost)
}

func (s *Server) handleEnqueueWithdrawalArchive(w http.ResponseWriter, r *http.Request) {
	s.handleEnqueueTransition(w, r, command.TypeWithdrawalArchive)
}

func (s *Server) handleEnqueueDepositRecord(w http.ResponseWriter, r *http.Request) {
	var request enqueueDepositRecordCommandRequest
	if !decodeJSONBody(w, r, &request) {
		return
	}

	if envelope, ok := s.idempotencyCache.Get(command.TypeDepositRecord, request.IdempotencyKey); ok {
		writeCommandResponse(w, envelope, true)
		return
	}

	envelope, idempotent, err := s.commandService.EnqueueDepositRecord(r.Context(), service.EnqueuePaymentCreateCommandInput{
		UserID:         request.UserID,
		IdempotencyKey: request.IdempotencyKey,
		TransactionID:  request.TransactionID,
		PostingKey:     request.PostingKey,
		Amount:         request.Amount,
		Currency:       request.Currency,
		EffectiveAt:    request.EffectiveAt,
		CreatedAt:      request.CreatedAt,
	})
	if err != nil {
		if isClientError(err) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		slog.Error("enqueue deposit record command", "user_id", request.UserID, "idempotency_key", request.IdempotencyKey, "transaction_id", request.TransactionID, "currency", request.Currency, "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	s.idempotencyCache.Put(command.TypeDepositRecord, request.IdempotencyKey, envelope)
	writeCommandResponse(w, envelope, idempotent)
}

func (s *Server) handleEnqueueTransactionPost(w http.ResponseWriter, r *http.Request) {
	s.handleEnqueueTransition(w, r, command.TypeTransactionPost)
}

func (s *Server) handleEnqueueTransactionArchive(w http.ResponseWriter, r *http.Request) {
	s.handleEnqueueTransition(w, r, command.TypeTransactionArchive)
}

func (s *Server) handleEnqueueTransition(w http.ResponseWriter, r *http.Request, commandType command.Type) {
	var request enqueueTransitionCommandRequest
	if !decodeJSONBody(w, r, &request) {
		return
	}

	if envelope, ok := s.idempotencyCache.Get(commandType, request.IdempotencyKey); ok {
		writeCommandResponse(w, envelope, true)
		return
	}

	input := service.EnqueueTransitionCommandInput{
		UserID:         request.UserID,
		IdempotencyKey: request.IdempotencyKey,
		TransactionID:  request.TransactionID,
	}

	var (
		envelope   command.Envelope
		idempotent bool
		err        error
	)

	switch commandType {
	case command.TypeTransactionPost:
		envelope, idempotent, err = s.commandService.EnqueueTransactionPost(r.Context(), input)
	case command.TypeTransactionArchive:
		envelope, idempotent, err = s.commandService.EnqueueTransactionArchive(r.Context(), input)
	case command.TypeWithdrawalPost:
		envelope, idempotent, err = s.commandService.EnqueueWithdrawalPost(r.Context(), input)
	case command.TypeWithdrawalArchive:
		envelope, idempotent, err = s.commandService.EnqueueWithdrawalArchive(r.Context(), input)
	default:
		slog.Error("enqueue transition command", "error", "unsupported command type", "type", commandType)
		writeError(w, http.StatusInternalServerError, "unsupported command type")
		return
	}
	if err != nil {
		if isClientError(err) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		slog.Error("enqueue transition command", "user_id", request.UserID, "idempotency_key", request.IdempotencyKey, "transaction_id", request.TransactionID, "type", commandType, "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	s.idempotencyCache.Put(commandType, request.IdempotencyKey, envelope)
	writeCommandResponse(w, envelope, idempotent)
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}

	return true
}

func writeCommandResponse(w http.ResponseWriter, envelope command.Envelope, idempotent bool) {
	statusCode := http.StatusAccepted
	if idempotent {
		statusCode = http.StatusOK
	}

	writeJSON(w, statusCode, commandResponse{
		CommandID:      envelope.CommandID,
		IdempotencyKey: envelope.IdempotencyKey,
		ShardID:        string(envelope.ShardID),
		Type:           string(envelope.Type),
		Status:         string(envelope.Status),
		AttemptCount:   envelope.AttemptCount,
		NextAttemptAt:  envelope.NextAttemptAt,
		Result:         envelope.Result,
		ErrorCode:      envelope.ErrorCode,
		ErrorMessage:   envelope.ErrorMessage,
		CreatedAt:      envelope.CreatedAt,
		UpdatedAt:      envelope.UpdatedAt,
	})
}
