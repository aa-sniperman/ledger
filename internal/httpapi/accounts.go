package httpapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/sniperman/ledger/internal/command"
	"github.com/sniperman/ledger/internal/service"
	"github.com/sniperman/ledger/internal/store"
)

type accountBalanceResponse struct {
	AccountID      string    `json:"account_id"`
	Currency       string    `json:"currency"`
	NormalBalance  string    `json:"normal_balance"`
	CurrentVersion int64     `json:"current_version"`
	Posted         int64     `json:"posted"`
	Pending        int64     `json:"pending"`
	Available      int64     `json:"available"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (s *Server) handleGetAccountBalances(w http.ResponseWriter, r *http.Request) {
	accountID := r.PathValue("id")
	if accountID == "" {
		writeError(w, http.StatusBadRequest, "account id is required")
		return
	}

	balance, err := s.queryService.GetAccountBalance(r.Context(), accountID)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeError(w, http.StatusNotFound, err.Error())
		case isClientError(err):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "internal server error")
		}
		return
	}

	writeJSON(w, http.StatusOK, toAccountBalanceResponse(balance))
}

func (s *Server) handleGetUserBalance(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("id")
	if userID == "" {
		writeError(w, http.StatusBadRequest, "user id is required")
		return
	}
	currency := r.PathValue("currency")
	if currency == "" {
		writeError(w, http.StatusBadRequest, "currency is required")
		return
	}

	balance, err := s.queryService.GetUserBalance(r.Context(), userID, currency)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeError(w, http.StatusNotFound, err.Error())
		case isClientError(err):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "internal server error")
		}
		return
	}

	writeJSON(w, http.StatusOK, toAccountBalanceResponse(balance))
}

func (s *Server) handleGetCommand(w http.ResponseWriter, r *http.Request) {
	commandID := r.PathValue("id")
	if commandID == "" {
		writeError(w, http.StatusBadRequest, "command id is required")
		return
	}

	envelope, err := s.queryService.GetCommand(r.Context(), commandID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusOK, toCommandResponse(envelope))
}

func toAccountBalanceResponse(balance service.AccountBalanceView) accountBalanceResponse {
	return accountBalanceResponse{
		AccountID:      balance.AccountID,
		Currency:       balance.Currency,
		NormalBalance:  string(balance.NormalBalance),
		CurrentVersion: balance.CurrentVersion,
		Posted:         balance.Posted,
		Pending:        balance.Pending,
		Available:      balance.Available,
		UpdatedAt:      balance.UpdatedAt,
	}
}

func toCommandResponse(envelope command.Envelope) commandResponse {
	return commandResponse{
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
	}
}
