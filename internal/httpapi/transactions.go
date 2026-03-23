package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/sniperman/ledger/internal/domain"
	"github.com/sniperman/ledger/internal/service"
	"github.com/sniperman/ledger/internal/store"
)

type createTransactionRequest struct {
	Flow          string                          `json:"flow"`
	TransactionID string                          `json:"transaction_id"`
	PostingKey    string                          `json:"posting_key"`
	Type          string                          `json:"type"`
	Status        string                          `json:"status"`
	EffectiveAt   time.Time                       `json:"effective_at"`
	CreatedAt     time.Time                       `json:"created_at"`
	Entries       []createTransactionEntryRequest `json:"entries"`
}

type createTransactionEntryRequest struct {
	EntryID   string `json:"entry_id"`
	AccountID string `json:"account_id"`
	Amount    int64  `json:"amount"`
	Currency  string `json:"currency"`
	Direction string `json:"direction"`
}

type transactionResponse struct {
	TransactionID string                     `json:"transaction_id"`
	PostingKey    string                     `json:"posting_key"`
	Type          string                     `json:"type"`
	Status        string                     `json:"status"`
	EffectiveAt   time.Time                  `json:"effective_at"`
	CreatedAt     time.Time                  `json:"created_at"`
	Entries       []transactionEntryResponse `json:"entries"`
}

type transactionEntryResponse struct {
	EntryID        string     `json:"entry_id"`
	TransactionID  string     `json:"transaction_id"`
	AccountID      string     `json:"account_id"`
	AccountVersion int64      `json:"account_version"`
	Amount         int64      `json:"amount"`
	Currency       string     `json:"currency"`
	Direction      string     `json:"direction"`
	Status         string     `json:"status"`
	EffectiveAt    time.Time  `json:"effective_at"`
	CreatedAt      time.Time  `json:"created_at"`
	DiscardedAt    *time.Time `json:"discarded_at,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	var request createTransactionRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	input, err := toCreateTransactionInput(request)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	transaction, idempotent, err := s.transactionService.Create(r.Context(), input)
	if err != nil {
		if errors.Is(err, domain.ErrBalanceLockFailed) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}

		if isClientError(err) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	statusCode := http.StatusCreated
	if idempotent {
		statusCode = http.StatusOK
	}

	writeJSON(w, statusCode, toTransactionResponse(transaction))
}

func (s *Server) handlePostTransaction(w http.ResponseWriter, r *http.Request) {
	s.handleTransitionTransaction(w, r, domain.TransactionStatusPosted)
}

func (s *Server) handleGetTransaction(w http.ResponseWriter, r *http.Request) {
	transactionID := r.PathValue("id")
	if transactionID == "" {
		writeError(w, http.StatusBadRequest, "transaction id is required")
		return
	}

	transaction, err := s.queryService.GetTransaction(r.Context(), transactionID)
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

	writeJSON(w, http.StatusOK, toTransactionResponse(transaction))
}

func (s *Server) handleArchiveTransaction(w http.ResponseWriter, r *http.Request) {
	s.handleTransitionTransaction(w, r, domain.TransactionStatusArchived)
}

func (s *Server) handleTransitionTransaction(w http.ResponseWriter, r *http.Request, nextStatus domain.TransactionStatus) {
	transactionID := r.PathValue("id")
	if transactionID == "" {
		writeError(w, http.StatusBadRequest, "transaction id is required")
		return
	}

	var (
		transaction domain.Transaction
		err         error
	)

	switch nextStatus {
	case domain.TransactionStatusPosted:
		transaction, err = s.transactionService.Post(r.Context(), transactionID)
	case domain.TransactionStatusArchived:
		transaction, err = s.transactionService.Archive(r.Context(), transactionID)
	default:
		writeError(w, http.StatusInternalServerError, "unsupported transaction transition")
		return
	}

	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeError(w, http.StatusNotFound, err.Error())
		case errors.Is(err, domain.ErrInvalidTransition), errors.Is(err, domain.ErrInvalidTransaction), errors.Is(err, domain.ErrInvalidEntry):
			writeError(w, http.StatusConflict, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "internal server error")
		}
		return
	}

	writeJSON(w, http.StatusOK, toTransactionResponse(transaction))
}

func toCreateTransactionInput(request createTransactionRequest) (service.CreateTransactionInput, error) {
	entries := make([]service.CreateEntryInput, 0, len(request.Entries))

	for _, entry := range request.Entries {
		entries = append(entries, service.CreateEntryInput{
			EntryID:   entry.EntryID,
			AccountID: entry.AccountID,
			Amount:    entry.Amount,
			Currency:  entry.Currency,
			Direction: domain.Direction(entry.Direction),
		})
	}

	return service.CreateTransactionInput{
		Flow:          service.TransactionFlow(request.Flow),
		TransactionID: request.TransactionID,
		PostingKey:    request.PostingKey,
		Type:          request.Type,
		Status:        domain.TransactionStatus(request.Status),
		EffectiveAt:   request.EffectiveAt,
		CreatedAt:     request.CreatedAt,
		Entries:       entries,
	}, nil
}

func toTransactionResponse(transaction domain.Transaction) transactionResponse {
	entries := make([]transactionEntryResponse, 0, len(transaction.Entries))

	for _, entry := range transaction.Entries {
		entries = append(entries, transactionEntryResponse{
			EntryID:        entry.ID,
			TransactionID:  entry.TransactionID,
			AccountID:      entry.AccountID,
			AccountVersion: entry.AccountVersion,
			Amount:         entry.Money.Amount,
			Currency:       entry.Money.Currency,
			Direction:      string(entry.Direction),
			Status:         string(entry.Status),
			EffectiveAt:    entry.EffectiveAt,
			CreatedAt:      entry.CreatedAt,
			DiscardedAt:    entry.DiscardedAt,
		})
	}

	return transactionResponse{
		TransactionID: transaction.ID,
		PostingKey:    transaction.PostingKey,
		Type:          transaction.Type,
		Status:        string(transaction.Status),
		EffectiveAt:   transaction.EffectiveAt,
		CreatedAt:     transaction.CreatedAt,
		Entries:       entries,
	}
}

func isClientError(err error) bool {
	return errors.Is(err, domain.ErrInvalidAccount) ||
		errors.Is(err, domain.ErrInvalidMoney) ||
		errors.Is(err, domain.ErrInvalidEntry) ||
		errors.Is(err, domain.ErrInvalidTransaction) ||
		errors.Is(err, domain.ErrInvalidTransition) ||
		errors.Is(err, domain.ErrMissingAccount) ||
		errors.Is(err, domain.ErrAccountCurrencyMismatch) ||
		errors.Is(err, domain.ErrAccountVersionMismatch) ||
		errors.Is(err, domain.ErrBalanceLockFailed)
}

func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, errorResponse{Error: message})
}
