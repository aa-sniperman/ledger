package httpapi

import (
	"errors"
	"net/http"
	"time"

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

	balance, err := s.accountService.GetBalance(r.Context(), accountID)
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
