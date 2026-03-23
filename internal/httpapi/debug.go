package httpapi

import (
	"net/http"
	"time"

	"github.com/sniperman/ledger/internal/devsetup"
)

type debugSeedRequest struct {
	Users    []string `json:"users"`
	Currency string   `json:"currency"`
	Amount   int64    `json:"amount"`
	SkipMint bool     `json:"skip_mint"`
}

func (s *Server) handleDebugDevSetupSeed(w http.ResponseWriter, r *http.Request) {
	var request debugSeedRequest
	if !decodeJSONBody(w, r, &request) {
		return
	}

	result, err := s.devSetupService.Seed(r.Context(), devsetup.SeedInput{
		UserIDs:  request.Users,
		Currency: request.Currency,
		Amount:   request.Amount,
		SkipMint: request.SkipMint,
		Now:      time.Now().UTC(),
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, result)
}
