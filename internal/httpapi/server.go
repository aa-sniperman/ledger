package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/service"
)

type Server struct {
	db                 *sql.DB
	accountService     *service.AccountService
	transactionService *service.TransactionService
	httpServer         *http.Server
}

type healthResponse struct {
	Status string `json:"status"`
}

func New(cfg config.Config, db *sql.DB) *Server {
	server := &Server{
		db:                 db,
		accountService:     service.NewAccountService(db),
		transactionService: service.NewTransactionService(db),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.handleHealth)
	mux.HandleFunc("GET /readyz", server.handleReady)
	mux.HandleFunc("GET /accounts/{id}/balances", server.handleGetAccountBalances)
	mux.HandleFunc("POST /transactions", server.handleCreateTransaction)
	mux.HandleFunc("POST /transactions/{id}/post", server.handlePostTransaction)
	mux.HandleFunc("POST /transactions/{id}/archive", server.handleArchiveTransaction)

	server.httpServer = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       30 * time.Second,
	}

	return server
}

func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := s.db.PingContext(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, healthResponse{Status: "database_unavailable"})
		return
	}

	writeJSON(w, http.StatusOK, healthResponse{Status: "ready"})
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	_ = json.NewEncoder(w).Encode(payload)
}
