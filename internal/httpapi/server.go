package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/devsetup"
	"github.com/sniperman/ledger/internal/kafkabus"
	"github.com/sniperman/ledger/internal/service"
	"github.com/sniperman/ledger/internal/sharding"
)

type Server struct {
	db                 *sql.DB
	accountService     *service.AccountService
	commandService     *service.CommandService
	devSetupService    *devsetup.Service
	queryService       *service.QueryService
	transactionService *service.TransactionService
	idempotencyCache   *idempotencyCache
	httpServer         *http.Server
	closers            []io.Closer
}

type healthResponse struct {
	Status string `json:"status"`
}

func New(cfg config.Config, db *sql.DB) *Server {
	shardIDs := cfg.ShardIDs
	if len(shardIDs) == 0 {
		shardIDs = []sharding.ShardID{"shard-a"}
	}

	router, err := sharding.NewRouter(shardIDs, cfg.SystemAccountPoolSizes)
	if err != nil {
		panic(err)
	}
	registry, err := sharding.NewSingleDBRegistry(shardIDs, db)
	if err != nil {
		panic(err)
	}

	return NewWithRegistry(cfg, db, router, registry)
}

func NewWithRegistry(cfg config.Config, db *sql.DB, router sharding.Router, registry *sharding.DBRegistry) *Server {
	if registry == nil {
		panic("nil shard db registry")
	}

	commandService := service.NewCommandService(db, router, registry)
	closers := make([]io.Closer, 0, 1)
	if cfg.KafkaEnabled {
		publisher, err := kafkabus.NewPublisher(cfg.KafkaBrokers, cfg.KafkaCommandsTopic)
		if err != nil {
			panic(err)
		}
		commandService = service.NewCommandServiceWithPublisher(db, router, registry, publisher)
		closers = append(closers, publisher)
	}

	server := &Server{
		db:                 db,
		accountService:     service.NewAccountService(db),
		commandService:     commandService,
		devSetupService:    devsetup.NewService(router, registry, commandService),
		queryService:       service.NewQueryService(db, router, registry),
		transactionService: service.NewTransactionService(db),
		idempotencyCache:   newIdempotencyCache(cfg.APIIdempotencyCacheTTL),
		closers:            closers,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /docs", server.handleSwaggerUI)
	mux.HandleFunc("GET /openapi.json", server.handleOpenAPI)
	mux.HandleFunc("GET /healthz", server.handleHealth)
	mux.HandleFunc("GET /readyz", server.handleReady)
	mux.HandleFunc("GET /users/{id}/balances/{currency}", server.handleGetUserBalance)
	mux.HandleFunc("GET /accounts/{id}/balances", server.handleGetAccountBalances)
	mux.HandleFunc("GET /transactions/{id}", server.handleGetTransaction)
	mux.HandleFunc("POST /commands/payments.withdrawals.create", server.handleEnqueueWithdrawalCreate)
	mux.HandleFunc("POST /commands/payments.withdrawals.post", server.handleEnqueueWithdrawalPost)
	mux.HandleFunc("POST /commands/payments.withdrawals.archive", server.handleEnqueueWithdrawalArchive)
	mux.HandleFunc("POST /commands/payments.deposits.record", server.handleEnqueueDepositRecord)
	if cfg.DebugAPIEnabled {
		mux.HandleFunc("POST /debug/devsetup/seed", server.handleDebugDevSetupSeed)
	}

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
	err := s.httpServer.Shutdown(ctx)
	for _, closer := range s.closers {
		if closeErr := closer.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
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
