package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/database"
	"github.com/sniperman/ledger/internal/sharding"
)

type cleanupSummary struct {
	LegacyAccountsDeleted int
	TransactionsDeleted   int
	EntriesDeleted        int
	BalanceRowsDeleted    int
}

func main() {
	var dryRun bool
	flag.BoolVar(&dryRun, "dry-run", false, "show what would be deleted without modifying data")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	centralDB, shardDBs, err := openCleanupEnvironment(ctx, cfg)
	if err != nil {
		slog.Error("open environment", "error", err)
		os.Exit(1)
	}
	defer closeCleanupEnvironment(centralDB, shardDBs)

	legacyTransactionIDs := make([]string, 0)
	legacyAccountIDs := make([]string, 0)
	for shardID, db := range shardDBs {
		summary, accountIDs, transactionIDs, cleanupErr := cleanupLegacyShardData(ctx, db, dryRun)
		if cleanupErr != nil {
			slog.Error("cleanup shard data", "shard_id", shardID, "error", cleanupErr)
			os.Exit(1)
		}

		legacyAccountIDs = append(legacyAccountIDs, accountIDs...)
		legacyTransactionIDs = append(legacyTransactionIDs, transactionIDs...)
		slog.Info(
			"processed shard cleanup",
			"shard_id", shardID,
			"dry_run", dryRun,
			"legacy_accounts", summary.LegacyAccountsDeleted,
			"transactions", summary.TransactionsDeleted,
			"entries", summary.EntriesDeleted,
			"balance_rows", summary.BalanceRowsDeleted,
		)
	}

	commandCount, locatorCount, err := cleanupCentralMetadata(ctx, centralDB, uniqueStrings(legacyAccountIDs), uniqueStrings(legacyTransactionIDs), dryRun)
	if err != nil {
		slog.Error("cleanup central metadata", "error", err)
		os.Exit(1)
	}

	slog.Info(
		"processed central cleanup",
		"dry_run", dryRun,
		"transaction_locators", locatorCount,
		"commands", commandCount,
	)
}

func openCleanupEnvironment(ctx context.Context, cfg config.Config) (*sql.DB, map[sharding.ShardID]*sql.DB, error) {
	centralDB, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, nil, err
	}

	if len(cfg.ShardDatabaseURLs) == 0 {
		shardID := cfg.ShardIDs[0]
		return centralDB, map[sharding.ShardID]*sql.DB{shardID: centralDB}, nil
	}

	shardDBs, err := database.OpenMany(ctx, cfg.ShardDatabaseURLs)
	if err != nil {
		_ = centralDB.Close()
		return nil, nil, err
	}

	return centralDB, shardDBs, nil
}

func closeCleanupEnvironment(centralDB *sql.DB, shardDBs map[sharding.ShardID]*sql.DB) {
	seen := map[*sql.DB]struct{}{}
	for _, db := range shardDBs {
		if db == nil {
			continue
		}
		if _, ok := seen[db]; ok {
			continue
		}
		seen[db] = struct{}{}
		_ = db.Close()
	}

	if centralDB != nil {
		if _, ok := seen[centralDB]; !ok {
			_ = centralDB.Close()
		}
	}
}

func cleanupLegacyShardData(ctx context.Context, db *sql.DB, dryRun bool) (cleanupSummary, []string, []string, error) {
	accountIDs, err := loadLegacyAccountIDs(ctx, db)
	if err != nil {
		return cleanupSummary{}, nil, nil, err
	}
	if len(accountIDs) == 0 {
		return cleanupSummary{}, nil, nil, nil
	}

	transactionIDs, err := loadTransactionIDsForAccounts(ctx, db, accountIDs)
	if err != nil {
		return cleanupSummary{}, nil, nil, err
	}

	summary := cleanupSummary{
		LegacyAccountsDeleted: len(accountIDs),
		TransactionsDeleted:   len(transactionIDs),
		EntriesDeleted:        countEntryRows(ctx, db, transactionIDs),
		BalanceRowsDeleted:    len(accountIDs),
	}

	if dryRun {
		return summary, accountIDs, transactionIDs, nil
	}

	if err := deleteByValues(ctx, db, `DELETE FROM ledger.ledger_entries WHERE transaction_id IN (%s)`, transactionIDs); err != nil {
		return cleanupSummary{}, nil, nil, err
	}
	if err := deleteByValues(ctx, db, `DELETE FROM ledger.ledger_transactions WHERE transaction_id IN (%s)`, transactionIDs); err != nil {
		return cleanupSummary{}, nil, nil, err
	}
	if err := deleteByValues(ctx, db, `DELETE FROM ledger.account_current_balances WHERE account_id IN (%s)`, accountIDs); err != nil {
		return cleanupSummary{}, nil, nil, err
	}
	if err := deleteByValues(ctx, db, `DELETE FROM ledger.accounts WHERE account_id IN (%s)`, accountIDs); err != nil {
		return cleanupSummary{}, nil, nil, err
	}

	return summary, accountIDs, transactionIDs, nil
}

func cleanupCentralMetadata(ctx context.Context, db *sql.DB, accountIDs, transactionIDs []string, dryRun bool) (int, int, error) {
	commandIDs, err := loadLegacyCommandIDs(ctx, db, accountIDs, transactionIDs)
	if err != nil {
		return 0, 0, err
	}

	if dryRun {
		return len(commandIDs), len(transactionIDs), nil
	}

	if err := deleteByValues(ctx, db, `DELETE FROM ledger.transaction_locators WHERE transaction_id IN (%s)`, transactionIDs); err != nil {
		return 0, 0, err
	}
	if err := deleteByValues(ctx, db, `DELETE FROM ledger.ledger_commands WHERE command_id IN (%s)`, commandIDs); err != nil {
		return 0, 0, err
	}

	return len(commandIDs), len(transactionIDs), nil
}

func loadLegacyAccountIDs(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT account_id FROM ledger.accounts`)
	if err != nil {
		return nil, fmt.Errorf("query accounts: %w", err)
	}
	defer rows.Close()

	accountIDs := make([]string, 0)
	for rows.Next() {
		var accountID string
		if err := rows.Scan(&accountID); err != nil {
			return nil, fmt.Errorf("scan account id: %w", err)
		}
		if isLegacySeedAccountID(accountID) {
			accountIDs = append(accountIDs, accountID)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate accounts: %w", err)
	}

	return uniqueStrings(accountIDs), nil
}

func loadTransactionIDsForAccounts(ctx context.Context, db *sql.DB, accountIDs []string) ([]string, error) {
	if len(accountIDs) == 0 {
		return nil, nil
	}

	query, args := selectDistinctValuesQuery(`SELECT DISTINCT transaction_id FROM ledger.ledger_entries WHERE account_id IN (%s)`, accountIDs)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query transactions for accounts: %w", err)
	}
	defer rows.Close()

	transactionIDs := make([]string, 0)
	for rows.Next() {
		var transactionID string
		if err := rows.Scan(&transactionID); err != nil {
			return nil, fmt.Errorf("scan transaction id: %w", err)
		}
		transactionIDs = append(transactionIDs, transactionID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate transactions: %w", err)
	}

	return uniqueStrings(transactionIDs), nil
}

func loadLegacyCommandIDs(ctx context.Context, db *sql.DB, accountIDs, transactionIDs []string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT command_id, payload_json::text, COALESCE(result_json::text, '') FROM ledger.ledger_commands`)
	if err != nil {
		return nil, fmt.Errorf("query commands: %w", err)
	}
	defer rows.Close()

	commandIDs := make([]string, 0)
	for rows.Next() {
		var (
			commandID  string
			payload    string
			resultJSON string
		)
		if err := rows.Scan(&commandID, &payload, &resultJSON); err != nil {
			return nil, fmt.Errorf("scan command row: %w", err)
		}

		for _, transactionID := range transactionIDs {
			if strings.Contains(payload, transactionID) || strings.Contains(resultJSON, transactionID) {
				commandIDs = append(commandIDs, commandID)
				break
			}
		}
		for _, accountID := range accountIDs {
			if strings.Contains(payload, accountID) || strings.Contains(resultJSON, accountID) {
				commandIDs = append(commandIDs, commandID)
				break
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate commands: %w", err)
	}

	return uniqueStrings(commandIDs), nil
}

func countEntryRows(ctx context.Context, db *sql.DB, transactionIDs []string) int {
	if len(transactionIDs) == 0 {
		return 0
	}

	query, args := countValuesQuery(`SELECT COUNT(*) FROM ledger.ledger_entries WHERE transaction_id IN (%s)`, transactionIDs)
	var count int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0
	}
	return count
}

func deleteByValues(ctx context.Context, db *sql.DB, template string, values []string) error {
	if len(values) == 0 {
		return nil
	}

	query, args := valuesQuery(template, values)
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("exec delete %q: %w", query, err)
	}
	return nil
}

func selectDistinctValuesQuery(template string, values []string) (string, []any) {
	return valuesQuery(template, values)
}

func countValuesQuery(template string, values []string) (string, []any) {
	return valuesQuery(template, values)
}

func valuesQuery(template string, values []string) (string, []any) {
	placeholders := make([]string, len(values))
	args := make([]any, len(values))
	for i, value := range values {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = value
	}
	return fmt.Sprintf(template, strings.Join(placeholders, ",")), args
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	slices.Sort(unique)
	return unique
}

func isLegacySeedAccountID(accountID string) bool {
	prefix, remainder, ok := strings.Cut(accountID, ":")
	if !ok || remainder == "" {
		return false
	}

	if prefix == sharding.UserWalletAccountPrefix {
		return strings.Count(remainder, ":") == 0
	}

	parts := strings.Split(remainder, ":")
	switch sharding.SystemAccountRole(prefix) {
	case sharding.SystemAccountRoleCashIn, sharding.SystemAccountRolePayoutHold, sharding.SystemAccountRoleSettlement:
		if len(parts) == 1 {
			return true
		}
		if len(parts) == 2 {
			_, err := strconv.Atoi(parts[1])
			return err == nil
		}
	}

	return false
}
