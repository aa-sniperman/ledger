package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
)

type Runner struct {
	db *sql.DB
	fs fs.FS
}

func NewRunner(db *sql.DB, migrationFS fs.FS) *Runner {
	return &Runner{
		db: db,
		fs: migrationFS,
	}
}

func (r *Runner) Up(ctx context.Context) ([]string, error) {
	if err := r.ensureMigrationsTable(ctx); err != nil {
		return nil, err
	}

	available, err := r.availableMigrations()
	if err != nil {
		return nil, err
	}

	applied, err := r.appliedMigrations(ctx)
	if err != nil {
		return nil, err
	}

	var executed []string

	for _, name := range available {
		if _, ok := applied[name]; ok {
			continue
		}

		contents, err := fs.ReadFile(r.fs, name)
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", name, err)
		}

		if err := r.applyMigration(ctx, name, string(contents)); err != nil {
			return nil, err
		}

		executed = append(executed, name)
	}

	return executed, nil
}

func (r *Runner) ensureMigrationsTable(ctx context.Context) error {
	const query = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	filename TEXT PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

	if _, err := r.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}

	return nil
}

func (r *Runner) availableMigrations() ([]string, error) {
	entries, err := fs.ReadDir(r.fs, ".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	names := make([]string, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if filepath.Ext(name) != ".sql" || !strings.HasSuffix(name, ".up.sql") {
			continue
		}

		names = append(names, name)
	}

	slices.Sort(names)
	return names, nil
}

func (r *Runner) appliedMigrations(ctx context.Context) (map[string]struct{}, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT filename FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("query applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]struct{})

	for rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}

		applied[filename] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}

	return applied, nil
}

func (r *Runner) applyMigration(ctx context.Context, filename, sqlText string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration transaction %s: %w", filename, err)
	}

	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, sqlText); err != nil {
		return fmt.Errorf("execute migration %s: %w", filename, err)
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (filename) VALUES ($1)`, filename); err != nil {
		return fmt.Errorf("record migration %s: %w", filename, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", filename, err)
	}

	return nil
}
