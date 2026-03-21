package config

import "testing"

func TestLoadUsesDefaultTestDatabaseURL(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("TEST_DATABASE_URL", "")
	t.Setenv("SHUTDOWN_TIMEOUT", "")
	t.Setenv("HTTP_READ_TIMEOUT", "")
	t.Setenv("HTTP_WRITE_TIMEOUT", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if cfg.TestDatabaseURL != defaultTestDatabaseURL {
		t.Fatalf("expected default test database URL %q, got %q", defaultTestDatabaseURL, cfg.TestDatabaseURL)
	}
}

func TestLoadUsesTestDatabaseURLEnvOverride(t *testing.T) {
	t.Setenv("TEST_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/custom_ledger_test?sslmode=disable")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if cfg.TestDatabaseURL != "postgres://postgres:postgres@localhost:5432/custom_ledger_test?sslmode=disable" {
		t.Fatalf("expected test database URL override to be applied, got %q", cfg.TestDatabaseURL)
	}
}
