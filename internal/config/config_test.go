package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadUsesDefaultTestDatabaseURL(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("TEST_DATABASE_URL", "")
	t.Setenv("SHARD_IDS", "")
	t.Setenv("SHARD_DATABASE_URLS", "")
	t.Setenv("TEST_SHARD_DATABASE_URLS", "")
	t.Setenv("SHUTDOWN_TIMEOUT", "")
	t.Setenv("HTTP_READ_TIMEOUT", "")
	t.Setenv("HTTP_WRITE_TIMEOUT", "")
	t.Setenv("WORKER_POLL_INTERVAL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if cfg.TestDatabaseURL != defaultTestDatabaseURL {
		t.Fatalf("expected default test database URL %q, got %q", defaultTestDatabaseURL, cfg.TestDatabaseURL)
	}

	if len(cfg.ShardIDs) != 1 || string(cfg.ShardIDs[0]) != "shard-a" {
		t.Fatalf("expected default shard ids [shard-a], got %+v", cfg.ShardIDs)
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

func TestLoadParsesShardIDs(t *testing.T) {
	t.Setenv("SHARD_IDS", "shard-a, shard-b,shard-c")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if len(cfg.ShardIDs) != 3 || string(cfg.ShardIDs[0]) != "shard-a" || string(cfg.ShardIDs[2]) != "shard-c" {
		t.Fatalf("expected parsed shard ids, got %+v", cfg.ShardIDs)
	}
}

func TestLoadParsesShardDatabaseURLs(t *testing.T) {
	t.Setenv("SHARD_DATABASE_URLS", "shard-a=postgres://localhost/a,shard-b=postgres://localhost/b")
	t.Setenv("TEST_SHARD_DATABASE_URLS", "shard-a=postgres://localhost/test_a,shard-b=postgres://localhost/test_b")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if len(cfg.ShardDatabaseURLs) != 2 || cfg.ShardDatabaseURLs["shard-a"] != "postgres://localhost/a" {
		t.Fatalf("expected parsed shard database urls, got %+v", cfg.ShardDatabaseURLs)
	}

	if len(cfg.TestShardDatabaseURLs) != 2 || cfg.TestShardDatabaseURLs["shard-b"] != "postgres://localhost/test_b" {
		t.Fatalf("expected parsed test shard database urls, got %+v", cfg.TestShardDatabaseURLs)
	}
}

func TestLoadReadsDotEnvWhenProcessEnvIsUnset(t *testing.T) {
	for _, key := range []string{
		"APP_ENV",
		"HTTP_ADDR",
		"DATABASE_URL",
		"TEST_DATABASE_URL",
		"SHARD_IDS",
		"SHARD_DATABASE_URLS",
		"TEST_SHARD_DATABASE_URLS",
	} {
		t.Setenv(key, "")
	}

	tempDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDir, ".env"), []byte("TEST_SHARD_DATABASE_URLS=shard-a=postgres://localhost/shard_a_test,shard-b=postgres://localhost/shard_b_test\n"), 0o644); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir temp dir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(workingDir)
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if len(cfg.TestShardDatabaseURLs) != 2 || cfg.TestShardDatabaseURLs["shard-a"] != "postgres://localhost/shard_a_test" {
		t.Fatalf("expected TEST_SHARD_DATABASE_URLS from .env, got %+v", cfg.TestShardDatabaseURLs)
	}
}

func TestLoadPrefersProcessEnvOverDotEnv(t *testing.T) {
	t.Setenv("TEST_SHARD_DATABASE_URLS", "shard-a=postgres://localhost/from_env")

	tempDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDir, ".env"), []byte("TEST_SHARD_DATABASE_URLS=shard-a=postgres://localhost/from_dotenv\n"), 0o644); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir temp dir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(workingDir)
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if got := cfg.TestShardDatabaseURLs["shard-a"]; got != "postgres://localhost/from_env" {
		t.Fatalf("expected process env to win over .env, got %q", got)
	}
}

func TestLoadParsesWorkerPollInterval(t *testing.T) {
	t.Setenv("WORKER_POLL_INTERVAL", "750ms")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if cfg.WorkerPollInterval != 750*time.Millisecond {
		t.Fatalf("expected worker poll interval 750ms, got %s", cfg.WorkerPollInterval)
	}
}
