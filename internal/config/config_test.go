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
	t.Setenv("SYSTEM_ACCOUNT_POOL_SIZES", "")
	t.Setenv("WORKER_SHARD_IDS", "")
	t.Setenv("SHARD_DATABASE_URLS", "")
	t.Setenv("TEST_SHARD_DATABASE_URLS", "")
	t.Setenv("SHUTDOWN_TIMEOUT", "")
	t.Setenv("HTTP_READ_TIMEOUT", "")
	t.Setenv("HTTP_WRITE_TIMEOUT", "")
	t.Setenv("WORKER_POLL_INTERVAL", "")
	t.Setenv("API_IDEMPOTENCY_CACHE_TTL", "")
	t.Setenv("KAFKA_ENABLED", "")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_COMMANDS_TOPIC", "")
	t.Setenv("KAFKA_CONSUMER_GROUP", "")
	t.Setenv("KAFKA_AUTO_CREATE_TOPIC", "")
	t.Setenv("KAFKA_TOPIC_PARTITIONS", "")
	t.Setenv("KAFKA_REPLICATION_FACTOR", "")
	t.Setenv("DEBUG_API_ENABLED", "")

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
	if len(cfg.WorkerShardIDs) != 1 || string(cfg.WorkerShardIDs[0]) != "shard-a" {
		t.Fatalf("expected default worker shard ids [shard-a], got %+v", cfg.WorkerShardIDs)
	}
	if cfg.SystemAccountPoolSizes["payout_holding"] != 4 || cfg.SystemAccountPoolSizes["cash_in_clearing"] != 4 {
		t.Fatalf("expected default system account pool sizes, got %+v", cfg.SystemAccountPoolSizes)
	}
	if !cfg.DebugAPIEnabled {
		t.Fatal("expected debug api to default to enabled in development")
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
	if len(cfg.WorkerShardIDs) != 3 || string(cfg.WorkerShardIDs[1]) != "shard-b" {
		t.Fatalf("expected worker shard ids to default to all shard ids, got %+v", cfg.WorkerShardIDs)
	}
}

func TestLoadParsesWorkerShardSubset(t *testing.T) {
	t.Setenv("SHARD_IDS", "shard-a,shard-b,shard-c")
	t.Setenv("WORKER_SHARD_IDS", "shard-b, shard-c")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if len(cfg.WorkerShardIDs) != 2 || string(cfg.WorkerShardIDs[0]) != "shard-b" || string(cfg.WorkerShardIDs[1]) != "shard-c" {
		t.Fatalf("expected worker shard subset, got %+v", cfg.WorkerShardIDs)
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

func TestLoadParsesSystemAccountPoolSizes(t *testing.T) {
	t.Setenv("SYSTEM_ACCOUNT_POOL_SIZES", "payout_holding=16,cash_in_clearing=8")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if cfg.SystemAccountPoolSizes["payout_holding"] != 16 {
		t.Fatalf("expected payout_holding pool size 16, got %+v", cfg.SystemAccountPoolSizes)
	}
	if cfg.SystemAccountPoolSizes["cash_in_clearing"] != 8 {
		t.Fatalf("expected cash_in_clearing pool size 8, got %+v", cfg.SystemAccountPoolSizes)
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

func TestLoadParsesAPIIdempotencyCacheTTL(t *testing.T) {
	t.Setenv("API_IDEMPOTENCY_CACHE_TTL", "45s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if cfg.APIIdempotencyCacheTTL != 45*time.Second {
		t.Fatalf("expected API idempotency cache ttl 45s, got %s", cfg.APIIdempotencyCacheTTL)
	}
}

func TestLoadParsesDebugAPIEnabled(t *testing.T) {
	t.Setenv("DEBUG_API_ENABLED", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if cfg.DebugAPIEnabled {
		t.Fatal("expected debug api to be disabled")
	}
}

func TestLoadParsesKafkaSettings(t *testing.T) {
	t.Setenv("KAFKA_ENABLED", "true")
	t.Setenv("KAFKA_BROKERS", "localhost:9092, localhost:9093")
	t.Setenv("KAFKA_COMMANDS_TOPIC", "custom.commands")
	t.Setenv("KAFKA_CONSUMER_GROUP", "custom-worker")
	t.Setenv("KAFKA_AUTO_CREATE_TOPIC", "true")
	t.Setenv("KAFKA_TOPIC_PARTITIONS", "8")
	t.Setenv("KAFKA_REPLICATION_FACTOR", "2")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected config load to succeed, got %v", err)
	}

	if !cfg.KafkaEnabled {
		t.Fatal("expected kafka to be enabled")
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[1] != "localhost:9093" {
		t.Fatalf("expected parsed kafka brokers, got %+v", cfg.KafkaBrokers)
	}
	if cfg.KafkaCommandsTopic != "custom.commands" {
		t.Fatalf("expected kafka topic override, got %q", cfg.KafkaCommandsTopic)
	}
	if cfg.KafkaConsumerGroup != "custom-worker" {
		t.Fatalf("expected kafka group override, got %q", cfg.KafkaConsumerGroup)
	}
	if !cfg.KafkaAutoCreateTopic {
		t.Fatal("expected kafka auto create topic to be enabled")
	}
	if cfg.KafkaTopicPartitions != 8 {
		t.Fatalf("expected kafka partitions 8, got %d", cfg.KafkaTopicPartitions)
	}
	if cfg.KafkaReplicationFactor != 2 {
		t.Fatalf("expected kafka replication factor 2, got %d", cfg.KafkaReplicationFactor)
	}
}
