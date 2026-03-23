package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sniperman/ledger/internal/sharding"
)

const (
	defaultAppEnv          = "development"
	defaultHTTPAddr        = ":8080"
	defaultDatabaseURL     = "postgres://postgres:postgres@localhost:5432/ledger?sslmode=disable"
	defaultTestDatabaseURL = "postgres://postgres:postgres@localhost:5432/ledger_test?sslmode=disable"
	defaultShardIDs        = "shard-a"
	defaultShutdownTimeout = 10 * time.Second
	defaultReadTimeout     = 5 * time.Second
	defaultWriteTimeout    = 10 * time.Second
	defaultWorkerPoll      = 250 * time.Millisecond
)

type Config struct {
	AppEnv                string
	HTTPAddr              string
	DatabaseURL           string
	TestDatabaseURL       string
	ShardIDs              []sharding.ShardID
	ShardDatabaseURLs     map[sharding.ShardID]string
	TestShardDatabaseURLs map[sharding.ShardID]string
	ShutdownTimeout       time.Duration
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	WorkerPollInterval    time.Duration
}

func Load() (Config, error) {
	if err := loadDotEnv(".env"); err != nil {
		return Config{}, err
	}

	cfg := Config{
		AppEnv:             getEnv("APP_ENV", defaultAppEnv),
		HTTPAddr:           getEnv("HTTP_ADDR", defaultHTTPAddr),
		DatabaseURL:        getEnv("DATABASE_URL", defaultDatabaseURL),
		TestDatabaseURL:    getEnv("TEST_DATABASE_URL", defaultTestDatabaseURL),
		ShutdownTimeout:    defaultShutdownTimeout,
		ReadTimeout:        defaultReadTimeout,
		WriteTimeout:       defaultWriteTimeout,
		WorkerPollInterval: defaultWorkerPoll,
	}

	var err error

	cfg.ShutdownTimeout, err = getDuration("SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout)
	if err != nil {
		return Config{}, err
	}

	cfg.ReadTimeout, err = getDuration("HTTP_READ_TIMEOUT", cfg.ReadTimeout)
	if err != nil {
		return Config{}, err
	}

	cfg.WriteTimeout, err = getDuration("HTTP_WRITE_TIMEOUT", cfg.WriteTimeout)
	if err != nil {
		return Config{}, err
	}

	cfg.WorkerPollInterval, err = getDuration("WORKER_POLL_INTERVAL", cfg.WorkerPollInterval)
	if err != nil {
		return Config{}, err
	}

	cfg.ShardIDs, err = getShardIDs("SHARD_IDS", defaultShardIDs)
	if err != nil {
		return Config{}, err
	}

	cfg.ShardDatabaseURLs, err = getShardDatabaseURLs("SHARD_DATABASE_URLS")
	if err != nil {
		return Config{}, err
	}

	cfg.TestShardDatabaseURLs, err = getShardDatabaseURLs("TEST_SHARD_DATABASE_URLS")
	if err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func loadDotEnv(path string) error {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("parse %s:%d: expected KEY=VALUE", path, lineNumber)
		}

		key = strings.TrimSpace(key)
		if key == "" {
			return fmt.Errorf("parse %s:%d: empty key", path, lineNumber)
		}

		if existingValue, exists := os.LookupEnv(key); exists && strings.TrimSpace(existingValue) != "" {
			continue
		}

		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"'`)

		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("set %s from %s:%d: %w", key, path, lineNumber, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	return nil
}

func getEnv(key, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback
	}

	return value
}

func getDuration(key string, fallback time.Duration) (time.Duration, error) {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}

	return duration, nil
}

func getShardIDs(key, fallback string) ([]sharding.ShardID, error) {
	value := getEnv(key, fallback)
	parts := strings.Split(value, ",")
	shardIDs := make([]sharding.ShardID, 0, len(parts))

	for _, part := range parts {
		shardID := sharding.ShardID(strings.TrimSpace(part))
		if shardID == "" {
			continue
		}
		if err := shardID.Validate(); err != nil {
			return nil, fmt.Errorf("parse %s: %w", key, err)
		}
		shardIDs = append(shardIDs, shardID)
	}

	if len(shardIDs) == 0 {
		return nil, fmt.Errorf("parse %s: at least one shard id is required", key)
	}

	return shardIDs, nil
}

func getShardDatabaseURLs(key string) (map[sharding.ShardID]string, error) {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return nil, nil
	}

	entries := strings.Split(value, ",")
	urls := make(map[sharding.ShardID]string, len(entries))

	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		shardIDPart, urlPart, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("parse %s: invalid shard database entry %q", key, entry)
		}

		shardID := sharding.ShardID(strings.TrimSpace(shardIDPart))
		if err := shardID.Validate(); err != nil {
			return nil, fmt.Errorf("parse %s: %w", key, err)
		}

		url := strings.TrimSpace(urlPart)
		if url == "" {
			return nil, fmt.Errorf("parse %s: empty database url for shard %q", key, shardID)
		}

		urls[shardID] = url
	}

	if len(urls) == 0 {
		return nil, nil
	}

	return urls, nil
}
