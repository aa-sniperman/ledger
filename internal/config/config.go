package config

import (
	"fmt"
	"os"
	"time"
)

const (
	defaultAppEnv          = "development"
	defaultHTTPAddr        = ":8080"
	defaultDatabaseURL     = "postgres://postgres:postgres@localhost:5432/ledger?sslmode=disable"
	defaultTestDatabaseURL = "postgres://postgres:postgres@localhost:5432/ledger_test?sslmode=disable"
	defaultShutdownTimeout = 10 * time.Second
	defaultReadTimeout     = 5 * time.Second
	defaultWriteTimeout    = 10 * time.Second
)

type Config struct {
	AppEnv          string
	HTTPAddr        string
	DatabaseURL     string
	TestDatabaseURL string
	ShutdownTimeout time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		AppEnv:          getEnv("APP_ENV", defaultAppEnv),
		HTTPAddr:        getEnv("HTTP_ADDR", defaultHTTPAddr),
		DatabaseURL:     getEnv("DATABASE_URL", defaultDatabaseURL),
		TestDatabaseURL: getEnv("TEST_DATABASE_URL", defaultTestDatabaseURL),
		ShutdownTimeout: defaultShutdownTimeout,
		ReadTimeout:     defaultReadTimeout,
		WriteTimeout:    defaultWriteTimeout,
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

	return cfg, nil
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
