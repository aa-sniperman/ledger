SHELL := /bin/bash

ifneq (,$(wildcard .env))
include .env
export
endif

GOCACHE ?= /tmp/go-build
GO := env GOCACHE=$(GOCACHE) go

SEED_USERS ?= user_123,user_456
SEED_AMOUNT ?= 10000
SEED_CURRENCY ?= USD

.PHONY: help api worker seed seed-accounts clean-legacy-seed migrate migrate-central migrate-shards test test-http test-service test-worker

help:
	@echo "Available targets:"
	@echo "  make api              Run the API server"
	@echo "  make worker           Run the command worker (set WORKER_SHARD_IDS to limit shard ownership)"
	@echo "  make seed             Seed users, system accounts, and mint balances"
	@echo "  make seed-accounts    Seed users and system accounts only"
	@echo "  make clean-legacy-seed Remove legacy single-currency seed data"
	@echo "  make migrate          Migrate central DB and all shard DBs"
	@echo "  make migrate-central  Migrate only DATABASE_URL"
	@echo "  make migrate-shards   Migrate every DB listed in SHARD_DATABASE_URLS"
	@echo "  make test             Run the full test suite"
	@echo "  make test-http        Run HTTP API tests"
	@echo "  make test-service     Run service tests"
	@echo "  make test-worker      Run worker tests"

api:
	$(GO) run ./cmd/api

worker:
	$(GO) run ./cmd/worker

seed:
	$(GO) run ./cmd/devsetup -users "$(SEED_USERS)" -amount "$(SEED_AMOUNT)" -currency "$(SEED_CURRENCY)"

seed-accounts:
	$(GO) run ./cmd/devsetup -users "$(SEED_USERS)" -currency "$(SEED_CURRENCY)" -skip-mint

clean-legacy-seed:
	$(GO) run ./cmd/devcleanup

migrate: migrate-central migrate-shards

migrate-central:
	DATABASE_URL="$(DATABASE_URL)" $(GO) run ./cmd/migrate

migrate-shards:
	@if [[ -z "$(SHARD_DATABASE_URLS)" ]]; then echo "SHARD_DATABASE_URLS is not configured" >&2; exit 1; fi
	@IFS=','; for entry in $(SHARD_DATABASE_URLS); do \
		shard_id="$${entry%%=*}"; \
		url="$${entry#*=}"; \
		if [[ "$$shard_id" == "$$entry" || -z "$$url" ]]; then \
			echo "invalid SHARD_DATABASE_URLS entry: $$entry" >&2; \
			exit 1; \
		fi; \
		echo "migrating shard $$shard_id"; \
		DATABASE_URL="$$url" $(GO) run ./cmd/migrate || exit 1; \
	done

test:
	$(GO) test -count=1 ./...

test-http:
	$(GO) test -count=1 ./internal/httpapi

test-service:
	$(GO) test -count=1 ./internal/service

test-worker:
	$(GO) test -count=1 ./internal/worker
