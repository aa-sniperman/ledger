package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sniperman/ledger/internal/config"
	"github.com/sniperman/ledger/internal/sharding"
	"github.com/sniperman/ledger/internal/testutil"
)

func TestOpenAPIEndpoint(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	server := New(config.Config{
		HTTPAddr:     ":0",
		ShardIDs:     []sharding.ShardID{"shard-a"},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}, db)
	httpServer := httptest.NewServer(server.httpServer.Handler)
	defer httpServer.Close()

	response, err := http.Get(httpServer.URL + "/openapi.json")
	if err != nil {
		t.Fatalf("get openapi spec: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /openapi.json, got %d", response.StatusCode)
	}
	if contentType := response.Header.Get("Content-Type"); !strings.Contains(contentType, "application/json") {
		t.Fatalf("expected application/json content type, got %q", contentType)
	}

	var document map[string]any
	if err := json.NewDecoder(response.Body).Decode(&document); err != nil {
		t.Fatalf("decode openapi spec: %v", err)
	}

	if document["openapi"] != "3.0.3" {
		t.Fatalf("expected OpenAPI version 3.0.3, got %+v", document["openapi"])
	}

	paths, ok := document["paths"].(map[string]any)
	if !ok {
		t.Fatalf("expected paths object, got %+v", document["paths"])
	}
	if _, ok := paths["/commands/payments.withdrawals.create"]; !ok {
		t.Fatalf("expected withdrawal create path in spec, got %+v", paths)
	}
}

func TestSwaggerUIEndpoint(t *testing.T) {
	db := testutil.OpenTestDB(t)
	testutil.ResetTestDB(t, db)

	server := New(config.Config{
		HTTPAddr:     ":0",
		ShardIDs:     []sharding.ShardID{"shard-a"},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}, db)
	httpServer := httptest.NewServer(server.httpServer.Handler)
	defer httpServer.Close()

	response, err := http.Get(httpServer.URL + "/docs")
	if err != nil {
		t.Fatalf("get swagger ui: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /docs, got %d", response.StatusCode)
	}
	if contentType := response.Header.Get("Content-Type"); !strings.Contains(contentType, "text/html") {
		t.Fatalf("expected text/html content type, got %q", contentType)
	}

	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read swagger ui body: %v", err)
	}

	body := string(bodyBytes)
	if !strings.Contains(body, "SwaggerUIBundle") || !strings.Contains(body, "/openapi.json") {
		t.Fatalf("expected swagger ui HTML to reference SwaggerUIBundle and /openapi.json, got %q", body)
	}
}
