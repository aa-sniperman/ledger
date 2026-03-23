package httpapi

import (
	"encoding/json"
	"net/http"
)

const swaggerUIHTML = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Ledger API Docs</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
    <style>
      body { margin: 0; background: #f6f7fb; }
      .topbar { display: none; }
    </style>
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
      window.onload = function () {
        window.ui = SwaggerUIBundle({
          url: '/openapi.json',
          dom_id: '#swagger-ui'
        });
      };
    </script>
  </body>
</html>
`

func (s *Server) handleSwaggerUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerUIHTML))
}

func (s *Server) handleOpenAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(openAPISpec())
}

func openAPISpec() map[string]any {
	return map[string]any{
		"openapi": "3.0.3",
		"info": map[string]any{
			"title":       "Ledger Payment API",
			"version":     "0.1.0",
			"description": "Async payment-oriented API for user deposits and withdrawals. Public callers provide only user/payment intent; the server derives the shard-local system account.",
		},
		"servers": []map[string]any{
			{"url": "/"},
		},
		"tags": []map[string]any{
			{"name": "System", "description": "Operational and documentation endpoints"},
			{"name": "Payments", "description": "Payment-oriented async write commands"},
			{"name": "Commands", "description": "Async command status endpoints"},
			{"name": "Accounts", "description": "Account balance queries"},
			{"name": "Transactions", "description": "Ledger transaction queries"},
		},
		"paths": map[string]any{
			"/healthz": map[string]any{
				"get": map[string]any{
					"summary": "Health check",
					"tags":    []string{"System"},
					"responses": map[string]any{
						"200": map[string]any{"description": "Service is healthy"},
					},
				},
			},
			"/readyz": map[string]any{
				"get": map[string]any{
					"summary": "Readiness check",
					"tags":    []string{"System"},
					"responses": map[string]any{
						"200": map[string]any{"description": "Service is ready"},
						"503": map[string]any{"description": "Database unavailable"},
					},
				},
			},
			"/commands/payments.withdrawals.create": map[string]any{
				"post": map[string]any{
					"summary":     "Create a withdrawal hold",
					"description": "Creates an async withdrawal hold command. The server derives the shard-local payout holding account from the provided user.",
					"tags":        []string{"Payments"},
					"requestBody": jsonRequestBodyRef("WithdrawalCreateRequest"),
					"responses":   commandResponses(),
				},
			},
			"/commands/payments.withdrawals.post": map[string]any{
				"post": map[string]any{
					"summary":     "Finalize a withdrawal hold as posted",
					"description": "Transitions a pending withdrawal transaction to posted after the external payment rail succeeds.",
					"tags":        []string{"Payments"},
					"requestBody": jsonRequestBodyRef("TransitionCommandRequest"),
					"responses":   commandResponses(),
				},
			},
			"/commands/payments.withdrawals.archive": map[string]any{
				"post": map[string]any{
					"summary":     "Archive a withdrawal hold",
					"description": "Transitions a pending withdrawal transaction to archived after the external payment rail fails or is canceled.",
					"tags":        []string{"Payments"},
					"requestBody": jsonRequestBodyRef("TransitionCommandRequest"),
					"responses":   commandResponses(),
				},
			},
			"/commands/payments.deposits.record": map[string]any{
				"post": map[string]any{
					"summary":     "Record a deposit",
					"description": "Creates an async deposit recording command. The server derives the shard-local cash-in clearing account from the provided user.",
					"tags":        []string{"Payments"},
					"requestBody": jsonRequestBodyRef("DepositRecordRequest"),
					"responses":   commandResponses(),
				},
			},
			"/commands/{id}": map[string]any{
				"get": map[string]any{
					"summary": "Get command status",
					"tags":    []string{"Commands"},
					"parameters": []map[string]any{
						pathParameter("id", "Command ID"),
					},
					"responses": map[string]any{
						"200": jsonResponseRef("CommandResponse", "Command status"),
						"404": jsonResponseRef("ErrorResponse", "Command not found"),
					},
				},
			},
			"/accounts/{id}/balances": map[string]any{
				"get": map[string]any{
					"summary": "Get current account balance",
					"tags":    []string{"Accounts"},
					"parameters": []map[string]any{
						pathParameter("id", "Account ID"),
					},
					"responses": map[string]any{
						"200": jsonResponseRef("AccountBalanceResponse", "Current account balance"),
						"404": jsonResponseRef("ErrorResponse", "Account not found"),
					},
				},
			},
			"/transactions/{id}": map[string]any{
				"get": map[string]any{
					"summary": "Get transaction details",
					"tags":    []string{"Transactions"},
					"parameters": []map[string]any{
						pathParameter("id", "Transaction ID"),
					},
					"responses": map[string]any{
						"200": jsonResponseRef("TransactionResponse", "Transaction detail"),
						"404": jsonResponseRef("ErrorResponse", "Transaction not found"),
					},
				},
			},
		},
		"components": map[string]any{
			"schemas": map[string]any{
				"WithdrawalCreateRequest": map[string]any{
					"type":     "object",
					"required": []string{"user_id", "idempotency_key", "transaction_id", "posting_key", "amount", "currency", "effective_at", "created_at"},
					"properties": map[string]any{
						"user_id":         stringSchema("User ID"),
						"idempotency_key": stringSchema("Idempotency key"),
						"transaction_id":  stringSchema("Transaction ID"),
						"posting_key":     stringSchema("Posting key"),
						"amount":          map[string]any{"type": "integer", "format": "int64", "description": "Minor units"},
						"currency":        stringSchema("Currency code"),
						"effective_at":    dateTimeSchema("Effective timestamp"),
						"created_at":      dateTimeSchema("Creation timestamp"),
					},
				},
				"DepositRecordRequest": map[string]any{
					"type":     "object",
					"required": []string{"user_id", "idempotency_key", "transaction_id", "posting_key", "amount", "currency", "effective_at", "created_at"},
					"properties": map[string]any{
						"user_id":         stringSchema("User ID"),
						"idempotency_key": stringSchema("Idempotency key"),
						"transaction_id":  stringSchema("Transaction ID"),
						"posting_key":     stringSchema("Posting key"),
						"amount":          map[string]any{"type": "integer", "format": "int64", "description": "Minor units"},
						"currency":        stringSchema("Currency code"),
						"effective_at":    dateTimeSchema("Effective timestamp"),
						"created_at":      dateTimeSchema("Creation timestamp"),
					},
				},
				"TransitionCommandRequest": map[string]any{
					"type":     "object",
					"required": []string{"user_id", "idempotency_key", "transaction_id"},
					"properties": map[string]any{
						"user_id":         stringSchema("User ID"),
						"idempotency_key": stringSchema("Idempotency key"),
						"transaction_id":  stringSchema("Transaction ID"),
					},
				},
				"CommandResponse": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"command_id":      stringSchema("Command ID"),
						"idempotency_key": stringSchema("Idempotency key"),
						"shard_id":        stringSchema("Owning shard"),
						"type":            stringSchema("Command type"),
						"status":          stringSchema("Command status"),
						"attempt_count":   map[string]any{"type": "integer"},
						"next_attempt_at": dateTimeSchema("Retry timestamp"),
						"result":          map[string]any{"type": "object", "additionalProperties": true},
						"error_code":      stringSchema("Error code"),
						"error_message":   stringSchema("Error message"),
						"created_at":      dateTimeSchema("Creation timestamp"),
						"updated_at":      dateTimeSchema("Update timestamp"),
					},
				},
				"AccountBalanceResponse": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"account_id":      stringSchema("Account ID"),
						"currency":        stringSchema("Currency code"),
						"normal_balance":  stringSchema("Normal balance direction"),
						"current_version": map[string]any{"type": "integer", "format": "int64"},
						"posted":          map[string]any{"type": "integer", "format": "int64"},
						"pending":         map[string]any{"type": "integer", "format": "int64"},
						"available":       map[string]any{"type": "integer", "format": "int64"},
						"updated_at":      dateTimeSchema("Last balance update"),
					},
				},
				"TransactionEntryResponse": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"entry_id":        stringSchema("Entry ID"),
						"transaction_id":  stringSchema("Transaction ID"),
						"account_id":      stringSchema("Account ID"),
						"account_version": map[string]any{"type": "integer", "format": "int64"},
						"amount":          map[string]any{"type": "integer", "format": "int64"},
						"currency":        stringSchema("Currency code"),
						"direction":       stringSchema("Entry direction"),
						"status":          stringSchema("Entry status"),
						"effective_at":    dateTimeSchema("Effective timestamp"),
						"created_at":      dateTimeSchema("Creation timestamp"),
						"discarded_at":    dateTimeSchema("Discard timestamp"),
					},
				},
				"TransactionResponse": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"transaction_id": stringSchema("Transaction ID"),
						"posting_key":    stringSchema("Posting key"),
						"type":           stringSchema("Transaction type"),
						"status":         stringSchema("Transaction status"),
						"effective_at":   dateTimeSchema("Effective timestamp"),
						"created_at":     dateTimeSchema("Creation timestamp"),
						"entries": map[string]any{
							"type":  "array",
							"items": map[string]any{"$ref": "#/components/schemas/TransactionEntryResponse"},
						},
					},
				},
				"ErrorResponse": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"error": stringSchema("Error message"),
					},
				},
			},
		},
	}
}

func jsonRequestBodyRef(schema string) map[string]any {
	return map[string]any{
		"required": true,
		"content": map[string]any{
			"application/json": map[string]any{
				"schema": map[string]any{"$ref": "#/components/schemas/" + schema},
			},
		},
	}
}

func commandResponses() map[string]any {
	return map[string]any{
		"202": jsonResponseRef("CommandResponse", "Command accepted"),
		"200": jsonResponseRef("CommandResponse", "Existing command returned for idempotent replay"),
		"400": jsonResponseRef("ErrorResponse", "Invalid request"),
		"500": jsonResponseRef("ErrorResponse", "Internal server error"),
	}
}

func jsonResponseRef(schema, description string) map[string]any {
	return map[string]any{
		"description": description,
		"content": map[string]any{
			"application/json": map[string]any{
				"schema": map[string]any{"$ref": "#/components/schemas/" + schema},
			},
		},
	}
}

func pathParameter(name, description string) map[string]any {
	return map[string]any{
		"name":        name,
		"in":          "path",
		"required":    true,
		"description": description,
		"schema": map[string]any{
			"type": "string",
		},
	}
}

func stringSchema(description string) map[string]any {
	return map[string]any{
		"type":        "string",
		"description": description,
	}
}

func dateTimeSchema(description string) map[string]any {
	return map[string]any{
		"type":        "string",
		"format":      "date-time",
		"description": description,
	}
}
