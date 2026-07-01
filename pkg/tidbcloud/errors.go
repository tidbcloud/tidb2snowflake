package tidbcloud

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// APIError is a non-2xx response from the TiDB Cloud OpenAPI. The body follows
// the grpc-gateway error shape: {"code", "message", "details"}.
type APIError struct {
	// StatusCode is the HTTP status code.
	StatusCode int `json:"-"`
	// Code is the gRPC status code returned in the body, when present.
	Code int `json:"code"`
	// Message is the human-readable error message.
	Message string `json:"message"`
	// Details carries the raw error details, when present.
	Details []json.RawMessage `json:"details,omitempty"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("tidbcloud: API error (http %d, code %d): %s", e.StatusCode, e.Code, e.Message)
}

func parseAPIError(status int, body []byte) *APIError {
	e := &APIError{StatusCode: status}
	if len(body) > 0 {
		// Best-effort decode; fall back to the raw body as the message.
		_ = json.Unmarshal([]byte(sanitizeHTTPBody(body)), e)
	}
	if e.Message == "" {
		e.Message = sanitizeLogText(strings.TrimSpace(string(body)))
		if e.Message == "" {
			e.Message = http.StatusText(status)
		}
	}
	e.Message = sanitizeLogText(e.Message)
	return e
}
