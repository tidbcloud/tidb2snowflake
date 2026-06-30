// Package tidbcloud is a small client for the TiDB Cloud Serverless OpenAPI
// (v1beta1). It covers the Export and Changefeed services that tidb2snowflake
// uses to drive a snapshot export and an incremental changefeed for a cluster.
//
// Authentication uses an API key (public/private) over HTTP Digest, matching
// the TiDB Cloud Serverless OpenAPI.
package tidbcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/icholy/digest"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	// DefaultHost is the global host for the TiDB Cloud Serverless OpenAPI.
	DefaultHost = "serverless.tidbapi.com"

	defaultTimeout    = 30 * time.Second
	defaultMaxRetries = 3
	maxBackoff        = 5 * time.Second
)

var sensitiveRawValueRE = regexp.MustCompile(`(?i)((?:"?(?:authorization|cookie|set-cookie|password|private[-_.]?key|public[-_.]?key|api[-_.]?key|secret|secret[-_.]?access[-_.]?key|access[-_.]?key[-_.]?secret|token|access[-_.]?token|refresh[-_.]?token|session[-_.]?token|credential|credentials|signature|x[-_.]?amz[-_.]?signature)"?)\s*[:=]\s*)(")?[^",&}\s]+(")?`)

// Client talks to the TiDB Cloud Serverless OpenAPI using HTTP Digest
// authentication with an API key (public/private).
type Client struct {
	baseURL    string
	publicKey  string
	privateKey string
	timeout    time.Duration
	maxRetries int

	baseTransport http.RoundTripper
	httpClient    *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithHost overrides the API host (default DefaultHost). The scheme is always
// https.
func WithHost(host string) Option {
	return func(c *Client) {
		if host != "" {
			c.baseURL = "https://" + host
		}
	}
}

// WithTimeout sets the per-request timeout (default 30s).
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithMaxRetries sets how many times retriable failures (network errors and
// 429/5xx responses) are retried (default 3).
func WithMaxRetries(n int) Option {
	return func(c *Client) {
		if n >= 0 {
			c.maxRetries = n
		}
	}
}

// WithBaseTransport sets the underlying RoundTripper that the digest auth
// transport wraps. Useful for tests and custom proxies.
func WithBaseTransport(rt http.RoundTripper) Option {
	return func(c *Client) {
		c.baseTransport = rt
	}
}

// NewClient creates a Client authenticated with the given public/private API key.
func NewClient(publicKey, privateKey string, opts ...Option) (*Client, error) {
	if publicKey == "" || privateKey == "" {
		return nil, errors.New("tidbcloud: public and private API key are required")
	}
	c := &Client{
		baseURL:    "https://" + DefaultHost,
		publicKey:  publicKey,
		privateKey: privateKey,
		timeout:    defaultTimeout,
		maxRetries: defaultMaxRetries,
	}
	for _, o := range opts {
		o(c)
	}
	base := c.baseTransport
	if base == nil {
		base = http.DefaultTransport
	}
	c.httpClient = &http.Client{
		Timeout: c.timeout,
		Transport: &digest.Transport{
			Username:  c.publicKey,
			Password:  c.privateKey,
			Transport: base,
		},
	}
	return c, nil
}

// do executes an HTTP request against the API, JSON-encoding body (if non-nil)
// and JSON-decoding the success response into out (if non-nil). It retries
// network errors and retriable status codes with exponential backoff.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var reqBody []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("tidbcloud: marshal request body: %w", err)
		}
		reqBody = b
	}

	url := c.baseURL + path
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff(attempt)):
			}
		}

		var reader io.Reader
		if reqBody != nil {
			reader = bytes.NewReader(reqBody)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, reader)
		if err != nil {
			return fmt.Errorf("tidbcloud: build request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		if reqBody != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Context cancellation is not retriable.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = fmt.Errorf("tidbcloud: %s %s: %w", method, path, err)
			if attempt == c.maxRetries {
				logTiDBCloudRequestFailure(method, path, req.Header, reqBody, 0, nil, nil, lastErr)
			}
			continue
		}

		respBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("tidbcloud: read response body: %w", readErr)
			if attempt == c.maxRetries {
				logTiDBCloudRequestFailure(method, path, req.Header, reqBody, resp.StatusCode, resp.Header, nil, lastErr)
			}
			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if out != nil && len(respBody) > 0 {
				if err := json.Unmarshal(respBody, out); err != nil {
					decodeErr := fmt.Errorf("tidbcloud: decode response: %w", err)
					logTiDBCloudRequestFailure(method, path, req.Header, reqBody, resp.StatusCode, resp.Header, respBody, decodeErr)
					return decodeErr
				}
			}
			return nil
		}

		apiErr := parseAPIError(resp.StatusCode, respBody)
		if isRetriableStatus(resp.StatusCode) && attempt < c.maxRetries {
			lastErr = apiErr
			continue
		}
		logTiDBCloudRequestFailure(method, path, req.Header, reqBody, resp.StatusCode, resp.Header, respBody, apiErr)
		return apiErr
	}
	return lastErr
}

func logTiDBCloudRequestFailure(method, path string, reqHeaders http.Header, reqBody []byte, status int, respHeaders http.Header, respBody []byte, err error) {
	fields := []zap.Field{
		zap.String("method", method),
		zap.String("path", path),
		zap.String("requestHeaders", sanitizeLogHeaders(reqHeaders)),
		zap.String("requestBody", sanitizeLogBody(reqBody)),
		zap.String("error", sanitizeLogError(err)),
	}
	if status > 0 {
		fields = append(fields, zap.Int("responseStatus", status))
	}
	if respHeaders != nil {
		fields = append(fields, zap.String("responseHeaders", sanitizeLogHeaders(respHeaders)))
	}
	if respBody != nil {
		fields = append(fields, zap.String("responseBody", sanitizeLogBody(respBody)))
	}
	log.Error("tidbcloud request failed", fields...)
}

func sanitizeLogBody(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var v any
	if err := json.Unmarshal(body, &v); err == nil {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(sanitizeLogJSONValue(v)); err == nil {
			return strings.TrimSuffix(buf.String(), "\n")
		}
	}
	return redactSensitiveString(strings.TrimSpace(string(body)))
}

func sanitizeLogHeaders(headers http.Header) string {
	if len(headers) == 0 {
		return ""
	}
	sanitized := make(map[string][]string, len(headers))
	for key, values := range headers {
		if isSensitiveLogKey(key) {
			sanitized[key] = []string{"<redacted>"}
			continue
		}
		sanitizedValues := make([]string, len(values))
		for i, value := range values {
			sanitizedValues[i] = redactSensitiveString(value)
		}
		sanitized[key] = sanitizedValues
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(sanitized); err != nil {
		return ""
	}
	return strings.TrimSuffix(buf.String(), "\n")
}

func sanitizeLogError(err error) string {
	if err == nil {
		return ""
	}
	return redactSensitiveString(err.Error())
}

func sanitizeLogJSONValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, value := range x {
			switch {
			case isSensitiveLogKey(k):
				out[k] = "<redacted>"
			case isAccessKeyLogKey(k):
				out[k] = sanitizeAccessKeyLogValue(value)
			default:
				out[k] = sanitizeLogJSONValue(value)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, value := range x {
			out[i] = sanitizeLogJSONValue(value)
		}
		return out
	case string:
		return redactSensitiveString(x)
	default:
		return x
	}
}

func sanitizeAccessKeyLogValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, value := range x {
			if isAccessKeyMaterialLogKey(k) || isSensitiveLogKey(k) {
				out[k] = "<redacted>"
				continue
			}
			out[k] = sanitizeLogJSONValue(value)
		}
		return out
	default:
		return "<redacted>"
	}
}

func isAccessKeyLogKey(key string) bool {
	return normalizedLogKey(key) == "accesskey"
}

func isAccessKeyMaterialLogKey(key string) bool {
	switch normalizedLogKey(key) {
	case "id", "accesskeyid", "secret", "secretaccesskey", "accesskeysecret":
		return true
	default:
		return false
	}
}

func isSensitiveLogKey(key string) bool {
	normalized := normalizedLogKey(key)
	switch normalized {
	case "authorization", "cookie", "setcookie",
		"password", "privatekey", "publickey", "apikey",
		"secret", "secretaccesskey", "accesskeysecret",
		"token", "accesstoken", "refreshtoken", "sessiontoken",
		"credential", "credentials", "signature", "xamzsignature":
		return true
	default:
		return strings.Contains(normalized, "secret") ||
			strings.Contains(normalized, "password") ||
			strings.Contains(normalized, "token") ||
			strings.Contains(normalized, "credential") ||
			strings.Contains(normalized, "signature")
	}
}

func normalizedLogKey(key string) string {
	replacer := strings.NewReplacer("_", "", "-", "", ".", "")
	return strings.ToLower(replacer.Replace(key))
}

func redactSensitiveString(s string) string {
	u, err := url.Parse(s)
	if err == nil && u.Scheme != "" && u.RawQuery != "" {
		values := u.Query()
		changed := false
		for key := range values {
			if isSensitiveLogKey(key) || isAccessKeyMaterialLogKey(key) {
				values[key] = []string{"<redacted>"}
				changed = true
			}
		}
		if changed {
			u.RawQuery = values.Encode()
			s = u.String()
		}
	}
	return sensitiveRawValueRE.ReplaceAllString(s, `${1}${2}<redacted>${2}`)
}

func backoff(attempt int) time.Duration {
	d := time.Duration(200*(1<<(attempt-1))) * time.Millisecond
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

func isRetriableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}
