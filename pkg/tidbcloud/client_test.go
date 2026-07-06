package tidbcloud

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// newTestClient builds a Client pointed at the given test server URL.
func newTestClient(t *testing.T, serverURL string, opts ...Option) *Client {
	t.Helper()
	c, err := NewClient("public-key", "private-key", opts...)
	require.NoError(t, err)
	c.baseURL = serverURL
	return c
}

func TestNewClientRequiresKeys(t *testing.T) {
	_, err := NewClient("", "priv")
	require.Error(t, err)
	_, err = NewClient("pub", "")
	require.Error(t, err)

	c, err := NewClient("pub", "priv")
	require.NoError(t, err)
	require.Equal(t, "https://"+DefaultHost, c.baseURL)
}

func TestWithHost(t *testing.T) {
	c, err := NewClient("pub", "priv", WithHost("eu-central-1.serverless.tidbapi.com"))
	require.NoError(t, err)
	require.Equal(t, "https://eu-central-1.serverless.tidbapi.com", c.baseURL)
}

// TestDigestAuth verifies the client answers an HTTP Digest challenge: the first
// request arrives without credentials and gets a 401 + WWW-Authenticate, and the
// retried request carries a Digest Authorization header with the public key.
func TestDigestAuth(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		auth := r.Header.Get("Authorization")
		if auth == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", qop="auth", nonce="abc123", opaque="xyz"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		require.True(t, strings.HasPrefix(auth, "Digest "), "expected Digest auth, got %q", auth)
		require.Contains(t, auth, `username="public-key"`)
		_ = n
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"exportId":"exp-1","state":"RUNNING"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	exp, err := c.GetExport(context.Background(), "10", "exp-1")
	require.NoError(t, err)
	require.Equal(t, "exp-1", exp.ExportID)
	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(2), "digest should require a challenge round-trip")
}

func TestDoAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":3,"message":"invalid snapshot_tso"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	_, err := c.GetExport(context.Background(), "10", "exp-1")
	require.Error(t, err)
	apiErr, ok := err.(*APIError)
	require.True(t, ok, "expected *APIError, got %T", err)
	require.Equal(t, http.StatusBadRequest, apiErr.StatusCode)
	require.Equal(t, 3, apiErr.Code)
	require.Contains(t, apiErr.Message, "invalid snapshot_tso")
}

func TestDoLogsSanitizedAPIErrorContext(t *testing.T) {
	logs := captureTiDBCloudLogs(t)
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		req.Header.Set("Authorization", "Bearer request-header-secret")
		req.Header.Set("X-Api-Key", "request-api-key-secret")
		body := `{"code":3,"message":"password=response-password-secret token=response-token-secret url=https://user:response-url-password-secret@example.com/path?api_key=response-query-secret"}`
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Status:     "400 Bad Request",
			Header: http.Header{
				"Set-Cookie":    []string{"session=response-cookie-secret"},
				"X-Api-Key":     []string{"response-api-key-secret"},
				"X-Request-Id":  []string{"request-id-1"},
				"Content-Type":  []string{"application/json"},
				"Cache-Control": []string{"no-store"},
			},
			Body:    io.NopCloser(strings.NewReader(body)),
			Request: req,
		}, nil
	})

	c := newRawHTTPTestClient("https://api.example.test", rt)
	var out Changefeed
	err := c.do(context.Background(), http.MethodPost, "/v1beta1/clusters/10/changefeeds?token=url-token-secret&safe=ok", map[string]any{
		"secret": "json-body-secret",
		"nested": map[string]any{
			"password": "json-password-secret",
		},
		"uri": "s3://user:uri-password-secret@bucket/path?secret_access_key=uri-query-secret&safe=ok",
		"raw": "password=raw-password-secret token: raw-token-secret",
	}, &out)
	require.Error(t, err)

	logText := observedLogContext(t, logs, "TiDB Cloud request failed")
	require.Contains(t, logText, "POST")
	require.Contains(t, logText, "requestHeaders")
	require.Contains(t, logText, "responseHeaders")
	require.Contains(t, logText, "requestBody")
	require.Contains(t, logText, "responseBody")
	require.Contains(t, logText, "Authorization")
	require.Contains(t, logText, "Set-Cookie")
	require.Contains(t, logText, "X-Request-Id")
	require.Contains(t, logText, "request-id-1")
	require.Contains(t, logText, "<redacted>")
	assertNoSecrets(t, logText,
		"request-header-secret",
		"request-api-key-secret",
		"response-password-secret",
		"response-token-secret",
		"response-url-password-secret",
		"response-query-secret",
		"response-cookie-secret",
		"response-api-key-secret",
		"url-token-secret",
		"json-body-secret",
		"json-password-secret",
		"uri-password-secret",
		"uri-query-secret",
		"raw-password-secret",
		"raw-token-secret",
	)
	assertNoSecrets(t, err.Error(), "response-password-secret", "response-token-secret", "response-query-secret")
}

func TestDoLogsSanitizedDecodeFailureContext(t *testing.T) {
	logs := captureTiDBCloudLogs(t)
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		req.Header.Set("Authorization", "Bearer decode-request-secret")
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header: http.Header{
				"X-Api-Token":  []string{"decode-response-header-secret"},
				"Content-Type": []string{"application/json"},
			},
			Body:    io.NopCloser(strings.NewReader(`{"token":"decode-response-body-secret"`)),
			Request: req,
		}, nil
	})

	c := newRawHTTPTestClient("https://api.example.test", rt)
	var out Export
	err := c.do(context.Background(), http.MethodPost, "/v1beta1/clusters/10/exports?private_key=decode-url-secret", map[string]any{
		"privateKey": "decode-request-body-secret",
	}, &out)
	require.Error(t, err)

	logText := observedLogContext(t, logs, "TiDB Cloud response decode failed")
	require.Contains(t, logText, "POST")
	require.Contains(t, logText, "requestHeaders")
	require.Contains(t, logText, "responseHeaders")
	require.Contains(t, logText, "requestBody")
	require.Contains(t, logText, "responseBody")
	require.Contains(t, logText, "<redacted>")
	assertNoSecrets(t, logText,
		"decode-request-secret",
		"decode-response-header-secret",
		"decode-response-body-secret",
		"decode-url-secret",
		"decode-request-body-secret",
	)
}

func TestDoRetriesOn503ThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"exportId":"exp-1","state":"SUCCEEDED"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL, WithMaxRetries(2))
	exp, err := c.GetExport(context.Background(), "10", "exp-1")
	require.NoError(t, err)
	require.Equal(t, ExportStateSucceeded, exp.State)
	require.Equal(t, int32(2), atomic.LoadInt32(&calls))
}

func TestDoRetriesUnauthorizedFiveTimesByDefaultThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) <= 5 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"exportId":"exp-1","state":"SUCCEEDED"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	exp, err := c.GetExport(context.Background(), "10", "exp-1")
	require.NoError(t, err)
	require.Equal(t, ExportStateSucceeded, exp.State)
	require.Equal(t, int32(6), atomic.LoadInt32(&calls))
}

func TestDoStopsAfterFiveBadGatewayRetriesByDefault(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("bad gateway"))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	_, err := c.GetExport(context.Background(), "10", "exp-1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "http 502")
	require.Equal(t, int32(6), atomic.LoadInt32(&calls))
}

func TestDoDoesNotRetryOn400(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":3,"message":"bad"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL, WithMaxRetries(3))
	_, err := c.GetExport(context.Background(), "10", "exp-1")
	require.Error(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&calls), "4xx must not be retried")
}

func TestDoContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	c := newTestClient(t, srv.URL, WithMaxRetries(100))
	_, err := c.GetExport(ctx, "10", "exp-1")
	require.Error(t, err)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func captureTiDBCloudLogs(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.ErrorLevel)
	restore := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{
		Core:  core,
		Level: zap.NewAtomicLevelAt(zapcore.ErrorLevel),
	})
	t.Cleanup(restore)
	return logs
}

func newRawHTTPTestClient(baseURL string, rt http.RoundTripper) *Client {
	return &Client{
		baseURL:    baseURL,
		maxRetries: 0,
		httpClient: &http.Client{Transport: rt},
	}
}

func observedLogContext(t *testing.T, logs *observer.ObservedLogs, message string) string {
	t.Helper()
	entries := logs.FilterMessage(message).All()
	require.Len(t, entries, 1)
	return fmt.Sprint(entries[0].ContextMap())
}

func assertNoSecrets(t *testing.T, text string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		require.NotContains(t, text, secret)
	}
}
