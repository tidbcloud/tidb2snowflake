package httpclient

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProxyFromEnvironmentUsesLowercaseHTTPAndHTTPSProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "")
	t.Setenv("HTTPS_PROXY", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	t.Setenv("http_proxy", "http://http-proxy.example:8080")
	t.Setenv("https_proxy", "http://https-proxy.example:8443")

	httpProxy, err := ProxyFromEnvironment(newProxyTestRequest(t, "http://api.example.com"))
	require.NoError(t, err)
	require.Equal(t, "http://http-proxy.example:8080", httpProxy.String())

	httpsProxy, err := ProxyFromEnvironment(newProxyTestRequest(t, "https://api.example.com"))
	require.NoError(t, err)
	require.Equal(t, "http://https-proxy.example:8443", httpsProxy.String())
}

func newProxyTestRequest(t *testing.T, rawURL string) *http.Request {
	t.Helper()
	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	return &http.Request{URL: u}
}
