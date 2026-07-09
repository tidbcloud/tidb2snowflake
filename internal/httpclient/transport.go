package httpclient

import (
	"net/http"
	"net/url"

	"golang.org/x/net/http/httpproxy"
)

func NewEnvironmentProxyTransport() *http.Transport {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &http.Transport{Proxy: ProxyFromEnvironment}
	}

	transport := base.Clone()
	transport.Proxy = ProxyFromEnvironment
	return transport
}

func ProxyFromEnvironment(req *http.Request) (*url.URL, error) {
	return httpproxy.FromEnvironment().ProxyFunc()(req.URL)
}
