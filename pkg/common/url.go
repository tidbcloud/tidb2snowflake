package common

import "net/url"

func RedactURLRawQuery(raw string) string {
	uri, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	uri.RawQuery = ""
	return uri.String()
}
