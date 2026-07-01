package tidbcloud

import (
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

const redactedLogValue = "<redacted>"

var (
	urlPattern   = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s"'<>\\]+`)
	rawKVPattern = regexp.MustCompile(
		`(?i)(\b|["'])(authorization|proxy-authorization|www-authenticate|x-api-key|api[-_]?key|access[-_]?key|access[-_]?token|refresh[-_]?token|token|secret(?:[-_]?access[-_]?key)?|password|passwd|private[-_]?key|session(?:id)?|cookie|set-cookie|signature|credential)(["']?\s*[:=]\s*)(["'][^"'\r\n]*["']|[^\s,&;}\]]+)`,
	)
)

func sanitizeHTTPHeaders(headers http.Header) http.Header {
	out := make(http.Header, len(headers))
	for key, values := range headers {
		sanitizedValues := make([]string, 0, len(values))
		for _, value := range values {
			if isSensitiveKey(key) {
				sanitizedValues = append(sanitizedValues, redactedLogValue)
				continue
			}
			sanitizedValues = append(sanitizedValues, sanitizeLogText(value))
		}
		out[key] = sanitizedValues
	}
	return out
}

func sanitizeHTTPBody(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var value any
	if err := json.Unmarshal(body, &value); err == nil {
		sanitized, err := json.Marshal(sanitizeJSONValue(value))
		if err == nil {
			return string(sanitized)
		}
	}
	return sanitizeLogText(string(body))
}

func sanitizeJSONValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			if isSensitiveKey(key) {
				out[key] = redactedLogValue
				continue
			}
			out[key] = sanitizeJSONValue(item)
		}
		return out
	case []any:
		out := make([]any, 0, len(v))
		for _, item := range v {
			out = append(out, sanitizeJSONValue(item))
		}
		return out
	case string:
		return sanitizeLogText(v)
	default:
		return value
	}
}

func sanitizeLogText(text string) string {
	if text == "" {
		return ""
	}
	text = urlPattern.ReplaceAllStringFunc(text, sanitizeURLString)
	return rawKVPattern.ReplaceAllString(text, "${1}${2}${3}"+redactedLogValue)
}

func sanitizeURLString(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" {
		return rawKVPattern.ReplaceAllString(raw, "${1}${2}${3}"+redactedLogValue)
	}
	if parsed.User != nil {
		parsed.User = url.User(redactedLogValue)
	}
	query := parsed.Query()
	for key, values := range query {
		for i, value := range values {
			if isSensitiveKey(key) {
				values[i] = redactedLogValue
				continue
			}
			values[i] = sanitizeLogText(value)
		}
		query[key] = values
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func isSensitiveKey(key string) bool {
	normalized := normalizeSecretKey(key)
	if normalized == "" {
		return false
	}
	for _, token := range []string{
		"authorization",
		"wwwauthenticate",
		"cookie",
		"token",
		"apikey",
		"accesskey",
		"secret",
		"password",
		"passwd",
		"privatekey",
		"credential",
		"signature",
		"session",
	} {
		if strings.Contains(normalized, token) {
			return true
		}
	}
	return false
}

func normalizeSecretKey(key string) string {
	var builder strings.Builder
	for _, r := range strings.ToLower(key) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}
