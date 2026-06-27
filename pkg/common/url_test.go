package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestURLForLog(t *testing.T) {
	require.Equal(t,
		"s3://bucket/path",
		RedactURLRawQuery("s3://bucket/path?access-key=AKIA&secret-access-key=secret"))
	require.Equal(t, "s3://bucket/path", RedactURLRawQuery("s3://bucket/path"))
}
