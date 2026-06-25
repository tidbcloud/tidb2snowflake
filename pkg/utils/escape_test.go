package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEscapeString(t *testing.T) {
	require.Equal(t, `a\'b\\c\"d\n`, EscapeString("a'b\\c\"d\n"))
}
