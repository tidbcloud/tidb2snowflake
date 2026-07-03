package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfirmChangefeedDeletionAcceptsExactID(t *testing.T) {
	var out bytes.Buffer

	err := confirmChangefeedDeletion(strings.NewReader("cf-1\n"), &out, "cf-1")

	require.NoError(t, err)
	require.Contains(t, out.String(), "Type the changefeed id to confirm")
	require.Contains(t, out.String(), "cf-1")
}

func TestConfirmChangefeedDeletionRejectsMismatch(t *testing.T) {
	var out bytes.Buffer

	err := confirmChangefeedDeletion(strings.NewReader("yes\n"), &out, "cf-1")

	require.Error(t, err)
	require.Contains(t, err.Error(), "delete confirmation failed")
	require.Contains(t, out.String(), "Type the changefeed id to confirm")
}
