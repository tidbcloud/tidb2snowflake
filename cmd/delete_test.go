package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfirmChangefeedDeletionAcceptsYes(t *testing.T) {
	var out bytes.Buffer

	confirmed, err := confirmChangefeedDeletion(strings.NewReader("y\n"), &out)

	require.NoError(t, err)
	require.True(t, confirmed)
	require.Contains(t, out.String(), "Delete the changefeed associated with this task?")
	require.Contains(t, out.String(), "[y/N]")
	require.NotContains(t, out.String(), "cf-1")
}

func TestConfirmChangefeedDeletionCancelsNo(t *testing.T) {
	var out bytes.Buffer

	confirmed, err := confirmChangefeedDeletion(strings.NewReader("n\n"), &out)

	require.NoError(t, err)
	require.False(t, confirmed)
	require.Contains(t, out.String(), "Delete the changefeed associated with this task?")
	require.Contains(t, out.String(), "Delete canceled.")
}

func TestConfirmChangefeedDeletionCancelsEmptyInput(t *testing.T) {
	var out bytes.Buffer

	confirmed, err := confirmChangefeedDeletion(strings.NewReader("\n"), &out)

	require.NoError(t, err)
	require.False(t, confirmed)
	require.Contains(t, out.String(), "Delete the changefeed associated with this task?")
	require.Contains(t, out.String(), "Delete canceled.")
}

func TestConfirmChangefeedDeletionCancelsEOF(t *testing.T) {
	var out bytes.Buffer

	confirmed, err := confirmChangefeedDeletion(strings.NewReader(""), &out)

	require.NoError(t, err)
	require.False(t, confirmed)
	require.Contains(t, out.String(), "Delete the changefeed associated with this task?")
	require.Contains(t, out.String(), "Delete canceled.")
}
