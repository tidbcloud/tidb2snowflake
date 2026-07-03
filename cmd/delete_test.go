package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfirmChangefeedDeletionAcceptsYes(t *testing.T) {
	var out bytes.Buffer

	err := confirmChangefeedDeletion(strings.NewReader("y\n"), &out)

	require.NoError(t, err)
	require.Contains(t, out.String(), "Delete the changefeed recorded in state?")
	require.Contains(t, out.String(), "[y/N]")
	require.NotContains(t, out.String(), "cf-1")
}

func TestConfirmChangefeedDeletionRejectsNo(t *testing.T) {
	var out bytes.Buffer

	err := confirmChangefeedDeletion(strings.NewReader("n\n"), &out)

	require.Error(t, err)
	require.Contains(t, err.Error(), "delete canceled")
	require.Contains(t, out.String(), "Delete the changefeed recorded in state?")
}

func TestConfirmChangefeedDeletionRejectsEmptyInput(t *testing.T) {
	var out bytes.Buffer

	err := confirmChangefeedDeletion(strings.NewReader("\n"), &out)

	require.Error(t, err)
	require.Contains(t, err.Error(), "delete canceled")
	require.Contains(t, out.String(), "Delete the changefeed recorded in state?")
}
