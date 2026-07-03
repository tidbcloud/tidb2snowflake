package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRootCommandExposesCreateAndDelete(t *testing.T) {
	root := newRootCmd()

	var names []string
	for _, cmd := range root.Commands() {
		names = append(names, cmd.Name())
	}
	require.Contains(t, names, "create")
	require.Contains(t, names, "delete")
	require.Contains(t, names, "version")
	require.NotContains(t, names, "snowflake")
}
