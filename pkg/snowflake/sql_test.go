package snowflake

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStagePatternFromGlob(t *testing.T) {
	require.Equal(t, `.*db\.tbl\..*\.csv`, stagePatternFromGlob("db.tbl.*.csv"))
	require.Equal(t, `.*db\.table\$name\..*\.csv`, stagePatternFromGlob("db.table$name.*.csv"))
}

func TestSnapshotFileFormatCompression(t *testing.T) {
	require.Contains(t, snapshotFileFormat("none"), "COMPRESSION = 'NONE'")
	require.Contains(t, snapshotFileFormat("gzip"), "COMPRESSION = 'GZIP'")
	require.Contains(t, snapshotFileFormat("gzip"), `ESCAPE='\\'`)
}
