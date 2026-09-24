package spannerdef

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestReadFilesPreservesSQLLineBoundaries(t *testing.T) {
	tableA := "CREATE TABLE A(Id INT64 NOT NULL) PRIMARY KEY(Id);"
	tableB := "CREATE TABLE B(Id INT64 NOT NULL) PRIMARY KEY(Id);"
	for _, ending := range []string{"", " -- final comment", " -- final comment\n", "\r\n"} {
		t.Run(ending, func(t *testing.T) {
			a, b := filepath.Join(t.TempDir(), "a.sql"), filepath.Join(t.TempDir(), "b.sql")
			require.NoError(t, os.WriteFile(a, []byte(tableA+ending), 0600))
			require.NoError(t, os.WriteFile(b, []byte(tableB), 0600))
			single, err := ReadFiles([]string{a})
			require.NoError(t, err)
			require.Equal(t, tableA+ending, single)
			desired, err := ReadFiles([]string{a, b})
			require.NoError(t, err)
			plan, err := GenerateIdempotentDDLs(desired, tableA+tableB, GeneratorConfig{})
			require.NoError(t, err)
			require.Empty(t, plan)
		})
	}
}
