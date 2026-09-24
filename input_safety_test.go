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

func TestCheckedConfigRejectsUnrecognizedInput(t *testing.T) {
	for _, content := range []string{
		"skip_table: |\n  SchemaMigrations\n",
		"targetTables: Users\n",
		"skip_tables: Users\n---\nskip_tables: Other\n",
		"skip_tables: Users\n---\n",
		"skip_tables: [Users]\n",
		"skip_tables: Users\nskip_tables: Other\n",
	} {
		t.Run(content, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.yml")
			require.NoError(t, os.WriteFile(p, []byte(content), 0600))
			_, err := ParseGeneratorConfigChecked(p)
			require.Error(t, err)
		})
	}
}
func TestCheckedConfigPreservesValidConfiguration(t *testing.T) {
	for _, content := range []string{"", "# comment\n", "---\n", "skip_tables: |\n  SchemaMigrations\n  s.Other\n"} {
		p := filepath.Join(t.TempDir(), "config.yml")
		require.NoError(t, os.WriteFile(p, []byte(content), 0600))
		actual, err := ParseGeneratorConfigChecked(p)
		require.NoError(t, err)
		require.Equal(t, ParseGeneratorConfig(p), actual)
		if len(content) > 20 {
			require.Equal(t, []string{"SchemaMigrations", "s.Other"}, actual.SkipTables)
		}
	}
	_, err := ParseGeneratorConfigChecked(filepath.Join(t.TempDir(), "missing"))
	require.ErrorIs(t, err, os.ErrNotExist)
}
