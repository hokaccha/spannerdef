package spannerdef

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestColumnAttributeChanges(t *testing.T) {
	schema := func(attr string) string { return "CREATE TABLE T (Id INT64 NOT NULL, N " + attr + ") PRIMARY KEY (Id)" }
	for _, tc := range []struct{ old, new, want string }{
		{"INT64", "INT64 DEFAULT (1)", "SET DEFAULT (1)"},
		{"INT64 DEFAULT (1)", "INT64 DEFAULT (2)", "SET DEFAULT (2)"},
		{"INT64 DEFAULT (1)", "INT64", "DROP DEFAULT"},
		{"INT64 DEFAULT (1)", "INT64 NOT NULL DEFAULT (1)", "INT64 NOT NULL DEFAULT (1)"},
		{"INT64 NOT NULL DEFAULT (1)", "INT64 DEFAULT (2)", "INT64 DEFAULT (2)"},
		{"INT64 NOT NULL DEFAULT (1)", "INT64", "INT64"},
		{"STRING(10) DEFAULT ('x')", "STRING(20) DEFAULT ('x')", `STRING(20) DEFAULT ("x")`},
	} {
		t.Run(tc.new, func(t *testing.T) {
			ddls, err := GenerateIdempotentDDLs(schema(tc.new), schema(tc.old), GeneratorConfig{})
			require.NoError(t, err)
			require.Equal(t, []string{"ALTER TABLE T ALTER COLUMN N " + tc.want}, ddls)
		})
	}
}
func TestOmniColumnAttributeChanges(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	for _, attr := range []string{"STRING(10)", "STRING(10) DEFAULT ('x')", "STRING(10) NOT NULL DEFAULT ('x')", "STRING(20) NOT NULL DEFAULT ('y')", "STRING(20)", "STRING(20) DEFAULT ('z')", "STRING(20)"} {
		ddl := "CREATE TABLE T (Id INT64 NOT NULL, N " + attr + ") PRIMARY KEY (Id)"
		require.NotEmpty(t, applySchema(t, db, ddl, false))
		require.Empty(t, applySchema(t, db, ddl, false))
	}
}

func TestCommitTimestampDefaultOptionOrdering(t *testing.T) {
	plain := `CREATE TABLE T(Id INT64, N TIMESTAMP) PRIMARY KEY(Id)`
	enabled := `CREATE TABLE T(Id INT64, N TIMESTAMP DEFAULT(PENDING_COMMIT_TIMESTAMP()) OPTIONS(allow_commit_timestamp=true)) PRIMARY KEY(Id)`
	ddls, err := GenerateIdempotentDDLs(enabled, plain, GeneratorConfig{})
	require.NoError(t, err)
	require.Equal(t, []string{"ALTER TABLE T ALTER COLUMN N SET OPTIONS (allow_commit_timestamp = true)", "ALTER TABLE T ALTER COLUMN N SET DEFAULT (PENDING_COMMIT_TIMESTAMP())"}, ddls)
	ddls, err = GenerateIdempotentDDLs(plain, enabled, GeneratorConfig{})
	require.NoError(t, err)
	require.Equal(t, []string{"ALTER TABLE T ALTER COLUMN N DROP DEFAULT", "ALTER TABLE T ALTER COLUMN N SET OPTIONS (allow_commit_timestamp = null)"}, ddls)
}
func TestOmniCommitTimestampDefaultChanges(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	for _, attr := range []string{"TIMESTAMP", "TIMESTAMP OPTIONS(allow_commit_timestamp=true)", "TIMESTAMP DEFAULT(CURRENT_TIMESTAMP())", "TIMESTAMP DEFAULT((PENDING_COMMIT_TIMESTAMP())) OPTIONS(allow_commit_timestamp=true)", "TIMESTAMP DEFAULT(CURRENT_TIMESTAMP())", "TIMESTAMP DEFAULT(PENDING_COMMIT_TIMESTAMP()) OPTIONS(allow_commit_timestamp=true)", "TIMESTAMP"} {
		ddl := "CREATE TABLE T(Id INT64, N " + attr + ") PRIMARY KEY(Id)"
		require.NotEmpty(t, applySchema(t, db, ddl, false))
		require.Empty(t, applySchema(t, db, ddl, false))
	}
}

func TestSwitchCommitTimestampDefaultModes(t *testing.T) {
	normal := `CREATE TABLE T(Id INT64, N TIMESTAMP DEFAULT(CURRENT_TIMESTAMP())) PRIMARY KEY(Id)`
	pending := `CREATE TABLE T(Id INT64, N TIMESTAMP DEFAULT(PENDING_COMMIT_TIMESTAMP()) OPTIONS(allow_commit_timestamp=true)) PRIMARY KEY(Id)`
	for _, tc := range []struct{ old, next, option, expr string }{
		{normal, pending, "true", "PENDING_COMMIT_TIMESTAMP()"},
		{pending, normal, "null", "CURRENT_TIMESTAMP()"},
	} {
		ddls, err := GenerateIdempotentDDLs(tc.next, tc.old, GeneratorConfig{})
		require.NoError(t, err)
		require.Equal(t, []string{"ALTER TABLE T ALTER COLUMN N DROP DEFAULT", "ALTER TABLE T ALTER COLUMN N SET OPTIONS (allow_commit_timestamp = " + tc.option + ")", "ALTER TABLE T ALTER COLUMN N SET DEFAULT (" + tc.expr + ")"}, ddls)
	}
}

func TestDefaultNormalizationAndOptionTransitions(t *testing.T) {
	schema := func(attr string) string { return "CREATE TABLE T(Id INT64, N " + attr + ") PRIMARY KEY(Id)" }
	for _, tc := range []struct {
		old, next string
		want      []string
	}{
		{"TIMESTAMP OPTIONS(allow_commit_timestamp=true)", "TIMESTAMP DEFAULT(CURRENT_TIMESTAMP())", []string{"ALTER TABLE T ALTER COLUMN N SET OPTIONS (allow_commit_timestamp = null)", "ALTER TABLE T ALTER COLUMN N SET DEFAULT (CURRENT_TIMESTAMP())"}},
		{"TIMESTAMP", "TIMESTAMP DEFAULT((PENDING_COMMIT_TIMESTAMP())) OPTIONS(allow_commit_timestamp=true)", []string{"ALTER TABLE T ALTER COLUMN N SET OPTIONS (allow_commit_timestamp = true)", "ALTER TABLE T ALTER COLUMN N SET DEFAULT ((PENDING_COMMIT_TIMESTAMP()))"}},
		{"TIMESTAMP DEFAULT(CURRENT_TIMESTAMP())", "TIMESTAMP DEFAULT((current_timestamp()))", nil},
	} {
		ddls, err := GenerateIdempotentDDLs(schema(tc.next), schema(tc.old), GeneratorConfig{})
		require.NoError(t, err)
		if tc.want == nil {
			require.Empty(t, ddls)
		} else {
			require.Equal(t, tc.want, ddls)
		}
	}
}
