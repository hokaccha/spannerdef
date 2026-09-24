package spannerdef

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestKeyDefinitionRoundTrip(t *testing.T) {
	table := "CREATE TABLE T (Id INT64 NOT NULL, N INT64) PRIMARY KEY (Id DESC);"
	for _, prefix := range []string{"CREATE INDEX", "CREATE UNIQUE INDEX", "CREATE NULL_FILTERED INDEX", "CREATE UNIQUE NULL_FILTERED INDEX"} {
		desired := table + prefix + " IX ON T (N DESC) STORING (Id)"
		ddls, err := GenerateIdempotentDDLs(desired, "", GeneratorConfig{})
		require.NoError(t, err)
		require.Len(t, ddls, 2)
		require.Contains(t, ddls[0], "PRIMARY KEY (Id DESC)")
		require.Equal(t, prefix+" IX ON T (N DESC) STORING (Id)", ddls[1])
		diff, err := GenerateIdempotentDDLs(desired, strings.Join(ddls, ";"), GeneratorConfig{})
		require.NoError(t, err)
		require.Empty(t, diff)
	}
}

func TestInlinePrimaryKeyNormalization(t *testing.T) {
	current := "CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id ASC)"
	for _, desired := range []string{
		"CREATE TABLE T (Id INT64 NOT NULL PRIMARY KEY)",
		"CREATE TABLE T (Id INT64 NOT NULL, PRIMARY KEY (Id))",
	} {
		ddls, err := GenerateIdempotentDDLs(desired, "", GeneratorConfig{})
		require.NoError(t, err)
		require.Len(t, ddls, 1)
		require.Contains(t, ddls[0], "PRIMARY KEY (Id)")
		diff, err := GenerateIdempotentDDLs(desired, current, GeneratorConfig{})
		require.NoError(t, err)
		require.Empty(t, diff)
	}
}

func TestKeyDefinitionChangesAreNotIgnored(t *testing.T) {
	table := "CREATE TABLE T (Id INT64 NOT NULL, N INT64) PRIMARY KEY (Id);"
	current := table + "CREATE INDEX IX ON T(N)"
	for _, desired := range []string{
		table + "CREATE INDEX IX ON T(N DESC)",
		table + "CREATE UNIQUE INDEX IX ON T(N)",
		table + "CREATE NULL_FILTERED INDEX IX ON T(N)",
		table + "CREATE INDEX IX ON T(N) STORING(Id)",
		strings.Replace(current, "PRIMARY KEY (Id)", "PRIMARY KEY (Id DESC)", 1),
	} {
		ddls, err := GenerateIdempotentDDLs(desired, current, GeneratorConfig{})
		require.ErrorContains(t, err, "unsupported")
		require.Empty(t, ddls)
	}
	ddls, err := GenerateIdempotentDDLs(table+"CREATE INDEX IX ON T(N ASC)", current, GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, ddls)
}

func TestStoringColumnOrderDoesNotChangeIndex(t *testing.T) {
	table := "CREATE TABLE T (Id INT64 NOT NULL, N INT64, A INT64, B INT64, C INT64) PRIMARY KEY(Id);"
	current := table + "CREATE INDEX IX ON T(N) STORING(A, B)"
	desired := table + "CREATE INDEX IX ON T(N) STORING(B, A)"
	ddls, err := GenerateIdempotentDDLs(desired, current, GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, ddls)
	ddls, err = GenerateIdempotentDDLs(desired, table, GeneratorConfig{})
	require.NoError(t, err)
	require.Equal(t, []string{"CREATE INDEX IX ON T (N) STORING (B, A)"}, ddls)
	for _, columns := range []string{"A", "B, C", "A, B, C"} {
		ddls, err = GenerateIdempotentDDLs(table+"CREATE INDEX IX ON T(N) STORING("+columns+")", current, GeneratorConfig{})
		require.ErrorContains(t, err, "unsupported definition change")
		require.Empty(t, ddls)
	}
}
