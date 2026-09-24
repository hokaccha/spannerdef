package spannerdef

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestColumnOnUpdateRoundTrip(t *testing.T) {
	column := "UpdatedAt TIMESTAMP DEFAULT (PENDING_COMMIT_TIMESTAMP()) ON UPDATE (PENDING_COMMIT_TIMESTAMP()) OPTIONS (allow_commit_timestamp = true)"
	desired := "CREATE TABLE T (Id INT64 NOT NULL, " + column + ") PRIMARY KEY (Id)"
	ddls, err := GenerateIdempotentDDLs(desired, "", GeneratorConfig{})
	require.NoError(t, err)
	require.Len(t, ddls, 1)
	require.Contains(t, ddls[0], column)
	diff, err := GenerateIdempotentDDLs(desired, ddls[0], GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, diff)
	ddls, err = GenerateIdempotentDDLs(desired, "CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id)", GeneratorConfig{})
	require.NoError(t, err)
	require.Equal(t, []string{"ALTER TABLE T ADD COLUMN " + column}, ddls)
}

func TestColumnOnUpdateChangesAreNotIgnored(t *testing.T) {
	current := "CREATE TABLE T (Id INT64 NOT NULL, UpdatedAt TIMESTAMP DEFAULT (PENDING_COMMIT_TIMESTAMP()) ON UPDATE (PENDING_COMMIT_TIMESTAMP()) OPTIONS (allow_commit_timestamp = true)) PRIMARY KEY (Id)"
	without := strings.Replace(current, " ON UPDATE (PENDING_COMMIT_TIMESTAMP())", "", 1)
	for _, pair := range [][2]string{
		{current, without}, {without, current},
		{current, strings.Replace(current, "UpdatedAt TIMESTAMP", "UpdatedAt INT64", 1)},
		{current, strings.Replace(current, " OPTIONS (allow_commit_timestamp = true)", "", 1)},
	} {
		ddls, err := GenerateIdempotentDDLs(pair[1], pair[0], GeneratorConfig{})
		require.ErrorContains(t, err, "unsupported ON UPDATE change for column T.UpdatedAt")
		require.Empty(t, ddls)
	}
}
