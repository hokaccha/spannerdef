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

func TestOnUpdateFunctionCaseDoesNotBlockOtherChanges(t *testing.T) {
	current := "CREATE TABLE T (Id INT64 NOT NULL, U TIMESTAMP DEFAULT (PENDING_COMMIT_TIMESTAMP()) ON UPDATE (PENDING_COMMIT_TIMESTAMP()) OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY(Id)"
	for _, desired := range []string{
		strings.Replace(current, "ON UPDATE (PENDING_COMMIT_TIMESTAMP())", "ON UPDATE (pending_commit_timestamp())", 1),
		strings.ReplaceAll(current, "PENDING_COMMIT_TIMESTAMP", "pending_commit_timestamp"),
	} {
		ddls, err := GenerateIdempotentDDLs(desired, current, GeneratorConfig{})
		require.NoError(t, err)
		require.Empty(t, ddls)
		desired = strings.Replace(desired, "Id INT64 NOT NULL,", "Id INT64 NOT NULL, N INT64,", 1)
		ddls, err = GenerateIdempotentDDLs(desired, current, GeneratorConfig{})
		require.NoError(t, err)
		require.Equal(t, []string{"ALTER TABLE T ADD COLUMN N INT64"}, ddls)
	}
	// Actual expression changes must still be rejected.
	desired := strings.Replace(current, "ON UPDATE (PENDING_COMMIT_TIMESTAMP())", "ON UPDATE (CURRENT_TIMESTAMP())", 1)
	ddls, err := GenerateIdempotentDDLs(desired, current, GeneratorConfig{})
	require.ErrorContains(t, err, "unsupported ON UPDATE change")
	require.Empty(t, ddls)
}
