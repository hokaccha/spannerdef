package spannerdef

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestOmniColumnOnUpdate(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	desired := `CREATE TABLE T (Id INT64 NOT NULL, UpdatedAt TIMESTAMP DEFAULT (PENDING_COMMIT_TIMESTAMP()) ON UPDATE (PENDING_COMMIT_TIMESTAMP()) OPTIONS (allow_commit_timestamp = true)) PRIMARY KEY(Id);`
	require.NotEmpty(t, applySchema(t, db, desired, false))
	require.Empty(t, applySchema(t, db, desired, false))
	require.Empty(t, applySchema(t, db, strings.ReplaceAll(desired, "PENDING_COMMIT_TIMESTAMP", "pending_commit_timestamp"), false))
}
