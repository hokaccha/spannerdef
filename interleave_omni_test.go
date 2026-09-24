package spannerdef

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOmniInterleaveEnforcement(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	desired := `CREATE TABLE P (Id INT64 NOT NULL) PRIMARY KEY(Id);
CREATE TABLE C (Id INT64 NOT NULL, N INT64 NOT NULL) PRIMARY KEY(Id, N), INTERLEAVE IN P;`
	require.NotEmpty(t, applySchema(t, db, desired, false))
	require.Empty(t, applySchema(t, db, desired, false))
}
