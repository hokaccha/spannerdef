package spannerdef

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOmniQualifiedNames(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	desired := `CREATE SCHEMA a; CREATE SCHEMA b;
CREATE TABLE a.T (Id INT64 NOT NULL) PRIMARY KEY(Id);
CREATE TABLE b.T (Id INT64 NOT NULL, N INT64) PRIMARY KEY(Id);
CREATE INDEX b.IX ON b.T(N);`
	require.NotEmpty(t, applySchema(t, db, desired, false))
	require.Empty(t, applySchema(t, db, desired, false))
}
