package spannerdef

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOmniForeignKeyEnforcement(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	desired := `CREATE TABLE P (Id INT64 NOT NULL) PRIMARY KEY(Id);
CREATE TABLE C (Id INT64 NOT NULL, PId INT64, CONSTRAINT FK FOREIGN KEY(PId) REFERENCES P(Id) NOT ENFORCED) PRIMARY KEY(Id);`
	require.NotEmpty(t, applySchema(t, db, desired, false))
	require.Empty(t, applySchema(t, db, desired, false))
}
