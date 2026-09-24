package spannerdef

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestOmniKeyDefinitions(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	desired := `CREATE TABLE T (Id INT64 NOT NULL PRIMARY KEY, N INT64, A INT64, B INT64);
CREATE NULL_FILTERED INDEX IX ON T(N DESC) STORING(A, B);`
	require.NotEmpty(t, applySchema(t, db, desired, false))
	require.Empty(t, applySchema(t, db, desired, false))
	require.Empty(t, applySchema(t, db, strings.Replace(desired, "STORING(A, B)", "STORING(B, A)", 1), false))
}
