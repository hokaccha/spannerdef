package spannerdef

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOmniPrimaryKeyForms(t *testing.T) {
	for _, ddl := range []string{`CREATE TABLE T (N INT64) PRIMARY KEY ()`, `CREATE TABLE T (N INT64)`} {
		t.Run(ddl, func(t *testing.T) {
			t.Parallel()
			db := recreateDatabase(t, getTestConfig(t))
			require.NotEmpty(t, applySchema(t, db, ddl, false))
			require.Empty(t, applySchema(t, db, ddl, false))
		})
	}
}
