package spannerdef

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/stretchr/testify/require"
)

func TestOmniBatchedExecutionAndOperationRecovery(t *testing.T) {
	db, _ := newTestDatabase(t, getTestConfig(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	started := time.Now()
	var operations []string
	db.operationObserver = func(name string) { operations = append(operations, name) }
	require.NoError(t, db.ExecDDLContext(ctx, "CREATE TABLE T(Id INT64 NOT NULL) PRIMARY KEY(Id)"))
	t.Logf("table created after %s", time.Since(started))
	var indexes []string
	for i := range 11 {
		indexes = append(indexes, fmt.Sprintf("CREATE INDEX I%d ON T(Id)", i))
	}
	require.NoError(t, db.ExecDDLsContext(ctx, indexes))
	t.Logf("index batches completed after %s", time.Since(started))
	require.Len(t, operations, 3)
	for _, name := range operations {
		require.NoError(t, db.WaitDDLOperation(ctx, name))
	}
	t.Logf("operation recovery verified after %s", time.Since(started))
	dump, err := db.DumpDDLsContext(ctx)
	require.NoError(t, err)
	require.Equal(t, 11, strings.Count(dump, "CREATE INDEX"))
	// Export replay on a fresh database exercises the returned dependency order.
	parsed, err := memefish.ParseDDLs("", dump)
	require.NoError(t, err)
	statements := make([]string, len(parsed))
	for i, ddl := range parsed {
		statements[i] = ddl.SQL()
	}
	other, _ := newTestDatabase(t, getTestConfig(t))
	t.Logf("starting export replay after %s", time.Since(started))
	require.NoError(t, other.ExecDDLsContext(ctx, statements))
	t.Logf("export replay completed after %s", time.Since(started))
	replay, err := other.DumpDDLsContext(ctx)
	require.NoError(t, err)
	diff, err := GenerateIdempotentDDLs(dump, replay, GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, diff)
}
