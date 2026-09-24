package spannerdef

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type legacyRuntimeDB struct {
	dumpErr error
	closed  bool
	calls   int
}

func (d *legacyRuntimeDB) DumpDDLs() (string, error) { d.calls++; return "", d.dumpErr }
func (d *legacyRuntimeDB) ExecDDL(string) error      { d.calls++; return nil }
func (d *legacyRuntimeDB) ExecDDLs([]string) error   { d.calls++; return nil }
func (d *legacyRuntimeDB) Close() error              { d.closed = true; return nil }

type contextualRuntimeDB struct {
	legacyRuntimeDB
	seen context.Context
}

func (d *contextualRuntimeDB) DumpDDLsContext(ctx context.Context) (string, error) {
	d.seen = ctx
	return "", nil
}
func (d *contextualRuntimeDB) ExecDDLsContext(ctx context.Context, _ []string) error {
	d.seen = ctx
	return ctx.Err()
}
func TestRunContextReturnsErrorsAndAllowsCleanup(t *testing.T) {
	sentinel := errors.New("transport failed")
	db := &legacyRuntimeDB{dumpErr: sentinel}
	err := func() error { defer db.Close(); return RunContext(context.Background(), db, &Options{Export: true}) }()
	require.ErrorIs(t, err, sentinel)
	require.True(t, db.closed)
}
func TestRuntimeContextAndLegacyFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := &contextualRuntimeDB{}
	require.NoError(t, RunContext(ctx, db, &Options{Export: true}))
	require.Same(t, ctx, db.seen)
	require.NoError(t, RunDDLsContext(ctx, db, []string{"CREATE TABLE T(Id INT64) PRIMARY KEY(Id)"}, false, true))
	require.Same(t, ctx, db.seen)
	legacy := &legacyRuntimeDB{}
	require.NoError(t, RunDDLs(legacy, []string{"CREATE TABLE T(Id INT64) PRIMARY KEY(Id)"}, false, true))
	require.Equal(t, 1, legacy.calls)
	cancel()
	require.ErrorIs(t, RunContext(ctx, legacy, &Options{}), context.Canceled)
	require.ErrorIs(t, RunDDLsContext(ctx, legacy, []string{"DROP TABLE T"}, true, true), context.Canceled)
	require.Equal(t, 1, legacy.calls)
}
