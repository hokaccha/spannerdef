package spannerdef

import (
	"context"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

type outputErrorDatabase struct {
	recordingDatabase
	schema string
}

func (d *outputErrorDatabase) DumpDDLs() (string, error) { return d.schema, nil }

func withReadOnlyStdout(t *testing.T) {
	t.Helper()
	stdout, err := os.Open(os.DevNull)
	require.NoError(t, err)
	old := os.Stdout
	os.Stdout = stdout
	t.Cleanup(func() {
		os.Stdout = old
		require.NoError(t, stdout.Close())
	})
}

func TestRunContextReturnsOutputErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		schema  string
		options Options
		message string
	}{
		{"export schema", "CREATE TABLE T(Id INT64) PRIMARY KEY(Id)", Options{Export: true}, "write exported DDLs"},
		{"export empty schema", "", Options{Export: true}, "write exported DDLs"},
		{"unchanged schema", "", Options{}, "write unchanged plan"},
		{"dry run", "", Options{DesiredDDLs: "CREATE TABLE T(Id INT64) PRIMARY KEY(Id)", DryRun: true}, "write dry-run plan"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withReadOnlyStdout(t)
			db := &outputErrorDatabase{schema: tc.schema}
			err := RunContext(context.Background(), db, &tc.options)
			require.ErrorContains(t, err, tc.message)
			require.ErrorIs(t, err, syscall.EBADF)
			require.Empty(t, db.sent)
		})
	}
}

func TestRunDDLsContextDoesNotExecuteAfterPlanOutputError(t *testing.T) {
	withReadOnlyStdout(t)
	db := &recordingDatabase{}
	err := RunDDLsContext(context.Background(), db, []string{"CREATE TABLE T(Id INT64) PRIMARY KEY(Id)"}, false, false)
	require.ErrorContains(t, err, "write apply plan")
	require.ErrorIs(t, err, syscall.EBADF)
	require.Empty(t, db.sent)
}
