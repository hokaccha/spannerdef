package spannerdef

import (
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"strings"
	"testing"
)

type recordingDatabase struct{ sent []string }

func (d *recordingDatabase) DumpDDLs() (string, error)   { return "", nil }
func (d *recordingDatabase) ExecDDL(sql string) error    { d.sent = append(d.sent, sql); return nil }
func (d *recordingDatabase) ExecDDLs(sql []string) error { d.sent = append(d.sent, sql...); return nil }
func (d *recordingDatabase) Close() error                { return nil }
func TestExecutionPlan(t *testing.T) {
	ddls := []string{`CREATE TABLE T(Id INT64, S STRING(MAX) DEFAULT ('DROP TABLE')) PRIMARY KEY(Id)`, "drop index IX", "ALTER TABLE T DROP COLUMN N", "ALTER TABLE T DROP CONSTRAINT CK"}
	db := &recordingDatabase{}
	require.NoError(t, RunDDLs(db, ddls, false, true))
	require.Equal(t, ddls[:1], db.sent)
	plan, err := planDDLs(ddls, false)
	require.NoError(t, err)
	r, w, err := os.Pipe()
	require.NoError(t, err)
	old := os.Stdout
	os.Stdout = w
	err = showDDLs(ddls, false)
	os.Stdout = old
	require.NoError(t, w.Close())
	require.NoError(t, err)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	for _, step := range plan {
		line := step.SQL + ";"
		if step.Skip {
			line = "-- Skipped: " + line
		}
		require.Contains(t, string(out), line)
	}
	db = &recordingDatabase{}
	require.NoError(t, RunDDLs(db, ddls, true, true))
	require.Equal(t, ddls, db.sent)
}
func TestExecutionPlanPreservesConstraintReplacements(t *testing.T) {
	ddls := []string{"ALTER TABLE T DROP CONSTRAINT CK", "ALTER TABLE T ADD CONSTRAINT CK CHECK (N > 2)"}
	db := &recordingDatabase{}
	require.NoError(t, RunDDLs(db, ddls, false, true))
	require.Equal(t, ddls, db.sent)
}
func TestExecutionPlanRejectsConflictsBeforeExecution(t *testing.T) {
	for _, ddls := range [][]string{{"DROP TABLE s", "CREATE SCHEMA s"}, {"DROP INDEX T", "CREATE TABLE T (Id INT64) PRIMARY KEY(Id)"}, {"CREATE TABLE T (Id INT64) PRIMARY KEY(Id)", "invalid ddl"}} {
		db := &recordingDatabase{}
		require.Error(t, RunDDLs(db, ddls, false, true))
		require.Empty(t, db.sent)
	}
}
func TestDropColumnPreservesCheckWhenDisabled(t *testing.T) {
	ddls, err := GenerateIdempotentDDLs(`CREATE TABLE T(Id INT64) PRIMARY KEY(Id)`, `CREATE TABLE T(Id INT64, N INT64, CONSTRAINT CK CHECK(N>0)) PRIMARY KEY(Id)`, GeneratorConfig{})
	require.NoError(t, err)
	db := &recordingDatabase{}
	require.NoError(t, RunDDLs(db, ddls, false, true))
	require.Empty(t, db.sent)
	require.Contains(t, strings.Join(ddls, ";"), "DROP CONSTRAINT")
}

func TestConstraintReplacementCase(t *testing.T) {
	ddls := []string{"ALTER TABLE T DROP CONSTRAINT CK", "ALTER TABLE T ADD CONSTRAINT ck CHECK(N>2)"}
	db := &recordingDatabase{}
	require.NoError(t, RunDDLs(db, ddls, false, true))
	require.Equal(t, ddls, db.sent)
}
func TestSkippedTableKeepsForeignKeys(t *testing.T) {
	ddls := []string{"ALTER TABLE C DROP CONSTRAINT FK", "DROP TABLE C"}
	db := &recordingDatabase{}
	require.NoError(t, RunDDLs(db, ddls, false, true))
	require.Empty(t, db.sent)
}

func TestRetainedConstraintsBlockDependentChanges(t *testing.T) {
	for _, ddls := range [][]string{
		{"ALTER TABLE T DROP CONSTRAINT CK", "ALTER TABLE T ALTER COLUMN V BYTES(MAX)"},
		{"DROP TABLE C", "ALTER TABLE P ADD ROW DELETION POLICY(OLDER_THAN(CreatedAt, INTERVAL 7 DAY))"},
		{"DROP TABLE C", "ALTER TABLE P ALTER COLUMN Id STRING(20) NOT NULL"},
		{"DROP INDEX IX", "ALTER TABLE P ALTER COLUMN Id INT64"},
		{"ALTER TABLE C DROP CONSTRAINT FK", "ALTER TABLE C SET ON DELETE CASCADE"},
		{"ALTER TABLE C DROP CONSTRAINT FK", "ALTER TABLE P ADD CONSTRAINT NewCK CHECK(Id>0)"},
		{"ALTER TABLE C DROP CONSTRAINT FK", "ALTER TABLE P ADD COLUMN Extra INT64", "ALTER TABLE P ALTER COLUMN Code BYTES(10)"},
		{"ALTER TABLE T DROP CONSTRAINT CK", "CREATE TABLE U(Id INT64, N INT64, CONSTRAINT CK CHECK(N>0)) PRIMARY KEY(Id)"},
		{"ALTER TABLE T DROP CONSTRAINT CK", "ALTER TABLE U ADD CONSTRAINT CK CHECK(N>0)"},
		{"ALTER TABLE T DROP CONSTRAINT CK", "CREATE INDEX CK ON T(N)"},
		{"ALTER TABLE s.T DROP CONSTRAINT CK", "CREATE INDEX s.CK ON s.T(N)"},
		{"ALTER TABLE C DROP CONSTRAINT FK", "ALTER TABLE P ADD ROW DELETION POLICY(OLDER_THAN(CreatedAt, INTERVAL 7 DAY))"},
	} {
		db := &recordingDatabase{}
		require.ErrorContains(t, RunDDLs(db, ddls, false, true), "skipped")
		require.Empty(t, db.sent)
		require.NoError(t, RunDDLs(db, ddls, true, true))
		require.Equal(t, ddls, db.sent)
	}
}
