package spannerdef

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestDependencyOrdering(t *testing.T) {
	for _, tc := range []struct {
		name, current, desired string
		before, after          string
	}{
		{"parent key length", `CREATE TABLE P(Id STRING(10) NOT NULL) PRIMARY KEY(Id)`, `CREATE TABLE P(Id STRING(20) NOT NULL) PRIMARY KEY(Id); CREATE TABLE C(Id STRING(20) NOT NULL, CId INT64 NOT NULL) PRIMARY KEY(Id,CId), INTERLEAVE IN PARENT P`, "ALTER COLUMN Id STRING(20)", "CREATE TABLE C"},
		{"generated drop", `CREATE TABLE T (Id INT64 NOT NULL, N INT64, G INT64 AS(N+1) STORED) PRIMARY KEY(Id)`, `CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY(Id)`, "DROP COLUMN G", "DROP COLUMN N"},
		{"new FK parent", `CREATE TABLE C(Id INT64 NOT NULL, PId INT64) PRIMARY KEY(Id)`, `CREATE TABLE P(Id INT64 NOT NULL) PRIMARY KEY(Id); CREATE TABLE C(Id INT64 NOT NULL, PId INT64, CONSTRAINT FK FOREIGN KEY(PId) REFERENCES P(Id)) PRIMARY KEY(Id)`, "CREATE TABLE P", "ADD CONSTRAINT FK"},
		{"drop FK parent", `CREATE TABLE P(Id INT64 NOT NULL) PRIMARY KEY(Id); CREATE TABLE C(Id INT64 NOT NULL, PId INT64, CONSTRAINT FK FOREIGN KEY(PId) REFERENCES P(Id)) PRIMARY KEY(Id)`, `CREATE TABLE C(Id INT64 NOT NULL, PId INT64) PRIMARY KEY(Id)`, "DROP CONSTRAINT FK", "DROP TABLE P"},
		{"drop interleave", `CREATE TABLE P(Id INT64) PRIMARY KEY(Id); CREATE TABLE C(Id INT64, CId INT64) PRIMARY KEY(Id,CId), INTERLEAVE IN PARENT P`, "", "DROP TABLE C", "DROP TABLE P"},
		{"cyclic FK", "", `CREATE TABLE A(Id INT64 NOT NULL, BId INT64, CONSTRAINT AB FOREIGN KEY(BId) REFERENCES B(Id)) PRIMARY KEY(Id); CREATE TABLE B(Id INT64 NOT NULL, AId INT64, CONSTRAINT BA FOREIGN KEY(AId) REFERENCES A(Id)) PRIMARY KEY(Id)`, "CREATE TABLE B", "ADD CONSTRAINT AB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 10; i++ {
				ddls, err := GenerateIdempotentDDLs(tc.desired, tc.current, GeneratorConfig{})
				require.NoError(t, err)
				joined := strings.Join(ddls, ";\n")
				a, b := strings.Index(joined, tc.before), strings.Index(joined, tc.after)
				require.GreaterOrEqual(t, a, 0)
				require.Greater(t, b, a)
			}
		})
	}
}
func TestOmniDependencyOrdering(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	schemas := []string{
		`CREATE TABLE C(Id INT64 NOT NULL, PId INT64, N INT64, G INT64 AS(N+1) STORED) PRIMARY KEY(Id)`,
		`CREATE TABLE P(Id INT64 NOT NULL) PRIMARY KEY(Id); CREATE TABLE C(Id INT64 NOT NULL, PId INT64, CONSTRAINT FK FOREIGN KEY(PId) REFERENCES P(Id)) PRIMARY KEY(Id)`,
		`CREATE TABLE C(Id INT64 NOT NULL, PId INT64) PRIMARY KEY(Id)`,
	}
	for _, ddl := range schemas {
		require.NotEmpty(t, applySchema(t, db, ddl, true))
		require.Empty(t, applySchema(t, db, ddl, true))
	}
}

func TestOmniInterleaveParentKeyAlter(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	old := `CREATE TABLE P(Id STRING(10) NOT NULL) PRIMARY KEY(Id)`
	next := `CREATE TABLE P(Id STRING(20) NOT NULL) PRIMARY KEY(Id); CREATE TABLE C(Id STRING(20) NOT NULL, CId INT64 NOT NULL) PRIMARY KEY(Id,CId), INTERLEAVE IN PARENT P`
	require.NotEmpty(t, applySchema(t, db, old, false))
	require.NotEmpty(t, applySchema(t, db, next, false))
	require.Empty(t, applySchema(t, db, next, false))
}

func TestRetainedTableConstraintNameConflict(t *testing.T) {
	old := `CREATE TABLE T(Id INT64, CONSTRAINT CK CHECK(Id>0)) PRIMARY KEY(Id)`
	next := `CREATE TABLE U(Id INT64, CONSTRAINT CK CHECK(Id>0)) PRIMARY KEY(Id)`
	ddls, err := GenerateIdempotentDDLs(next, old, GeneratorConfig{})
	require.NoError(t, err)
	db := &recordingDatabase{}
	require.ErrorContains(t, RunDDLs(db, ddls, false, true), "skipped")
	require.Empty(t, db.sent)
	require.NoError(t, RunDDLs(db, ddls, true, true))
	require.Equal(t, ddls, db.sent)
}
