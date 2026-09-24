package spannerdef

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func interleaveSchema(action string) string {
	return `CREATE TABLE P (Id INT64 NOT NULL) PRIMARY KEY (Id); CREATE TABLE C (Id INT64 NOT NULL, CId INT64 NOT NULL) PRIMARY KEY (Id, CId), INTERLEAVE IN PARENT P ` + action
}
func TestInterleaveDeleteChanges(t *testing.T) {
	for _, tc := range []struct{ old, new, want string }{
		{"", "ON DELETE CASCADE", "ALTER TABLE C SET ON DELETE CASCADE"},
		{"ON DELETE CASCADE", "", "ALTER TABLE C SET ON DELETE NO ACTION"},
		{"", "ON DELETE NO ACTION", ""},
	} {
		ddls, err := GenerateIdempotentDDLs(interleaveSchema(tc.new), interleaveSchema(tc.old), GeneratorConfig{})
		require.NoError(t, err)
		if tc.want == "" {
			require.Empty(t, ddls)
		} else {
			require.Equal(t, []string{tc.want}, ddls)
		}
	}
}
func TestOmniInterleaveDeleteChanges(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	for _, action := range []string{"", "ON DELETE CASCADE", "ON DELETE NO ACTION"} {
		ddl := interleaveSchema(action)
		require.NotEmpty(t, applySchema(t, db, ddl, false))
		require.Empty(t, applySchema(t, db, ddl, false))
	}
}

func TestInterleaveDeleteRemovesTTLFirst(t *testing.T) {
	parent := `CREATE TABLE P(Id INT64, CreatedAt TIMESTAMP) PRIMARY KEY(Id)`
	child := `; CREATE TABLE C(Id INT64, CId INT64) PRIMARY KEY(Id,CId), INTERLEAVE IN PARENT P ON DELETE `
	old := parent + `, ROW DELETION POLICY(OLDER_THAN(CreatedAt, INTERVAL 7 DAY))` + child + "CASCADE"
	next := parent + child + "NO ACTION"
	ddls, err := GenerateIdempotentDDLs(next, old, GeneratorConfig{})
	require.NoError(t, err)
	require.Equal(t, []string{"ALTER TABLE P DROP ROW DELETION POLICY", "ALTER TABLE C SET ON DELETE NO ACTION"}, ddls)
}
