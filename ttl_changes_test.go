package spannerdef

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestTTLChanges(t *testing.T) {
	base := `CREATE TABLE T (Id INT64 NOT NULL, CreatedAt TIMESTAMP) PRIMARY KEY (Id)`
	policy := func(days string) string {
		return base + `, ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL ` + days + ` DAY))`
	}
	for _, tc := range []struct{ current, desired, want string }{
		{base, policy("0"), "ALTER TABLE T ADD ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 0 DAY))"},
		{policy("7"), policy("30"), "ALTER TABLE T REPLACE ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 30 DAY))"},
		{policy("7"), base, "ALTER TABLE T DROP ROW DELETION POLICY"},
	} {
		ddls, err := GenerateIdempotentDDLs(tc.desired, tc.current, GeneratorConfig{})
		require.NoError(t, err)
		require.Equal(t, []string{tc.want}, ddls)
	}
	ddls, err := GenerateIdempotentDDLs(strings.ReplaceAll(policy("7"), "OLDER_THAN(CreatedAt", "OLDER_THAN(createdat"), policy("7"), GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, ddls)
	ddls, err = GenerateIdempotentDDLs(policy("0"), "", GeneratorConfig{})
	require.NoError(t, err)
	require.Contains(t, ddls[0], "INTERVAL 0 DAY")
	ddls, err = GenerateIdempotentDDLs(policy("0"), policy("0"), GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, ddls)
	desired := `CREATE TABLE T (Id INT64 NOT NULL, Later TIMESTAMP) PRIMARY KEY (Id), ROW DELETION POLICY (OLDER_THAN(Later, INTERVAL 30 DAY))`
	ddls, err = GenerateIdempotentDDLs(desired, policy("7"), GeneratorConfig{})
	require.NoError(t, err)
	require.Equal(t, []string{"ALTER TABLE T ADD COLUMN Later TIMESTAMP", "ALTER TABLE T REPLACE ROW DELETION POLICY (OLDER_THAN(Later, INTERVAL 30 DAY))", "ALTER TABLE T DROP COLUMN CreatedAt"}, ddls)
}
func TestOmniTTLChanges(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	base := `CREATE TABLE T (Id INT64 NOT NULL, CreatedAt TIMESTAMP) PRIMARY KEY (Id)`
	for _, ddl := range []string{base, base + `, ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 0 DAY))`, base + `, ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 30 DAY))`, base} {
		require.NotEmpty(t, applySchema(t, db, ddl, true))
		require.Empty(t, applySchema(t, db, ddl, true))
	}
}

func TestTTLForeignKeyAndChildOrdering(t *testing.T) {
	parent := `CREATE TABLE P(Id INT64, CreatedAt TIMESTAMP) PRIMARY KEY(Id)`
	child := `CREATE TABLE C(Id INT64, PId INT64, CONSTRAINT FK FOREIGN KEY(PId) REFERENCES P(Id)) PRIMARY KEY(Id)`
	noFK := `CREATE TABLE C(Id INT64, PId INT64) PRIMARY KEY(Id)`
	policy := `, ROW DELETION POLICY(OLDER_THAN(CreatedAt, INTERVAL 7 DAY))`
	for _, tc := range []struct{ old, next, before, after string }{
		{parent + ";" + child, parent + policy + ";" + noFK, "DROP CONSTRAINT FK", "ADD ROW DELETION POLICY"},
		{parent + policy + ";" + noFK, parent + ";" + child, "DROP ROW DELETION POLICY", "ADD CONSTRAINT FK"},
		{parent + policy, parent + `; CREATE TABLE C(Id INT64, CId INT64) PRIMARY KEY(Id,CId), INTERLEAVE IN PARENT P ON DELETE NO ACTION`, "DROP ROW DELETION POLICY", "CREATE TABLE C"},
	} {
		ddls, err := GenerateIdempotentDDLs(tc.next, tc.old, GeneratorConfig{})
		require.NoError(t, err)
		joined := strings.Join(ddls, ";")
		require.Greater(t, strings.Index(joined, tc.after), strings.Index(joined, tc.before))
		require.Contains(t, joined, tc.before)
	}
}
func TestOmniTTLForeignKeyTransitions(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	parent := `CREATE TABLE P(Id INT64, CreatedAt TIMESTAMP) PRIMARY KEY(Id)`
	withFK := parent + `; CREATE TABLE C(Id INT64, PId INT64, CONSTRAINT FK FOREIGN KEY(PId) REFERENCES P(Id)) PRIMARY KEY(Id)`
	withTTL := parent + `, ROW DELETION POLICY(OLDER_THAN(CreatedAt, INTERVAL 7 DAY)); CREATE TABLE C(Id INT64, PId INT64) PRIMARY KEY(Id)`
	for _, ddl := range []string{withFK, withTTL, withFK} {
		require.NotEmpty(t, applySchema(t, db, ddl, true))
		require.Empty(t, applySchema(t, db, ddl, true))
	}
}
func TestCascadeTTLOrdering(t *testing.T) {
	parent := `CREATE TABLE P(Id INT64, CreatedAt TIMESTAMP) PRIMARY KEY(Id)`
	child := `; CREATE TABLE C(Id INT64, CId INT64) PRIMARY KEY(Id,CId), INTERLEAVE IN PARENT P ON DELETE `
	noTTL := parent + child + "NO ACTION"
	withTTL := parent + `, ROW DELETION POLICY(OLDER_THAN(CreatedAt, INTERVAL 7 DAY))` + child + "CASCADE"
	for _, tc := range []struct{ old, next, before, after string }{
		{noTTL, withTTL, "SET ON DELETE CASCADE", "ADD ROW DELETION POLICY"},
		{withTTL, noTTL, "DROP ROW DELETION POLICY", "SET ON DELETE NO ACTION"},
	} {
		ddls, err := GenerateIdempotentDDLs(tc.next, tc.old, GeneratorConfig{})
		require.NoError(t, err)
		joined := strings.Join(ddls, ";")
		require.Contains(t, joined, tc.before)
		require.Greater(t, strings.Index(joined, tc.after), strings.Index(joined, tc.before))
	}
}
func TestOmniCascadeTTLTransitions(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	parent := `CREATE TABLE P(Id INT64, CreatedAt TIMESTAMP) PRIMARY KEY(Id)`
	child := `; CREATE TABLE C(Id INT64, CId INT64) PRIMARY KEY(Id,CId), INTERLEAVE IN PARENT P ON DELETE `
	noTTL := parent + child + "NO ACTION"
	withTTL := parent + `, ROW DELETION POLICY(OLDER_THAN(CreatedAt, INTERVAL 7 DAY))` + child + "CASCADE"
	for _, ddl := range []string{noTTL, withTTL, noTTL} {
		require.NotEmpty(t, applySchema(t, db, ddl, true))
		require.Empty(t, applySchema(t, db, ddl, true))
	}
}
