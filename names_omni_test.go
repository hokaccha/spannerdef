package spannerdef

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOmniQualifiedNames(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	desired := `CREATE SCHEMA a; CREATE SCHEMA b;
CREATE TABLE a.T (Id INT64 NOT NULL) PRIMARY KEY(Id);
CREATE TABLE b.T (Id INT64 NOT NULL, N INT64) PRIMARY KEY(Id);
CREATE INDEX b.IX ON b.T(N);`
	require.NotEmpty(t, applySchema(t, db, desired, false))
	require.Empty(t, applySchema(t, db, desired, false))
}

func TestOmniReplaceTableWithNamedSchema(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	current := "CREATE TABLE Accounts (Id INT64 NOT NULL) PRIMARY KEY(Id)"
	desired := "CREATE SCHEMA Accounts; CREATE TABLE Accounts.Users (Id INT64 NOT NULL) PRIMARY KEY(Id)"
	require.NotEmpty(t, applySchema(t, db, current, false))
	require.NotEmpty(t, applySchema(t, db, desired, true))
	require.Empty(t, applySchema(t, db, desired, false))
}

func TestOmniCaseOnlyNameChangesAreRejected(t *testing.T) {
	t.Parallel()
	db := recreateDatabase(t, getTestConfig(t))
	current := "CREATE TABLE Users (Id STRING(36) NOT NULL, UserName STRING(100)) PRIMARY KEY(Id); CREATE INDEX UsersByName ON Users(UserName)"
	require.NotEmpty(t, applySchema(t, db, current, false))
	dumped, err := db.DumpDDLs()
	require.NoError(t, err)
	for _, desired := range []string{
		"CREATE TABLE users (Id STRING(36) NOT NULL, UserName STRING(100)) PRIMARY KEY(Id); CREATE INDEX UsersByName ON users(UserName)",
		"CREATE TABLE Users (Id STRING(36) NOT NULL, Username STRING(100)) PRIMARY KEY(Id); CREATE INDEX UsersByName ON Users(Username)",
		"CREATE TABLE Users (Id STRING(36) NOT NULL, UserName STRING(100)) PRIMARY KEY(Id); CREATE INDEX usersbyname ON Users(UserName)",
	} {
		ddls, err := GenerateIdempotentDDLs(desired, dumped, GeneratorConfig{})
		require.ErrorContains(t, err, "differs only in case")
		require.Empty(t, ddls)
	}
	require.Empty(t, applySchema(t, db, current, false))
}
