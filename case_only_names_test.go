package spannerdef

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCaseOnlyNameChangesAreRejected(t *testing.T) {
	current := "CREATE SCHEMA s; CREATE TABLE Users (Id STRING(36) NOT NULL, UserName STRING(100), CONSTRAINT NameSet CHECK (UserName IS NOT NULL)) PRIMARY KEY (Id); CREATE INDEX UsersByName ON Users (UserName)"
	for _, tc := range []struct{ name, desired, message string }{
		{"table", "CREATE SCHEMA s; CREATE TABLE users (Id STRING(36) NOT NULL, UserName STRING(100), CONSTRAINT NameSet CHECK (UserName IS NOT NULL)) PRIMARY KEY (Id); CREATE INDEX UsersByName ON users (UserName)", "table users differs only in case from existing Users"},
		{"column", "CREATE SCHEMA s; CREATE TABLE Users (Id STRING(36) NOT NULL, Username STRING(100), CONSTRAINT NameSet CHECK (Username IS NOT NULL)) PRIMARY KEY (Id); CREATE INDEX UsersByName ON Users (Username)", "column Users.Username differs only in case from existing Users.UserName"},
		{"index", "CREATE SCHEMA s; CREATE TABLE Users (Id STRING(36) NOT NULL, UserName STRING(100), CONSTRAINT NameSet CHECK (UserName IS NOT NULL)) PRIMARY KEY (Id); CREATE INDEX UsersByname ON Users (UserName)", "index UsersByname differs only in case from existing UsersByName"},
		{"constraint", "CREATE SCHEMA s; CREATE TABLE Users (Id STRING(36) NOT NULL, UserName STRING(100), CONSTRAINT nameset CHECK (UserName IS NOT NULL)) PRIMARY KEY (Id); CREATE INDEX UsersByName ON Users (UserName)", "constraint Users.nameset differs only in case from existing Users.NameSet"},
		{"schema", "CREATE SCHEMA S; CREATE SCHEMA s; CREATE TABLE Users (Id STRING(36) NOT NULL, UserName STRING(100), CONSTRAINT NameSet CHECK (UserName IS NOT NULL)) PRIMARY KEY (Id); CREATE INDEX UsersByName ON Users (UserName)", "schema S differs only in case from existing s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ddls, err := GenerateIdempotentDDLs(tc.desired, current, GeneratorConfig{})
			require.ErrorContains(t, err, tc.message)
			require.Empty(t, ddls)
		})
	}
}

func TestExactNamesAreNotReportedAsCaseOnlyChanges(t *testing.T) {
	current := "CREATE TABLE Users (Id STRING(36) NOT NULL, UserName STRING(100)) PRIMARY KEY (Id); CREATE INDEX UsersByName ON Users (UserName)"
	ddls, err := GenerateIdempotentDDLs(current, current, GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, ddls)
	ddls, err = GenerateIdempotentDDLs("CREATE TABLE Accounts (Id STRING(36) NOT NULL) PRIMARY KEY (Id)", current, GeneratorConfig{})
	require.NoError(t, err)
	require.Contains(t, ddls, "DROP TABLE Users")
}
