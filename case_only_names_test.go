package spannerdef

import (
	"testing"

	"github.com/stretchr/testify/require"
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

			currentSchema, err := ParseDDLs(current)
			require.NoError(t, err)
			desiredSchema, err := ParseDDLs(tc.desired)
			require.NoError(t, err)
			ddls, err = GenerateDDLsChecked(currentSchema, desiredSchema)
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

func TestCaseOnlyTableChangesWithFilters(t *testing.T) {
	for _, names := range [][2]string{{"Users", "users"}, {"s.Users", "s.users"}} {
		t.Run(names[0]+"/"+names[1], func(t *testing.T) {
			current := "CREATE SCHEMA s; CREATE TABLE " + names[0] + " (Id INT64 NOT NULL) PRIMARY KEY(Id)"
			desired := "CREATE SCHEMA s; CREATE TABLE " + names[1] + " (Id INT64 NOT NULL) PRIMARY KEY(Id)"
			for _, tc := range []struct {
				name   string
				config GeneratorConfig
			}{
				{"target current", GeneratorConfig{TargetTables: []string{names[0]}}},
				{"target desired", GeneratorConfig{TargetTables: []string{names[1]}}},
				{"skip current", GeneratorConfig{SkipTables: []string{names[0]}}},
				{"skip desired", GeneratorConfig{SkipTables: []string{names[1]}}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ddls, err := GenerateIdempotentDDLs(desired, current, tc.config)
					require.ErrorContains(t, err, "differs only in case")
					require.Empty(t, ddls)
				})
			}
		})
	}
}

func TestCaseOnlyChangesOutsideManagedTablesAreIgnored(t *testing.T) {
	// Unsupported features on excluded tables must remain outside validation.
	current := "CREATE TABLE Users (Id INT64 NOT NULL) PRIMARY KEY(Id); CREATE TABLE Keep (Id INT64 NOT NULL) PRIMARY KEY(Id)"
	desired := "CREATE TABLE users (Id INT64 NOT NULL) PRIMARY KEY(Id), OPTIONS (columnar_policy = 'enabled'); CREATE TABLE Keep (Id INT64 NOT NULL) PRIMARY KEY(Id)"
	_, err := ParseDDLs(desired)
	require.ErrorContains(t, err, "unsupported table OPTIONS")
	for _, config := range []GeneratorConfig{
		{TargetTables: []string{"Keep"}},
		{SkipTables: []string{"Users", "users"}},
	} {
		ddls, err := GenerateIdempotentDDLs(desired, current, config)
		require.NoError(t, err)
		require.Empty(t, ddls)
	}
}

func TestCaseOnlyTableInventoryDoesNotHideRealDrops(t *testing.T) {
	current := "CREATE TABLE Users (Id INT64 NOT NULL) PRIMARY KEY(Id)"
	ddls, err := GenerateIdempotentDDLs("", current, GeneratorConfig{TargetTables: []string{"Users"}})
	require.NoError(t, err)
	require.Equal(t, []string{"DROP TABLE Users"}, ddls)
}

func TestCaseOnlyFilteredTableReferencesAreRejected(t *testing.T) {
	table := "CREATE TABLE Users (Id INT64 NOT NULL, Email STRING(100)) PRIMARY KEY(Id);"
	for _, tc := range []struct{ name, current, desired string }{
		{"unique index", "CREATE UNIQUE INDEX UsersByEmail ON Users(Email)", "CREATE UNIQUE INDEX usersbyemail ON users(Email)"},
		{"constraint", "ALTER TABLE Users ADD CONSTRAINT EmailSet CHECK(Email IS NOT NULL)", "ALTER TABLE users ADD CONSTRAINT emailset CHECK(Email IS NOT NULL)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, config := range []GeneratorConfig{
				{TargetTables: []string{"Users"}},
				{SkipTables: []string{"users"}},
			} {
				ddls, err := GenerateIdempotentDDLs(table+tc.desired, table+tc.current, config)
				require.ErrorContains(t, err, "table users differs only in case from existing Users")
				require.Empty(t, ddls)
			}
			// The same statements are still ignored when the entire table is unmanaged.
			ddls, err := GenerateIdempotentDDLs(table+tc.desired, table+tc.current, GeneratorConfig{TargetTables: []string{"Other"}})
			require.NoError(t, err)
			require.Empty(t, ddls)
		})
	}
}
