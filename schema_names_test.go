package spannerdef

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestQualifiedNamesStayDistinct(t *testing.T) {
	desired := "CREATE SCHEMA a; CREATE SCHEMA b; CREATE TABLE a.T (Id INT64 NOT NULL) PRIMARY KEY (Id); CREATE TABLE b.T (Id INT64 NOT NULL, N INT64) PRIMARY KEY (Id); CREATE INDEX b.IX ON b.T(N)"
	schema, err := ParseDDLs(desired)
	require.NoError(t, err)
	require.Len(t, schema.Tables, 2)
	require.Contains(t, schema.Tables, "a.T")
	require.Contains(t, schema.Tables, "b.T")
	ddls, err := GenerateIdempotentDDLs(desired, "", GeneratorConfig{})
	require.NoError(t, err)
	require.Len(t, ddls, 5)
	require.Equal(t, []string{"CREATE SCHEMA a", "CREATE SCHEMA b"}, ddls[:2])
	require.Equal(t, "CREATE INDEX b.IX ON b.T (N)", ddls[4])
	diff, err := GenerateIdempotentDDLs(desired, strings.Join(ddls, ";"), GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, diff)
}

func TestQualifiedNamesFilteringAndAlter(t *testing.T) {
	current := "CREATE SCHEMA a; CREATE SCHEMA b; CREATE TABLE a.T (Id INT64 NOT NULL) PRIMARY KEY (Id); CREATE TABLE b.T (Id INT64 NOT NULL) PRIMARY KEY (Id);"
	desired := strings.ReplaceAll(current, "Id INT64 NOT NULL)", "Id INT64 NOT NULL, N INT64)")
	for _, config := range []GeneratorConfig{{TargetTables: []string{"b.T"}}, {SkipTables: []string{"a.T"}}} {
		ddls, err := GenerateIdempotentDDLs(desired, current, config)
		require.NoError(t, err)
		require.Equal(t, []string{"ALTER TABLE b.T ADD COLUMN N INT64"}, ddls)
	}
}

func TestQualifiedDependenciesAndQuotedNames(t *testing.T) {
	desired := "CREATE SCHEMA s; CREATE TABLE s.C (Id INT64 NOT NULL, N INT64, CONSTRAINT FK FOREIGN KEY(N) REFERENCES s.P(Id)) PRIMARY KEY (Id), INTERLEAVE IN PARENT s.P; CREATE TABLE s.P (Id INT64 NOT NULL) PRIMARY KEY(Id)"
	ddls, err := GenerateIdempotentDDLs(desired, "", GeneratorConfig{})
	require.NoError(t, err)
	require.Len(t, ddls, 3)
	require.Contains(t, ddls[1], "CREATE TABLE s.P")
	require.Contains(t, ddls[2], "REFERENCES s.P (Id)")
	require.Contains(t, ddls[2], "INTERLEAVE IN PARENT s.P")
	quoted := "CREATE SCHEMA `Order`; CREATE TABLE `Order`.`Select` (`From` INT64 NOT NULL) PRIMARY KEY (`From`)"
	ddls, err = GenerateIdempotentDDLs(quoted, "", GeneratorConfig{})
	require.NoError(t, err)
	require.Len(t, ddls, 2)
	require.Contains(t, ddls[1], "`Order`.`Select`")
	require.Contains(t, ddls[1], "`From` INT64 NOT NULL")
	diff, err := GenerateIdempotentDDLs(quoted, strings.Join(ddls, ";"), GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, diff)
}

func TestNamedSchemaDeletionIsExplicitlyUnsupported(t *testing.T) {
	ddls, err := GenerateIdempotentDDLs("", "CREATE SCHEMA s", GeneratorConfig{})
	require.ErrorContains(t, err, "dropping named schema s is not supported")
	require.Empty(t, ddls)
}

func TestFilteredNewTableDoesNotRecreateExistingNamespace(t *testing.T) {
	current := "CREATE SCHEMA a; CREATE TABLE a.Existing (Id INT64 NOT NULL) PRIMARY KEY(Id)"
	desired := "CREATE SCHEMA a; CREATE TABLE a.NewTable (Id INT64 NOT NULL) PRIMARY KEY(Id)"
	ddls, err := GenerateIdempotentDDLs(desired, current, GeneratorConfig{TargetTables: []string{"a.NewTable"}})
	require.NoError(t, err)
	require.Len(t, ddls, 1)
	require.Contains(t, ddls[0], "CREATE TABLE a.NewTable")
}
