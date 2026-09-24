package spannerdef

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUnsupportedDDLDoesNotProducePartialPlans(t *testing.T) {
	for _, sql := range []string{
		"CREATE VIEW V SQL SECURITY INVOKER AS SELECT 1 AS N",
		"CREATE SEQUENCE S OPTIONS (sequence_kind = 'bit_reversed_positive')",
		"CREATE CHANGE STREAM C FOR ALL",
		"CREATE SEARCH INDEX SI ON T(Tokens)",
		"CREATE VECTOR INDEX VI ON T(Embedding) WHERE Embedding IS NOT NULL OPTIONS (distance_type = 'COSINE')",
		"CREATE PROPERTY GRAPH G NODE TABLES(T)",
		"ALTER DATABASE db SET OPTIONS (columnar_policy = 'enabled')",
		"CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id), OPTIONS (columnar_policy = 'enabled')",
		"CREATE INDEX IX ON T(Id) OPTIONS (columnar_policy = 'enabled')",
		"CREATE INDEX IX ON T(Id) WHERE Id IS NOT NULL",
		"CREATE INDEX IX ON T(Id), INTERLEAVE IN T",
		"ALTER TABLE T ADD COLUMN N INT64",
		"ALTER TABLE T SET OPTIONS (columnar_policy = 'enabled')",
		"ALTER TABLE T DROP COLUMN N",
	} {
		t.Run(sql, func(t *testing.T) {
			base := "CREATE TABLE Base (Id INT64 NOT NULL) PRIMARY KEY (Id);"
			for _, pair := range [][2]string{{base + sql, ""}, {base, sql}} {
				ddls, err := GenerateIdempotentDDLs(pair[0], pair[1], GeneratorConfig{})
				require.ErrorContains(t, err, "unsupported")
				require.Empty(t, ddls)
			}
		})
	}
}

func TestAlterConstraintRequiresItsTable(t *testing.T) {
	_, err := ParseDDLs("ALTER TABLE Missing ADD CONSTRAINT CK CHECK (Id > 0)")
	require.ErrorContains(t, err, "unknown table Missing")
	// The legitimate GetDatabaseDdl form remains order independent.
	sql := "ALTER TABLE T ADD CONSTRAINT CK CHECK (Id > 0); CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY(Id)"
	schema, err := ParseDDLs(sql)
	require.NoError(t, err)
	require.Contains(t, schema.Tables["T"].Constraints, "CK")
}

func TestUnsupportedDDLRespectsTableFilters(t *testing.T) {
	current := "CREATE TABLE Managed (Id INT64 NOT NULL) PRIMARY KEY(Id);"
	desired := "CREATE TABLE Managed (Id INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(Id);"
	for _, excluded := range []string{
		"CREATE TABLE Ignored (Id INT64 NOT NULL) PRIMARY KEY(Id), OPTIONS (columnar_policy = 'enabled')",
		"CREATE INDEX IX ON Ignored(Id) OPTIONS (columnar_policy = 'enabled')",
		"CREATE SEARCH INDEX SI ON Ignored(Tokens)",
		"CREATE VECTOR INDEX VI ON Ignored(Embedding) WHERE Embedding IS NOT NULL OPTIONS (distance_type = 'COSINE')",
		"ALTER TABLE Ignored SET OPTIONS (columnar_policy = 'enabled')",
		"ALTER TABLE Ignored ADD CONSTRAINT CK CHECK (Id > 0)",
	} {
		t.Run(excluded, func(t *testing.T) {
			for _, config := range []GeneratorConfig{{SkipTables: []string{"Ignored"}}, {TargetTables: []string{"Managed"}}} {
				for _, pair := range [][2]string{{desired, current + excluded}, {desired + excluded, current}} {
					ddls, err := GenerateIdempotentDDLs(pair[0], pair[1], config)
					require.NoError(t, err)
					require.Equal(t, []string{"ALTER TABLE Managed ADD COLUMN Name STRING(MAX)"}, ddls)
				}
			}
			_, err := ParseDDLs(excluded)
			require.Error(t, err, "standalone parsing must still reject unsupported statements")
		})
	}
	ddls, err := GenerateIdempotentDDLs(desired, current+"CREATE SEQUENCE S OPTIONS (sequence_kind = 'bit_reversed_positive')", GeneratorConfig{TargetTables: []string{"Managed"}})
	require.ErrorContains(t, err, "unsupported")
	require.Empty(t, ddls)
}
