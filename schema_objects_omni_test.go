package spannerdef

import (
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

func TestOmniSchemaObjects(t *testing.T) {
	for _, ddl := range objectDefinitions {
		t.Run(ddl, func(t *testing.T) {
			t.Parallel()
			db := recreateDatabase(t, getTestConfig(t))
			desired := objectTable + ddl
			require.NotEmpty(t, applySchema(t, db, desired, false))
			dump, err := db.DumpDDLs()
			require.NoError(t, err)
			t.Log(dump)
			require.Empty(t, applySchema(t, db, desired, false))
			require.NotEmpty(t, applySchema(t, db, "", true))
			require.Empty(t, applySchema(t, db, "", true))
		})
	}
}

func TestOmniSchemaObjectChanges(t *testing.T) {
	cases := []struct{ name, initial, desired string }{
		{"sequence", `CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive',start_with_counter=10,skip_range_min=1,skip_range_max=5)`, `CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive',start_with_counter=10,skip_range_min=1,skip_range_max=8)`},
		{"sequence_remove_range", `CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive',skip_range_min=1,skip_range_max=5)`, `CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive')`},
		{"view", `CREATE VIEW V SQL SECURITY INVOKER AS SELECT T.Id FROM T`, `CREATE VIEW V SQL SECURITY INVOKER AS SELECT T.Id, T.Name FROM T`},
		{"search", `CREATE SEARCH INDEX SI ON T(Tokens)`, `CREATE SEARCH INDEX SI ON T(Tokens) STORING(Name)`},
		{"vector", `CREATE VECTOR INDEX VI ON T(Embedding) WHERE Embedding IS NOT NULL OPTIONS(distance_type='COSINE')`, `CREATE VECTOR INDEX VI ON T(Embedding) WHERE Embedding IS NOT NULL OPTIONS(distance_type='EUCLIDEAN')`},
		{"stream", `CREATE CHANGE STREAM C FOR T(Name) OPTIONS(retention_period='7d')`, `CREATE CHANGE STREAM C FOR T(Extra) OPTIONS(retention_period='3d')`},
		{"stream_suspend", `CREATE CHANGE STREAM C FOR ALL OPTIONS(exclude_insert=true)`, `CREATE CHANGE STREAM C`},
		{"graph", `CREATE PROPERTY GRAPH G NODE TABLES(T)`, `CREATE PROPERTY GRAPH G NODE TABLES(T LABEL Entity PROPERTIES(Id, Name))`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := recreateDatabase(t, getTestConfig(t))
			applySchema(t, db, objectTable+tc.initial, false)
			require.Empty(t, applySchema(t, db, objectTable+tc.initial, false))
			require.NotEmpty(t, applySchema(t, db, objectTable+tc.desired, true))
			require.Empty(t, applySchema(t, db, objectTable+tc.desired, true))
			applySchema(t, db, "", true)
		})
	}
}

func TestOmniSchemaObjectDependencies(t *testing.T) {
	db := recreateDatabase(t, getTestConfig(t))
	initial := `CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive');
 CREATE TABLE T (Id INT64 NOT NULL DEFAULT(GET_NEXT_SEQUENCE_VALUE(SEQUENCE S)), N INT64, Extra STRING(MAX)) PRIMARY KEY(Id);
 CREATE VIEW Z SQL SECURITY INVOKER AS SELECT T.Id, T.N FROM T;
 CREATE VIEW A SQL SECURITY INVOKER AS SELECT Z.Id FROM Z;
 CREATE PROPERTY GRAPH G NODE TABLES(T);`
	applySchema(t, db, initial, false)
	require.Empty(t, applySchema(t, db, initial, false))
	desired := `CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive');
 CREATE TABLE T (Id INT64 NOT NULL DEFAULT(GET_NEXT_SEQUENCE_VALUE(SEQUENCE S)), N INT64) PRIMARY KEY(Id);
 CREATE VIEW Z SQL SECURITY INVOKER AS SELECT T.Id, T.N FROM T;
 CREATE VIEW A SQL SECURITY INVOKER AS SELECT Z.Id FROM Z;
 CREATE PROPERTY GRAPH G NODE TABLES(T);`
	require.NotEmpty(t, applySchema(t, db, desired, true))
	require.Empty(t, applySchema(t, db, desired, true))
	applySchema(t, db, "", true)
}

func TestOmniGraphEdgesAndProperties(t *testing.T) {
	if os.Getenv("SPANNER_TEST_EMULATOR") == "1" {
		// v1.5.58 PrintLabel serializes property names without expressions and
		// omits NO PROPERTIES. Omni preserves both; do not normalize away
		// actual expression changes to accommodate the emulator's lossy dump.
		// https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/blob/v1.5.58/backend/schema/printer/print_ddl.cc#L521
		t.Skip("Emulator 1.5.58 graph export loses derived expressions and NO PROPERTIES; covered by Omni")
	}
	db := recreateDatabase(t, getTestConfig(t))
	ddl := `CREATE TABLE N (Id INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(Id);
 CREATE TABLE E (Id INT64 NOT NULL, Src INT64, Dst INT64) PRIMARY KEY(Id);
 CREATE PROPERTY GRAPH G NODE TABLES(N AS Person LABEL Entity PROPERTIES(Name AS DisplayName))
 EDGE TABLES(E AS Link SOURCE KEY(Src) REFERENCES Person DESTINATION KEY(Dst) REFERENCES Person LABEL Related NO PROPERTIES);`
	applySchema(t, db, ddl, false)
	dump, err := db.DumpDDLs()
	require.NoError(t, err)
	t.Log(dump)
	require.Empty(t, applySchema(t, db, ddl, false))
	applySchema(t, db, "", true)
}

func TestOmniSearchIndexClauses(t *testing.T) {
	db := recreateDatabase(t, getTestConfig(t))
	ddl := `CREATE TABLE T (Id INT64 NOT NULL, Tenant INT64, Score INT64 NOT NULL, Name STRING(MAX), Tokens TOKENLIST AS(TOKENIZE_FULLTEXT(Name)) HIDDEN) PRIMARY KEY(Id);
 CREATE SEARCH INDEX SI ON T(Tokens) STORING(Name) PARTITION BY Tenant ORDER BY Score DESC WHERE Tenant IS NOT NULL OPTIONS(sort_order_sharding=true);`
	applySchema(t, db, ddl, false)
	require.Empty(t, applySchema(t, db, ddl, false))
	applySchema(t, db, "", true)
}

func TestOmniObjectReviewRegressions(t *testing.T) {
	db := recreateDatabase(t, getTestConfig(t))
	initial := `CREATE TABLE T (Id INT64 NOT NULL, N STRING(MAX), Extra STRING(MAX)) PRIMARY KEY(Id);
 CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive',start_with_counter=10);
 CREATE VIEW V SQL SECURITY INVOKER AS SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE S) AS State;
 CREATE CHANGE STREAM C FOR T(N);
 CREATE PROPERTY GRAPH G NODE TABLES(T);`
	applySchema(t, db, initial, false)
	desired := `CREATE TABLE T (Id INT64 NOT NULL, N STRING(MAX), Extra STRING(MAX)) PRIMARY KEY(Id);
 CREATE SEQUENCE s OPTIONS(sequence_kind='bit_reversed_positive');
 CREATE VIEW v SQL SECURITY INVOKER AS SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE s) AS State;
 CREATE CHANGE STREAM c FOR t(n);
 CREATE PROPERTY GRAPH g NODE TABLES(t);`
	require.Empty(t, applySchema(t, db, desired, false))
	// The retained stream should allow dropping an untracked column. The graph
	// is rebuilt because its default properties include that column.
	desired = strings.Replace(desired, ", Extra STRING(MAX)", "", 1)
	require.NotEmpty(t, applySchema(t, db, desired, true))
	require.Empty(t, applySchema(t, db, desired, true))
	applySchema(t, db, "", true)
}

func TestOmniGraphImplicitKey(t *testing.T) {
	db := recreateDatabase(t, getTestConfig(t))
	ddl := `CREATE TABLE T (Name STRING(MAX)); CREATE PROPERTY GRAPH G NODE TABLES(T);`
	applySchema(t, db, ddl, false)
	require.Empty(t, applySchema(t, db, ddl, false))
	applySchema(t, db, "", true)
}
