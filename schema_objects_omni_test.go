package spannerdef

import (
	"github.com/stretchr/testify/require"
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
