package spannerdef

import (
	"github.com/cloudspannerecosystem/memefish"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

var objectDefinitions = []string{
	`CREATE SEQUENCE S OPTIONS (sequence_kind = 'bit_reversed_positive')`,
	`CREATE VIEW V SQL SECURITY INVOKER AS SELECT T.Id, T.Name FROM T`,
	`CREATE SEARCH INDEX SI ON T(Tokens) STORING (Name)`,
	`CREATE VECTOR INDEX VI ON T(Embedding) WHERE Embedding IS NOT NULL OPTIONS (distance_type = 'COSINE')`,
	`CREATE CHANGE STREAM C FOR T(Name) OPTIONS (retention_period = '7d')`,
	`CREATE PROPERTY GRAPH G NODE TABLES (T)`,
}

const objectTable = `CREATE TABLE T (Id INT64 NOT NULL, Name STRING(MAX), Extra STRING(MAX), Embedding ARRAY<FLOAT32>(vector_length=>3), Tokens TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN) PRIMARY KEY (Id);`

func TestSchemaObjectsLifecycle(t *testing.T) {
	for _, ddl := range objectDefinitions {
		t.Run(ddl, func(t *testing.T) {
			desired := objectTable + ddl
			plan, err := GenerateIdempotentDDLs(desired, "", GeneratorConfig{})
			require.NoError(t, err)
			require.Len(t, plan, 2)
			for _, sql := range plan {
				_, err := memefish.ParseDDL("", sql)
				require.NoError(t, err)
			}
			replay, err := GenerateIdempotentDDLs(desired, strings.Join(plan, ";"), GeneratorConfig{})
			require.NoError(t, err)
			require.Empty(t, replay)
			drops, err := GenerateIdempotentDDLs("", desired, GeneratorConfig{})
			require.NoError(t, err)
			protected, err := planDDLs(drops, false)
			require.NoError(t, err)
			for _, step := range protected {
				require.True(t, step.Skip, step.SQL)
			}
			_, err = planDDLs(drops, true)
			require.NoError(t, err)
		})
	}
}
func TestSchemaObjectDependencyOrdering(t *testing.T) {
	desired := `CREATE VIEW A SQL SECURITY INVOKER AS SELECT Id FROM Z;
 CREATE VIEW Z SQL SECURITY INVOKER AS SELECT Id FROM T;
 CREATE PROPERTY GRAPH G NODE TABLES (A KEY(Id) PROPERTIES(Id));
 CREATE TABLE T (Id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE S))) PRIMARY KEY(Id);
 CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive');`
	plan, err := GenerateIdempotentDDLs(desired, "", GeneratorConfig{})
	require.NoError(t, err)
	require.Len(t, plan, 5)
	for i, prefix := range []string{"CREATE SEQUENCE S", "CREATE TABLE T", "CREATE VIEW Z", "CREATE VIEW A", "CREATE PROPERTY GRAPH G"} {
		require.True(t, strings.HasPrefix(plan[i], prefix), plan)
	}
	drops, err := GenerateIdempotentDDLs("", desired, GeneratorConfig{})
	require.NoError(t, err)
	require.Equal(t, []string{"DROP PROPERTY GRAPH G", "DROP VIEW A", "DROP VIEW Z", "DROP TABLE T", "DROP SEQUENCE S"}, drops)
	_, err = GenerateIdempotentDDLs(`CREATE VIEW A SQL SECURITY INVOKER AS SELECT * FROM B; CREATE VIEW B SQL SECURITY INVOKER AS SELECT * FROM A`, "", GeneratorConfig{})
	require.ErrorContains(t, err, "cyclic")
}
func TestSchemaObjectAlterations(t *testing.T) {
	tests := []struct {
		old, next string
		want      []string
	}{
		{`CREATE SEQUENCE S BIT_REVERSED_POSITIVE SKIP RANGE 1, 10 OPTIONS(sequence_kind='bit_reversed_positive')`, `CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive', skip_range_min=1, skip_range_max=20)`, []string{`ALTER SEQUENCE S SET OPTIONS (skip_range_max = 20)`}},
		{`CREATE VIEW V SQL SECURITY INVOKER AS SELECT 1 AS N`, `CREATE VIEW V SQL SECURITY INVOKER AS SELECT 2 AS N`, []string{`CREATE OR REPLACE VIEW V SQL SECURITY INVOKER AS SELECT 2 AS N`}},
		{`CREATE CHANGE STREAM C FOR T(Name) OPTIONS(retention_period='7d')`, `CREATE CHANGE STREAM C FOR T(Extra) OPTIONS(retention_period='3d')`, []string{`ALTER CHANGE STREAM C SET FOR T(Extra)`, `ALTER CHANGE STREAM C SET OPTIONS (retention_period = "3d")`}},
		{`CREATE CHANGE STREAM C FOR ALL`, `CREATE CHANGE STREAM C`, []string{`ALTER CHANGE STREAM C DROP FOR ALL`}},
		{`CREATE PROPERTY GRAPH G NODE TABLES(T)`, `CREATE PROPERTY GRAPH G NODE TABLES(T LABEL Entity)`, []string{`CREATE OR REPLACE PROPERTY GRAPH G NODE TABLES (T KEY (Id) LABEL Entity PROPERTIES (Embedding, Extra, Id, Name, Tokens))`}},
		{`CREATE SEARCH INDEX SI ON T(Tokens)`, `CREATE SEARCH INDEX SI ON T(Tokens) STORING(Name)`, []string{`DROP SEARCH INDEX SI`, `CREATE SEARCH INDEX SI ON T(Tokens) STORING (Name)`}},
	}
	for _, tc := range tests {
		t.Run(tc.next, func(t *testing.T) {
			plan, err := GenerateIdempotentDDLs(objectTable+tc.next, objectTable+tc.old, GeneratorConfig{})
			require.NoError(t, err)
			require.Equal(t, tc.want, plan)
		})
	}
}
func TestSchemaObjectProtectionAndDependencies(t *testing.T) {
	current := objectTable + `CREATE VIEW V SQL SECURITY INVOKER AS SELECT Id FROM T; CREATE VIEW W SQL SECURITY INVOKER AS SELECT Id FROM V;`
	desired := strings.Replace(current, "Extra STRING(MAX), ", "", 1)
	plan, err := GenerateIdempotentDDLs(desired, current, GeneratorConfig{})
	require.NoError(t, err)
	require.Equal(t, []string{"DROP VIEW W", "DROP VIEW V", "ALTER TABLE T DROP COLUMN Extra", "CREATE VIEW V SQL SECURITY INVOKER AS SELECT Id FROM T", "CREATE VIEW W SQL SECURITY INVOKER AS SELECT Id FROM V"}, plan)
	_, err = planDDLs(plan, false)
	require.ErrorContains(t, err, "skipped drop")
	_, err = planDDLs(plan, true)
	require.NoError(t, err)
	_, err = GenerateIdempotentDDLs(`CREATE VIEW V SQL SECURITY INVOKER AS SELECT Id FROM T`, current, GeneratorConfig{})
	require.ErrorContains(t, err, "removed object")
	_, err = GenerateIdempotentDDLs(objectTable+`CREATE SEQUENCE T OPTIONS(sequence_kind='bit_reversed_positive')`, "", GeneratorConfig{})
	require.ErrorContains(t, err, "duplicate schema name")
}
func TestSchemaObjectEquivalentDefinitions(t *testing.T) {
	tests := [][2]string{
		{`CREATE SEQUENCE S BIT_REVERSED_POSITIVE START COUNTER WITH 10 SKIP RANGE 1, 5 OPTIONS(sequence_kind='bit_reversed_positive')`, `CREATE SEQUENCE IF NOT EXISTS S OPTIONS(skip_range_max=5, start_with_counter=10, sequence_kind='bit_reversed_positive',skip_range_min=1)`},
		{`CREATE VIEW V SQL SECURITY INVOKER AS SELECT 1 AS N`, `CREATE OR REPLACE VIEW V SQL SECURITY INVOKER AS SELECT 1 AS N`},
		{`CREATE CHANGE STREAM C FOR T(Name,Extra) OPTIONS(exclude_insert=true,retention_period='3d')`, `CREATE CHANGE STREAM C FOR T(Extra,Name) OPTIONS(retention_period='3d',exclude_insert=true)`},
	}
	for _, tc := range tests {
		plan, err := GenerateIdempotentDDLs(tc[0], tc[1], GeneratorConfig{})
		require.NoError(t, err)
		require.Empty(t, plan)
	}
}
func TestSchemaObjectCTEScope(t *testing.T) {
	ddl := `CREATE VIEW V SQL SECURITY INVOKER AS WITH T AS (SELECT Id FROM Z) SELECT * FROM T; CREATE VIEW Z SQL SECURITY INVOKER AS SELECT 1 AS Id;`
	plan, err := GenerateIdempotentDDLs(ddl, "", GeneratorConfig{})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(plan[0], "CREATE VIEW Z"), plan)
}

func TestSequenceHexSeedDoesNotResetCounter(t *testing.T) {
	plan, err := GenerateIdempotentDDLs(`CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive',start_with_counter=0xA)`, `CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive',start_with_counter=10)`, GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, plan)
}
func TestStreamEmptyColumnListIsNotAllColumns(t *testing.T) {
	plan, err := GenerateIdempotentDDLs(objectTable+`CREATE CHANGE STREAM C FOR T()`, objectTable+`CREATE CHANGE STREAM C FOR T`, GeneratorConfig{})
	require.NoError(t, err)
	require.Equal(t, []string{`ALTER CHANGE STREAM C SET FOR T()`}, plan)
}
func TestObjectDuplicateOptions(t *testing.T) {
	_, err := ParseDDLs(`CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive', start_with_counter=10, START_WITH_COUNTER=20)`)
	require.ErrorContains(t, err, "duplicate option")
}

func TestObjectCaseOnlyNamesPreserveState(t *testing.T) {
	for _, ddl := range objectDefinitions {
		original, err := memefish.ParseDDL("", ddl)
		require.NoError(t, err)
		o := schemaObject(original)
		changed := strings.Replace(ddl, " "+o.Name+" ", " "+strings.ToLower(o.Name)+" ", 1)
		plan, err := GenerateIdempotentDDLs(objectTable+changed, objectTable+ddl, GeneratorConfig{})
		require.NoError(t, err)
		require.Empty(t, plan, ddl)
	}
}
func TestGraphSourceCaseInsensitive(t *testing.T) {
	plan, err := GenerateIdempotentDDLs(objectTable+`CREATE PROPERTY GRAPH G NODE TABLES(t)`, objectTable+`CREATE PROPERTY GRAPH G NODE TABLES(T)`, GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, plan)
}
func TestStreamAllowsSafeColumnChanges(t *testing.T) {
	for _, tracking := range []string{"T(Name)", "T", "T()"} {
		initial := objectTable + `CREATE CHANGE STREAM C FOR ` + tracking
		for _, desired := range []string{strings.Replace(initial, "Extra STRING(MAX), ", "", 1), strings.Replace(initial, "Extra STRING(MAX)", "Extra BYTES(MAX)", 1)} {
			plan, err := GenerateIdempotentDDLs(desired, initial, GeneratorConfig{})
			require.NoError(t, err)
			require.Len(t, plan, 1)
		}
	}
	// Type changes to an explicitly tracked, non-generated column are allowed.
	initial := objectTable + `CREATE CHANGE STREAM C FOR T(Extra)`
	plan, err := GenerateIdempotentDDLs(strings.Replace(initial, "Extra STRING(MAX)", "Extra BYTES(MAX)", 1), initial, GeneratorConfig{})
	require.NoError(t, err)
	require.Len(t, plan, 1)
	_, err = GenerateIdempotentDDLs(strings.Replace(initial, "Extra STRING(MAX), ", "", 1), initial, GeneratorConfig{})
	require.ErrorContains(t, err, "missing column")
}
func TestSequenceSeedOmissionDoesNotRebuildView(t *testing.T) {
	initial := `CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive',start_with_counter=10); CREATE VIEW V SQL SECURITY INVOKER AS SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE S) AS State`
	plan, err := GenerateIdempotentDDLs(strings.Replace(initial, ",start_with_counter=10", "", 1), initial, GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, plan)
}

func TestGraphQueryViewDependency(t *testing.T) {
	desired := objectTable + `CREATE VIEW A SQL SECURITY INVOKER AS SELECT q.id FROM GRAPH_TABLE(Z MATCH (n) COLUMNS(n.Id AS id)) AS q; CREATE PROPERTY GRAPH Z NODE TABLES(T)`
	plan, err := GenerateIdempotentDDLs(desired, "", GeneratorConfig{})
	require.NoError(t, err)
	require.Len(t, plan, 3)
	require.True(t, strings.HasPrefix(plan[1], "CREATE PROPERTY GRAPH Z"), plan)
	require.True(t, strings.HasPrefix(plan[2], "CREATE VIEW A"), plan)
}
func TestStreamReferenceCaseNoOp(t *testing.T) {
	plan, err := GenerateIdempotentDDLs(objectTable+`CREATE CHANGE STREAM c FOR t(name)`, objectTable+`CREATE CHANGE STREAM C FOR T(Name)`, GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, plan)
}

func TestObjectExpressionFieldCaseIsSignificant(t *testing.T) {
	cases := []string{
		`CREATE VIEW V SQL SECURITY INVOKER AS SELECT (JSON '{"Foo":1,"foo":2}').Foo AS Value`,
		`CREATE TABLE T (Id INT64 NOT NULL, J JSON) PRIMARY KEY(Id); CREATE VIEW V SQL SECURITY INVOKER AS SELECT T.J.Foo AS Value FROM T`,
		`CREATE TABLE T (Id INT64 NOT NULL, J JSON) PRIMARY KEY(Id); CREATE PROPERTY GRAPH G NODE TABLES(T PROPERTIES(J.Foo AS Value))`,
	}
	for _, initial := range cases {
		desired := strings.Replace(initial, ".Foo", ".foo", 1)
		plan, err := GenerateIdempotentDDLs(desired, initial, GeneratorConfig{})
		require.NoError(t, err)
		require.Len(t, plan, 1)
		require.Contains(t, plan[0], "CREATE OR REPLACE")
	}
}

func TestObjectAlterationsUseExistingNameSpelling(t *testing.T) {
	cases := []struct{ current, desired, prefix string }{
		{`CREATE SEQUENCE S OPTIONS(sequence_kind='bit_reversed_positive',skip_range_min=1,skip_range_max=10)`, `CREATE SEQUENCE s OPTIONS(sequence_kind='bit_reversed_positive',skip_range_min=1,skip_range_max=20)`, `ALTER SEQUENCE S`},
		{`CREATE CHANGE STREAM C FOR ALL OPTIONS(retention_period='7d')`, `CREATE CHANGE STREAM c FOR ALL OPTIONS(retention_period='3d')`, `ALTER CHANGE STREAM C`},
		{`CREATE VIEW V SQL SECURITY INVOKER AS SELECT 1 AS N`, `CREATE VIEW v SQL SECURITY INVOKER AS SELECT 2 AS N`, `CREATE OR REPLACE VIEW V`},
	}
	for _, tc := range cases {
		plan, err := GenerateIdempotentDDLs(tc.desired, tc.current, GeneratorConfig{})
		require.NoError(t, err)
		require.Len(t, plan, 1)
		require.True(t, strings.HasPrefix(plan[0], tc.prefix), plan)
	}
}
