package spannerdef

import (
	"strings"
	"testing"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/stretchr/testify/require"
)

const testQueue = `CREATE QUEUE Q (Id INT64 NOT NULL, Payload JSON NOT NULL) PRIMARY KEY (Id)`

func TestQueueLifecycle(t *testing.T) {
	for _, ddl := range []string{
		testQueue,
		`CREATE SCHEMA S; CREATE QUEUE IF NOT EXISTS S.Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL HIDDEN) PRIMARY KEY (Id DESC), OPTIONS (disable_send=true)`,
		`CREATE QUEUE Q (Id TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true), Payload STRING(MAX) NOT NULL) PRIMARY KEY(Id), ROW DELETION POLICY (OLDER_THAN(Id, INTERVAL 0 DAY))`,
		`CREATE QUEUE Q (Id INT64 NOT NULL AS (CAST(JSON_VALUE(Payload, '$.id') AS INT64)) STORED, Payload JSON NOT NULL) PRIMARY KEY(Id)`,
	} {
		t.Run(ddl, func(t *testing.T) {
			create, err := GenerateIdempotentDDLs(ddl, "", GeneratorConfig{})
			require.NoError(t, err)
			require.NotEmpty(t, create)
			for _, sql := range create {
				_, err := memefish.ParseDDL("", sql)
				require.NoError(t, err)
			}
			diff, err := GenerateIdempotentDDLs(ddl, strings.Join(create, ";"), GeneratorConfig{})
			require.NoError(t, err)
			require.Empty(t, diff)
			// Keep named schemas, whose removal is intentionally unsupported.
			desired := ""
			if strings.HasPrefix(ddl, "CREATE SCHEMA") {
				desired = "CREATE SCHEMA S"
			}
			drops, err := GenerateIdempotentDDLs(desired, ddl, GeneratorConfig{})
			require.NoError(t, err)
			require.Len(t, drops, 1)
			require.True(t, strings.HasPrefix(drops[0], "DROP QUEUE "))
			protected, err := planDDLs(drops, false)
			require.NoError(t, err)
			require.True(t, protected[0].Skip)
			enabled, err := planDDLs(drops, true)
			require.NoError(t, err)
			require.False(t, enabled[0].Skip)
		})
	}
}

func TestQueueChanges(t *testing.T) {
	ttl := `, ROW DELETION POLICY (OLDER_THAN(Id, INTERVAL 0 DAY))`
	for _, tc := range []struct {
		name, old, next string
		want            []string
	}{
		{"options", testQueue, testQueue + `, OPTIONS(disable_send=true,disable_delivery=true)`, []string{`ALTER QUEUE Q SET OPTIONS (disable_delivery = true, disable_send = true)`}},
		{"reset", testQueue + `, OPTIONS(receive_mode='PULL',disable_send=true,locality_group='fast')`, testQueue, []string{`ALTER QUEUE Q SET OPTIONS (disable_send = NULL, locality_group = NULL)`}},
		{"ttl add", testQueue, testQueue + ttl, []string{`ALTER QUEUE Q ADD ROW DELETION POLICY ( OLDER_THAN ( Id, INTERVAL 0 DAY ))`}},
		{"ttl drop", testQueue + ttl, testQueue, []string{`ALTER QUEUE Q DROP ROW DELETION POLICY`}},
		{"ttl replace", testQueue + ttl, testQueue + strings.ReplaceAll(ttl, "0 DAY", "7 DAY"), []string{`ALTER QUEUE Q REPLACE ROW DELETION POLICY ( OLDER_THAN ( Id, INTERVAL 7 DAY ))`}},
		{"cascade", testQueue + `, INTERLEAVE IN PARENT P`, testQueue + `, INTERLEAVE IN PARENT P ON DELETE CASCADE`, []string{`ALTER QUEUE Q SET ON DELETE CASCADE`}},
		{"enforce", testQueue + `, INTERLEAVE IN P`, testQueue + `, INTERLEAVE IN PARENT P ON DELETE CASCADE`, []string{`ALTER QUEUE Q SET INTERLEAVE IN PARENT P ON DELETE NO ACTION`, `ALTER QUEUE Q SET ON DELETE CASCADE`}},
		{"unenforce", testQueue + `, INTERLEAVE IN PARENT P`, testQueue + `, INTERLEAVE IN P`, []string{`ALTER QUEUE Q SET INTERLEAVE IN P`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.HasPrefix(tc.name, "ttl") {
				tc.old = strings.ReplaceAll(tc.old, "Id INT64", "Id TIMESTAMP")
				tc.next = strings.ReplaceAll(tc.next, "Id INT64", "Id TIMESTAMP")
			}
			ddls, err := GenerateIdempotentDDLs(tc.next, tc.old, GeneratorConfig{})
			require.NoError(t, err)
			var want []string
			for _, sql := range tc.want {
				node, err := memefish.ParseDDL("", sql)
				require.NoError(t, err)
				want = append(want, node.SQL())
			}
			require.Equal(t, want, ddls)
			_, err = planDDLs(ddls, false)
			require.NoError(t, err)
			_, err = planDDLs(append([]string{"DROP QUEUE Other"}, ddls...), false)
			require.ErrorContains(t, err, "--enable-drop")
		})
	}
}

func TestQueueNormalization(t *testing.T) {
	exported := `CREATE QUEUE Q (Payload JSON NOT NULL, Id INT64 NOT NULL) PRIMARY KEY(Id ASC), OPTIONS (receive_mode='PULL', disable_send=false, disable_delivery=NULL, locality_group=NULL)`
	ddls, err := GenerateIdempotentDDLs(testQueue, exported, GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, ddls)
	ddls, err = GenerateIdempotentDDLs(testQueue+`, INTERLEAVE IN PARENT P`, testQueue+`, INTERLEAVE IN PARENT P ON DELETE NO ACTION`, GeneratorConfig{})
	require.NoError(t, err)
	require.Empty(t, ddls)
}

func TestQueueUnsafeChanges(t *testing.T) {
	for _, next := range []string{
		strings.ReplaceAll(testQueue, "Payload JSON", "Payload BYTES(MAX)"),
		strings.ReplaceAll(testQueue, "PRIMARY KEY (Id)", "PRIMARY KEY (Id DESC)"),
		strings.ReplaceAll(testQueue, "Id INT64", "Id STRING(MAX)"),
		testQueue + `, INTERLEAVE IN P`,
		`CREATE TABLE Q (Id INT64 NOT NULL) PRIMARY KEY(Id)`,
	} {
		ddls, err := GenerateIdempotentDDLs(next, testQueue, GeneratorConfig{})
		require.Error(t, err)
		require.Empty(t, ddls)
	}
	_, err := GenerateIdempotentDDLs(testQueue+`, OPTIONS(disable_send=true, DISABLE_SEND=false)`, "", GeneratorConfig{})
	require.ErrorContains(t, err, "duplicate option")
	for _, sql := range []string{`ALTER QUEUE Q SET OPTIONS(disable_send=true)`, `DROP QUEUE Q`} {
		_, err := GenerateIdempotentDDLs(sql, "", GeneratorConfig{})
		require.ErrorContains(t, err, "unsupported DDL")
	}
}

func TestQueueDependencies(t *testing.T) {
	parent := `CREATE TABLE P (Id INT64 NOT NULL, N INT64) PRIMARY KEY(Id);`
	queue := testQueue + `, INTERLEAVE IN PARENT P;`
	view := `CREATE VIEW V SQL SECURITY INVOKER AS SELECT Id FROM Q;`
	desired := view + queue + parent
	create, err := GenerateIdempotentDDLs(desired, "", GeneratorConfig{})
	require.NoError(t, err)
	require.Len(t, create, 3)
	for i, prefix := range []string{"CREATE TABLE P", "CREATE QUEUE Q", "CREATE VIEW V"} {
		require.True(t, strings.HasPrefix(create[i], prefix), create)
	}
	drops, err := GenerateIdempotentDDLs("", desired, GeneratorConfig{})
	require.NoError(t, err)
	require.Equal(t, []string{"DROP VIEW V", "DROP QUEUE Q", "DROP TABLE P"}, drops)
	_, err = GenerateIdempotentDDLs(queue, desired, GeneratorConfig{})
	require.ErrorContains(t, err, "still references removed object")
	_, err = GenerateIdempotentDDLs(strings.ReplaceAll(desired, "N INT64", "N STRING(MAX)"), desired, GeneratorConfig{})
	require.ErrorContains(t, err, "queue Q depends on structurally changed")
	// Table filters do not manage global queue objects.
	ddls, err := GenerateIdempotentDDLs(testQueue, "", GeneratorConfig{TargetTables: []string{"P"}})
	require.NoError(t, err)
	require.Len(t, ddls, 1)
	_, err = GenerateIdempotentDDLs(testQueue+`;CREATE TABLE C (Id INT64 NOT NULL) PRIMARY KEY(Id), INTERLEAVE IN PARENT Q`, "", GeneratorConfig{})
	require.ErrorContains(t, err, "interleaved in queue")
	nested := testQueue + `;CREATE QUEUE A (Id INT64 NOT NULL, N INT64 NOT NULL, Payload JSON NOT NULL) PRIMARY KEY(Id,N), INTERLEAVE IN PARENT Q`
	create, err = GenerateIdempotentDDLs(nested, "", GeneratorConfig{})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(create[0], "CREATE QUEUE Q"))
	drops, err = GenerateIdempotentDDLs("", nested, GeneratorConfig{})
	require.NoError(t, err)
	require.Equal(t, []string{"DROP QUEUE A", "DROP QUEUE Q"}, drops)
}

func TestQueueEnforcementBeforeParentTTL(t *testing.T) {
	parent := `CREATE TABLE P (Id TIMESTAMP NOT NULL) PRIMARY KEY(Id)`
	queue := `CREATE QUEUE Q (Id TIMESTAMP NOT NULL, Payload JSON NOT NULL) PRIMARY KEY(Id), INTERLEAVE IN `
	ttl := `, ROW DELETION POLICY(OLDER_THAN(Id,INTERVAL 7 DAY))`
	for _, tc := range []struct {
		old, next string
		prefix    []string
	}{
		{"PARENT P", "P", []string{"ALTER QUEUE Q SET INTERLEAVE IN P", "ALTER TABLE P ADD ROW DELETION POLICY"}},
		{"P", "PARENT P ON DELETE CASCADE", []string{"ALTER QUEUE Q SET INTERLEAVE IN PARENT P ON DELETE NO ACTION", "ALTER QUEUE Q SET ON DELETE CASCADE", "ALTER TABLE P ADD ROW DELETION POLICY"}},
	} {
		ddls, err := GenerateIdempotentDDLs(parent+ttl+";"+queue+tc.next, parent+";"+queue+tc.old, GeneratorConfig{})
		require.NoError(t, err)
		require.Len(t, ddls, len(tc.prefix))
		for i, prefix := range tc.prefix {
			require.True(t, strings.HasPrefix(ddls[i], prefix), ddls)
		}
	}
	_, err := GenerateIdempotentDDLs(parent+";"+queue+"PARENT P ON DELETE CASCADE", parent+ttl+";"+queue+"P", GeneratorConfig{})
	require.ErrorContains(t, err, "removing parent P TTL in a separate migration")
}

func TestQueueRejectsExistingTableChildren(t *testing.T) {
	current := testQueue + `;CREATE TABLE C (Id INT64 NOT NULL) PRIMARY KEY(Id), INTERLEAVE IN PARENT Q`
	for _, desired := range []string{"", testQueue, current} {
		ddls, err := GenerateIdempotentDDLs(desired, current, GeneratorConfig{})
		require.ErrorContains(t, err, "interleaved in queue")
		require.Empty(t, ddls)
	}
}

func TestQueueExecutionDropProtection(t *testing.T) {
	db := &recordingDatabase{}
	require.NoError(t, RunDDLs(db, []string{"DROP QUEUE Q"}, false, true))
	require.Empty(t, db.sent)
	require.NoError(t, RunDDLs(db, []string{"DROP QUEUE Q"}, true, true))
	require.Equal(t, []string{"DROP QUEUE Q"}, db.sent)
	db = &recordingDatabase{}
	require.Error(t, RunDDLs(db, []string{"DROP QUEUE Q", testQueue}, false, true))
	require.Empty(t, db.sent)
}

func TestQueueEnforcementChecksAncestorTTL(t *testing.T) {
	parent := `CREATE TABLE P (Id TIMESTAMP NOT NULL) PRIMARY KEY(Id), ROW DELETION POLICY(OLDER_THAN(Id,INTERVAL 7 DAY));`
	child := `CREATE QUEUE R (Id TIMESTAMP NOT NULL, Payload JSON NOT NULL) PRIMARY KEY(Id), INTERLEAVE IN `
	for _, middle := range []string{
		`CREATE QUEUE Q (Id TIMESTAMP NOT NULL, Payload JSON NOT NULL) PRIMARY KEY(Id), INTERLEAVE IN PARENT P ON DELETE CASCADE;`,
		`CREATE TABLE Q (Id TIMESTAMP NOT NULL) PRIMARY KEY(Id), INTERLEAVE IN PARENT P ON DELETE CASCADE;`,
	} {
		ddls, err := GenerateIdempotentDDLs(parent+middle+child+"PARENT Q ON DELETE CASCADE", parent+middle+child+"Q", GeneratorConfig{})
		require.ErrorContains(t, err, "removing parent P TTL")
		require.Empty(t, ddls)
		middle = strings.ReplaceAll(middle, "PARENT P ON DELETE CASCADE", "P")
		ddls, err = GenerateIdempotentDDLs(parent+middle+child+"PARENT Q ON DELETE CASCADE", parent+middle+child+"Q", GeneratorConfig{})
		require.NoError(t, err)
		require.Len(t, ddls, 2)
	}
}
