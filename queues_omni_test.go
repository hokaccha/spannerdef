package spannerdef

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOmniQueues(t *testing.T) {
	if os.Getenv("SPANNER_TEST_QUEUES") != "1" {
		t.Skip("set SPANNER_TEST_QUEUES=1 with a Queue-capable backend")
	}
	db := recreateDatabase(t, getTestConfig(t))
	base := `CREATE TABLE P (Id TIMESTAMP NOT NULL) PRIMARY KEY(Id);
 CREATE QUEUE Q (Id TIMESTAMP NOT NULL, N INT64 NOT NULL, Payload JSON NOT NULL) PRIMARY KEY(Id,N), INTERLEAVE IN PARENT P ON DELETE CASCADE`
	for _, desired := range []string{
		base,
		base + `, OPTIONS(disable_send=true,disable_delivery=true)`,
		base + `, ROW DELETION POLICY(OLDER_THAN(Id,INTERVAL 7 DAY))`,
		base + `, ROW DELETION POLICY(OLDER_THAN(Id,INTERVAL 0 DAY))`,
		base,
		"",
	} {
		require.NotEmpty(t, applySchema(t, db, desired, true))
		require.Empty(t, applySchema(t, db, desired, true))
	}
}

func TestOmniQueueInterleaveChanges(t *testing.T) {
	if os.Getenv("SPANNER_TEST_QUEUES") != "1" {
		t.Skip("set SPANNER_TEST_QUEUES=1 with a Queue-capable backend")
	}
	db := recreateDatabase(t, getTestConfig(t))
	parent := `CREATE TABLE P (Id TIMESTAMP NOT NULL) PRIMARY KEY(Id)`
	queue := `CREATE QUEUE Q (Id TIMESTAMP NOT NULL, N INT64 NOT NULL, Payload JSON NOT NULL) PRIMARY KEY(Id,N), INTERLEAVE IN `
	ttl := `, ROW DELETION POLICY(OLDER_THAN(Id,INTERVAL 7 DAY))`
	for _, desired := range []string{
		parent + ";" + queue + "P",
		parent + ttl + ";" + queue + "PARENT P ON DELETE CASCADE",
		parent + ";" + queue + "PARENT P ON DELETE NO ACTION",
		parent + ttl + ";" + queue + "P",
		parent + ";" + queue + "P",
		"",
	} {
		require.NotEmpty(t, applySchema(t, db, desired, true))
		require.Empty(t, applySchema(t, db, desired, true))
	}
}

func TestOmniQueueDefinitions(t *testing.T) {
	if os.Getenv("SPANNER_TEST_QUEUES") != "1" {
		t.Skip("set SPANNER_TEST_QUEUES=1 with a Queue-capable backend")
	}
	for _, ddl := range []string{
		`CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL HIDDEN) PRIMARY KEY(Id DESC), OPTIONS(receive_mode='PULL',disable_send=false,disable_delivery=false)`,
		`CREATE QUEUE Q (Id INT64 NOT NULL AS (CAST(JSON_VALUE(Payload, '$.id') AS INT64)) STORED, Payload JSON NOT NULL) PRIMARY KEY(Id)`,
		`CREATE QUEUE Q (Id TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp=true), Payload STRING(MAX) NOT NULL) PRIMARY KEY(Id), ROW DELETION POLICY(OLDER_THAN(Id, INTERVAL 0 DAY))`,
	} {
		t.Run(ddl, func(t *testing.T) {
			t.Parallel()
			db := recreateDatabase(t, getTestConfig(t))
			require.NotEmpty(t, applySchema(t, db, ddl, true))
			dump, err := db.DumpDDLs()
			require.NoError(t, err)
			t.Log(dump)
			require.Empty(t, applySchema(t, db, ddl, true))
			require.NotEmpty(t, applySchema(t, db, "", true))
		})
	}
}
