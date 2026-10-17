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
