package spannerdef

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	dbadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	gax "github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// Test-only fast-polling helpers. The generated LRO op.Wait() uses a 1-minute
// polling interval, which is fine against production Spanner but makes tests
// against Spanner Omni (where each DDL batch completes in ~tens of seconds)
// wait roughly a minute per operation. We keep this behaviour out of
// production code and let tests drive their own polling.

const ddlPollInterval = 1 * time.Second

// pollCallOptions extend the per-request polling call. The default
// GetOperation timeout is 10s with retries only on Unavailable, which
// occasionally surfaces DeadlineExceeded while Omni is busy.
func pollCallOptions() []gax.CallOption {
	return []gax.CallOption{
		gax.WithTimeout(60 * time.Second),
		gax.WithRetry(func() gax.Retryer {
			return gax.OnCodes([]codes.Code{
				codes.Unavailable,
				codes.DeadlineExceeded,
			}, gax.Backoff{
				Initial:    500 * time.Millisecond,
				Max:        10 * time.Second,
				Multiplier: 2.0,
			})
		}),
	}
}

func waitDDLOp(ctx context.Context, op *dbadmin.UpdateDatabaseDdlOperation) error {
	opts := pollCallOptions()
	for !op.Done() {
		if err := op.Poll(ctx, opts...); err != nil {
			return err
		}
		if op.Done() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(ddlPollInterval):
		}
	}
	return nil
}

func waitCreateDBOp(ctx context.Context, op *dbadmin.CreateDatabaseOperation) error {
	opts := pollCallOptions()
	for !op.Done() {
		if _, err := op.Poll(ctx, opts...); err != nil {
			return err
		}
		if op.Done() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(ddlPollInterval):
		}
	}
	return nil
}

// execDDLsFast runs a batch of DDLs against databasePath using the admin
// client with a short polling interval. Used only in test setup/teardown so
// we don't wait on the generated client's 60-second default.
func execDDLsFast(ctx context.Context, admin *dbadmin.DatabaseAdminClient, databasePath string, ddls []string) error {
	if len(ddls) == 0 {
		return nil
	}
	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   databasePath,
		Statements: ddls,
	})
	if err != nil {
		return fmt.Errorf("UpdateDatabaseDdl: %v", err)
	}
	return waitDDLOp(ctx, op)
}

// Each test run produces unique database IDs so the test suite can
// parallelise without colliding, and repeat runs against the same Omni
// instance don't fight over stale databases.
var (
	testRunID      = time.Now().Unix() % 1_000_000
	testDBSequence uint64
)

func uniqueDatabaseID() string {
	n := atomic.AddUint64(&testDBSequence, 1)
	return fmt.Sprintf("td%d-%d", testRunID, n)
}

// newTestDatabase creates a fresh, empty database for a single test and
// returns both a Database handle and the underlying admin client so tests
// can apply their own DDLs with fast polling. Cleanup (close + drop) is
// registered with t.Cleanup.
func newTestDatabase(t *testing.T, base Config) (*SpannerDatabase, *SpannerAdminDatabase) {
	t.Helper()

	config := base
	config.DatabaseID = uniqueDatabaseID()

	adminDB, err := NewAdminDatabase(config)
	require.NoError(t, err)

	ctx := context.Background()
	op, err := adminDB.DatabaseAdminClient().CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          "projects/" + config.ProjectID + "/instances/" + config.InstanceID,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", config.DatabaseID),
	})
	require.NoError(t, err)
	require.NoError(t, waitCreateDBOp(ctx, op))

	db, err := NewDatabase(config)
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
		// Drop asynchronously; we don't want teardown to block the next test
		// from starting. If the drop itself errors (e.g. connection closed
		// because Omni shut down) we don't care.
		go func() {
			_ = adminDB.DropDatabase(context.Background())
			adminDB.Close()
		}()
	})

	return db, adminDB
}
