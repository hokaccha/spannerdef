package spannerdef

import (
	"context"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// getTestConfig returns the base Config used by every integration test.
// SPANNER_EMULATOR_HOST must point at a Spanner Omni gRPC endpoint; each
// individual test creates its own database via newTestDatabase.
func getTestConfig(t *testing.T) Config {
	t.Helper()

	host := os.Getenv("SPANNER_EMULATOR_HOST")
	if host == "" {
		t.Skip("SPANNER_EMULATOR_HOST not set; run `make omni-up && make test`")
	}

	if !isOmniRunning(host) {
		t.Skip("Spanner Omni not reachable at " + host + "; run `make omni-up`")
	}

	return Config{
		ProjectID:  getEnvOrDefault("SPANNER_PROJECT_ID", "default"),
		InstanceID: getEnvOrDefault("SPANNER_INSTANCE_ID", "default"),
		DatabaseID: getEnvOrDefault("SPANNER_DATABASE_ID", "testdb"),
	}
}

func getEnvOrDefault(envVar, defaultValue string) string {
	if value := os.Getenv(envVar); value != "" {
		return value
	}
	return defaultValue
}

func isOmniRunning(host string) bool {
	conn, err := net.DialTimeout("tcp", host, 1*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func databaseExists(t *testing.T, ctx context.Context, projectID, instanceID, databaseID string) bool {
	t.Helper()

	adminDB, err := NewAdminDatabase(Config{
		ProjectID:  projectID,
		InstanceID: instanceID,
		DatabaseID: databaseID,
	})
	if err != nil {
		return false
	}
	defer adminDB.Close()

	databasePath := "projects/" + projectID + "/instances/" + instanceID + "/databases/" + databaseID
	_, err = adminDB.DatabaseAdminClient().GetDatabase(ctx, &databasepb.GetDatabaseRequest{
		Name: databasePath,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false
		}
		return false
	}
	return true
}

func TestNewDatabase(t *testing.T) {
	t.Parallel()
	config := getTestConfig(t)

	db, _ := newTestDatabase(t, config)
	assert.NotNil(t, db)
	assert.Equal(t, config.ProjectID, db.projectID)
	assert.Equal(t, config.InstanceID, db.instanceID)
}

func TestSpannerDatabase_DumpDDLs(t *testing.T) {
	t.Parallel()
	config := getTestConfig(t)

	db, _ := newTestDatabase(t, config)

	ddls, err := db.DumpDDLs()
	require.NoError(t, err)
	// Empty database returns either "" or ";".
	trimmed := strings.TrimSpace(strings.Trim(ddls, ";"))
	assert.Empty(t, trimmed)
}

func TestSpannerDatabase_ExecDDL(t *testing.T) {
	t.Parallel()
	config := getTestConfig(t)

	db, _ := newTestDatabase(t, config)

	ddl := `CREATE TABLE TestTable (
		Id INT64 NOT NULL,
		Name STRING(100)
	) PRIMARY KEY (Id)`

	err := db.ExecDDL(ddl)
	require.NoError(t, err)

	ddls, err := db.DumpDDLs()
	require.NoError(t, err)
	assert.Contains(t, ddls, "TestTable")
	assert.Contains(t, ddls, "STRING(100)")

	err = db.ExecDDL("DROP TABLE TestTable")
	require.NoError(t, err)
}

func TestSpannerDatabase_ExecDDLs(t *testing.T) {
	t.Parallel()
	config := getTestConfig(t)

	db, _ := newTestDatabase(t, config)

	ddls := []string{
		`CREATE TABLE Users (
			Id INT64 NOT NULL,
			Name STRING(100)
		) PRIMARY KEY (Id)`,
		`CREATE TABLE Posts (
			Id INT64 NOT NULL,
			Title STRING(255)
		) PRIMARY KEY (Id)`,
	}

	err := db.ExecDDLs(ddls)
	require.NoError(t, err)

	resultDDLs, err := db.DumpDDLs()
	require.NoError(t, err)
	assert.Contains(t, resultDDLs, "Users")
	assert.Contains(t, resultDDLs, "Posts")
}

func TestNewAdminDatabase(t *testing.T) {
	t.Parallel()
	config := getTestConfig(t)

	adminDB, err := NewAdminDatabase(config)
	require.NoError(t, err)
	assert.NotNil(t, adminDB)
	assert.Equal(t, config.ProjectID, adminDB.projectID)
	assert.Equal(t, config.InstanceID, adminDB.instanceID)
	assert.Equal(t, config.DatabaseID, adminDB.databaseID)

	err = adminDB.Close()
	assert.NoError(t, err)
}

func TestSpannerAdminDatabase_DatabaseLifecycle(t *testing.T) {
	t.Parallel()
	config := getTestConfig(t)

	config.DatabaseID = uniqueDatabaseID()

	adminDB, err := NewAdminDatabase(config)
	require.NoError(t, err)
	defer adminDB.Close()

	ctx := context.Background()

	// Use the same fast-polling path the other test helpers use so the
	// lifecycle test doesn't sit on the generated client's 1-minute poll.
	op, err := adminDB.DatabaseAdminClient().CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          "projects/" + config.ProjectID + "/instances/" + config.InstanceID,
		CreateStatement: "CREATE DATABASE `" + config.DatabaseID + "`",
	})
	require.NoError(t, err)
	require.NoError(t, waitCreateDBOp(ctx, op))

	db, err := NewDatabase(config)
	require.NoError(t, err)
	_, err = db.DumpDDLs()
	require.NoError(t, err)
	db.Close()

	err = adminDB.DropDatabase(ctx)
	require.NoError(t, err)

	exists := databaseExists(t, ctx, config.ProjectID, config.InstanceID, config.DatabaseID)
	assert.False(t, exists, "Database should not exist after drop")
}

func TestSpannerDatabase_RowDeletionPolicy(t *testing.T) {
	t.Parallel()
	config := getTestConfig(t)

	db := recreateDatabase(t, config)

	ddl := `CREATE TABLE events (
		id INT64 NOT NULL,
		name STRING(100),
		event_date TIMESTAMP NOT NULL
	) PRIMARY KEY (id),
	ROW DELETION POLICY (OLDER_THAN(event_date, INTERVAL 30 DAY))`

	require.NoError(t, execDDLsFast(context.Background(), db.adminClient, db.databasePath, []string{ddl}))

	dumpedDDLs, err := db.DumpDDLs()
	require.NoError(t, err)
	assert.Contains(t, dumpedDDLs, "ROW DELETION POLICY")
	assert.Contains(t, dumpedDDLs, "OLDER_THAN(event_date, INTERVAL 30 DAY)")
}

func TestSpannerDatabase_RowDeletionPolicyWithInterleave(t *testing.T) {
	t.Parallel()
	config := getTestConfig(t)

	db := recreateDatabase(t, config)

	ddls := []string{
		`CREATE TABLE users (
			id INT64 NOT NULL
		) PRIMARY KEY (id)`,
		`CREATE TABLE events (
			id INT64 NOT NULL,
			event_id INT64 NOT NULL,
			event_date TIMESTAMP NOT NULL
		) PRIMARY KEY (id, event_id),
		INTERLEAVE IN PARENT users ON DELETE CASCADE,
		ROW DELETION POLICY (OLDER_THAN(event_date, INTERVAL 90 DAY))`,
	}

	require.NoError(t, execDDLsFast(context.Background(), db.adminClient, db.databasePath, ddls))

	dumpedDDLs, err := db.DumpDDLs()
	require.NoError(t, err)

	assert.Contains(t, dumpedDDLs, "INTERLEAVE IN PARENT users")
	assert.Contains(t, dumpedDDLs, "ROW DELETION POLICY")
	assert.Contains(t, dumpedDDLs, "OLDER_THAN(event_date, INTERVAL 90 DAY)")
}

func TestClientOptions(t *testing.T) {
	ctx := context.Background()
	base := option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test"}))
	plain := Config{ProjectID: "p", InstanceID: "i", DatabaseID: "d"}
	impersonated := Config{ProjectID: "p", InstanceID: "i", DatabaseID: "d", ImpersonateServiceAccount: "sa@example.iam.gserviceaccount.com"}

	t.Run("no impersonation returns no options", func(t *testing.T) {
		t.Setenv("SPANNER_EMULATOR_HOST", "")
		opts, err := clientOptions(ctx, plain, base)
		assert.NoError(t, err)
		assert.Nil(t, opts)
	})

	t.Run("impersonation returns a token source option", func(t *testing.T) {
		t.Setenv("SPANNER_EMULATOR_HOST", "")
		opts, err := clientOptions(ctx, impersonated, base)
		assert.NoError(t, err)
		assert.Len(t, opts, 1)
	})

	t.Run("the emulator ignores credentials so impersonation is skipped", func(t *testing.T) {
		t.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
		opts, err := clientOptions(ctx, impersonated, base)
		assert.NoError(t, err)
		assert.Nil(t, opts)
	})
}
