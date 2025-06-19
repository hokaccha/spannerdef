package spannerdef

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instanceadmin "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// getTestConfig returns the test database configuration for Spanner emulator
func getTestConfig(t *testing.T) Config {
	t.Helper()

	// Check if running against Spanner emulator
	emulatorHost := getEnvOrDefault("SPANNER_EMULATOR_HOST", "localhost:9010")

	// Verify emulator is actually running by checking if host is reachable
	if !isEmulatorRunning(emulatorHost) {
		t.Skip("Spanner emulator tests require emulator. Please start it with: docker-compose up -d")
	}

	// Set up environment variables for emulator
	setupEmulatorEnvironment(emulatorHost)

	// Default values for emulator
	projectID := getEnvOrDefault("SPANNER_PROJECT_ID", "test-project")
	instanceID := getEnvOrDefault("SPANNER_INSTANCE_ID", "test-instance")
	databaseID := getEnvOrDefault("SPANNER_DATABASE_ID", "test-database")

	// Ensure instance and database exist
	ensureInstanceAndDatabase(t, projectID, instanceID, databaseID)

	return Config{
		ProjectID:  projectID,
		InstanceID: instanceID,
		DatabaseID: databaseID,
	}
}

// getEnvOrDefault returns the environment variable value or a default value
func getEnvOrDefault(envVar, defaultValue string) string {
	if value := os.Getenv(envVar); value != "" {
		return value
	}
	return defaultValue
}

// isEmulatorRunning checks if Spanner emulator is running on the given host
func isEmulatorRunning(host string) bool {
	conn, err := net.DialTimeout("tcp", host, 1*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// setupEmulatorEnvironment sets up environment variables for emulator
func setupEmulatorEnvironment(emulatorHost string) {
	// Set SPANNER_EMULATOR_HOST if not already set
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		os.Setenv("SPANNER_EMULATOR_HOST", emulatorHost)
	}

	// Set other emulator-related environment variables
	if os.Getenv("CLOUDSDK_API_ENDPOINT_OVERRIDES_SPANNER") == "" {
		os.Setenv("CLOUDSDK_API_ENDPOINT_OVERRIDES_SPANNER", "http://localhost:9020/")
	}
	if os.Getenv("SPANNER_EMULATOR_HOST_REST") == "" {
		os.Setenv("SPANNER_EMULATOR_HOST_REST", "localhost:9020")
	}
	if os.Getenv("CLOUDSDK_CORE_PROJECT") == "" {
		os.Setenv("CLOUDSDK_CORE_PROJECT", "test-project")
	}
	if os.Getenv("CLOUDSDK_AUTH_DISABLE_CREDENTIALS") == "" {
		os.Setenv("CLOUDSDK_AUTH_DISABLE_CREDENTIALS", "true")
	}
}

// ensureInstanceAndDatabase creates instance and database if they don't exist
func ensureInstanceAndDatabase(t *testing.T, projectID, instanceID, databaseID string) {
	t.Helper()

	ctx := context.Background()

	// Create instance if it doesn't exist
	if !instanceExists(t, ctx, projectID, instanceID) {
		createInstance(t, ctx, projectID, instanceID)
	}

	// Create database if it doesn't exist
	if !databaseExists(t, ctx, projectID, instanceID, databaseID) {
		createDatabase(t, ctx, projectID, instanceID, databaseID)
	}
}

// Test-only helper functions for instance management
func createInstanceAdminClient(ctx context.Context) (*instanceadmin.InstanceAdminClient, error) {
	return instanceadmin.NewInstanceAdminClient(ctx)
}

// Helper functions for instance and database management (test-only)
func instanceExists(t *testing.T, ctx context.Context, projectID, instanceID string) bool {
	t.Helper()

	instanceAdminClient, err := createInstanceAdminClient(ctx)
	if err != nil {
		t.Logf("Failed to create instance admin client: %v", err)
		return false
	}
	defer instanceAdminClient.Close()

	instancePath := "projects/" + projectID + "/instances/" + instanceID
	_, err = instanceAdminClient.GetInstance(ctx, &instancepb.GetInstanceRequest{
		Name: instancePath,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			t.Logf("Instance %s does not exist", instanceID)
			return false
		}
		t.Logf("Error checking instance existence: %v", err)
		return false
	}
	t.Logf("Instance %s exists", instanceID)
	return true
}

func createInstance(t *testing.T, ctx context.Context, projectID, instanceID string) {
	t.Helper()

	t.Logf("Creating Spanner instance: %s", instanceID)

	instanceAdminClient, err := createInstanceAdminClient(ctx)
	require.NoError(t, err)
	defer instanceAdminClient.Close()

	instancePath := "projects/" + projectID + "/instances/" + instanceID
	parentPath := "projects/" + projectID

	op, err := instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     parentPath,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Name:        instancePath,
			Config:      "projects/" + projectID + "/instanceConfigs/emulator-config",
			DisplayName: "Test instance for integration tests",
			NodeCount:   1,
		},
	})
	require.NoError(t, err)
	_, err = op.Wait(ctx)
	require.NoError(t, err)

	t.Logf("Instance %s created successfully", instanceID)
}

func databaseExists(t *testing.T, ctx context.Context, projectID, instanceID, databaseID string) bool {
	t.Helper()

	adminDB, err := NewAdminDatabase(Config{
		ProjectID:  projectID,
		InstanceID: instanceID,
		DatabaseID: databaseID,
	})
	if err != nil {
		t.Logf("Failed to create admin client: %v", err)
		return false
	}
	defer adminDB.Close()

	databasePath := "projects/" + projectID + "/instances/" + instanceID + "/databases/" + databaseID
	_, err = adminDB.DatabaseAdminClient().GetDatabase(ctx, &databasepb.GetDatabaseRequest{
		Name: databasePath,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			t.Logf("Database %s does not exist", databaseID)
			return false
		}
		t.Logf("Error checking database existence: %v", err)
		return false
	}
	t.Logf("Database %s exists", databaseID)
	return true
}

func createDatabase(t *testing.T, ctx context.Context, projectID, instanceID, databaseID string) {
	t.Helper()

	t.Logf("Creating Spanner database: %s", databaseID)

	adminDB, err := NewAdminDatabase(Config{
		ProjectID:  projectID,
		InstanceID: instanceID,
		DatabaseID: databaseID,
	})
	require.NoError(t, err)
	defer adminDB.Close()

	instancePath := "projects/" + projectID + "/instances/" + instanceID
	createStatement := "CREATE DATABASE `" + databaseID + "`"

	op, err := adminDB.DatabaseAdminClient().CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          instancePath,
		CreateStatement: createStatement,
	})
	require.NoError(t, err)
	_, err = op.Wait(ctx)
	require.NoError(t, err)

	t.Logf("Database %s created successfully", databaseID)
}

func TestNewDatabase(t *testing.T) {
	config := getTestConfig(t)

	db, err := NewDatabase(config)
	require.NoError(t, err)
	assert.NotNil(t, db)
	assert.Equal(t, config.ProjectID, db.projectID)
	assert.Equal(t, config.InstanceID, db.instanceID)
	assert.Equal(t, config.DatabaseID, db.databaseID)

	err = db.Close()
	assert.NoError(t, err)
}

func TestSpannerDatabase_DumpDDLs(t *testing.T) {
	config := getTestConfig(t)

	db, err := NewDatabase(config)
	require.NoError(t, err)
	defer db.Close()

	// Test dumping empty database
	ddls, err := db.DumpDDLs()
	require.NoError(t, err)
	// Empty database should return empty string or just semicolon
	trimmed := strings.TrimSpace(strings.Trim(ddls, ";"))
	assert.Empty(t, trimmed)
}

func TestSpannerDatabase_ExecDDL(t *testing.T) {
	config := getTestConfig(t)

	db, err := NewDatabase(config)
	require.NoError(t, err)
	defer db.Close()

	// Create a test table
	ddl := `CREATE TABLE TestTable (
		Id INT64 NOT NULL,
		Name STRING(100)
	) PRIMARY KEY (Id)`

	err = db.ExecDDL(ddl)
	require.NoError(t, err)

	// Verify table was created
	ddls, err := db.DumpDDLs()
	require.NoError(t, err)
	assert.Contains(t, ddls, "TestTable")
	assert.Contains(t, ddls, "STRING(100)")

	// Clean up
	err = db.ExecDDL("DROP TABLE TestTable")
	require.NoError(t, err)
}

func TestSpannerDatabase_ExecDDLs(t *testing.T) {
	config := getTestConfig(t)

	db, err := NewDatabase(config)
	require.NoError(t, err)
	defer db.Close()

	// Create multiple DDLs
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

	err = db.ExecDDLs(ddls)
	require.NoError(t, err)

	// Verify tables were created
	resultDDLs, err := db.DumpDDLs()
	require.NoError(t, err)
	assert.Contains(t, resultDDLs, "Users")
	assert.Contains(t, resultDDLs, "Posts")

	// Clean up
	cleanupDDLs := []string{
		"DROP TABLE Users",
		"DROP TABLE Posts",
	}
	err = db.ExecDDLs(cleanupDDLs)
	require.NoError(t, err)
}

func TestNewAdminDatabase(t *testing.T) {
	config := getTestConfig(t)

	adminDB, err := NewAdminDatabase(config)
	require.NoError(t, err)
	assert.NotNil(t, adminDB)
	assert.Equal(t, config.ProjectID, adminDB.projectID)
	assert.Equal(t, config.InstanceID, adminDB.instanceID)
	assert.Equal(t, config.DatabaseID, adminDB.databaseID)

	// Test that admin database was created successfully
	assert.NotNil(t, adminDB)

	err = adminDB.Close()
	assert.NoError(t, err)
}

func TestSpannerAdminDatabase_DatabaseLifecycle(t *testing.T) {
	config := getTestConfig(t)

	// Use a unique database name for this test
	config.DatabaseID = "test-lifecycle-db"

	adminDB, err := NewAdminDatabase(config)
	require.NoError(t, err)
	defer adminDB.Close()

	ctx := context.Background()

	// Create database
	err = adminDB.CreateDatabase(ctx)
	require.NoError(t, err)

	// Verify database exists by trying to connect to it
	db, err := NewDatabase(config)
	require.NoError(t, err)

	// Test basic operation
	ddls, err := db.DumpDDLs()
	require.NoError(t, err)
	_ = ddls // Database should be accessible

	db.Close()

	// Drop database
	err = adminDB.DropDatabase(ctx)
	require.NoError(t, err)

	// Verify database no longer exists by checking with admin client
	// Note: In emulator, connection might still work briefly after drop
	exists := databaseExists(t, ctx, config.ProjectID, config.InstanceID, config.DatabaseID)
	assert.False(t, exists, "Database should not exist after drop")
}
