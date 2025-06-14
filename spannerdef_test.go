package spannerdef_test

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ubie-sandbox/spannerdef"
	"github.com/ubie-sandbox/spannerdef/database"
	"github.com/ubie-sandbox/spannerdef/database/spanner"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestCase represents a single integration test case
type TestCase struct {
	Name     string
	Current  string // Current schema state
	Desired  string // Desired schema state
	Expected string // Expected DDL output (empty means no changes expected)
}

// getTestConfig returns the test database configuration for Spanner emulator
func getTestConfig(t *testing.T) database.Config {
	// Check if running against Spanner emulator
	emulatorHost := getEnvOrDefault("SPANNER_EMULATOR_HOST", "localhost:9010")

	// Verify emulator is actually running by checking if host is reachable
	if !isEmulatorRunning(emulatorHost) {
		t.Skipf("Integration tests require Spanner emulator. Please start it with: docker-compose up -d")
	}

	// Set up environment variables for emulator
	setupEmulatorEnvironment(emulatorHost)

	// Default values for emulator (can be overridden by environment variables)
	projectID := getEnvOrDefault("SPANNER_PROJECT_ID", "test-project")
	instanceID := getEnvOrDefault("SPANNER_INSTANCE_ID", "test-instance")
	databaseID := getEnvOrDefault("SPANNER_DATABASE_ID", "test-database")

	// Ensure instance and database exist
	ensureInstanceAndDatabase(t, projectID, instanceID, databaseID)

	t.Logf("Running integration tests against Spanner emulator (host: %s, project: %s)", emulatorHost, projectID)

	return database.Config{
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

// instanceExists checks if a Spanner instance exists using Admin SDK
func instanceExists(t *testing.T, ctx context.Context, projectID, instanceID string) bool {
	t.Helper()

	// Create the admin database client for instance operations
	adminDB, err := spanner.NewAdminDatabase(database.Config{
		ProjectID:  projectID,
		InstanceID: instanceID,
		DatabaseID: "dummy", // Not used for instance operations
	})
	if err != nil {
		t.Logf("Failed to create admin client: %v", err)
		return false
	}
	defer adminDB.Close()

	// Try to get the instance
	instancePath := "projects/" + projectID + "/instances/" + instanceID
	_, err = adminDB.InstanceAdminClient().GetInstance(ctx, &instancepb.GetInstanceRequest{
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

// createInstance creates a Spanner instance using Admin SDK
func createInstance(t *testing.T, ctx context.Context, projectID, instanceID string) {
	t.Helper()

	t.Logf("Creating Spanner instance: %s", instanceID)

	// Create the admin database client for instance operations
	adminDB, err := spanner.NewAdminDatabase(database.Config{
		ProjectID:  projectID,
		InstanceID: instanceID,
		DatabaseID: "dummy", // Not used for instance operations
	})
	require.NoError(t, err)
	defer adminDB.Close()

	// Create the instance
	instancePath := "projects/" + projectID + "/instances/" + instanceID
	parentPath := "projects/" + projectID

	op, err := adminDB.InstanceAdminClient().CreateInstance(ctx, &instancepb.CreateInstanceRequest{
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

	// Wait for the operation to complete
	_, err = op.Wait(ctx)
	require.NoError(t, err)

	t.Logf("Instance %s created successfully", instanceID)
}

// databaseExists checks if a Spanner database exists using Admin SDK
func databaseExists(t *testing.T, ctx context.Context, projectID, instanceID, databaseID string) bool {
	t.Helper()

	// Create the admin database client
	adminDB, err := spanner.NewAdminDatabase(database.Config{
		ProjectID:  projectID,
		InstanceID: instanceID,
		DatabaseID: databaseID,
	})
	if err != nil {
		t.Logf("Failed to create admin client: %v", err)
		return false
	}
	defer adminDB.Close()

	// Try to get the database
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

// createDatabase creates a Spanner database using Admin SDK
func createDatabase(t *testing.T, ctx context.Context, projectID, instanceID, databaseID string) {
	t.Helper()

	t.Logf("Creating Spanner database: %s", databaseID)

	// Create the admin database client
	adminDB, err := spanner.NewAdminDatabase(database.Config{
		ProjectID:  projectID,
		InstanceID: instanceID,
		DatabaseID: databaseID,
	})
	require.NoError(t, err)
	defer adminDB.Close()

	// Create the database
	instancePath := "projects/" + projectID + "/instances/" + instanceID
	createStatement := "CREATE DATABASE `" + databaseID + "`"

	op, err := adminDB.DatabaseAdminClient().CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          instancePath,
		CreateStatement: createStatement,
	})
	require.NoError(t, err)

	// Wait for the operation to complete
	_, err = op.Wait(ctx)
	require.NoError(t, err)

	t.Logf("Database %s created successfully", databaseID)
}

// recreateDatabase drops and recreates the test database for a clean state
func recreateDatabase(t *testing.T, config database.Config) database.Database {
	// For emulator, we can recreate the database for each test
	adminDB, err := spanner.NewAdminDatabase(config)
	require.NoError(t, err)
	defer adminDB.Close()

	// Drop and recreate database
	ctx := context.Background()
	err = adminDB.DropDatabase(ctx)
	if err != nil && !strings.Contains(err.Error(), "not found") {
		t.Logf("Warning: Could not drop database: %v", err)
	}

	err = adminDB.CreateDatabase(ctx)
	require.NoError(t, err, "Failed to create test database")

	// Wait a moment for database to be ready
	time.Sleep(100 * time.Millisecond)

	// Return regular database connection
	db, err := spanner.NewDatabase(config)
	require.NoError(t, err)
	return db
}

// applySchema applies the given schema and returns the generated DDLs
func applySchema(t *testing.T, db database.Database, schema string, enableDrop bool) []string {
	t.Helper()

	// Generate DDLs
	currentDDLs, err := db.DumpDDLs()
	require.NoError(t, err)

	ddls, err := spannerdef.GenerateIdempotentDDLs(schema, currentDDLs, database.GeneratorConfig{})
	require.NoError(t, err)

	// Apply the DDLs
	if len(ddls) > 0 {
		err = database.RunDDLs(db, ddls, enableDrop, true)
		require.NoError(t, err)
	}

	return ddls
}

// assertDDLContains checks if the DDL list contains a statement matching the pattern
func assertDDLContains(t *testing.T, ddls []string, pattern string) {
	t.Helper()
	for _, ddl := range ddls {
		if strings.Contains(strings.ToUpper(ddl), strings.ToUpper(pattern)) {
			return
		}
	}
	t.Errorf("Expected DDL containing '%s', got: %v", pattern, ddls)
}

// assertDDLNotContains checks if the DDL list does not contain a statement matching the pattern
func assertDDLNotContains(t *testing.T, ddls []string, pattern string) {
	t.Helper()
	for _, ddl := range ddls {
		if strings.Contains(strings.ToUpper(ddl), strings.ToUpper(pattern)) {
			t.Errorf("Unexpected DDL containing '%s': %s", pattern, ddl)
		}
	}
}

// TestBasicOperations tests basic DDL operations
func TestBasicOperations(t *testing.T) {
	config := getTestConfig(t)

	t.Run("CreateTable", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100),
				Email STRING(255)
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE Users")

		// Verify idempotency
		ddls = applySchema(t, db, schema, false)
		assert.Empty(t, ddls, "Second application should be idempotent")

		// Verify table exists in dump
		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)
		assert.Contains(t, currentDDLs, "Users")
	})

	t.Run("AddColumn", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		// Initial schema
		initialSchema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);
		`
		applySchema(t, db, initialSchema, false)

		// Add column
		updatedSchema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100),
				Email STRING(255),
				CreatedAt TIMESTAMP
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, updatedSchema, false)
		assertDDLContains(t, ddls, "ADD COLUMN Email")
		assertDDLContains(t, ddls, "ADD COLUMN CreatedAt")
	})

	t.Run("DropColumn", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		// Initial schema with extra column
		initialSchema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100),
				Email STRING(255),
				Temporary STRING(50)
			) PRIMARY KEY (Id);
		`
		applySchema(t, db, initialSchema, false)

		// Remove column
		updatedSchema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100),
				Email STRING(255)
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, updatedSchema, true) // Enable drop
		assertDDLContains(t, ddls, "DROP COLUMN Temporary")
	})

	t.Run("CreateIndex", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100),
				Email STRING(255)
			) PRIMARY KEY (Id);

			CREATE INDEX IdxUsersName ON Users (Name);
			CREATE UNIQUE INDEX IdxUsersEmail ON Users (Email);
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE Users")
		assertDDLContains(t, ddls, "CREATE INDEX IdxUsersName")
		assertDDLContains(t, ddls, "CREATE UNIQUE INDEX IdxUsersEmail")
	})

	t.Run("DropIndex", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		// Initial schema with index
		initialSchema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100),
				Email STRING(255)
			) PRIMARY KEY (Id);

			CREATE INDEX IdxUsersName ON Users (Name);
			CREATE INDEX IdxUsersEmail ON Users (Email);
		`
		applySchema(t, db, initialSchema, false)

		// Remove one index
		updatedSchema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100),
				Email STRING(255)
			) PRIMARY KEY (Id);

			CREATE INDEX IdxUsersName ON Users (Name);
		`

		ddls := applySchema(t, db, updatedSchema, true) // Enable drop
		assertDDLContains(t, ddls, "DROP INDEX IdxUsersEmail")
		assertDDLNotContains(t, ddls, "DROP INDEX IdxUsersName")
	})

	t.Run("DropTable", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		// Initial schema with multiple tables
		initialSchema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);

			CREATE TABLE Posts (
				Id INT64 NOT NULL,
				Title STRING(255)
			) PRIMARY KEY (Id);
		`
		applySchema(t, db, initialSchema, false)

		// Remove one table
		updatedSchema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, updatedSchema, true) // Enable drop
		assertDDLContains(t, ddls, "DROP TABLE Posts")
		assertDDLNotContains(t, ddls, "DROP TABLE Users")
	})
}

// TestSpannerSpecificFeatures tests Spanner-specific DDL features
func TestSpannerSpecificFeatures(t *testing.T) {
	config := getTestConfig(t)

	t.Run("ArrayColumns", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100),
				Tags ARRAY<STRING(50)>,
				Scores ARRAY<INT64>
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE Users")

		// Verify array columns in dump
		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)
		assert.Contains(t, currentDDLs, "ARRAY<STRING")
		assert.Contains(t, currentDDLs, "ARRAY<INT64>")
	})

	t.Run("CommitTimestamp", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Events (
				Id INT64 NOT NULL,
				Name STRING(100),
				CreatedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true)
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE Events")

		// Note: Spanner emulator may not preserve OPTIONS in DDL dump
		// This is an emulator limitation, not a spannerdef issue
	})

	t.Run("IndexWithStoring", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100),
				Email STRING(255),
				Age INT64
			) PRIMARY KEY (Id);

			CREATE INDEX IdxUsersName ON Users (Name) STORING (Email, Age);
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE INDEX IdxUsersName")

		// Verify STORING clause in dump
		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)
		assert.Contains(t, currentDDLs, "STORING")
	})

	t.Run("InterleavedTables", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);

			CREATE TABLE Posts (
				UserId INT64 NOT NULL,
				PostId INT64 NOT NULL,
				Title STRING(255)
			) PRIMARY KEY (UserId, PostId),
			INTERLEAVE IN PARENT Users ON DELETE CASCADE;
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE Users")
		assertDDLContains(t, ddls, "CREATE TABLE Posts")

		// Note: Spanner emulator may not preserve INTERLEAVE clauses in DDL dump
		// This is an emulator limitation, not a spannerdef issue
	})
}

// TestEdgeCases tests edge cases and error conditions
func TestEdgeCases(t *testing.T) {
	config := getTestConfig(t)

	t.Run("EmptySchema", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		ddls := applySchema(t, db, "", false)
		assert.Empty(t, ddls, "Empty schema should generate no DDLs")
	})

	t.Run("NoChanges", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);
		`

		// Apply schema twice
		applySchema(t, db, schema, false)
		ddls := applySchema(t, db, schema, false)
		assert.Empty(t, ddls, "Identical schema should generate no DDLs")
	})

	t.Run("DropWithoutEnableDrop", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		// Initial schema
		initialSchema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100),
				Email STRING(255)
			) PRIMARY KEY (Id);
		`
		applySchema(t, db, initialSchema, false)

		// Remove column without enabling drop
		updatedSchema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);
		`

		// Generate DDLs but check that DROP statements are skipped during execution
		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)

		ddls, err := spannerdef.GenerateIdempotentDDLs(updatedSchema, currentDDLs, database.GeneratorConfig{})
		require.NoError(t, err)

		// DDLs should be generated but DROP COLUMN should be skipped when executed
		assertDDLContains(t, ddls, "DROP COLUMN")

		// Apply with enableDrop=false - this should skip the DROP COLUMN
		err = database.RunDDLs(db, ddls, false, true)
		require.NoError(t, err)

		// Verify the column is still there (wasn't dropped)
		afterDDLs, err := db.DumpDDLs()
		require.NoError(t, err)
		assert.Contains(t, afterDDLs, "Email", "Email column should still exist after skipped DROP")
	})

	t.Run("ComplexColumnTypes", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE ComplexTypes (
				Id INT64 NOT NULL,
				BigString STRING(MAX),
				BigBytes BYTES(MAX),
				FloatVal FLOAT64,
				BoolVal BOOL,
				DateVal DATE,
				TimestampVal TIMESTAMP,
				JsonVal JSON,
				ArrayOfJson ARRAY<JSON>
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE ComplexTypes")

		// Verify complex types in dump
		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)
		assert.Contains(t, currentDDLs, "STRING(MAX)")
		assert.Contains(t, currentDDLs, "BYTES(MAX)")
		assert.Contains(t, currentDDLs, "FLOAT64")
		assert.Contains(t, currentDDLs, "JSON")
	})
}

// TestConcurrentOperations tests behavior with multiple tables and indexes
func TestConcurrentOperations(t *testing.T) {
	config := getTestConfig(t)

	t.Run("MultipleTablesAndIndexes", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100),
				Email STRING(255)
			) PRIMARY KEY (Id);

			CREATE TABLE Posts (
				Id INT64 NOT NULL,
				UserId INT64 NOT NULL,
				Title STRING(255),
				Content STRING(MAX),
				CreatedAt TIMESTAMP
			) PRIMARY KEY (Id);

			CREATE TABLE Comments (
				Id INT64 NOT NULL,
				PostId INT64 NOT NULL,
				UserId INT64 NOT NULL,
				Content STRING(MAX),
				CreatedAt TIMESTAMP
			) PRIMARY KEY (Id);

			CREATE INDEX IdxUsersEmail ON Users (Email);
			CREATE INDEX IdxPostsUserId ON Posts (UserId);
			CREATE INDEX IdxPostsCreatedAt ON Posts (CreatedAt);
			CREATE INDEX IdxCommentsPostId ON Comments (PostId);
			CREATE INDEX IdxCommentsUserId ON Comments (UserId);
		`

		ddls := applySchema(t, db, schema, false)

		// Verify all tables and indexes are created
		assert.GreaterOrEqual(t, len(ddls), 8, "Should have DDLs for 3 tables and 5 indexes")

		// Verify idempotency with complex schema
		ddls = applySchema(t, db, schema, false)
		assert.Empty(t, ddls, "Complex schema should be idempotent")
	})
}

// TestExport tests the export functionality
func TestExport(t *testing.T) {
	config := getTestConfig(t)

	t.Run("ExportEmptyDatabase", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)
		// Spanner emulator may return ";" for empty database
		trimmed := strings.TrimSpace(strings.Trim(currentDDLs, ";"))
		assert.Empty(t, trimmed, "Empty database should export empty schema")
	})

	t.Run("ExportWithTables", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);

			CREATE INDEX IdxUsersName ON Users (Name);
		`

		applySchema(t, db, schema, false)

		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)
		assert.Contains(t, currentDDLs, "CREATE TABLE Users")
		assert.Contains(t, currentDDLs, "CREATE INDEX IdxUsersName")
	})
}

// TestConfigFiltering tests table filtering functionality
func TestConfigFiltering(t *testing.T) {
	config := getTestConfig(t)

	t.Run("TargetTables", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);

			CREATE TABLE Posts (
				Id INT64 NOT NULL,
				Title STRING(255)
			) PRIMARY KEY (Id);
		`

		// Apply with target table filter
		options := &spannerdef.Options{
			DesiredDDLs: schema,
			Config: database.GeneratorConfig{
				TargetTables: []string{"Users"},
			},
		}

		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)

		ddls, err := spannerdef.GenerateIdempotentDDLs(schema, currentDDLs, options.Config)
		require.NoError(t, err)

		// Should only create Users table
		assertDDLContains(t, ddls, "CREATE TABLE Users")
		assertDDLNotContains(t, ddls, "CREATE TABLE Posts")
	})

	t.Run("SkipTables", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);

			CREATE TABLE Posts (
				Id INT64 NOT NULL,
				Title STRING(255)
			) PRIMARY KEY (Id);
		`

		// Apply with skip table filter
		options := &spannerdef.Options{
			DesiredDDLs: schema,
			Config: database.GeneratorConfig{
				SkipTables: []string{"Posts"},
			},
		}

		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)

		ddls, err := spannerdef.GenerateIdempotentDDLs(schema, currentDDLs, options.Config)
		require.NoError(t, err)

		// Should only create Users table
		assertDDLContains(t, ddls, "CREATE TABLE Users")
		assertDDLNotContains(t, ddls, "CREATE TABLE Posts")
	})
}
