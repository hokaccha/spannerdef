package spannerdef_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ubie-sandbox/spannerdef"
	"github.com/ubie-sandbox/spannerdef/database"
	"github.com/ubie-sandbox/spannerdef/database/spanner"
)

// TestCase represents a single integration test case
type TestCase struct {
	Name     string
	Current  string // Current schema state
	Desired  string // Desired schema state
	Expected string // Expected DDL output (empty means no changes expected)
}

// getTestConfig returns the test database configuration from environment variables
func getTestConfig(t *testing.T) database.Config {
	// Skip by default to avoid accidental schema changes
	if os.Getenv("RUN_INTEGRATION_TESTS") == "" {
		t.Skip("Set RUN_INTEGRATION_TESTS=1 to run tests that modify the database")
	}

	projectID := os.Getenv("SPANNER_PROJECT_ID")
	if projectID == "" {
		t.Fatalf("SPANNER_PROJECT_ID is not set")
	}

	instanceID := os.Getenv("SPANNER_INSTANCE_ID")
	if instanceID == "" {
		t.Fatalf("SPANNER_INSTANCE_ID is not set")
	}

	databaseID := os.Getenv("SPANNER_DATABASE_ID")
	if databaseID == "" {
		t.Fatalf("SPANNER_DATABASE_ID is not set")
	}

	return database.Config{
		ProjectID:  projectID,
		InstanceID: instanceID,
		DatabaseID: databaseID,
	}
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

// TestIntegration_BasicOperations tests basic DDL operations
func TestIntegration_BasicOperations(t *testing.T) {
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

// TestIntegration_SpannerSpecificFeatures tests Spanner-specific DDL features
func TestIntegration_SpannerSpecificFeatures(t *testing.T) {
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

// TestIntegration_EdgeCases tests edge cases and error conditions
func TestIntegration_EdgeCases(t *testing.T) {
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

// TestIntegration_ConcurrentOperations tests behavior with multiple tables and indexes
func TestIntegration_ConcurrentOperations(t *testing.T) {
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

// TestIntegration_Export tests the export functionality
func TestIntegration_Export(t *testing.T) {
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

// TestIntegration_ConfigFiltering tests table filtering functionality
func TestIntegration_ConfigFiltering(t *testing.T) {
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
