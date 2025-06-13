package integration_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ubie-sandbox/spannerdef"
	"github.com/ubie-sandbox/spannerdef/database"
	"github.com/ubie-sandbox/spannerdef/database/spanner"
)

// getTestConfig returns the test database configuration from environment variables
func getTestConfig(t *testing.T) database.Config {
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

// TestIntegration_RealSpanner tests against a real Spanner database
func TestIntegration_RealSpanner(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := getTestConfig(t)

	db, err := spanner.NewDatabase(config)
	require.NoError(t, err)
	defer db.Close()

	// Test export functionality
	t.Run("Export", func(t *testing.T) {
		options := &spannerdef.Options{
			Export: true,
		}

		spannerdef.Run(db, options)
		// Just check that it doesn't panic or error
	})

	// Test dry run functionality
	t.Run("DryRun", func(t *testing.T) {
		testSchema := `
			CREATE TABLE test_users (
				id INT64 NOT NULL,
				name STRING(100),
				created_at TIMESTAMP
			) PRIMARY KEY (id);

			CREATE INDEX idx_test_name ON test_users (name);
		`

		options := &spannerdef.Options{
			DesiredDDLs: testSchema,
			DryRun:      true,
		}

		spannerdef.Run(db, options)
		// Just check that it doesn't panic or error
	})
}

// TestIntegration_SchemaChange tests actual schema changes
// CAUTION: This test makes actual changes to the database
func TestIntegration_SchemaChange(t *testing.T) {
	// Skip by default to avoid accidental schema changes
	if os.Getenv("RUN_INTEGRATION_TESTS") == "" {
		t.Skip("Set RUN_INTEGRATION_TESTS=1 to run tests that modify the database")
	}

	config := getTestConfig(t)

	db, err := spanner.NewDatabase(config)
	require.NoError(t, err)
	defer db.Close()

	// Clean up any existing test table
	t.Cleanup(func() {
		db.ExecDDL("DROP TABLE IF EXISTS integration_test_table")
	})

	t.Run("CreateTable", func(t *testing.T) {
		testSchema := `
			CREATE TABLE integration_test_table (
				id INT64 NOT NULL,
				name STRING(100)
			) PRIMARY KEY (id);
		`

		options := &spannerdef.Options{
			DesiredDDLs: testSchema,
			EnableDrop:  false, // Don't drop existing tables
		}

		spannerdef.Run(db, options)

		// Verify the table was created by exporting the schema
		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)
		assert.Contains(t, currentDDLs, "integration_test_table")
	})

	t.Run("AddColumn", func(t *testing.T) {
		testSchema := `
			CREATE TABLE integration_test_table (
				id INT64 NOT NULL,
				name STRING(100),
				email STRING(255)
			) PRIMARY KEY (id);
		`

		options := &spannerdef.Options{
			DesiredDDLs: testSchema,
			EnableDrop:  false, // Don't drop existing tables
		}

		spannerdef.Run(db, options)

		// Verify the column was added
		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)
		assert.Contains(t, currentDDLs, "email")
	})

	t.Run("CreateIndex", func(t *testing.T) {
		testSchema := `
			CREATE TABLE integration_test_table (
				id INT64 NOT NULL,
				name STRING(100),
				email STRING(255)
			) PRIMARY KEY (id);

			CREATE INDEX idx_integration_name ON integration_test_table (name);
		`

		options := &spannerdef.Options{
			DesiredDDLs: testSchema,
			EnableDrop:  false, // Don't drop existing tables
		}

		spannerdef.Run(db, options)

		// Verify the index was created
		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)
		assert.Contains(t, currentDDLs, "idx_integration_name")
	})
}
