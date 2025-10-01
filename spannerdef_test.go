package spannerdef

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCase represents a single integration test case
type TestCase struct {
	Name     string
	Current  string // Current schema state
	Desired  string // Desired schema state
	Expected string // Expected DDL output (empty means no changes expected)
}

// recreateDatabase drops and recreates the test database for a clean state
func recreateDatabase(t *testing.T, config Config) Database {
	// For emulator, we can recreate the database for each test
	adminDB, err := NewAdminDatabase(config)
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
	db, err := NewDatabase(config)
	require.NoError(t, err)
	return db
}

// applySchema applies the given schema and returns the generated DDLs
func applySchema(t *testing.T, db Database, schema string, enableDrop bool) []string {
	t.Helper()

	// Generate DDLs
	currentDDLs, err := db.DumpDDLs()
	require.NoError(t, err)

	ddls, err := GenerateIdempotentDDLs(schema, currentDDLs, GeneratorConfig{})
	require.NoError(t, err)

	// Apply the DDLs
	if len(ddls) > 0 {
		err = RunDDLs(db, ddls, enableDrop, true)
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
				Id INT64 NOT NULL,
				PostId INT64 NOT NULL,
				Title STRING(255)
			) PRIMARY KEY (Id, PostId),
			INTERLEAVE IN PARENT Users ON DELETE CASCADE;
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE Users")
		assertDDLContains(t, ddls, "CREATE TABLE Posts")

		// Note: Spanner emulator may not preserve INTERLEAVE clauses in DDL dump
		// This is an emulator limitation, not a spannerdef issue
	})

	t.Run("InterleavedTablesOrder", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		// Test that parent table is created before child table
		// This would fail in the real Spanner if order is wrong
		schema := `
			CREATE TABLE Posts (
				Id INT64 NOT NULL,
				PostId INT64 NOT NULL,
				Title STRING(255)
			) PRIMARY KEY (Id, PostId),
			INTERLEAVE IN PARENT Users ON DELETE CASCADE;

			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, schema, false)

		// Verify that Users table DDL comes before Posts table DDL
		usersIndex := -1
		postsIndex := -1
		for i, ddl := range ddls {
			if strings.Contains(ddl, "CREATE TABLE Users") {
				usersIndex = i
			}
			if strings.Contains(ddl, "CREATE TABLE Posts") {
				postsIndex = i
			}
		}

		if usersIndex != -1 && postsIndex != -1 {
			assert.Less(t, usersIndex, postsIndex, "Users table should be created before Posts table")
		}

		assertDDLContains(t, ddls, "CREATE TABLE Users")
		assertDDLContains(t, ddls, "CREATE TABLE Posts")
	})

	t.Run("ColumnOrdering", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		// Test that column order is preserved as written in DDL
		schema := `
			CREATE TABLE Test (
				third_column STRING(100),
				first_column INT64 NOT NULL,
				second_column BOOL,
				fourth_column TIMESTAMP
			) PRIMARY KEY (first_column);
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE Test")

		// Verify that the generated DDL preserves column order
		for _, ddl := range ddls {
			if strings.Contains(ddl, "CREATE TABLE Test") {
				// Check that columns appear in the original order
				thirdPos := strings.Index(ddl, "third_column")
				firstPos := strings.Index(ddl, "first_column")
				secondPos := strings.Index(ddl, "second_column")
				fourthPos := strings.Index(ddl, "fourth_column")

				if thirdPos != -1 && firstPos != -1 && secondPos != -1 && fourthPos != -1 {
					assert.Less(t, thirdPos, firstPos, "third_column should come before first_column")
					assert.Less(t, firstPos, secondPos, "first_column should come before second_column")
					assert.Less(t, secondPos, fourthPos, "second_column should come before fourth_column")
				}
				break
			}
		}
	})

	t.Run("DefaultClauseSupport", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(100),
				IsActive BOOL NOT NULL DEFAULT (TRUE),
				CreatedAt TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP())
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE Users")

		// Verify idempotency with default clauses
		ddls = applySchema(t, db, schema, false)
		assert.Empty(t, ddls, "Schema with DEFAULT clauses should be idempotent")
	})

	t.Run("ColumnTypeChange", func(t *testing.T) {
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

		// Change column type
		updatedSchema := `
			CREATE TABLE Users (
				Id INT64 NOT NULL,
				Name STRING(200)
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, updatedSchema, false)
		assertDDLContains(t, ddls, "ALTER TABLE Users ALTER COLUMN Name STRING(200)")
	})

	t.Run("CheckConstraints", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Products (
				Id INT64 NOT NULL,
				Name STRING(100),
				Price FLOAT64,
				Stock INT64,
				CONSTRAINT CK_Products_Price CHECK (Price >= 0),
				CONSTRAINT CK_Products_Stock CHECK (Stock >= 0 AND Stock <= 10000)
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE Products")
		assertDDLContains(t, ddls, "CONSTRAINT CK_Products_Price CHECK (Price >= 0)")
		assertDDLContains(t, ddls, "CONSTRAINT CK_Products_Stock CHECK (Stock >= 0 AND Stock <= 10000)")

		// Verify idempotency with check constraints
		ddls = applySchema(t, db, schema, false)
		assert.Empty(t, ddls, "Schema with CHECK constraints should be idempotent")
	})

	t.Run("AddCheckConstraint", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		// Initial schema without check constraint
		initialSchema := `
			CREATE TABLE Products (
				Id INT64 NOT NULL,
				Name STRING(100),
				Price FLOAT64
			) PRIMARY KEY (Id);
		`
		applySchema(t, db, initialSchema, false)

		// Add check constraint
		updatedSchema := `
			CREATE TABLE Products (
				Id INT64 NOT NULL,
				Name STRING(100),
				Price FLOAT64,
				CONSTRAINT CK_Products_Price CHECK (Price >= 0)
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, updatedSchema, false)
		assertDDLContains(t, ddls, "ADD CONSTRAINT CK_Products_Price CHECK")
	})

	t.Run("DropCheckConstraint", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		// Initial schema with check constraint
		initialSchema := `
			CREATE TABLE Products (
				Id INT64 NOT NULL,
				Name STRING(100),
				Price FLOAT64,
				CONSTRAINT CK_Products_Price CHECK (Price >= 0)
			) PRIMARY KEY (Id);
		`
		applySchema(t, db, initialSchema, false)

		// Remove check constraint
		updatedSchema := `
			CREATE TABLE Products (
				Id INT64 NOT NULL,
				Name STRING(100),
				Price FLOAT64
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, updatedSchema, true) // Enable drop
		assertDDLContains(t, ddls, "DROP CONSTRAINT CK_Products_Price")
	})

	t.Run("ModifyCheckConstraint", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		// Initial schema with check constraint
		initialSchema := `
			CREATE TABLE Products (
				Id INT64 NOT NULL,
				Name STRING(100),
				Price FLOAT64,
				CONSTRAINT CK_Products_Price CHECK (Price >= 0)
			) PRIMARY KEY (Id);
		`
		applySchema(t, db, initialSchema, false)

		// Modify check constraint
		updatedSchema := `
			CREATE TABLE Products (
				Id INT64 NOT NULL,
				Name STRING(100),
				Price FLOAT64,
				CONSTRAINT CK_Products_Price CHECK (Price >= 0 AND Price <= 1000000)
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, updatedSchema, true) // Enable drop
		// Should drop and re-add the constraint
		assertDDLContains(t, ddls, "DROP CONSTRAINT CK_Products_Price")
		assertDDLContains(t, ddls, "ADD CONSTRAINT CK_Products_Price CHECK")
	})

	t.Run("MultipleCheckConstraints", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Employees (
				Id INT64 NOT NULL,
				Name STRING(100),
				Age INT64,
				Salary FLOAT64,
				Department STRING(50),
				CONSTRAINT CK_Employees_Age CHECK (Age >= 18 AND Age <= 100),
				CONSTRAINT CK_Employees_Salary CHECK (Salary > 0),
				CONSTRAINT CK_Employees_Department CHECK (Department IN ('Engineering', 'Sales', 'Marketing', 'HR'))
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE Employees")
		assertDDLContains(t, ddls, "CONSTRAINT CK_Employees_Age CHECK (Age >= 18 AND Age <= 100)")
		assertDDLContains(t, ddls, "CONSTRAINT CK_Employees_Salary CHECK (Salary > 0)")
		assertDDLContains(t, ddls, "CONSTRAINT CK_Employees_Department CHECK (Department IN (\"Engineering\", \"Sales\", \"Marketing\", \"HR\"))")

		// Verify idempotency with multiple check constraints
		ddls = applySchema(t, db, schema, false)
		assert.Empty(t, ddls, "Schema with multiple CHECK constraints should be idempotent")
	})

	t.Run("ForeignKeyConstraints", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Customers (
				Id INT64 NOT NULL,
				Name STRING(100),
				Email STRING(255)
			) PRIMARY KEY (Id);

			CREATE TABLE Orders (
				Id INT64 NOT NULL,
				CustomerId INT64 NOT NULL,
				OrderDate TIMESTAMP,
				Total FLOAT64,
				CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId) REFERENCES Customers (Id)
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE Customers")
		assertDDLContains(t, ddls, "CREATE TABLE Orders")
		assertDDLContains(t, ddls, "CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId) REFERENCES Customers (Id)")

		// Verify idempotency with foreign key constraints
		ddls = applySchema(t, db, schema, false)
		assert.Empty(t, ddls, "Schema with FOREIGN KEY constraints should be idempotent")
	})

	t.Run("AddForeignKeyConstraint", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		// Initial schema without foreign key
		initialSchema := `
			CREATE TABLE Customers (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);

			CREATE TABLE Orders (
				Id INT64 NOT NULL,
				CustomerId INT64 NOT NULL,
				OrderDate TIMESTAMP
			) PRIMARY KEY (Id);
		`
		applySchema(t, db, initialSchema, false)

		// Add foreign key constraint
		updatedSchema := `
			CREATE TABLE Customers (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);

			CREATE TABLE Orders (
				Id INT64 NOT NULL,
				CustomerId INT64 NOT NULL,
				OrderDate TIMESTAMP,
				CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId) REFERENCES Customers (Id)
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, updatedSchema, false)
		assertDDLContains(t, ddls, "ADD CONSTRAINT FK_Orders_Customers FOREIGN KEY")
	})

	t.Run("DropForeignKeyConstraint", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		// Initial schema with foreign key
		initialSchema := `
			CREATE TABLE Customers (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);

			CREATE TABLE Orders (
				Id INT64 NOT NULL,
				CustomerId INT64 NOT NULL,
				OrderDate TIMESTAMP,
				CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId) REFERENCES Customers (Id)
			) PRIMARY KEY (Id);
		`
		applySchema(t, db, initialSchema, false)

		// Remove foreign key constraint
		updatedSchema := `
			CREATE TABLE Customers (
				Id INT64 NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Id);

			CREATE TABLE Orders (
				Id INT64 NOT NULL,
				CustomerId INT64 NOT NULL,
				OrderDate TIMESTAMP
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, updatedSchema, true) // Enable drop
		assertDDLContains(t, ddls, "DROP CONSTRAINT FK_Orders_Customers")
	})

	t.Run("ForeignKeyWithMultipleColumns", func(t *testing.T) {
		db := recreateDatabase(t, config)
		defer db.Close()

		schema := `
			CREATE TABLE Regions (
				Country STRING(50) NOT NULL,
				Region STRING(50) NOT NULL,
				Name STRING(100)
			) PRIMARY KEY (Country, Region);

			CREATE TABLE Stores (
				Id INT64 NOT NULL,
				Country STRING(50) NOT NULL,
				Region STRING(50) NOT NULL,
				StoreName STRING(100),
				CONSTRAINT FK_Stores_Regions FOREIGN KEY (Country, Region) REFERENCES Regions (Country, Region)
			) PRIMARY KEY (Id);
		`

		ddls := applySchema(t, db, schema, false)
		assertDDLContains(t, ddls, "CREATE TABLE Regions")
		assertDDLContains(t, ddls, "CREATE TABLE Stores")
		assertDDLContains(t, ddls, "CONSTRAINT FK_Stores_Regions FOREIGN KEY (Country, Region) REFERENCES Regions (Country, Region)")

		// Verify idempotency
		ddls = applySchema(t, db, schema, false)
		assert.Empty(t, ddls, "Schema with multi-column FOREIGN KEY should be idempotent")
	})

	// Skipping this test as Spanner doesn't support ON DELETE in FOREIGN KEY constraints
	// within CREATE TABLE statements. ON DELETE is only supported for INTERLEAVE relationships.
	t.Run("ForeignKeyWithOnDelete", func(t *testing.T) {
		t.Skip("Spanner doesn't support ON DELETE in FOREIGN KEY constraints")
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

		ddls, err := GenerateIdempotentDDLs(updatedSchema, currentDDLs, GeneratorConfig{})
		require.NoError(t, err)

		// DDLs should be generated but DROP COLUMN should be skipped when executed
		assertDDLContains(t, ddls, "DROP COLUMN")

		// Apply with enableDrop=false - this should skip the DROP COLUMN
		err = RunDDLs(db, ddls, false, true)
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
		options := &Options{
			DesiredDDLs: schema,
			Config: GeneratorConfig{
				TargetTables: []string{"Users"},
			},
		}

		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)

		ddls, err := GenerateIdempotentDDLs(schema, currentDDLs, options.Config)
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
		options := &Options{
			DesiredDDLs: schema,
			Config: GeneratorConfig{
				SkipTables: []string{"Posts"},
			},
		}

		currentDDLs, err := db.DumpDDLs()
		require.NoError(t, err)

		ddls, err := GenerateIdempotentDDLs(schema, currentDDLs, options.Config)
		require.NoError(t, err)

		// Should only create Users table
		assertDDLContains(t, ddls, "CREATE TABLE Users")
		assertDDLNotContains(t, ddls, "CREATE TABLE Posts")
	})
}
