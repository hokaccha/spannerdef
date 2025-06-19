package spannerdef

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDDLs_EmptyInput(t *testing.T) {
	schema, err := ParseDDLs("")
	require.NoError(t, err)
	assert.Empty(t, schema.Tables)
	assert.Empty(t, schema.Indexes)
}

func TestParseDDLs_CreateTable(t *testing.T) {
	ddl := `
		CREATE TABLE users (
			id INT64 NOT NULL,
			name STRING(100),
			email STRING(255)
		) PRIMARY KEY (id)
	`

	schema, err := ParseDDLs(ddl)
	require.NoError(t, err)

	require.Len(t, schema.Tables, 1)

	table, exists := schema.Tables["users"]
	require.True(t, exists)
	assert.Equal(t, "users", table.Name)
	assert.Len(t, table.Columns, 3)
	assert.Equal(t, []string{"id"}, table.PrimaryKey)

	// Check columns
	idCol := table.Columns["id"]
	assert.Equal(t, "id", idCol.Name)
	assert.Equal(t, "INT64", idCol.Type)
	assert.True(t, idCol.NotNull)

	nameCol := table.Columns["name"]
	assert.Equal(t, "name", nameCol.Name)
	assert.Equal(t, "STRING(100)", nameCol.Type)
	assert.False(t, nameCol.NotNull)
}

func TestParseDDLs_CreateTableWithDefault(t *testing.T) {
	ddl := `
		CREATE TABLE users (
			id INT64 NOT NULL,
			name STRING(100),
			is_active BOOL NOT NULL DEFAULT (FALSE),
			created_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP())
		) PRIMARY KEY (id)
	`

	schema, err := ParseDDLs(ddl)
	require.NoError(t, err)

	require.Len(t, schema.Tables, 1)

	table, exists := schema.Tables["users"]
	require.True(t, exists)
	assert.Equal(t, "users", table.Name)
	assert.Len(t, table.Columns, 4)

	// Check is_active column with DEFAULT (FALSE)
	isActiveCol := table.Columns["is_active"]
	assert.Equal(t, "is_active", isActiveCol.Name)
	assert.Equal(t, "BOOL", isActiveCol.Type)
	assert.True(t, isActiveCol.NotNull)
	assert.Equal(t, "(FALSE)", isActiveCol.Default)

	// Check created_at column with DEFAULT (CURRENT_TIMESTAMP())
	createdAtCol := table.Columns["created_at"]
	assert.Equal(t, "created_at", createdAtCol.Name)
	assert.Equal(t, "TIMESTAMP", createdAtCol.Type)
	assert.True(t, createdAtCol.NotNull)
	assert.Equal(t, "(CURRENT_TIMESTAMP())", createdAtCol.Default)
}

func TestParseDDLs_CreateIndex(t *testing.T) {
	ddl := `
		CREATE TABLE users (
			id INT64 NOT NULL,
			name STRING(100),
			email STRING(255)
		) PRIMARY KEY (id);
		
		CREATE INDEX idx_name ON users (name);
		CREATE UNIQUE INDEX idx_email ON users (email) STORING (name);
	`

	schema, err := ParseDDLs(ddl)
	require.NoError(t, err)

	require.Len(t, schema.Tables, 1)
	require.Len(t, schema.Indexes, 2)

	// Check first index
	idx1, exists := schema.Indexes["idx_name"]
	require.True(t, exists)
	assert.Equal(t, "idx_name", idx1.Name)
	assert.Equal(t, "users", idx1.TableName)
	assert.False(t, idx1.Unique)
	assert.Equal(t, []string{"name"}, idx1.Columns)
	assert.Empty(t, idx1.Storing)

	// Check second index
	idx2, exists := schema.Indexes["idx_email"]
	require.True(t, exists)
	assert.Equal(t, "idx_email", idx2.Name)
	assert.Equal(t, "users", idx2.TableName)
	assert.True(t, idx2.Unique)
	assert.Equal(t, []string{"email"}, idx2.Columns)
	assert.Equal(t, []string{"name"}, idx2.Storing)
}

func TestGenerateDDLs_CreateTable(t *testing.T) {
	current := &Schema{
		Tables:  make(map[string]*Table),
		Indexes: make(map[string]*Index),
	}

	desired := &Schema{
		Tables: map[string]*Table{
			"users": {
				Name: "users",
				Columns: map[string]*Column{
					"id": {
						Name:    "id",
						Type:    "INT64",
						NotNull: true,
					},
					"name": {
						Name:    "name",
						Type:    "STRING(100)",
						NotNull: false,
					},
				},
				PrimaryKey: []string{"id"},
			},
		},
		Indexes: make(map[string]*Index),
	}

	ddls := GenerateDDLs(current, desired)
	require.Len(t, ddls, 1)

	// Check that the DDL creates the table
	assert.Contains(t, ddls[0], "CREATE TABLE users")
	assert.Contains(t, ddls[0], "id INT64 NOT NULL")
	assert.Contains(t, ddls[0], "name STRING(100)")
	assert.Contains(t, ddls[0], "PRIMARY KEY (id)")
}

func TestGenerateDDLs_DropTable(t *testing.T) {
	current := &Schema{
		Tables: map[string]*Table{
			"old_table": {
				Name: "old_table",
				Columns: map[string]*Column{
					"id": {
						Name:    "id",
						Type:    "INT64",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
			},
		},
		Indexes: make(map[string]*Index),
	}

	desired := &Schema{
		Tables:  make(map[string]*Table),
		Indexes: make(map[string]*Index),
	}

	ddls := GenerateDDLs(current, desired)
	require.Len(t, ddls, 1)
	assert.Equal(t, "DROP TABLE old_table", ddls[0])
}

func TestGenerateDDLs_AddColumn(t *testing.T) {
	current := &Schema{
		Tables: map[string]*Table{
			"users": {
				Name: "users",
				Columns: map[string]*Column{
					"id": {
						Name:    "id",
						Type:    "INT64",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
			},
		},
		Indexes: make(map[string]*Index),
	}

	desired := &Schema{
		Tables: map[string]*Table{
			"users": {
				Name: "users",
				Columns: map[string]*Column{
					"id": {
						Name:    "id",
						Type:    "INT64",
						NotNull: true,
					},
					"name": {
						Name:    "name",
						Type:    "STRING(100)",
						NotNull: false,
					},
				},
				PrimaryKey: []string{"id"},
			},
		},
		Indexes: make(map[string]*Index),
	}

	ddls := GenerateDDLs(current, desired)
	require.Len(t, ddls, 1)
	assert.Equal(t, "ALTER TABLE users ADD COLUMN name STRING(100)", ddls[0])
}

func TestGenerateDDLs_CreateIndex(t *testing.T) {
	current := &Schema{
		Tables: map[string]*Table{
			"users": {
				Name: "users",
				Columns: map[string]*Column{
					"id": {
						Name:    "id",
						Type:    "INT64",
						NotNull: true,
					},
					"name": {
						Name:    "name",
						Type:    "STRING(100)",
						NotNull: false,
					},
				},
				PrimaryKey: []string{"id"},
			},
		},
		Indexes: make(map[string]*Index),
	}

	desired := &Schema{
		Tables: map[string]*Table{
			"users": {
				Name: "users",
				Columns: map[string]*Column{
					"id": {
						Name:    "id",
						Type:    "INT64",
						NotNull: true,
					},
					"name": {
						Name:    "name",
						Type:    "STRING(100)",
						NotNull: false,
					},
				},
				PrimaryKey: []string{"id"},
			},
		},
		Indexes: map[string]*Index{
			"idx_name": {
				Name:      "idx_name",
				TableName: "users",
				Columns:   []string{"name"},
				Unique:    false,
			},
		},
	}

	ddls := GenerateDDLs(current, desired)
	require.Len(t, ddls, 1)
	assert.Equal(t, "CREATE INDEX idx_name ON users (name)", ddls[0])
}
