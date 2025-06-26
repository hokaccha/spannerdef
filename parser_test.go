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

func TestParseDDLs_InterleaveInParent(t *testing.T) {
	ddl := `
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

	schema, err := ParseDDLs(ddl)
	require.NoError(t, err)

	require.Len(t, schema.Tables, 2)

	// Check Users table (parent)
	usersTable, exists := schema.Tables["Users"]
	require.True(t, exists)
	assert.Equal(t, "Users", usersTable.Name)
	assert.Empty(t, usersTable.ParentTable)
	assert.Empty(t, usersTable.OnDelete)

	// Check Posts table (interleaved child)
	postsTable, exists := schema.Tables["Posts"]
	require.True(t, exists)
	assert.Equal(t, "Posts", postsTable.Name)
	assert.Equal(t, "Users", postsTable.ParentTable)
	assert.Equal(t, "ON DELETE CASCADE", postsTable.OnDelete)
}

func TestParseDDLs_InterleaveInParentNoAction(t *testing.T) {
	ddl := `
		CREATE TABLE Users (
			Id INT64 NOT NULL,
			Name STRING(100)
		) PRIMARY KEY (Id);

		CREATE TABLE Posts (
			Id INT64 NOT NULL,
			PostId INT64 NOT NULL,
			Title STRING(255)
		) PRIMARY KEY (Id, PostId),
		INTERLEAVE IN PARENT Users ON DELETE NO ACTION;
	`

	schema, err := ParseDDLs(ddl)
	require.NoError(t, err)

	postsTable := schema.Tables["Posts"]
	assert.Equal(t, "Users", postsTable.ParentTable)
	assert.Equal(t, "ON DELETE NO ACTION", postsTable.OnDelete)
}

func TestParseDDLs_InterleaveInParentWithoutOnDelete(t *testing.T) {
	ddl := `
		CREATE TABLE Users (
			Id INT64 NOT NULL,
			Name STRING(100)
		) PRIMARY KEY (Id);

		CREATE TABLE Posts (
			Id INT64 NOT NULL,
			PostId INT64 NOT NULL,
			Title STRING(255)
		) PRIMARY KEY (Id, PostId),
		INTERLEAVE IN PARENT Users;
	`

	schema, err := ParseDDLs(ddl)
	require.NoError(t, err)

	postsTable := schema.Tables["Posts"]
	assert.Equal(t, "Users", postsTable.ParentTable)
	assert.Empty(t, postsTable.OnDelete)
}

func TestGenerateDDLs_CreateInterleaveTable(t *testing.T) {
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
				},
				PrimaryKey: []string{"id"},
			},
			"posts": {
				Name: "posts",
				Columns: map[string]*Column{
					"id": {
						Name:    "id",
						Type:    "INT64",
						NotNull: true,
					},
					"post_id": {
						Name:    "post_id",
						Type:    "INT64",
						NotNull: true,
					},
					"title": {
						Name:    "title",
						Type:    "STRING(255)",
						NotNull: false,
					},
				},
				PrimaryKey:  []string{"id", "post_id"},
				ParentTable: "users",
				OnDelete:    "ON DELETE CASCADE",
			},
		},
		Indexes: make(map[string]*Index),
	}

	ddls := GenerateDDLs(current, desired)
	require.Len(t, ddls, 1)

	expectedDDL := `CREATE TABLE posts (
  id INT64 NOT NULL,
  post_id INT64 NOT NULL,
  title STRING(255)
) PRIMARY KEY (id, post_id),
INTERLEAVE IN PARENT users ON DELETE CASCADE`

	assert.Equal(t, expectedDDL, ddls[0])
}

func TestGenerateDDLs_TableCreationOrder(t *testing.T) {
	current := &Schema{
		Tables:  make(map[string]*Table),
		Indexes: make(map[string]*Index),
	}

	desired := &Schema{
		Tables: map[string]*Table{
			"Posts": {
				Name: "Posts",
				Columns: map[string]*Column{
					"Id": {
						Name:    "Id",
						Type:    "INT64",
						NotNull: true,
					},
					"PostId": {
						Name:    "PostId",
						Type:    "INT64",
						NotNull: true,
					},
				},
				PrimaryKey:  []string{"Id", "PostId"},
				ParentTable: "Users",
				OnDelete:    "ON DELETE CASCADE",
			},
			"Users": {
				Name: "Users",
				Columns: map[string]*Column{
					"Id": {
						Name:    "Id",
						Type:    "INT64",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"Id"},
			},
		},
		Indexes: make(map[string]*Index),
	}

	ddls := GenerateDDLs(current, desired)
	require.Len(t, ddls, 2)

	// Users table should come before Posts table
	assert.Contains(t, ddls[0], "CREATE TABLE Users")
	assert.Contains(t, ddls[1], "CREATE TABLE Posts")
	assert.Contains(t, ddls[1], "INTERLEAVE IN PARENT Users")
}

func TestGenerateDDLs_ColumnOrder(t *testing.T) {
	current := &Schema{
		Tables:  make(map[string]*Table),
		Indexes: make(map[string]*Index),
	}

	desired := &Schema{
		Tables: map[string]*Table{
			"test": {
				Name: "test",
				Columns: map[string]*Column{
					"id": {
						Name:    "id",
						Type:    "INT64",
						NotNull: true,
						Order:   0,
					},
					"name": {
						Name:    "name",
						Type:    "STRING(100)",
						NotNull: false,
						Order:   1,
					},
					"created_at": {
						Name:    "created_at",
						Type:    "TIMESTAMP",
						NotNull: true,
						Order:   2,
					},
				},
				PrimaryKey: []string{"id"},
			},
		},
		Indexes: make(map[string]*Index),
	}

	ddls := GenerateDDLs(current, desired)
	require.Len(t, ddls, 1)

	expectedDDL := `CREATE TABLE test (
  id INT64 NOT NULL,
  name STRING(100),
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (id)`

	assert.Equal(t, expectedDDL, ddls[0])
}

func TestParseDDLs_ColumnOrder(t *testing.T) {
	ddl := `
		CREATE TABLE test (
			id INT64 NOT NULL,
			name STRING(100),
			created_at TIMESTAMP NOT NULL
		) PRIMARY KEY (id)
	`

	schema, err := ParseDDLs(ddl)
	require.NoError(t, err)

	table := schema.Tables["test"]
	require.NotNil(t, table)

	// Check column order
	assert.Equal(t, 0, table.Columns["id"].Order)
	assert.Equal(t, 1, table.Columns["name"].Order)
	assert.Equal(t, 2, table.Columns["created_at"].Order)

	// Generate DDL and check order is preserved
	current := &Schema{Tables: make(map[string]*Table), Indexes: make(map[string]*Index)}
	ddls := GenerateDDLs(current, schema)
	require.Len(t, ddls, 1)

	expectedDDL := `CREATE TABLE test (
  id INT64 NOT NULL,
  name STRING(100),
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (id)`

	assert.Equal(t, expectedDDL, ddls[0])
}
