package spannerdef

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
)

// Schema represents a database schema
type Schema struct {
	Tables  map[string]*Table
	Indexes map[string]*Index
}

// Table represents a Spanner table
type Table struct {
	Name        string
	Columns     map[string]*Column
	PrimaryKey  []string
	ParentTable string // empty if not interleaved
	OnDelete    string // "ON DELETE CASCADE", "ON DELETE NO ACTION", or empty
}

// Column represents a table column
type Column struct {
	Name    string
	Type    string
	NotNull bool
	Default string // For DEFAULT clause value
	Options string // For column options like ALLOW COMMIT TIMESTAMP
	Order   int    // Original order in the DDL
}

// Index represents a Spanner index
type Index struct {
	Name         string
	TableName    string
	Columns      []string
	Unique       bool
	NullFiltered bool
	Storing      []string
}

// ParseDDLs parses DDL statements and returns a Schema
func ParseDDLs(ddls string) (*Schema, error) {
	schema := &Schema{
		Tables:  make(map[string]*Table),
		Indexes: make(map[string]*Index),
	}

	if strings.TrimSpace(ddls) == "" {
		return schema, nil
	}

	// Parse using memefish
	parsed, err := memefish.ParseDDLs("", ddls)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DDLs: %v", err)
	}

	for _, stmt := range parsed {
		if err := processStatement(schema, stmt); err != nil {
			return nil, fmt.Errorf("failed to process statement: %v", err)
		}
	}

	return schema, nil
}

// processStatement processes a parsed DDL statement
func processStatement(schema *Schema, stmt ast.DDL) error {
	switch s := stmt.(type) {
	case *ast.CreateTable:
		return processCreateTable(schema, s)
	case *ast.CreateIndex:
		return processCreateIndex(schema, s)
	default:
		// Ignore other DDL types for now
		return nil
	}
}

// processCreateTable processes CREATE TABLE statement
func processCreateTable(schema *Schema, stmt *ast.CreateTable) error {
	tableName := getPathName(stmt.Name)

	table := &Table{
		Name:    tableName,
		Columns: make(map[string]*Column),
	}

	// Process columns
	for i, col := range stmt.Columns {
		column := &Column{
			Name:    col.Name.Name,
			Type:    formatColumnType(col.Type),
			NotNull: col.NotNull,
			Order:   i,
		}

		// Extract DEFAULT clause if present
		if col.DefaultSemantics != nil {
			if defaultExpr, ok := col.DefaultSemantics.(*ast.ColumnDefaultExpr); ok {
				column.Default = "(" + defaultExpr.Expr.SQL() + ")"
			}
		}

		if col.PrimaryKey {
			column.Options = "PRIMARY KEY"
		}

		table.Columns[column.Name] = column
	}

	// Process primary key
	for _, key := range stmt.PrimaryKeys {
		table.PrimaryKey = append(table.PrimaryKey, key.Name.Name)
	}

	// Process interleave information
	if stmt.Cluster != nil {
		cluster := stmt.Cluster
		if cluster.TableName != nil && len(cluster.TableName.Idents) > 0 {
			table.ParentTable = cluster.TableName.Idents[len(cluster.TableName.Idents)-1].Name
		}
		table.OnDelete = string(cluster.OnDelete)
	}

	schema.Tables[tableName] = table
	return nil
}

// processCreateIndex processes CREATE INDEX statement
func processCreateIndex(schema *Schema, stmt *ast.CreateIndex) error {
	indexName := getPathName(stmt.Name)
	tableName := getPathName(stmt.TableName)

	index := &Index{
		Name:         indexName,
		TableName:    tableName,
		Unique:       stmt.Unique,
		NullFiltered: stmt.NullFiltered,
	}

	// Process key columns
	for _, key := range stmt.Keys {
		index.Columns = append(index.Columns, key.Name.Name)
	}

	// Process storing columns
	if stmt.Storing != nil {
		for _, storing := range stmt.Storing.Columns {
			index.Storing = append(index.Storing, storing.Name)
		}
	}

	schema.Indexes[indexName] = index
	return nil
}

// getPathName extracts the name from a Path
func getPathName(path *ast.Path) string {
	if path == nil || len(path.Idents) == 0 {
		return ""
	}
	// For simple cases, just return the last identifier
	return path.Idents[len(path.Idents)-1].Name
}

// formatColumnType formats a column type from AST to string
func formatColumnType(typeNode ast.SchemaType) string {
	if typeNode == nil {
		return "UNKNOWN"
	}
	// Use the SQL() method provided by memefish AST
	return typeNode.SQL()
}

// GenerateDDLs generates DDL statements to transform current schema to desired schema
func GenerateDDLs(current, desired *Schema) []string {
	var ddls []string

	// 1. Drop indexes first (required before dropping tables with indexes)
	dropIndexDDLs := generateDropIndexDDLs(current, desired)
	ddls = append(ddls, dropIndexDDLs...)

	// 2. Drop tables
	dropTableDDLs := generateDropTableDDLs(current, desired)
	ddls = append(ddls, dropTableDDLs...)

	// 3. Alter existing tables
	alterTableDDLs := generateAlterTableDDLs(current, desired)
	ddls = append(ddls, alterTableDDLs...)

	// 4. Create new tables
	createTableDDLs := generateCreateTableDDLs(current, desired)
	ddls = append(ddls, createTableDDLs...)

	// 5. Create new indexes
	createIndexDDLs := generateCreateIndexDDLs(current, desired)
	ddls = append(ddls, createIndexDDLs...)

	return ddls
}

// generateDropIndexDDLs generates DDLs to drop indexes
func generateDropIndexDDLs(current, desired *Schema) []string {
	var ddls []string

	// Drop indexes that no longer exist or whose tables will be dropped
	for indexName, index := range current.Indexes {
		shouldDrop := false

		// Drop if index doesn't exist in desired schema
		if _, exists := desired.Indexes[indexName]; !exists {
			shouldDrop = true
		}

		// Drop if the table for this index will be dropped
		if _, tableExists := desired.Tables[index.TableName]; !tableExists {
			shouldDrop = true
		}

		if shouldDrop {
			ddls = append(ddls, fmt.Sprintf("DROP INDEX %s", indexName))
		}
	}

	return ddls
}

// generateDropTableDDLs generates DDLs to drop tables
func generateDropTableDDLs(current, desired *Schema) []string {
	var ddls []string

	// Drop tables that no longer exist
	for tableName := range current.Tables {
		if _, exists := desired.Tables[tableName]; !exists {
			ddls = append(ddls, fmt.Sprintf("DROP TABLE %s", tableName))
		}
	}

	return ddls
}

// generateAlterTableDDLs generates DDLs to alter existing tables
func generateAlterTableDDLs(current, desired *Schema) []string {
	var ddls []string

	// Alter existing tables
	for tableName, desiredTable := range desired.Tables {
		if currentTable, exists := current.Tables[tableName]; exists {
			ddls = append(ddls, generateAlterTable(currentTable, desiredTable)...)
		}
	}

	return ddls
}

// generateCreateTableDDLs generates DDLs to create new tables
func generateCreateTableDDLs(current, desired *Schema) []string {
	var ddls []string

	// Find new tables that need to be created
	var newTables []*Table
	for tableName, table := range desired.Tables {
		if _, exists := current.Tables[tableName]; !exists {
			newTables = append(newTables, table)
		}
	}

	// Sort tables to respect parent-child dependencies
	sortedTables := sortTablesByDependency(newTables)

	// Create tables in dependency order
	for _, table := range sortedTables {
		ddls = append(ddls, generateCreateTable(table))
	}

	return ddls
}

// sortTablesByDependency sorts tables to ensure parent tables come before child tables
func sortTablesByDependency(tables []*Table) []*Table {
	var result []*Table
	processed := make(map[string]bool)
	
	// Create a map for quick lookup
	tableMap := make(map[string]*Table)
	for _, table := range tables {
		tableMap[table.Name] = table
	}
	
	var processTable func(table *Table)
	processTable = func(table *Table) {
		if processed[table.Name] {
			return
		}
		
		// If this table has a parent, process the parent first
		if table.ParentTable != "" {
			if parentTable, exists := tableMap[table.ParentTable]; exists {
				processTable(parentTable)
			}
		}
		
		// Process this table
		result = append(result, table)
		processed[table.Name] = true
	}
	
	// Process all tables
	for _, table := range tables {
		processTable(table)
	}
	
	return result
}

// generateCreateIndexDDLs generates DDLs to create new indexes
func generateCreateIndexDDLs(current, desired *Schema) []string {
	var ddls []string

	// Create new indexes
	for indexName, index := range desired.Indexes {
		if _, exists := current.Indexes[indexName]; !exists {
			ddls = append(ddls, generateCreateIndex(index))
		}
	}

	return ddls
}

// generateCreateTable generates CREATE TABLE DDL
func generateCreateTable(table *Table) string {
	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", table.Name))

	// Sort columns by original order
	type columnInfo struct {
		name  string
		order int
	}
	var columns []columnInfo
	for name, col := range table.Columns {
		columns = append(columns, columnInfo{name: name, order: col.Order})
	}
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].order < columns[j].order
	})

	var columnDefs []string
	for _, colInfo := range columns {
		col := table.Columns[colInfo.name]
		def := fmt.Sprintf("  %s %s", col.Name, col.Type)
		if col.NotNull {
			def += " NOT NULL"
		}
		if col.Default != "" {
			def += " DEFAULT " + col.Default
		}
		columnDefs = append(columnDefs, def)
	}

	ddl.WriteString(strings.Join(columnDefs, ",\n"))

	// Add primary key
	if len(table.PrimaryKey) > 0 {
		ddl.WriteString(fmt.Sprintf("\n) PRIMARY KEY (%s)", strings.Join(table.PrimaryKey, ", ")))
	} else {
		ddl.WriteString("\n)")
	}

	// Add interleave clause if present
	if table.ParentTable != "" {
		ddl.WriteString(",\n")
		ddl.WriteString(fmt.Sprintf("INTERLEAVE IN PARENT %s", table.ParentTable))
		if table.OnDelete != "" {
			ddl.WriteString(fmt.Sprintf(" %s", table.OnDelete))
		}
	}

	return ddl.String()
}

// generateCreateIndex generates CREATE INDEX DDL
func generateCreateIndex(index *Index) string {
	var parts []string

	if index.Unique {
		parts = append(parts, "CREATE UNIQUE INDEX")
	} else {
		parts = append(parts, "CREATE INDEX")
	}

	parts = append(parts, index.Name, "ON", index.TableName)
	parts = append(parts, fmt.Sprintf("(%s)", strings.Join(index.Columns, ", ")))

	if len(index.Storing) > 0 {
		parts = append(parts, fmt.Sprintf("STORING (%s)", strings.Join(index.Storing, ", ")))
	}

	return strings.Join(parts, " ")
}

// generateAlterTable generates ALTER TABLE DDLs for differences between tables
func generateAlterTable(current, desired *Table) []string {
	var ddls []string

	// Add new columns
	for colName, col := range desired.Columns {
		if _, exists := current.Columns[colName]; !exists {
			def := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", desired.Name, col.Name, col.Type)
			if col.NotNull {
				def += " NOT NULL"
			}
			if col.Default != "" {
				def += " DEFAULT " + col.Default
			}
			ddls = append(ddls, def)
		}
	}

	// Drop columns that no longer exist
	for colName := range current.Columns {
		if _, exists := desired.Columns[colName]; !exists {
			ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", desired.Name, colName))
		}
	}

	// Handle column type changes
	for colName, desiredCol := range desired.Columns {
		if currentCol, exists := current.Columns[colName]; exists {
			// Check if column type has changed
			if currentCol.Type != desiredCol.Type {
				def := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s", desired.Name, colName, desiredCol.Type)
				if desiredCol.NotNull {
					def += " NOT NULL"
				}
				ddls = append(ddls, def)
			}
		}
	}

	return ddls
}
