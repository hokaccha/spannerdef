package spannerdef

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
)

// Schema represents a database schema
type Schema struct {
	// Retain excluded table names, including references from indexes and ALTERs,
	// so filtering cannot hide a case-only change involving a managed table.
	// The excluded definitions themselves remain outside validation.
	excludedTableNames map[string]bool

	Objects      map[string]*SchemaObject
	NamedSchemas map[string]bool
	Tables       map[string]*Table
	Indexes      map[string]*Index
}

// Table represents a Spanner table
type Table struct {
	SchemaName              string
	Name                    string
	Columns                 map[string]*Column
	PrimaryKey              []string
	PrimaryKeyExplicit      bool
	InterleaveNotEnforced   bool
	ParentTable             string                 // empty if not interleaved
	OnDelete                string                 // "ON DELETE CASCADE", "ON DELETE NO ACTION", or empty
	Constraints             map[string]*Constraint // Named constraints (CHECK, etc.)
	RowDeletionPolicyColumn string                 // column name for row deletion policy
	RowDeletionPolicyDays   int64                  // number of days for row deletion policy
}

// Column represents a table column
type Column struct {
	OnUpdate       string                   // ON UPDATE clause
	generationExpr *ast.GeneratedColumnExpr // Parsed expression for semantic comparison
	additionOrder  int                      // Dependency order for ALTER TABLE ADD COLUMN
	Generation     string                   // Generated expression or identity clause
	Hidden         bool
	Name           string
	Type           string
	NotNull        bool
	Default        string // For DEFAULT clause value
	Options        string // For column options like ALLOW COMMIT TIMESTAMP
	Order          int    // Original order in the DDL
}

// Index represents a Spanner index
type Index struct {
	keys         []*ast.IndexKey
	SchemaName   string
	Name         string
	TableName    string
	Columns      []string
	Unique       bool
	NullFiltered bool
	Storing      []string
}

// Constraint represents a table constraint
type Constraint struct {
	NotEnforced      bool
	Name             string
	Type             string   // "CHECK", "FOREIGN KEY", etc.
	Expression       string   // For CHECK constraint
	Columns          []string // For FOREIGN KEY constraint
	ReferenceTable   string   // For FOREIGN KEY constraint
	ReferenceColumns []string // For FOREIGN KEY constraint
	OnDelete         string   // "CASCADE", "NO ACTION", or empty
}

// ParseDDLs parses DDL statements and returns a Schema
func ParseDDLs(ddls string) (*Schema, error) {
	return parseDDLs(ddls, GeneratorConfig{})
}

func parseDDLs(ddls string, config GeneratorConfig) (*Schema, error) {
	schema := &Schema{
		excludedTableNames: make(map[string]bool),
		Objects:            make(map[string]*SchemaObject),
		NamedSchemas:       make(map[string]bool),
		Tables:             make(map[string]*Table),
		Indexes:            make(map[string]*Index),
	}

	if strings.TrimSpace(ddls) == "" {
		return schema, nil
	}

	// Parse using memefish
	parsed, err := memefish.ParseDDLs("", ddls)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DDLs: %v", err)
	}

	// Excluded tables and their indexes/ALTERs are outside the managed schema.
	// Keep validating statements whose scope cannot be tied to a table.
	included := make([]ast.DDL, 0, len(parsed))
	for _, stmt := range parsed {
		if tableName := ddlTableName(stmt); tableName != "" && !shouldIncludeTable(tableName, config) {
			schema.excludedTableNames[tableName] = true
			continue
		}
		included = append(included, stmt)
	}
	parsed = included

	for _, stmt := range parsed {
		if err := validateSupportedDDL(stmt); err != nil {
			return nil, err
		}
	}

	// Two passes: create tables/indexes first, then apply alterations.
	// Spanner's GetDatabaseDdl emits foreign keys as separate ALTER TABLE
	// statements, and the statements may not be ordered so that tables
	// precede their own ALTERs (in particular, spanner.DumpDDLs sorts them
	// alphabetically, which puts ALTER before CREATE).
	for _, stmt := range parsed {
		if object := schemaObject(stmt); object != nil {
			if err := normalizeObject(object); err != nil {
				return nil, err
			}
			if schema.Objects[object.Name] != nil {
				return nil, fmt.Errorf("duplicate object %s", object.Name)
			}
			schema.Objects[object.Name] = object
			continue
		}
		switch s := stmt.(type) {
		case *ast.CreateSchema:
			if s.OrReplace {
				return nil, fmt.Errorf("CREATE OR REPLACE SCHEMA is not supported")
			}
			schema.NamedSchemas[s.Name.SQL()] = true
		case *ast.CreateTable:
			if err := processCreateTable(schema, s); err != nil {
				return nil, fmt.Errorf("failed to process statement: %v", err)
			}
		case *ast.CreateIndex:
			if err := processCreateIndex(schema, s); err != nil {
				return nil, fmt.Errorf("failed to process statement: %v", err)
			}
		case *ast.AlterTable:
			// Applied in the second pass after all CREATE TABLE statements.
		default:
			return nil, fmt.Errorf("unsupported DDL statement %T: %s", stmt, stmt.SQL())
		}
	}
	for _, stmt := range parsed {
		if s, ok := stmt.(*ast.AlterTable); ok {
			if err := processAlterTable(schema, s); err != nil {
				return nil, fmt.Errorf("failed to process statement: %v", err)
			}
		}
	}

	for _, object := range schema.Objects {
		if stream, ok := object.definition.(*ast.CreateChangeStream); ok {
			if targets, ok := stream.For.(*ast.ChangeStreamForTables); ok {
				for _, target := range targets.Tables {
					if table := tableByKey(schema, target.TableName.SQL()); table != nil {
						target.TableName = &ast.Ident{Name: tableFilterName(table.Name)}
						for i, name := range target.Columns {
							if column := columnByKey(table, name.SQL()); column != nil {
								target.Columns[i] = &ast.Ident{Name: tableFilterName(column.Name)}
							}
						}
					}
				}
			}
		}
		if err := normalizeGraph(object, schema); err != nil {
			return nil, err
		}
	}

	return schema, nil
}

// processCreateTable processes CREATE TABLE statement
func processCreateTable(schema *Schema, stmt *ast.CreateTable) error {
	tableName := getPathName(stmt.Name)

	table := &Table{
		Name:               tableName,
		SchemaName:         pathSchemaName(stmt.Name),
		Columns:            make(map[string]*Column),
		Constraints:        make(map[string]*Constraint),
		PrimaryKeyExplicit: stmt.PrimaryKeys != nil,
	}

	// Process columns
	generatedExpressions := make(map[*Column]ast.Expr)
	columnNames := make(map[string]*Column)
	for i, col := range stmt.Columns {
		column := &Column{
			Name:    col.Name.SQL(),
			Type:    formatColumnType(col.Type),
			NotNull: col.NotNull,
			Order:   i,
			Hidden:  !col.Hidden.Invalid(),
		}

		// Extract DEFAULT clause if present
		if col.DefaultSemantics != nil {
			switch expr := col.DefaultSemantics.(type) {
			case *ast.ColumnDefaultExpr:
				normalizeCommitTimestamp(expr.Expr)
				column.Default = "(" + expr.Expr.SQL() + ")"
				if expr.OnUpdate != nil {
					normalizeCommitTimestamp(expr.OnUpdate.Expr)
					column.OnUpdate = expr.OnUpdate.SQL()
				}
			case *ast.AutoIncrement:
				column.Generation = "GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE)"
			case *ast.IdentityColumn:
				column.Generation = canonicalIdentity(expr)
			case *ast.GeneratedColumnExpr:
				column.Generation = expr.SQL()
				generatedExpressions[column] = expr.Expr
				column.generationExpr = expr
			}
		}

		// Extract OPTIONS clause if present
		if col.Options != nil {
			column.Options = col.Options.SQL()
		}

		table.Columns[column.Name] = column
		if col.PrimaryKey {
			table.PrimaryKeyExplicit = true
			table.PrimaryKey = append(table.PrimaryKey, col.Name.SQL())
		}
		columnNames[strings.ToLower(col.Name.Name)] = column
	}
	for _, column := range table.Columns {
		if column.generationExpr != nil {
			normalizeGenerationExpression(column.generationExpr.Expr, columnNames)
		}
	}
	if err := setColumnAdditionOrder(table, generatedExpressions, columnNames); err != nil {
		return err
	}

	// Process primary key
	for _, key := range stmt.PrimaryKeys {
		table.PrimaryKey = append(table.PrimaryKey, canonicalKeySQL(key))
	}

	// Process table constraints
	for _, tc := range stmt.TableConstraints {
		if pk, ok := tc.Constraint.(*ast.TablePrimaryKey); ok {
			table.PrimaryKeyExplicit = true
			for _, key := range pk.Columns {
				table.PrimaryKey = append(table.PrimaryKey, canonicalKeySQL(key))
			}
		} else {
			registerTableConstraint(table, tc)
		}
	}

	// Process interleave information
	if stmt.Cluster != nil {
		cluster := stmt.Cluster
		table.InterleaveNotEnforced = !cluster.Enforced
		if cluster.TableName != nil && len(cluster.TableName.Idents) > 0 {
			table.ParentTable = getPathName(cluster.TableName)
		}
		table.OnDelete = string(cluster.OnDelete)
	}

	// Process row deletion policy
	if stmt.RowDeletionPolicy != nil && stmt.RowDeletionPolicy.RowDeletionPolicy != nil {
		policy := stmt.RowDeletionPolicy.RowDeletionPolicy
		table.RowDeletionPolicyColumn = policy.ColumnName.SQL()
		if column := columnNames[strings.ToLower(policy.ColumnName.Name)]; column != nil {
			table.RowDeletionPolicyColumn = column.Name
		}
		// Convert string value to int64
		days, err := strconv.ParseInt(policy.NumDays.Value, policy.NumDays.Base, 64)
		if err != nil {
			return fmt.Errorf("failed to parse row deletion policy days: %v", err)
		}
		table.RowDeletionPolicyDays = days
	}

	schema.Tables[tableName] = table
	return nil
}

// processAlterTable loads the ADD CONSTRAINT form emitted by GetDatabaseDdl.
// Other ALTER actions are rejected by validateSupportedDDL.
func processAlterTable(schema *Schema, stmt *ast.AlterTable) error {
	tableName := getPathName(stmt.Name)
	table, ok := schema.Tables[tableName]
	if !ok {
		return fmt.Errorf("ALTER TABLE references unknown table %s", tableName)
	}

	if add, ok := stmt.TableAlteration.(*ast.AddTableConstraint); ok && add.TableConstraint != nil {
		registerTableConstraint(table, add.TableConstraint)
	}

	return nil
}

// registerTableConstraint records a CHECK or FOREIGN KEY constraint on a table.
// Shared between CREATE TABLE parsing and ALTER TABLE ADD CONSTRAINT parsing —
// Spanner's GetDatabaseDdl returns foreign keys as separate ALTER TABLE
// statements, so both entry points must produce the same in-memory shape.
func registerTableConstraint(table *Table, tc *ast.TableConstraint) {
	constraintName := ""
	if tc.Name != nil {
		constraintName = tc.Name.SQL()
	}

	switch c := tc.Constraint.(type) {
	case *ast.Check:
		if constraintName == "" {
			constraintName = generatedConstraintName("CK", table, len(table.Constraints))
		}
		table.Constraints[constraintName] = &Constraint{
			Name:       constraintName,
			Type:       "CHECK",
			Expression: "(" + c.Expr.SQL() + ")",
		}
	case *ast.ForeignKey:
		if constraintName == "" {
			constraintName = generatedConstraintName("FK", table, len(table.Constraints))
		}

		var columns []string
		for _, col := range c.Columns {
			columns = append(columns, col.SQL())
		}

		var refColumns []string
		for _, col := range c.ReferenceColumns {
			refColumns = append(refColumns, col.SQL())
		}

		table.Constraints[constraintName] = &Constraint{
			Name:             constraintName,
			Type:             "FOREIGN KEY",
			Columns:          columns,
			ReferenceTable:   getPathName(c.ReferenceTable),
			ReferenceColumns: refColumns,
			OnDelete:         string(c.OnDelete),
			NotEnforced:      c.Enforcement == ast.NotEnforced,
		}
	}
}

// processCreateIndex processes CREATE INDEX statement
func processCreateIndex(schema *Schema, stmt *ast.CreateIndex) error {
	indexName := getPathName(stmt.Name)
	tableName := getPathName(stmt.TableName)

	index := &Index{
		Name:         indexName,
		SchemaName:   pathSchemaName(stmt.Name),
		TableName:    tableName,
		Unique:       stmt.Unique,
		NullFiltered: stmt.NullFiltered,
	}

	// Process key columns
	index.keys = stmt.Keys
	for _, key := range stmt.Keys {
		index.Columns = append(index.Columns, canonicalKeySQL(key))
		if key.Expr != nil {
			normalizeGenerationExpression(key.Expr, nil)
		}
	}

	// Process storing columns
	if stmt.Storing != nil {
		for _, storing := range stmt.Storing.Columns {
			index.Storing = append(index.Storing, storing.SQL())
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
	return path.SQL()
}

// formatColumnType formats a column type from AST to string
func formatColumnType(typeNode ast.SchemaType) string {
	if typeNode == nil {
		return "UNKNOWN"
	}
	// Use the SQL() method provided by memefish AST
	return typeNode.SQL()
}

// GenerateDDLs generates DDL statements for validated schemas.
// Deprecated: use GenerateDDLsChecked to receive dependency validation errors.
func GenerateDDLs(current, desired *Schema) []string {
	ddls, _ := GenerateDDLsChecked(current, desired)
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

// sortTablesByDependency sorts tables to ensure parent tables come before child tables
// and referenced tables come before tables with foreign keys
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

		processed[table.Name] = true
		// If this table has a parent, process the parent first
		if table.ParentTable != "" {
			if parentTable, exists := tableMap[table.ParentTable]; exists {
				processTable(parentTable)
			}
		}

		// If this table has foreign key constraints, process referenced tables first
		for _, constraint := range table.Constraints {
			if constraint.Type == "FOREIGN KEY" {
				if referencedTable, exists := tableMap[constraint.ReferenceTable]; exists {
					processTable(referencedTable)
				}
			}
		}

		// Process this table
		result = append(result, table)
		processed[table.Name] = true
	}

	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })
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
	fmt.Fprintf(&ddl, "CREATE TABLE %s (\n", table.Name)

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
		if columns[i].order != columns[j].order {
			return columns[i].order < columns[j].order
		}
		// If order is the same, sort by name for stable output
		return columns[i].name < columns[j].name
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
		if col.OnUpdate != "" {
			def += " " + col.OnUpdate
		}
		if col.Generation != "" {
			def += " " + col.Generation
		}
		if col.Hidden {
			def += " HIDDEN"
		}
		if col.Options != "" {
			def += " " + col.Options
		}
		columnDefs = append(columnDefs, def)
	}

	ddl.WriteString(strings.Join(columnDefs, ",\n"))

	// Add constraints
	if len(table.Constraints) > 0 {
		// Sort constraints by name for consistent output
		var constraintNames []string
		for name := range table.Constraints {
			constraintNames = append(constraintNames, name)
		}
		sort.Strings(constraintNames)

		for _, name := range constraintNames {
			constraint := table.Constraints[name]
			if constraint.Type == "CHECK" {
				ddl.WriteString(",\n  CONSTRAINT ")
				ddl.WriteString(name)
				ddl.WriteString(" CHECK ")
				ddl.WriteString(constraint.Expression)
			} else if constraint.Type == "FOREIGN KEY" {
				ddl.WriteString(",\n  CONSTRAINT ")
				ddl.WriteString(name)
				ddl.WriteString(" FOREIGN KEY (")
				ddl.WriteString(strings.Join(constraint.Columns, ", "))
				ddl.WriteString(") REFERENCES ")
				ddl.WriteString(constraint.ReferenceTable)
				ddl.WriteString(" (")
				ddl.WriteString(strings.Join(constraint.ReferenceColumns, ", "))
				ddl.WriteString(")")
				if constraint.OnDelete != "" {
					ddl.WriteString(" ")
					ddl.WriteString(constraint.OnDelete)
				}
				if constraint.NotEnforced {
					ddl.WriteString(" NOT ENFORCED")
				}
			}
		}
	}

	// Add primary key
	if table.PrimaryKeyExplicit || len(table.PrimaryKey) > 0 {
		fmt.Fprintf(&ddl, "\n) PRIMARY KEY (%s)", strings.Join(table.PrimaryKey, ", "))
	} else {
		ddl.WriteString("\n)")
	}

	// Add interleave clause if present
	if table.ParentTable != "" {
		ddl.WriteString(",\n")
		ddl.WriteString("INTERLEAVE IN ")
		if !table.InterleaveNotEnforced {
			ddl.WriteString("PARENT ")
		}
		ddl.WriteString(table.ParentTable)
		if table.OnDelete != "" {
			fmt.Fprintf(&ddl, " %s", table.OnDelete)
		}
	}

	// Add row deletion policy if present
	if table.RowDeletionPolicyColumn != "" {
		ddl.WriteString(",\n")
		fmt.Fprintf(&ddl, "ROW DELETION POLICY (OLDER_THAN(%s, INTERVAL %d DAY))",
			table.RowDeletionPolicyColumn, table.RowDeletionPolicyDays)
	}

	return ddl.String()
}

// generateCreateIndex generates CREATE INDEX DDL
func generateCreateIndex(index *Index) string {
	var parts []string

	parts = append(parts, "CREATE")
	if index.Unique {
		parts = append(parts, "UNIQUE")
	}
	if index.NullFiltered {
		parts = append(parts, "NULL_FILTERED")
	}
	parts = append(parts, "INDEX")

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
	if current.ParentTable != "" && canonicalOnDelete(current.OnDelete) != canonicalOnDelete(desired.OnDelete) {
		ddls = append(ddls, "ALTER TABLE "+desired.Name+" SET "+canonicalOnDelete(desired.OnDelete))
	}
	// Removing a parent policy is a prerequisite for disabling child CASCADE.
	if current.RowDeletionPolicyColumn != "" && desired.RowDeletionPolicyColumn == "" {
		ddls = append(ddls, "ALTER TABLE "+desired.Name+" DROP ROW DELETION POLICY")
	}

	// Add dependencies before generated columns, regardless of declaration order.
	columns := make([]*Column, 0, len(desired.Columns))
	for _, col := range desired.Columns {
		columns = append(columns, col)
	}
	sort.Slice(columns, func(i, j int) bool {
		if columns[i].additionOrder != columns[j].additionOrder {
			return columns[i].additionOrder < columns[j].additionOrder
		}
		if columns[i].Order != columns[j].Order {
			return columns[i].Order < columns[j].Order
		}
		return columns[i].Name < columns[j].Name
	})
	for _, col := range columns {
		colName := col.Name
		if _, exists := current.Columns[colName]; !exists {
			def := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", desired.Name, col.Name, col.Type)
			if col.NotNull {
				def += " NOT NULL"
			}
			if col.Default != "" {
				def += " DEFAULT " + col.Default
			}
			if col.OnUpdate != "" {
				def += " " + col.OnUpdate
			}
			if col.Generation != "" {
				def += " " + col.Generation
			}
			if col.Hidden {
				def += " HIDDEN"
			}
			if col.Options != "" {
				def += " " + col.Options
			}
			ddls = append(ddls, def)
		}
	}

	if desired.RowDeletionPolicyColumn != "" && (current.RowDeletionPolicyColumn != desired.RowDeletionPolicyColumn || current.RowDeletionPolicyDays != desired.RowDeletionPolicyDays) {
		action := "REPLACE"
		if current.RowDeletionPolicyColumn == "" {
			action = "ADD"
		}
		ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s %s ROW DELETION POLICY (OLDER_THAN(%s, INTERVAL %d DAY))", desired.Name, action, desired.RowDeletionPolicyColumn, desired.RowDeletionPolicyDays))
	}

	// Handle constraints
	// Drop constraints that no longer exist or have changed
	for constraintName, currentConstraint := range current.Constraints {
		desiredConstraint, exists := desired.Constraints[constraintName]
		needsDrop := !exists

		if exists {
			// Check if constraint needs to be dropped and recreated
			if currentConstraint.Type == "CHECK" && currentConstraint.Expression != desiredConstraint.Expression {
				needsDrop = true
			} else if currentConstraint.Type == "FOREIGN KEY" &&
				(strings.Join(currentConstraint.Columns, ",") != strings.Join(desiredConstraint.Columns, ",") ||
					currentConstraint.ReferenceTable != desiredConstraint.ReferenceTable ||
					strings.Join(currentConstraint.ReferenceColumns, ",") != strings.Join(desiredConstraint.ReferenceColumns, ",") ||
					currentConstraint.OnDelete != desiredConstraint.OnDelete ||
					currentConstraint.NotEnforced != desiredConstraint.NotEnforced) {
				needsDrop = true
			}
		}

		if needsDrop {
			ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", desired.Name, constraintName))
		}
	}

	// Remove generated columns before the columns they reference.
	dropping := make([]*Column, 0)
	for name, col := range current.Columns {
		if desired.Columns[name] == nil {
			dropping = append(dropping, col)
		}
	}
	sort.Slice(dropping, func(i, j int) bool {
		if dropping[i].additionOrder != dropping[j].additionOrder {
			return dropping[i].additionOrder > dropping[j].additionOrder
		}
		return dropping[i].Name < dropping[j].Name
	})
	for _, col := range dropping {
		ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", desired.Name, col.Name))
	}

	// Handle column type changes and OPTIONS changes
	for colName, desiredCol := range desired.Columns {
		if currentCol, exists := current.Columns[colName]; exists {
			// Switch between ordinary and commit-timestamp defaults without
			// leaving an incompatible default installed while options change.
			currentDefault := currentCol.Default
			optionsChanged := currentCol.Options != desiredCol.Options
			pending := "(PENDING_COMMIT_TIMESTAMP())"
			switchMode := optionsChanged && currentDefault != "" && !sameDefaultSQL(currentDefault, desiredCol.Default) && (sameDefaultSQL(currentDefault, pending) || sameDefaultSQL(desiredCol.Default, pending))
			optionSQL := desiredCol.Options
			if optionSQL == "" {
				optionSQL = nullifyOptions(currentCol.Options)
			}
			optionsFirst := optionsChanged && (switchMode || desiredCol.Default != "")
			if switchMode {
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", desired.Name, colName))
				currentDefault = ""
			}
			if optionsFirst {
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s", desired.Name, colName, optionSQL))
			}

			// Check if column type has changed
			if currentCol.Type != desiredCol.Type || currentCol.NotNull != desiredCol.NotNull {
				def := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s", desired.Name, colName, desiredCol.Type)
				if desiredCol.NotNull {
					def += " NOT NULL"
				}
				if desiredCol.Default != "" {
					def += " DEFAULT " + desiredCol.Default
				}
				ddls = append(ddls, def)
			} else if !sameDefaultSQL(currentDefault, desiredCol.Default) {
				if desiredCol.Default == "" {
					ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", desired.Name, colName))
				} else {
					ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", desired.Name, colName, desiredCol.Default))
				}
			}

			// Handle OPTIONS changes independently from type changes
			if !optionsFirst && currentCol.Options != desiredCol.Options {
				if desiredCol.Options != "" {
					ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s",
						desired.Name, colName, desiredCol.Options))
				} else {
					// Remove OPTIONS by setting each key to null
					ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET %s",
						desired.Name, colName, nullifyOptions(currentCol.Options)))
				}
			}
		}
	}

	// Add new constraints or re-add modified ones
	for constraintName, desiredConstraint := range desired.Constraints {
		currentConstraint, exists := current.Constraints[constraintName]
		needsRecreate := false

		if exists {
			// Check if constraint needs to be recreated
			if desiredConstraint.Type == "CHECK" && currentConstraint.Expression != desiredConstraint.Expression {
				needsRecreate = true
			} else if desiredConstraint.Type == "FOREIGN KEY" &&
				(strings.Join(currentConstraint.Columns, ",") != strings.Join(desiredConstraint.Columns, ",") ||
					currentConstraint.ReferenceTable != desiredConstraint.ReferenceTable ||
					strings.Join(currentConstraint.ReferenceColumns, ",") != strings.Join(desiredConstraint.ReferenceColumns, ",") ||
					currentConstraint.OnDelete != desiredConstraint.OnDelete ||
					currentConstraint.NotEnforced != desiredConstraint.NotEnforced) {
				needsRecreate = true
			}
		}

		if !exists || needsRecreate {
			if desiredConstraint.Type == "CHECK" {
				ddls = append(ddls, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK %s",
					desired.Name, constraintName, desiredConstraint.Expression))
			} else if desiredConstraint.Type == "FOREIGN KEY" {
				ddl := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
					desired.Name, constraintName,
					strings.Join(desiredConstraint.Columns, ", "),
					desiredConstraint.ReferenceTable,
					strings.Join(desiredConstraint.ReferenceColumns, ", "))
				if desiredConstraint.OnDelete != "" {
					ddl += " " + desiredConstraint.OnDelete
				}
				if desiredConstraint.NotEnforced {
					ddl += " NOT ENFORCED"
				}
				ddls = append(ddls, ddl)
			}
		}
	}

	return ddls
}

// optionKeyValueRe matches "key = value" pairs in OPTIONS clause,
// handling quoted string values that may contain commas or parentheses.
var optionKeyValueRe = regexp.MustCompile(`(\w+)\s*=\s*(?:"[^"]*"|[^,)]+)`)

// nullifyOptions takes an OPTIONS SQL string like "OPTIONS (key1 = value1, key2 = value2)"
// and returns "OPTIONS (key1 = null, key2 = null)" for removing options.
func nullifyOptions(optionsSQL string) string {
	return optionKeyValueRe.ReplaceAllString(optionsSQL, "${1} = null")
}

func canonicalOnDelete(action string) string {
	if action == "" {
		return "ON DELETE NO ACTION"
	}
	return action
}
