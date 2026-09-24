package spannerdef

import (
	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
	"sort"
)

// Separate table existence from FK creation, so new and existing tables can
// reference each other (including self references and FK cycles).
func generateOrderedDDLs(current, desired *Schema) []string {
	ddls := generateDropIndexDDLs(current, desired)
	sort.Strings(ddls)
	var constraintDrops, policyDrops, cascadeEnables, alters, constraintAdds []string
	for _, name := range sortedTableNames(current.Tables) {
		old := current.Tables[name]
		next := desired.Tables[name]
		if next == nil {
			for constraintName := range old.Constraints {
				// Keep removals explicit so drop filtering also knows which
				// constraint names remain occupied when the table is retained.
				constraintDrops = append(constraintDrops, "ALTER TABLE "+name+" DROP CONSTRAINT "+constraintName)
			}
			continue
		}
		for _, sql := range generateAlterTable(old, next) {
			node, err := memefish.ParseDDL("", sql)
			if err == nil {
				if alter, ok := node.(*ast.AlterTable); ok {
					switch action := alter.TableAlteration.(type) {
					case *ast.DropRowDeletionPolicy:
						policyDrops = append(policyDrops, sql)
						continue
					case *ast.SetOnDelete:
						if action.OnDelete == ast.OnDeleteCascade {
							cascadeEnables = append(cascadeEnables, sql)
							continue
						}
					case *ast.DropConstraint:
						constraintDrops = append(constraintDrops, sql)
						continue
					case *ast.AddTableConstraint:
						constraintAdds = append(constraintAdds, sql)
						continue
					}
				}
			}
			alters = append(alters, sql)
		}
	}
	sort.Strings(constraintDrops)
	ddls = append(ddls, constraintDrops...)
	// Release TTL requirements before creating children or disabling CASCADE.
	ddls = append(ddls, policyDrops...)
	ddls = append(ddls, cascadeEnables...)
	var dropping []*Table
	for _, name := range sortedTableNames(current.Tables) {
		if desired.Tables[name] == nil {
			dropping = append(dropping, tableWithoutForeignKeys(current.Tables[name]))
		}
	}
	dropping = sortTablesByDependency(dropping)
	for i := len(dropping) - 1; i >= 0; i-- {
		ddls = append(ddls, "DROP TABLE "+dropping[i].Name)
	}
	var namespaces []string
	for name := range desired.NamedSchemas {
		if !current.NamedSchemas[name] {
			namespaces = append(namespaces, name)
		}
	}
	sort.Strings(namespaces)
	for _, name := range namespaces {
		ddls = append(ddls, "CREATE SCHEMA "+name)
	}
	// Existing parents must have their final key types before children are created.
	ddls = append(ddls, alters...)
	var creating []*Table
	for _, name := range sortedTableNames(desired.Tables) {
		if current.Tables[name] != nil {
			continue
		}
		table := desired.Tables[name]
		bare := tableWithoutForeignKeys(table)
		creating = append(creating, bare)
		// Only foreign keys were removed from bare, so this emits their ADDs.
		constraintAdds = append(constraintAdds, generateAlterTable(bare, table)...)
	}
	for _, table := range sortTablesByDependency(creating) {
		ddls = append(ddls, generateCreateTable(table))
	}
	sort.Strings(constraintAdds)
	ddls = append(ddls, constraintAdds...)
	indexes := generateCreateIndexDDLs(current, desired)
	sort.Strings(indexes)
	return append(ddls, indexes...)
}
func sortedTableNames(tables map[string]*Table) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
func tableWithoutForeignKeys(table *Table) *Table {
	copyTable := *table
	copyTable.Constraints = make(map[string]*Constraint)
	for name, c := range table.Constraints {
		if c.Type != "FOREIGN KEY" {
			copyTable.Constraints[name] = c
		}
	}
	return &copyTable
}
