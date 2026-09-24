package spannerdef

import (
	"fmt"
	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
	"strings"
)

type plannedDDL struct {
	SQL  string
	Skip bool
}

// Build the same plan for preview and execution. Parsing keeps string literals
// and quoted identifiers from being mistaken for destructive SQL operations.
func planDDLs(ddls []string, enableDrop bool) ([]plannedDDL, error) {
	nodes := make([]ast.DDL, len(ddls))
	replacements := make(map[string]bool)
	for i, sql := range ddls {
		node, err := memefish.ParseDDL("", sql)
		if err != nil {
			return nil, fmt.Errorf("invalid generated DDL: %w", err)
		}
		nodes[i] = node
		if alter, ok := node.(*ast.AlterTable); ok {
			if add, ok := alter.TableAlteration.(*ast.AddTableConstraint); ok && add.TableConstraint.Name != nil {
				replacements[strings.ToLower(alter.Name.SQL()+"/"+add.TableConstraint.Name.SQL())] = true
			}
		}
	}
	plan := make([]plannedDDL, len(ddls))
	skippedNames := make(map[string]bool)
	hasSkippedDrop := false
	for i, node := range nodes {
		skip := false
		if !enableDrop {
			switch n := node.(type) {
			case *ast.DropTable:
				skip = true
				skippedNames[strings.ToLower(n.Name.SQL())] = true
			case *ast.DropIndex:
				skip = true
				skippedNames[strings.ToLower(n.Name.SQL())] = true
			case *ast.AlterTable:
				switch a := n.TableAlteration.(type) {
				case *ast.DropColumn:
					skip = true
				case *ast.DropConstraint:
					skip = !replacements[strings.ToLower(n.Name.SQL()+"/"+a.Name.SQL())]
					if skip {
						skippedNames[strings.ToLower(constraintObjectName(n.Name, a.Name))] = true
					}
				}
			}
		}
		plan[i] = plannedDDL{SQL: ddls[i], Skip: skip}
		hasSkippedDrop = hasSkippedDrop || skip
	}
	// Creating an object with the name of a skipped drop cannot be applied.
	// Reject before sending any batch rather than failing after partial changes.
	for _, node := range nodes {
		var names []string
		switch n := node.(type) {
		case *ast.CreateSchema:
			names = append(names, n.Name.SQL())
		case *ast.CreateIndex:
			names = append(names, n.Name.SQL())
		case *ast.CreateTable:
			names = append(names, n.Name.SQL())
			for _, constraint := range n.TableConstraints {
				if constraint.Name != nil {
					names = append(names, constraintObjectName(n.Name, constraint.Name))
				}
			}
		case *ast.AlterTable:
			if add, ok := n.TableAlteration.(*ast.AddTableConstraint); ok && add.TableConstraint.Name != nil {
				names = append(names, constraintObjectName(n.Name, add.TableConstraint.Name))
			}
		}
		for _, name := range names {
			if skippedNames[strings.ToLower(name)] {
				return nil, fmt.Errorf("creating %s requires a skipped drop; use --enable-drop to apply this plan", name)
			}
		}
	}

	// Retained constraints, indexes and interleaved children can prevent
	// alterations. Without the original schema, DDL strings cannot identify
	// every dependency. Reject mixed alteration/removal plans conservatively
	// before sending any statements; additions of columns/tables remain allowed.

	for _, node := range nodes {
		alter, ok := node.(*ast.AlterTable)
		if !ok {
			continue
		}
		switch alter.TableAlteration.(type) {
		case *ast.AlterColumn, *ast.SetOnDelete, *ast.AddTableConstraint:
			if hasSkippedDrop {
				return nil, fmt.Errorf("altering %s may require skipped drops; retain those constraints or use --enable-drop", alter.Name.SQL())
			}
		case *ast.AddRowDeletionPolicy, *ast.ReplaceRowDeletionPolicy:
			if hasSkippedDrop {
				return nil, fmt.Errorf("changing TTL may require skipped drops; retain those constraints or use --enable-drop")
			}
		}
	}
	return plan, nil
}

func constraintObjectName(table *ast.Path, name *ast.Ident) string {
	path := &ast.Path{Idents: append(append([]*ast.Ident(nil), table.Idents[:len(table.Idents)-1]...), name)}
	return path.SQL()
}
