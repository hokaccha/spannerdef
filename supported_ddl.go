package spannerdef

import (
	"fmt"
	"github.com/cloudspannerecosystem/memefish/ast"
)

// A syntactically valid statement is not necessarily represented by Schema.
// Reject unsupported objects before building a partial migration plan.
func validateSupportedDDL(stmt ast.DDL) error {
	switch s := stmt.(type) {
	case *ast.CreateTable:
		if s.Options != nil {
			return fmt.Errorf("unsupported table OPTIONS on %s", s.Name.SQL())
		}
		if len(s.Synonyms) != 0 {
			return fmt.Errorf("unsupported table SYNONYM on %s", s.Name.SQL())
		}
		for _, col := range s.Columns {
			if col.PlacementKey != nil {
				return fmt.Errorf("unsupported PLACEMENT KEY on %s.%s", s.Name.SQL(), col.Name.SQL())
			}
		}
	case *ast.CreateIndex:
		if s.Options != nil || s.Where != nil || s.InterleaveIn != nil {
			return fmt.Errorf("unsupported index OPTIONS, WHERE, or INTERLEAVE clause on %s", s.Name.SQL())
		}
	case *ast.AlterTable:
		if add, ok := s.TableAlteration.(*ast.AddTableConstraint); !ok || add.TableConstraint == nil {
			return fmt.Errorf("unsupported ALTER TABLE action on %s: only ADD CONSTRAINT is supported in schema definitions", s.Name.SQL())
		}
	}
	return nil
}

func ddlTableName(stmt ast.DDL) string {
	switch s := stmt.(type) {
	case *ast.CreateTable:
		return getPathName(s.Name)
	case *ast.AlterTable:
		return getPathName(s.Name)
	case *ast.CreateIndex:
		return getPathName(s.TableName)
	case *ast.CreateSearchIndex:
		return getPathName(s.TableName)
	case *ast.CreateVectorIndex:
		return s.TableName.Name
	default:
		return ""
	}
}
