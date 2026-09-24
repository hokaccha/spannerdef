package spannerdef

import (
	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
)

// Spanner permits at most ten statements requiring backfill/data validation
// per batch. Preserve statement order and conservatively count every statement
// except schema/table creation and indexes immediately following their new
// table. Unrecognized syntax is passed through, counted as a costly statement;
// ExecDDLs remains usable for server DDL beyond our desired-schema parser.
func ddlBatches(statements []string) [][]string {
	const maxCost = 10
	var batches [][]string
	start, cost := 0, 0
	freshTable := ""
	for i, sql := range statements {
		statementCost, nextFresh := 1, ""
		node, err := memefish.ParseDDL("", sql)
		if err == nil {
			switch n := node.(type) {
			case *ast.CreateSchema:
				statementCost = 0
			case *ast.CreateTable:
				// Constraints can create backing indexes on existing referenced tables.
				if len(n.TableConstraints) == 0 {
					statementCost = 0
				}
				if !n.IfNotExists && len(n.TableConstraints) == 0 {
					nextFresh = objectKey(n.Name.SQL())
				}
			case *ast.CreateIndex:
				if freshTable != "" && freshTable == objectKey(n.TableName.SQL()) {
					statementCost = 0
					nextFresh = freshTable
				}
			}
		}
		if cost+statementCost > maxCost {
			batches = append(batches, statements[start:i])
			start = i
			cost = 0
		}
		cost += statementCost
		freshTable = nextFresh
	}
	if start < len(statements) {
		batches = append(batches, statements[start:])
	}
	return batches
}
