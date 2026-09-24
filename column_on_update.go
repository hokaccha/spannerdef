package spannerdef

import (
	"fmt"
	"github.com/cloudspannerecosystem/memefish/ast"
	"strings"
)

func validateOnUpdateChanges(current, desired *Schema) error {
	for name, next := range desired.Tables {
		prev, ok := current.Tables[name]
		if !ok {
			continue
		}
		for colName, col := range next.Columns {
			old, ok := prev.Columns[colName]
			if !ok {
				continue
			}
			// Replacing the type definition can implicitly remove ON UPDATE or DEFAULT.
			// Changing commit timestamp options may also invalidate the expression.
			if (old.OnUpdate != "" || col.OnUpdate != "") &&
				(old.OnUpdate != col.OnUpdate || old.Default != col.Default || old.Type != col.Type || old.NotNull != col.NotNull || old.Options != col.Options) {
				return fmt.Errorf("unsupported ON UPDATE change for column %s.%s; apply it explicitly before updating the desired schema", name, colName)
			}
		}
	}
	return nil
}

// Function names are case insensitive; string literals and column names are not
// normalized here. Restrict this to the commit-timestamp built-in used by ON UPDATE.
func normalizeCommitTimestamp(expr ast.Expr) {
	ast.Inspect(expr, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && len(call.Func.Idents) == 1 && strings.EqualFold(call.Func.Idents[0].Name, "PENDING_COMMIT_TIMESTAMP") {
			call.Func.Idents[0].Name = "PENDING_COMMIT_TIMESTAMP"
		}
		return true
	})
}
