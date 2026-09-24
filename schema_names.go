package spannerdef

import (
	"fmt"
	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
	"strings"
)

func pathSchemaName(path *ast.Path) string {
	if path == nil || len(path.Idents) < 2 {
		return ""
	}
	return (&ast.Path{Idents: path.Idents[:len(path.Idents)-1]}).SQL()
}

func generatedConstraintName(prefix string, table *Table, ordinal int) string {
	// Constraint names are local identifiers, never qualified paths.
	name := strings.TrimPrefix(table.Name, table.SchemaName+".")
	name = strings.Trim(name, "`")
	return (&ast.Ident{Name: fmt.Sprintf("%s_%s_%d", prefix, name, ordinal)}).SQL()
}

// tableFilterName separates the identity used by config from SQL quoting.
// A raw config name such as Order need not be a valid unquoted SQL expression.
func tableFilterName(name string) string {
	expr, err := memefish.ParseExpr("", name)
	if err != nil {
		return name
	}
	switch path := expr.(type) {
	case *ast.Ident:
		return path.Name
	case *ast.Path:
		parts := make([]string, len(path.Idents))
		for i, ident := range path.Idents {
			parts[i] = ident.Name
		}
		return strings.Join(parts, ".")
	default:
		return name
	}
}
