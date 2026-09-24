package spannerdef

import (
	"fmt"
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
