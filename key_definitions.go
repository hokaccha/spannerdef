package spannerdef

import (
	"fmt"
	"github.com/cloudspannerecosystem/memefish/ast"
	"slices"
)

func canonicalKeySQL(key *ast.IndexKey) string {
	name := key.Name.SQL()
	if key.Dir == ast.DirectionDesc {
		return name + " DESC"
	}
	// Explicit ASC and an omitted direction have identical semantics.
	return name
}

func validateKeyChanges(current, desired *Schema) error {
	for name, next := range desired.Tables {
		if prev, ok := current.Tables[name]; ok && !slices.Equal(prev.PrimaryKey, next.PrimaryKey) {
			return fmt.Errorf("unsupported primary key change for table %s", name)
		}
	}
	for name, next := range desired.Indexes {
		if prev, ok := current.Indexes[name]; ok &&
			(prev.TableName != next.TableName || prev.Unique != next.Unique || prev.NullFiltered != next.NullFiltered || !slices.Equal(prev.Columns, next.Columns) || !slices.Equal(prev.Storing, next.Storing)) {
			return fmt.Errorf("unsupported definition change for index %s; migrate the index explicitly", name)
		}
	}
	return nil
}
