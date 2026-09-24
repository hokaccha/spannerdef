package spannerdef

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
)

// GenerateDDLsChecked includes schema objects and reports migrations which
// cannot be safely scheduled. Prefer GenerateIdempotentDDLs for SQL input.
func GenerateDDLsChecked(current, desired *Schema) ([]string, error) {
	return generateObjectDDLs(current, desired)
}

func generateObjectDDLs(current, desired *Schema) ([]string, error) {
	if err := validateObjectNames(current, desired); err != nil {
		return nil, err
	}
	oldOrder, err := orderedObjects(current.Objects)
	if err != nil {
		return nil, err
	}
	newOrder, err := orderedObjects(desired.Objects)
	if err != nil {
		return nil, err
	}
	base := generateOrderedDDLs(current, desired)
	oldObjects, newObjects := objectsByKey(current.Objects), objectsByKey(desired.Objects)
	columnChanges := map[string]map[string]bool{}
	droppedTables := map[string]bool{}
	// Structural changes require dependent views/graphs/indexes to be released
	// first. Rebuild the transitive closure, even if their text is unchanged.
	affected := map[string]bool{}
	for _, sql := range base {
		node, e := memefish.ParseDDL("", sql)
		if e != nil {
			return nil, e
		}
		switch n := node.(type) {
		case *ast.DropTable:
			affected[objectKey(n.Name.SQL())] = true
			droppedTables[objectKey(n.Name.SQL())] = true
		case *ast.AlterTable:
			key := objectKey(n.Name.SQL())
			if columnChanges[key] == nil {
				columnChanges[key] = map[string]bool{}
			}
			switch a := n.TableAlteration.(type) {
			case *ast.DropColumn:
				columnChanges[key][objectKey(a.Name.SQL())] = true
				affected[key] = true
			case *ast.AlterColumn:
				affected[key] = true
			}
		}
	}
	rebuild := map[string]bool{}
	for _, o := range oldOrder {
		next := newObjects[objectKey(o.Name)]
		if next == nil {
			affected[objectKey(o.Name)] = true
			continue
		}
		for ref := range objectReferences(o.definition) {
			if affected[ref] && o.Kind != "CHANGE STREAM" {
				rebuild[objectKey(o.Name)] = true
			}
		}
		if o.Kind == "SEARCH INDEX" || o.Kind == "VECTOR INDEX" {
			rebuild[objectKey(o.Name)] = rebuild[objectKey(o.Name)] || !sameObject(o, next)
		}
		if rebuild[objectKey(o.Name)] || ((o.Kind == "VIEW" || o.Kind == "PROPERTY GRAPH") && !sameObject(o, next)) {
			affected[objectKey(o.Name)] = true
		}
	}
	var before, after, sequences, sequenceDrops []string
	for i := len(oldOrder) - 1; i >= 0; i-- {
		o := oldOrder[i]
		if newObjects[objectKey(o.Name)] == nil || rebuild[objectKey(o.Name)] {
			sql := "DROP " + o.Kind + " " + o.Name
			if o.Kind == "SEQUENCE" {
				sequenceDrops = append(sequenceDrops, sql)
			} else {
				before = append(before, sql)
			}
		}
	}
	for _, o := range newOrder {
		old := oldObjects[objectKey(o.Name)]
		if old == nil || rebuild[objectKey(o.Name)] {
			if o.Kind == "SEQUENCE" {
				sequences = append(sequences, o.definition.SQL())
			} else {
				after = append(after, o.definition.SQL())
			}
			continue
		}
		switch n := o.definition.(type) {
		case *ast.CreateSequence:
			oldSeq := old.definition.(*ast.CreateSequence)
			delta := changedOptions(oldSeq.Options, n.Options, true)
			if delta != nil {
				sequences = append(sequences, "ALTER SEQUENCE "+old.Name+" SET "+delta.SQL())
			}
		case *ast.CreateChangeStream:
			prev := old.definition.(*ast.CreateChangeStream)
			if streamForSQL(prev.For) != streamForSQL(n.For) {
				sql := "ALTER CHANGE STREAM " + old.Name + " DROP FOR ALL"
				if n.For != nil {
					sql = "ALTER CHANGE STREAM " + old.Name + " SET " + n.For.SQL()
				}
				if streamNeedsNewColumns(n, current) {
					// An intermediate suspension would introduce an unrequested capture gap.
					// Require two explicit migrations if old tracked columns must first go.
					if streamBlockedByChanges(prev, droppedTables, columnChanges) {
						return nil, fmt.Errorf("change stream %s needs new columns and releases altered tables in the same migration; change its FOR clause in a separate migration", o.Name)
					}
					after = append(after, sql)
				} else {
					before = append(before, sql)
				}
			} else if streamBlockedByChanges(prev, droppedTables, columnChanges) {
				return nil, fmt.Errorf("change stream %s explicitly tracks a structurally changed table; update its FOR clause before altering that table", o.Name)
			}
			if delta := changedOptions(prev.Options, n.Options, false); delta != nil {
				after = append(after, "ALTER CHANGE STREAM "+old.Name+" SET "+delta.SQL())
			}
		case *ast.CreateView:
			if !sameObject(old, o) {
				copyView := *n
				copyView.OrReplace = true
				copyView.Name = old.definition.(*ast.CreateView).Name
				after = append(after, copyView.SQL())
			}
		case *ast.CreatePropertyGraph:
			if !sameObject(old, o) {
				copyGraph := *n
				copyGraph.OrReplace = true
				copyGraph.Name = old.definition.(*ast.CreatePropertyGraph).Name
				after = append(after, copyGraph.SQL())
			}
		}
	}
	// Sequence creation follows namespace creation but precedes column defaults.
	split := 0
	for i, sql := range base {
		node, _ := memefish.ParseDDL("", sql)
		if _, ok := node.(*ast.CreateSchema); ok {
			split = i + 1
		}
	}
	result := append(before, base[:split]...)
	result = append(result, sequences...)
	result = append(result, base[split:]...)
	result = append(result, after...)
	return append(result, sequenceDrops...), nil
}

func changedOptions(old, next *ast.Options, sequence bool) *ast.Options {
	a, b := optionValues(old), optionValues(next)
	result := map[string]ast.Expr{}
	for name, value := range b {
		if _, null := value.(*ast.NullLiteral); null && a[name] == nil {
			continue
		}
		if !equalGenerationAST(reflect.ValueOf(a[name]), reflect.ValueOf(value)) {
			result[name] = value
		}
	}
	for name, value := range a {
		if _, exists := b[name]; exists {
			continue
		}
		// The initial counter is a creation-time seed, not a desired live counter.
		// An omitted kind may inherit the database's default sequence kind.
		if sequence && (name == "start_with_counter" || name == "sequence_kind") {
			continue
		}
		if _, null := value.(*ast.NullLiteral); !null {
			result[name] = &ast.NullLiteral{}
		}
	}
	return makeOptions(result)
}

func streamNeedsNewColumns(stream *ast.CreateChangeStream, current *Schema) bool {
	tables, ok := stream.For.(*ast.ChangeStreamForTables)
	if !ok {
		return false
	}
	for _, target := range tables.Tables {
		table := tableByKey(current, target.TableName.SQL())
		if table == nil {
			return true
		}
		for _, col := range target.Columns {
			if columnByKey(table, col.SQL()) == nil {
				return true
			}
		}
	}
	return false
}
func streamBlockedByChanges(stream *ast.CreateChangeStream, droppedTables map[string]bool, columns map[string]map[string]bool) bool {
	tables, ok := stream.For.(*ast.ChangeStreamForTables)
	if !ok {
		return false
	} // FOR ALL automatically follows schema changes.
	for _, target := range tables.Tables {
		key := objectKey(target.TableName.SQL())
		if droppedTables[key] {
			return true
		}
		// FOR T tracks all current and future columns. FOR T() tracks only
		// the key. Only explicitly named non-key columns block changes.
		for _, column := range target.Columns {
			if columns[key][objectKey(column.SQL())] {
				return true
			}
		}
	}
	return false
}

func schemaNames(s *Schema) map[string]string {
	names := map[string]string{}
	for name := range s.NamedSchemas {
		names[objectKey(name)] = "SCHEMA"
	}
	for name, table := range s.Tables {
		names[objectKey(name)] = "TABLE"
		for constraint := range table.Constraints {
			full := constraint
			if table.SchemaName != "" {
				full = table.SchemaName + "." + constraint
			}
			names[objectKey(full)] = "CONSTRAINT"
		}
	}
	for name := range s.Indexes {
		names[objectKey(name)] = "INDEX"
	}
	return names
}
func validateObjectNames(current, desired *Schema) error {
	for _, s := range []*Schema{current, desired} {
		names := schemaNames(s)
		for _, name := range sortedObjectNames(s.Objects) {
			o := s.Objects[name]
			key := objectKey(name)
			if kind := names[key]; kind != "" {
				return fmt.Errorf("duplicate schema name %s (%s and %s)", name, kind, o.Kind)
			}
			names[key] = o.Kind
		}
	}
	oldNames, newNames := schemaNames(current), schemaNames(desired)
	for name, o := range current.Objects {
		oldNames[objectKey(name)] = o.Kind
	}
	for name, o := range desired.Objects {
		newNames[objectKey(name)] = o.Kind
	}
	for name, old := range oldNames {
		next := newNames[name]
		if next != "" && next != old && (isObjectKind(old) || isObjectKind(next)) {
			return fmt.Errorf("changing object kind for %s from %s to %s requires separate migrations", name, old, next)
		}
	}
	// Detect dangling references to objects removed by this migration. Unknown
	// references can name excluded tables or implicit UNNEST paths, so are left
	// to Spanner's semantic validation rather than guessed from SQL text.
	check := func(name string, node ast.Node) error {
		for ref := range objectReferences(node) {
			if oldNames[ref] != "" && newNames[ref] == "" {
				return fmt.Errorf("%s still references removed object %s", name, ref)
			}
		}
		return nil
	}
	for _, o := range desired.Objects {
		if stream, ok := o.definition.(*ast.CreateChangeStream); ok {
			if targets, ok := stream.For.(*ast.ChangeStreamForTables); ok {
				for _, target := range targets.Tables {
					if table := tableByKey(desired, target.TableName.SQL()); table != nil {
						for _, column := range target.Columns {
							if columnByKey(table, column.SQL()) == nil {
								return fmt.Errorf("change stream %s references missing column %s.%s", o.Name, target.TableName.SQL(), column.SQL())
							}
						}
					}
				}
			}
		}
		if err := check(o.Name, o.definition); err != nil {
			return err
		}
	}
	for _, t := range desired.Tables {
		node, err := memefish.ParseDDL("", generateCreateTable(t))
		if err != nil {
			return err
		}
		if err := check(t.Name, node); err != nil {
			return err
		}
	}
	return nil
}
func isObjectKind(kind string) bool {
	return strings.Contains("|SEQUENCE|VIEW|SEARCH INDEX|VECTOR INDEX|CHANGE STREAM|PROPERTY GRAPH|", "|"+kind+"|")
}
func sortedObjectNames(objects map[string]*SchemaObject) []string {
	names := make([]string, 0, len(objects))
	for name := range objects {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func streamForSQL(forClause ast.ChangeStreamFor) string {
	if forClause == nil {
		return ""
	}
	return strings.ToLower(forClause.SQL())
}
