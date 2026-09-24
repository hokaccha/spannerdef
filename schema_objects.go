package spannerdef

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
)

// SchemaObject retains the complete parsed definition of a non-table object.
// Its AST is private so callers cannot accidentally discard supported clauses.
type SchemaObject struct {
	Name       string
	Kind       string
	SchemaName string
	TableName  string
	definition ast.DDL
}

func schemaObject(stmt ast.DDL) *SchemaObject {
	o := &SchemaObject{definition: stmt}
	switch s := stmt.(type) {
	case *ast.CreateSequence:
		o.Kind, o.Name, o.SchemaName = "SEQUENCE", s.Name.SQL(), pathSchemaName(s.Name)
		s.IfNotExists = false
	case *ast.CreateView:
		o.Kind, o.Name, o.SchemaName = "VIEW", s.Name.SQL(), pathSchemaName(s.Name)
		s.OrReplace = false
	case *ast.CreateSearchIndex:
		o.Kind, o.Name, o.SchemaName, o.TableName = "SEARCH INDEX", s.Name.SQL(), pathSchemaName(s.Name), s.TableName.SQL()
	case *ast.CreateVectorIndex:
		o.Kind, o.Name, o.TableName = "VECTOR INDEX", s.Name.SQL(), s.TableName.SQL()
		s.IfNotExists = false
	case *ast.CreateChangeStream:
		o.Kind, o.Name = "CHANGE STREAM", s.Name.SQL()
	case *ast.CreatePropertyGraph:
		o.Kind, o.Name = "PROPERTY GRAPH", s.Name.SQL()
		s.IfNotExists, s.OrReplace = false, false
	default:
		return nil
	}
	return o
}

func objectKey(name string) string { return strings.ToLower(tableFilterName(name)) }

func normalizeObject(o *SchemaObject) error {
	var optionError error
	ast.Inspect(o.definition, func(node ast.Node) bool {
		if opts, ok := node.(*ast.Options); ok {
			seen := map[string]bool{}
			for _, r := range opts.Records {
				key := strings.ToLower(r.Name.Name)
				if seen[key] {
					optionError = fmt.Errorf("duplicate option %s on %s", key, o.Name)
				}
				seen[key] = true
			}
		}
		return true
	})
	if optionError != nil {
		return optionError
	}
	if s, ok := o.definition.(*ast.CreateSequence); ok {
		values := optionValues(s.Options)
		for _, param := range s.Params {
			switch p := param.(type) {
			case *ast.BitReversedPositive:
				values["sequence_kind"] = &ast.StringLiteral{Value: "bit_reversed_positive"}
			case *ast.StartCounterWith:
				values["start_with_counter"] = p.Counter
			case *ast.SkipRange:
				values["skip_range_min"], values["skip_range_max"] = p.Min, p.Max
			}
		}
		s.Params = nil
		s.Options = makeOptions(values)
	}
	// Preserve literal case/content. Normalize option ordering and unordered
	// membership lists without rewriting expressions or ordered keys.
	ast.Inspect(o.definition, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.Options:
			for _, r := range n.Records {
				r.Name.Name = strings.ToLower(r.Name.Name)
				ast.Inspect(r.Value, func(value ast.Node) bool {
					if number, ok := value.(*ast.IntLiteral); ok {
						digits := number.Value
						if number.Base == 16 {
							digits = strings.TrimPrefix(strings.TrimPrefix(digits, "0x"), "0X")
						}
						if parsed, err := strconv.ParseInt(digits, number.Base, 64); err == nil {
							number.Base = 10
							number.Value = strconv.FormatInt(parsed, 10)
						}
					}
					return true
				})
			}
			sort.Slice(n.Records, func(i, j int) bool { return n.Records[i].Name.Name < n.Records[j].Name.Name })
		case *ast.Storing:
			sort.Slice(n.Columns, func(i, j int) bool { return n.Columns[i].SQL() < n.Columns[j].SQL() })
		case *ast.ChangeStreamForTables:
			sort.Slice(n.Tables, func(i, j int) bool {
				return objectKey(n.Tables[i].TableName.SQL()) < objectKey(n.Tables[j].TableName.SQL())
			})
			for _, table := range n.Tables {
				sort.Slice(table.Columns, func(i, j int) bool { return objectKey(table.Columns[i].SQL()) < objectKey(table.Columns[j].SQL()) })
			}
		}
		return true
	})
	return nil
}

func optionValues(opts *ast.Options) map[string]ast.Expr {
	result := map[string]ast.Expr{}
	if opts != nil {
		for _, r := range opts.Records {
			result[strings.ToLower(r.Name.Name)] = r.Value
		}
	}
	return result
}
func makeOptions(values map[string]ast.Expr) *ast.Options {
	if len(values) == 0 {
		return nil
	}
	result := &ast.Options{}
	var names []string
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		result.Records = append(result.Records, &ast.OptionsDef{Name: &ast.Ident{Name: name}, Value: values[name]})
	}
	return result
}
func sameObject(a, b *SchemaObject) bool {
	if a.Kind != b.Kind {
		return false
	}
	canonical := func(ddl ast.DDL) ast.DDL {
		node, err := memefish.ParseDDL("", ddl.SQL())
		if err != nil {
			return ddl
		}
		// Only schema identities are case-insensitive. Arbitrary expression
		// paths can select case-sensitive JSON/proto fields.
		lowerPath := func(path *ast.Path) {
			for _, id := range path.Idents {
				id.Name = strings.ToLower(id.Name)
			}
		}
		lowerIdent := func(id *ast.Ident) {
			if id != nil {
				id.Name = strings.ToLower(id.Name)
			}
		}
		ast.Inspect(node, func(n ast.Node) bool {
			switch value := n.(type) {
			case *ast.CreateSequence:
				lowerPath(value.Name)
			case *ast.CreateView:
				lowerPath(value.Name)
			case *ast.CreateSearchIndex:
				lowerPath(value.Name)
				lowerPath(value.TableName)
			case *ast.CreateVectorIndex:
				lowerIdent(value.Name)
				lowerIdent(value.TableName)
			case *ast.CreatePropertyGraph:
				lowerIdent(value.Name)
			case *ast.CreateChangeStream:
				lowerIdent(value.Name)
			case *ast.TableName:
				lowerIdent(value.Table)
			case *ast.SequenceArg:
				switch name := value.Expr.(type) {
				case *ast.Ident:
					lowerIdent(name)
				case *ast.Path:
					lowerPath(name)
				}
			case *ast.PropertyGraphElement:
				lowerIdent(value.Name)
				lowerIdent(value.Alias)
			case *ast.PropertyGraphElementLabelLabelName:
				lowerIdent(value.Name)
			case *ast.PropertyGraphSourceKey:
				lowerIdent(value.ElementReference)
			case *ast.PropertyGraphDestinationKey:
				lowerIdent(value.ElementReference)
			case *ast.GraphTableExpr:
				lowerPath(value.GraphName)
			case *ast.GQLGraphClause:
				lowerPath(value.PropertyGraphName)
			}
			return true
		})
		return node
	}
	return equalGenerationAST(reflect.ValueOf(canonical(a.definition)), reflect.ValueOf(canonical(b.definition)))
}

// objectReferences walks actual table/sequence reference nodes, not identifier
// text in literals or SELECT aliases. WITH bindings are scoped to their query.
func objectReferences(node ast.Node) map[string]bool {
	refs := map[string]bool{}
	var visit func(ast.Node, map[string]bool)
	visit = func(root ast.Node, scope map[string]bool) {
		ast.Inspect(root, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.Query:
				local := map[string]bool{}
				for k, v := range scope {
					local[k] = v
				}
				if n.With != nil {
					for _, cte := range n.With.CTEs {
						visit(cte.QueryExpr, local)
						local[objectKey(cte.Name.SQL())] = true
					}
				}
				copyQuery := *n
				copyQuery.With = nil
				visitQueryParts := []ast.Node{copyQuery.Query}
				if n.OrderBy != nil {
					visitQueryParts = append(visitQueryParts, n.OrderBy)
				}
				for _, p := range n.PipeOperators {
					visitQueryParts = append(visitQueryParts, p)
				}
				for _, part := range visitQueryParts {
					visit(part, local)
				}
				return false
			case *ast.TableName:
				if !scope[objectKey(n.Table.SQL())] {
					refs[objectKey(n.Table.SQL())] = true
				}
			case *ast.PathTableExpr:
				// Qualified schema names are dependencies; implicit UNNEST paths are
				// resolved against the known schema when constructing the plan.
				refs[objectKey(n.Path.SQL())] = true
			case *ast.SequenceArg:
				refs[objectKey(n.Expr.SQL())] = true
			case *ast.GraphTableExpr:
				refs[objectKey(n.GraphName.SQL())] = true
			case *ast.GQLGraphClause:
				refs[objectKey(n.PropertyGraphName.SQL())] = true
			case *ast.PropertyGraphElement:
				refs[objectKey(n.Name.SQL())] = true
			case *ast.CreateSearchIndex:
				refs[objectKey(n.TableName.SQL())] = true
				if n.Interleave != nil {
					refs[objectKey(n.Interleave.TableName.SQL())] = true
				}
			case *ast.CreateVectorIndex:
				refs[objectKey(n.TableName.SQL())] = true
			case *ast.ChangeStreamForTable:
				refs[objectKey(n.TableName.SQL())] = true
			}
			return true
		})
	}
	if node != nil {
		visit(node, map[string]bool{})
	}
	return refs
}

func orderedObjects(objects map[string]*SchemaObject) ([]*SchemaObject, error) {
	byKey := map[string]*SchemaObject{}
	var names []string
	for _, o := range objects {
		byKey[objectKey(o.Name)] = o
		names = append(names, objectKey(o.Name))
	}
	sort.Strings(names)
	state := map[string]int{}
	var result []*SchemaObject
	var visit func(string) error
	visit = func(name string) error {
		if state[name] == 2 {
			return nil
		}
		if state[name] == 1 {
			return fmt.Errorf("cyclic schema object dependency involving %s", name)
		}
		state[name] = 1
		o := byKey[name]
		var deps []string
		for dep := range objectReferences(o.definition) {
			if byKey[dep] != nil {
				deps = append(deps, dep)
			}
		}
		sort.Strings(deps)
		for _, dep := range deps {
			if err := visit(dep); err != nil {
				return err
			}
		}
		state[name] = 2
		result = append(result, o)
		return nil
	}
	for _, name := range names {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func objectsByKey(objects map[string]*SchemaObject) map[string]*SchemaObject {
	result := map[string]*SchemaObject{}
	for name, object := range objects {
		result[objectKey(name)] = object
	}
	return result
}
func tableByKey(schema *Schema, name string) *Table {
	for key, table := range schema.Tables {
		if objectKey(key) == objectKey(name) {
			return table
		}
	}
	return nil
}
func columnByKey(table *Table, name string) *Column {
	if table == nil {
		return nil
	}
	for key, column := range table.Columns {
		if objectKey(key) == objectKey(name) {
			return column
		}
	}
	return nil
}
