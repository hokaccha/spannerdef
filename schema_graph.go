package spannerdef

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"
)

// Expand the graph defaults using the declared base tables. GetDatabaseDdl
// returns these defaults explicitly, including the primary key and properties.
func normalizeGraph(object *SchemaObject, schema *Schema) error {
	graph, ok := object.definition.(*ast.CreatePropertyGraph)
	if !ok {
		return nil
	}
	nodes := map[string]*ast.PropertyGraphElement{}
	for _, element := range graph.Content.NodeTables.Tables.Elements {
		if err := normalizeGraphElement(element, schema, true); err != nil {
			return err
		}
		name := element.Name.SQL()
		if element.Alias != nil {
			name = element.Alias.SQL()
		}
		nodes[objectKey(name)] = element
	}
	if graph.Content.EdgeTables != nil {
		for _, element := range graph.Content.EdgeTables.Tables.Elements {
			if err := normalizeGraphElement(element, schema, false); err != nil {
				return err
			}
			keys, ok := element.Keys.(*ast.PropertyGraphEdgeElementKeys)
			if !ok {
				continue
			}
			if keys.Source.ReferenceColumns == nil {
				if node := nodes[objectKey(keys.Source.ElementReference.SQL())]; node != nil {
					if key, ok := node.Keys.(*ast.PropertyGraphNodeElementKey); ok {
						keys.Source.ReferenceColumns = key.Key.Keys
					}
				}
			}
			if keys.Destination.ReferenceColumns == nil {
				if node := nodes[objectKey(keys.Destination.ElementReference.SQL())]; node != nil {
					if key, ok := node.Keys.(*ast.PropertyGraphNodeElementKey); ok {
						keys.Destination.ReferenceColumns = key.Key.Keys
					}
				}
			}
		}
	}
	sort.Slice(graph.Content.NodeTables.Tables.Elements, func(i, j int) bool {
		return graph.Content.NodeTables.Tables.Elements[i].SQL() < graph.Content.NodeTables.Tables.Elements[j].SQL()
	})
	if graph.Content.EdgeTables != nil {
		sort.Slice(graph.Content.EdgeTables.Tables.Elements, func(i, j int) bool {
			return graph.Content.EdgeTables.Tables.Elements[i].SQL() < graph.Content.EdgeTables.Tables.Elements[j].SQL()
		})
	}
	return nil
}
func normalizeGraphElement(element *ast.PropertyGraphElement, schema *Schema, node bool) error {
	table := tableByKey(schema, element.Name.SQL())
	if table != nil {
		element.Name = &ast.Ident{Name: tableFilterName(table.Name)}
	}
	if element.Alias != nil && strings.EqualFold(element.Alias.Name, element.Name.Name) {
		element.Alias = nil
	}
	if table == nil {
		missingKey := node && element.Keys == nil
		if edge, ok := element.Keys.(*ast.PropertyGraphEdgeElementKeys); ok && edge.Element == nil {
			missingKey = true
		}
		if missingKey {
			return fmt.Errorf("graph element %s requires explicit KEY when source table keys are unavailable", element.Name.SQL())
		}
	}
	if table != nil && !table.PrimaryKeyExplicit && len(table.PrimaryKey) == 0 && columnByKey(table, "rowid") == nil {
		expanded := *table
		expanded.PrimaryKey = []string{"rowid"}
		expanded.Columns = make(map[string]*Column, len(table.Columns)+1)
		for name, column := range table.Columns {
			expanded.Columns[name] = column
		}
		expanded.Columns["rowid"] = &Column{Name: "rowid", Type: "INT64", Hidden: true, NotNull: true}
		table = &expanded
	}
	if table != nil {
		cols := &ast.PropertyGraphColumnNameList{}
		for _, key := range table.PrimaryKey {
			cols.ColumnNameList = append(cols.ColumnNameList, &ast.Ident{Name: strings.Trim(strings.TrimSuffix(strings.TrimSuffix(key, " DESC"), " ASC"), "`")})
		}
		if len(cols.ColumnNameList) > 0 {
			if node && element.Keys == nil {
				element.Keys = &ast.PropertyGraphNodeElementKey{Key: &ast.PropertyGraphElementKey{Keys: cols}}
			}
			if keys, ok := element.Keys.(*ast.PropertyGraphEdgeElementKeys); ok && keys.Element == nil {
				keys.Element = &ast.PropertyGraphElementKey{Keys: cols}
			}
		}
	}
	labelName := element.Name
	if element.Alias != nil {
		labelName = element.Alias
	}
	var labels *ast.PropertyGraphLabelAndPropertiesList
	switch p := element.Properties.(type) {
	case *ast.PropertyGraphLabelAndPropertiesList:
		labels = p
	case *ast.PropertyGraphSingleProperties:
		labels = &ast.PropertyGraphLabelAndPropertiesList{LabelAndProperties: []*ast.PropertyGraphLabelAndProperties{{Properties: p.Properties}}}
	case nil:
		labels = &ast.PropertyGraphLabelAndPropertiesList{LabelAndProperties: []*ast.PropertyGraphLabelAndProperties{{}}}
	}
	for _, label := range labels.LabelAndProperties {
		switch label.Label.(type) {
		case nil, *ast.PropertyGraphElementLabelDefaultLabel:
			label.Label = &ast.PropertyGraphElementLabelLabelName{Name: labelName}
		}
		if label.Properties == nil {
			label.Properties = &ast.PropertyGraphPropertiesAre{}
		}
		if all, ok := label.Properties.(*ast.PropertyGraphPropertiesAre); ok {
			if table == nil {
				return fmt.Errorf("graph element %s requires explicit PROPERTIES when its source table columns are unavailable", element.Name.SQL())
			}
			except := map[string]bool{}
			if all.ExceptColumns != nil {
				for _, col := range all.ExceptColumns.ColumnNameList {
					except[objectKey(col.SQL())] = true
				}
			}
			props := &ast.PropertyGraphDerivedPropertyList{}
			var names []string
			for name := range table.Columns {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				if !except[objectKey(name)] {
					props.DerivedProperties = append(props.DerivedProperties, &ast.PropertyGraphDerivedProperty{Expr: &ast.Ident{Name: strings.Trim(name, "`")}})
				}
			}
			if len(props.DerivedProperties) == 0 {
				label.Properties = &ast.PropertyGraphNoProperties{}
			} else {
				label.Properties = props
			}
		}
		if props, ok := label.Properties.(*ast.PropertyGraphDerivedPropertyList); ok {
			for _, prop := range props.DerivedProperties {
				if col, ok := prop.Expr.(*ast.Ident); ok && prop.Alias != nil && strings.EqualFold(col.Name, prop.Alias.Name) {
					prop.Alias = nil
				}
			}
			sort.Slice(props.DerivedProperties, func(i, j int) bool { return props.DerivedProperties[i].SQL() < props.DerivedProperties[j].SQL() })
		}
	}
	sort.Slice(labels.LabelAndProperties, func(i, j int) bool { return labels.LabelAndProperties[i].SQL() < labels.LabelAndProperties[j].SQL() })
	element.Properties = labels
	return nil
}
