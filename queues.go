package spannerdef

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/cloudspannerecosystem/memefish/ast"
)

func normalizeQueue(q *ast.CreateQueue) {
	// Column order is not significant; key order and expression text are.
	sort.Slice(q.Columns, func(i, j int) bool { return q.Columns[i].Name.SQL() < q.Columns[j].Name.SQL() })
	for _, key := range q.PrimaryKeys {
		if key.Dir == ast.DirectionAsc {
			key.Dir = ""
		}
	}
	if q.Cluster != nil && q.Cluster.Enforced && q.Cluster.OnDelete == "" {
		q.Cluster.OnDelete = ast.OnDeleteNoAction
	}
	// Explicit defaults in exports must not produce perpetual alterations.
	values := optionValues(q.Options)
	for name, value := range values {
		remove := false
		switch name {
		case "receive_mode":
			v, ok := value.(*ast.StringLiteral)
			remove = ok && v.Value == "PULL"
		case "disable_send", "disable_delivery":
			v, ok := value.(*ast.BoolLiteral)
			_, null := value.(*ast.NullLiteral)
			remove = null || (ok && !v.Value)
		case "locality_group":
			_, remove = value.(*ast.NullLiteral)
		}
		if remove {
			delete(values, name)
		}
	}
	q.Options = makeOptions(values)
}

func alterQueue(old, next *ast.CreateQueue) (before, after []string, err error) {
	// Queue columns and keys cannot be altered. Never rebuild a queue to
	// accommodate a definition change: doing so would discard its messages.
	a, b := *old, *next
	a.Name, b.Name = old.Name, old.Name
	a.Options, b.Options = nil, nil
	a.Cluster, b.Cluster = nil, nil
	a.RowDeletionPolicy, b.RowDeletionPolicy = nil, nil
	if !equalGenerationAST(reflect.ValueOf(&a), reflect.ValueOf(&b)) {
		return nil, nil, fmt.Errorf("unsupported column or primary key change for queue %s; migrate it explicitly before updating the desired schema", old.Name.SQL())
	}
	prefix := "ALTER QUEUE " + old.Name.SQL() + " "
	x, y := old.Cluster, next.Cluster
	if (x == nil) != (y == nil) || (x != nil && x.TableName.SQL() != y.TableName.SQL()) {
		return nil, nil, fmt.Errorf("unsupported interleave parent change for queue %s", old.Name.SQL())
	}
	if x != nil {
		if x.Enforced != y.Enforced {
			change := &ast.SetInterleaveIn{TableName: x.TableName, Enforced: y.Enforced, OnDelete: y.OnDelete}
			// Spanner requires a separate validation step before enabling CASCADE.
			if y.Enforced && y.OnDelete == ast.OnDeleteCascade {
				change.OnDelete = ast.OnDeleteNoAction
			}
			before = append(before, prefix+change.SQL())
			if y.Enforced && y.OnDelete == ast.OnDeleteCascade {
				before = append(before, prefix+"SET ON DELETE CASCADE")
			}
		} else if x.OnDelete != y.OnDelete {
			sql := prefix + "SET " + string(y.OnDelete)
			if y.OnDelete == ast.OnDeleteCascade {
				before = append(before, sql)
			} else {
				after = append(after, sql)
			}
		}
	}
	if !equalGenerationAST(reflect.ValueOf(old.RowDeletionPolicy), reflect.ValueOf(next.RowDeletionPolicy)) {
		switch {
		case next.RowDeletionPolicy == nil:
			before = append(before, prefix+"DROP ROW DELETION POLICY")
		case old.RowDeletionPolicy == nil:
			after = append(after, prefix+"ADD "+next.RowDeletionPolicy.RowDeletionPolicy.SQL())
		default:
			after = append(after, prefix+"REPLACE "+next.RowDeletionPolicy.RowDeletionPolicy.SQL())
		}
	}
	if delta := changedOptions(old.Options, next.Options, false); delta != nil {
		after = append(after, prefix+"SET "+delta.SQL())
	}
	return before, after, nil
}

// Enabling enforcement passes through NO ACTION, which is incompatible with
// a parent's existing TTL. Require its removal in a separate migration.
func validateQueueEnforcement(old, next *ast.CreateQueue, current *Schema) error {
	if old.Cluster == nil || next.Cluster == nil || old.Cluster.Enforced || !next.Cluster.Enforced {
		return nil
	}
	parent := old.Cluster.TableName.SQL()
	objects := objectsByKey(current.Objects)
	seen := map[string]bool{}
	for parent != "" && !seen[objectKey(parent)] {
		seen[objectKey(parent)] = true
		hasTTL, ancestor := false, ""
		if table := tableByKey(current, parent); table != nil {
			hasTTL = table.RowDeletionPolicyColumn != ""
			if !table.InterleaveNotEnforced {
				ancestor = table.ParentTable
			}
		}
		if object := objects[objectKey(parent)]; object != nil {
			if queue, ok := object.definition.(*ast.CreateQueue); ok {
				hasTTL = queue.RowDeletionPolicy != nil
				if queue.Cluster != nil && queue.Cluster.Enforced {
					ancestor = queue.Cluster.TableName.SQL()
				}
			}
		}
		if hasTTL {
			return fmt.Errorf("enabling interleave enforcement for queue %s requires removing parent %s TTL in a separate migration", old.Name.SQL(), parent)
		}
		parent = ancestor
	}
	return nil
}
