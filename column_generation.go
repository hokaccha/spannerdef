package spannerdef

import "fmt"

// Creation and ADD COLUMN preserve generation semantics. Altering an existing
// generated/identity column needs a dedicated migration plan, not ALTER TYPE.
func validateGenerationChanges(current, desired *Schema) error {
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
			if old.Generation != col.Generation || old.Hidden != col.Hidden ||
				((old.Generation != "" || col.Generation != "") && (old.Type != col.Type || old.NotNull != col.NotNull || old.Default != col.Default)) {
				return fmt.Errorf("unsupported generation or visibility change for column %s.%s; apply it explicitly before updating the desired schema", name, colName)
			}
		}
	}
	return nil
}
