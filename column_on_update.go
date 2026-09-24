package spannerdef

import "fmt"

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
