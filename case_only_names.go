package spannerdef

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Spanner rejects schema object names that differ only in case, yet resolves
// references case-sensitively. A desired name that matches an existing one
// only case-insensitively would otherwise be planned as a drop and a create,
// which loses data under --enable-drop and fails to apply without it.
func validateCaseOnlyNameChanges(current, desired *Schema) error {
	if err := checkCaseOnly("schema", "", current.NamedSchemas, desired.NamedSchemas); err != nil {
		return err
	}
	if err := checkCaseOnly("table", "", current.Tables, desired.Tables); err != nil {
		return err
	}
	if err := checkCaseOnly("index", "", current.Indexes, desired.Indexes); err != nil {
		return err
	}
	for _, name := range slices.Sorted(maps.Keys(desired.Tables)) {
		prev, ok := current.Tables[name]
		if !ok {
			continue
		}
		next := desired.Tables[name]
		if err := checkCaseOnly("column", name+".", prev.Columns, next.Columns); err != nil {
			return err
		}
		if err := checkCaseOnly("constraint", name+".", prev.Constraints, next.Constraints); err != nil {
			return err
		}
	}
	return nil
}

func checkCaseOnly[V any](kind, prefix string, current, desired map[string]V) error {
	existing := make(map[string]string, len(current))
	for name := range current {
		existing[strings.ToLower(name)] = name
	}
	for _, name := range slices.Sorted(maps.Keys(desired)) {
		if _, ok := current[name]; ok {
			continue
		}
		if prev, ok := existing[strings.ToLower(name)]; ok {
			return fmt.Errorf("%s %s%s differs only in case from existing %s%s; Spanner rejects names that differ only in case, so keep the existing name or rename it explicitly", kind, prefix, name, prefix, prev)
		}
	}
	return nil
}
