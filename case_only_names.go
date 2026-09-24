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
	managed := make(map[string]bool, len(current.Tables)+len(desired.Tables))
	for name := range current.Tables {
		managed[strings.ToLower(name)] = true
	}
	for name := range desired.Tables {
		managed[strings.ToLower(name)] = true
	}
	if err := checkCaseOnly("table", "", tableNamesForCaseCheck(current, managed), tableNamesForCaseCheck(desired, managed)); err != nil {
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

// Include an excluded spelling only when its case-insensitive identity is
// managed on at least one side. This preserves filtering of unrelated tables.
func tableNamesForCaseCheck(schema *Schema, managed map[string]bool) map[string]bool {
	names := make(map[string]bool, len(schema.Tables))
	for name := range schema.Tables {
		names[name] = true
	}
	for name := range schema.excludedTableNames {
		if managed[strings.ToLower(name)] {
			names[name] = true
		}
	}
	return names
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
