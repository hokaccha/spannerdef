package spannerdef

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

type Options struct {
	DesiredDDLs string
	DryRun      bool
	Export      bool
	EnableDrop  bool
	Config      GeneratorConfig
}

// Main function shared by spannerdef command
func Run(db Database, options *Options) {
	currentDDLs, err := db.DumpDDLs()
	if err != nil {
		log.Fatalf("Error on DumpDDLs: %s", err)
	}

	if options.Export {
		if currentDDLs == "" {
			fmt.Printf("-- No schema exists --\n")
		} else {
			fmt.Print(currentDDLs)
		}
		return
	}

	ddls, err := GenerateIdempotentDDLs(options.DesiredDDLs, currentDDLs, options.Config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if len(ddls) == 0 {
		fmt.Println("-- Nothing is modified --")
		return
	}

	if options.DryRun {
		if err := showDDLs(ddls, options.EnableDrop); err != nil {
			log.Fatal(err)
		}
		return
	}

	err = RunDDLs(db, ddls, options.EnableDrop, false)
	if err != nil {
		log.Fatal(err)
	}
}

// GenerateIdempotentDDLs generates DDLs to transform current schema to desired schema
func GenerateIdempotentDDLs(desiredDDLs, currentDDLs string, config GeneratorConfig) ([]string, error) {
	currentSchema, err := parseDDLs(currentDDLs, config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse current DDLs: %v", err)
	}

	desiredSchema, err := parseDDLs(desiredDDLs, config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse desired DDLs: %v", err)
	}

	// Dropping a namespace can affect objects this tool does not model.
	if len(config.TargetTables) == 0 && len(config.SkipTables) == 0 {
		for name := range currentSchema.NamedSchemas {
			if !desiredSchema.NamedSchemas[name] {
				return nil, fmt.Errorf("dropping named schema %s is not supported; retain its CREATE SCHEMA declaration", name)
			}
		}
	}

	// Apply filters based on config
	existingNamespaces := currentSchema.NamedSchemas
	currentSchema = filterSchema(currentSchema, config)
	// Keep namespace existence even when all of its current tables are filtered
	// out, so adding a selected table does not recreate an existing schema.
	currentSchema.NamedSchemas = existingNamespaces
	desiredSchema = filterSchema(desiredSchema, config)

	if err := validateCaseOnlyNameChanges(currentSchema, desiredSchema); err != nil {
		return nil, err
	}

	normalizeImplicitPrimaryKeys(currentSchema, desiredSchema)

	if err := validateGenerationChanges(currentSchema, desiredSchema); err != nil {
		return nil, err
	}
	if err := validateOnUpdateChanges(currentSchema, desiredSchema); err != nil {
		return nil, err
	}
	for name, desired := range desiredSchema.Tables {
		if current, ok := currentSchema.Tables[name]; ok &&
			(current.ParentTable != desired.ParentTable || current.InterleaveNotEnforced != desired.InterleaveNotEnforced) {
			return nil, fmt.Errorf("unsupported interleave change for table %s; apply it explicitly before updating the desired schema", name)
		}
	}

	if err := validateKeyChanges(currentSchema, desiredSchema); err != nil {
		return nil, err
	}
	return generateObjectDDLs(currentSchema, desiredSchema)
}

// filterSchema applies target/skip table filters
func filterSchema(s *Schema, config GeneratorConfig) *Schema {
	filtered := &Schema{
		excludedTableNames: s.excludedTableNames,
		Objects:            make(map[string]*SchemaObject),
		NamedSchemas:       make(map[string]bool),
		Tables:             make(map[string]*Table),
		Indexes:            make(map[string]*Index),
	}

	// Non-table objects are managed globally; table filters apply to indexes
	// attached to those tables, not to views, streams, graphs or sequences.
	for name, object := range s.Objects {
		if object.TableName == "" || shouldIncludeTable(object.TableName, config) {
			filtered.Objects[name] = object
			if s.NamedSchemas[object.SchemaName] {
				filtered.NamedSchemas[object.SchemaName] = true
			}
		}
	}
	// Filter tables
	for name, table := range s.Tables {
		if shouldIncludeTable(name, config) {
			filtered.Tables[name] = table
			if s.NamedSchemas[table.SchemaName] {
				filtered.NamedSchemas[table.SchemaName] = true
			}
		}
	}

	// Filter indexes (only include if their table is included)
	for name, index := range s.Indexes {
		if shouldIncludeTable(index.TableName, config) {
			filtered.Indexes[name] = index
			if s.NamedSchemas[index.SchemaName] {
				filtered.NamedSchemas[index.SchemaName] = true
			}
		}
	}

	if len(config.TargetTables) == 0 && len(config.SkipTables) == 0 {
		filtered.NamedSchemas = s.NamedSchemas
	}
	return filtered
}

// shouldIncludeTable checks if a table should be included based on config
func shouldIncludeTable(tableName string, config GeneratorConfig) bool {
	tableName = tableFilterName(tableName)

	// Check skip tables
	for _, skip := range config.SkipTables {
		if tableName == tableFilterName(skip) {
			return false
		}
	}

	// Check target tables (if specified, only include those)
	if len(config.TargetTables) > 0 {
		for _, target := range config.TargetTables {
			if tableName == tableFilterName(target) {
				return true
			}
		}
		return false
	}

	return true
}

func ParseFiles(files []string) []string {
	if len(files) == 0 {
		panic("ParseFiles got empty files")
	}

	result := make([]string, 0, len(files))
	for _, f := range files {
		result = append(result, strings.Split(f, ",")...)
	}
	for i, r := range result {
		result[i] = strings.TrimSpace(r)
	}
	return result
}

func ReadFiles(filepaths []string) (string, error) {
	var result strings.Builder
	for _, filepath := range filepaths {
		f, err := ReadFile(filepath)
		if err != nil {
			return "", err
		}
		_, err = result.WriteString(f)
		if err != nil {
			return "", err
		}
	}
	return result.String(), nil
}

func ReadFile(filepath string) (string, error) {
	var err error
	var buf []byte

	if filepath == "-" {
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			return "", fmt.Errorf("stdin is not piped")
		}

		buf, err = io.ReadAll(os.Stdin)
	} else {
		buf, err = os.ReadFile(filepath)
	}

	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func showDDLs(ddls []string, enableDrop bool) error {
	plan, err := planDDLs(ddls, enableDrop)
	if err != nil {
		return err
	}
	fmt.Println("-- dry run --")
	for _, step := range plan {
		if step.Skip {
			fmt.Printf("-- Skipped: %s;\n", step.SQL)
		} else {
			fmt.Printf("%s;\n", step.SQL)
		}
	}
	return nil
}
