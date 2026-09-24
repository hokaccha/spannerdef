package spannerdef

import (
	"context"
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

// Run preserves the legacy exit-on-error behavior.
// Deprecated: use RunContext to receive errors and allow deferred cleanup.
func Run(db Database, options *Options) {
	if err := RunContext(context.Background(), db, options); err != nil {
		log.Fatal(err)
	}
}

// RunContext executes the command without exiting the process. Context-aware
// databases receive ctx; legacy Database implementations remain supported but
// their in-flight calls cannot be interrupted by this wrapper.
func RunContext(ctx context.Context, db Database, options *Options) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var currentDDLs string
	var err error
	if d, ok := db.(interface {
		DumpDDLsContext(context.Context) (string, error)
	}); ok {
		currentDDLs, err = d.DumpDDLsContext(ctx)
	} else {
		currentDDLs, err = db.DumpDDLs()
	}
	if err != nil {
		return fmt.Errorf("dump DDLs: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if options.Export {
		if currentDDLs == "" {
			fmt.Println("-- No schema exists --")
		} else {
			fmt.Print(currentDDLs)
		}
		return nil
	}
	ddls, err := GenerateIdempotentDDLs(options.DesiredDDLs, currentDDLs, options.Config)
	if err != nil {
		return err
	}
	if len(ddls) == 0 {
		fmt.Println("-- Nothing is modified --")
		return nil
	}
	if options.DryRun {
		return showDDLs(ddls, options.EnableDrop)
	}
	return RunDDLsContext(ctx, db, ddls, options.EnableDrop, false)
}

// GenerateIdempotentDDLs generates DDLs to transform current schema to desired schema
func GenerateIdempotentDDLs(desiredDDLs, currentDDLs string, config GeneratorConfig) ([]string, error) {
	currentParsed, err := parseDDLs(currentDDLs, config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse current DDLs: %w", err)
	}

	desiredParsed, err := parseDDLs(desiredDDLs, config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse desired DDLs: %w", err)
	}

	if err := validateFilteredTableNameChanges(currentParsed, desiredParsed); err != nil {
		return nil, err
	}
	currentSchema, desiredSchema := currentParsed.Schema, desiredParsed.Schema

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
		Objects:      make(map[string]*SchemaObject),
		NamedSchemas: make(map[string]bool),
		Tables:       make(map[string]*Table),
		Indexes:      make(map[string]*Index),
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
	for i, filepath := range filepaths {
		f, err := ReadFile(filepath)
		if err != nil {
			return "", err
		}
		// SQL line comments must not consume the next file. A single file is
		// returned verbatim, and no statement terminators are invented.
		if i > 0 {
			result.WriteByte('\n')
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
		stat, statErr := os.Stdin.Stat()
		if statErr != nil {
			return "", fmt.Errorf("failed to stat stdin: %w", statErr)
		}
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
