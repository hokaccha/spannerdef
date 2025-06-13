package spannerdef

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/ubie-sandbox/spannerdef/database"
	"github.com/ubie-sandbox/spannerdef/schema"
)

type Options struct {
	DesiredDDLs string
	DryRun      bool
	Export      bool
	EnableDrop  bool
	Config      database.GeneratorConfig
}

// Main function shared by spannerdef command
func Run(db database.Database, options *Options) {
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
		showDDLs(ddls, options.EnableDrop)
		return
	}

	err = database.RunDDLs(db, ddls, options.EnableDrop)
	if err != nil {
		log.Fatal(err)
	}
}

// GenerateIdempotentDDLs generates DDLs to transform current schema to desired schema
func GenerateIdempotentDDLs(desiredDDLs, currentDDLs string, config database.GeneratorConfig) ([]string, error) {
	currentSchema, err := schema.ParseDDLs(currentDDLs)
	if err != nil {
		return nil, fmt.Errorf("failed to parse current DDLs: %v", err)
	}

	desiredSchema, err := schema.ParseDDLs(desiredDDLs)
	if err != nil {
		return nil, fmt.Errorf("failed to parse desired DDLs: %v", err)
	}

	// Apply filters based on config
	currentSchema = filterSchema(currentSchema, config)
	desiredSchema = filterSchema(desiredSchema, config)

	ddls := schema.GenerateDDLs(currentSchema, desiredSchema)
	return ddls, nil
}

// filterSchema applies target/skip table filters
func filterSchema(s *schema.Schema, config database.GeneratorConfig) *schema.Schema {
	filtered := &schema.Schema{
		Tables:  make(map[string]*schema.Table),
		Indexes: make(map[string]*schema.Index),
	}

	// Filter tables
	for name, table := range s.Tables {
		if shouldIncludeTable(name, config) {
			filtered.Tables[name] = table
		}
	}

	// Filter indexes (only include if their table is included)
	for name, index := range s.Indexes {
		if shouldIncludeTable(index.TableName, config) {
			filtered.Indexes[name] = index
		}
	}

	return filtered
}

// shouldIncludeTable checks if a table should be included based on config
func shouldIncludeTable(tableName string, config database.GeneratorConfig) bool {
	// Check skip tables
	for _, skip := range config.SkipTables {
		if tableName == skip {
			return false
		}
	}

	// Check target tables (if specified, only include those)
	if len(config.TargetTables) > 0 {
		for _, target := range config.TargetTables {
			if tableName == target {
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

func showDDLs(ddls []string, enableDropTable bool) {
	fmt.Println("-- dry run --")
	for _, ddl := range ddls {
		if !enableDropTable && strings.Contains(ddl, "DROP TABLE") {
			fmt.Printf("-- Skipped: %s\n", ddl)
			continue
		}
		fmt.Printf("%s\n", ddl)
	}
}
