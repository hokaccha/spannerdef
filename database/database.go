package database

import (
	"fmt"
	"log"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ProjectID  string
	InstanceID string
	DatabaseID string
	// Future: CredentialsFile string
}

type GeneratorConfig struct {
	TargetTables []string
	SkipTables   []string
}

// Database interface for Spanner
type Database interface {
	DumpDDLs() (string, error)
	ExecDDL(ddl string) error
	ExecDDLs(ddls []string) error
	Close() error
}

func RunDDLs(d Database, ddls []string, enableDrop bool) error {
	fmt.Println("-- Apply --")

	// Filter out destructive DDLs if enableDrop is false
	validDDLs := make([]string, 0, len(ddls))
	for _, ddl := range ddls {
		if !enableDrop && (strings.Contains(ddl, "DROP TABLE") ||
			strings.Contains(ddl, "DROP INDEX") ||
			strings.Contains(ddl, "DROP COLUMN")) {
			fmt.Printf("-- Skipped: %s\n", ddl)
			continue
		}
		fmt.Printf("%s\n", ddl)
		validDDLs = append(validDDLs, ddl)
	}

	if len(validDDLs) == 0 {
		return nil
	}

	// Execute all DDLs in batch
	return d.ExecDDLs(validDDLs)
}

func ParseGeneratorConfig(configFile string) GeneratorConfig {
	if configFile == "" {
		return GeneratorConfig{}
	}

	buf, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	var config struct {
		TargetTables string `yaml:"target_tables"`
		SkipTables   string `yaml:"skip_tables"`
	}

	err = yaml.Unmarshal(buf, &config)
	if err != nil {
		log.Fatal(err)
	}

	var targetTables []string
	if config.TargetTables != "" {
		targetTables = strings.Split(strings.Trim(config.TargetTables, "\n"), "\n")
	}

	var skipTables []string
	if config.SkipTables != "" {
		skipTables = strings.Split(strings.Trim(config.SkipTables, "\n"), "\n")
	}

	return GeneratorConfig{
		TargetTables: targetTables,
		SkipTables:   skipTables,
	}
}
